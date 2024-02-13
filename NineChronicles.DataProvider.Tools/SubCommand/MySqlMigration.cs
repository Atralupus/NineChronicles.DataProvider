using Libplanet.Action.State;
using Nekoyume.Model.Stake;

namespace NineChronicles.DataProvider.Tools.SubCommand
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Bencodex.Types;
    using Cocona;
    using Lib9c.Model.Order;
    using Libplanet;
    using Libplanet.Action;
    using Libplanet.Blockchain;
    using Libplanet.Blockchain.Policies;
    using Libplanet.Crypto;
    using Libplanet.RocksDBStore;
    using Libplanet.Store;
    using Libplanet.Types.Assets;
    using Libplanet.Types.Blocks;
    using MySqlConnector;
    using Nekoyume;
    using Nekoyume.Action;
    using Nekoyume.Action.Loader;
    using Nekoyume.Battle;
    using Nekoyume.Blockchain.Policy;
    using Nekoyume.Extensions;
    using Nekoyume.Helper;
    using Nekoyume.Model.Arena;
    using Nekoyume.Model.Item;
    using Nekoyume.Model.State;
    using Nekoyume.TableData;
    using Serilog;
    using Serilog.Events;

    public class MySqlMigration
    {
        private const string AgentDbName = "Agents";
        private const string AvatarDbName = "Avatars";
        private string BARDbName = "BattleArenaRanking";
        private string fbBARDbName = "BattleArenaRanking";
        private string _connectionString;
        private IStore _baseStore;
        private BlockChain _baseChain;
        private StreamWriter _barBulkFile;
        private StreamWriter _fbBarBulkFile;
        private StreamWriter _urBulkFile;
        private StreamWriter _agentBulkFile;
        private StreamWriter _avatarBulkFile;
        private List<string> _agentList;
        private List<string> _avatarList;
        private List<string> _barFiles;
        private List<string> _fbBarFiles;
        private List<string> _agentFiles;
        private List<string> _avatarFiles;

        [Command(Description = "Migrate action data in rocksdb store to mysql db.")]
        public void Migration(
            [Option('o', Description = "Rocksdb path to migrate.")]
            string storePath,
            [Option(
                "rocksdb-storetype",
                Description = "Store type of RocksDb (new or mono).")]
            string rocksdbStoreType,
            [Option(
                "mysql-server",
                Description = "A hostname of MySQL server.")]
            string mysqlServer,
            [Option(
                "mysql-port",
                Description = "A port of MySQL server.")]
            uint mysqlPort,
            [Option(
                "mysql-username",
                Description = "The name of MySQL user.")]
            string mysqlUsername,
            [Option(
                "mysql-password",
                Description = "The password of MySQL user.")]
            string mysqlPassword,
            [Option(
                "mysql-database",
                Description = "The name of MySQL database to use.")]
            string mysqlDatabase,
            [Option(
                "offset",
                Description = "offset of block index (no entry will migrate from the genesis block).")]
            int? offset = null,
            [Option(
                "limit",
                Description = "limit of block count (no entry will migrate to the chain tip).")]
            int? limit = null
        )
        {
            DateTimeOffset start = DateTimeOffset.UtcNow;
            var builder = new MySqlConnectionStringBuilder
            {
                Database = mysqlDatabase,
                UserID = mysqlUsername,
                Password = mysqlPassword,
                Server = mysqlServer,
                Port = mysqlPort,
                AllowLoadLocalInfile = true,
            };

            _connectionString = builder.ConnectionString;

            Console.WriteLine("Setting up RocksDBStore...");
            if (rocksdbStoreType == "new")
            {
                _baseStore = new RocksDBStore(
                    storePath,
                    dbConnectionCacheSize: 10000);
            }
            else
            {
                throw new CommandExitedException("Invalid rocksdb-storetype. Please enter 'new' or 'mono'", -1);
            }

            long totalLength = _baseStore.CountBlocks();

            if (totalLength == 0)
            {
                throw new CommandExitedException("Invalid rocksdb-store. Please enter a valid store path", -1);
            }

            if (!(_baseStore.GetCanonicalChainId() is Guid chainId))
            {
                Console.Error.WriteLine("There is no canonical chain: {0}", storePath);
                Environment.Exit(1);
                return;
            }

            if (!(_baseStore.IndexBlockHash(chainId, 0) is { } gHash))
            {
                Console.Error.WriteLine("There is no genesis block: {0}", storePath);
                Environment.Exit(1);
                return;
            }

            // Setup base store
            RocksDBKeyValueStore baseStateKeyValueStore = new RocksDBKeyValueStore(Path.Combine(storePath, "states"));
            TrieStateStore baseStateStore =
                new TrieStateStore(baseStateKeyValueStore);

            // Setup block policy
            IStagePolicy stagePolicy = new VolatileStagePolicy();
            LogEventLevel logLevel = LogEventLevel.Debug;
            var blockPolicySource = new BlockPolicySource();
            IBlockPolicy blockPolicy = blockPolicySource.GetPolicy();

            // Setup base chain & new chain
            Block genesis = _baseStore.GetBlock(gHash);
            var blockChainStates = new BlockChainStates(_baseStore, baseStateStore);
            var actionEvaluator = new ActionEvaluator(
                _ => blockPolicy.BlockAction,
                baseStateStore,
                new NCActionLoader());
            _baseChain = new BlockChain(blockPolicy, stagePolicy, _baseStore, baseStateStore, genesis, blockChainStates, actionEvaluator);

            // Prepare block hashes to append to new chain
            long height = _baseChain.Tip.Index;
            if (offset + limit > (int)height)
            {
                Console.Error.WriteLine(
                    "The sum of the offset and limit is greater than the chain tip index: {0}",
                    height);
                Environment.Exit(1);
                return;
            }

            Console.WriteLine("Start migration.");

            _barFiles = new List<string>();
            _fbBarFiles = new List<string>();
            _agentFiles = new List<string>();
            _avatarFiles = new List<string>();

            // lists to keep track of inserted addresses to minimize duplicates
            _agentList = new List<string>();
            _avatarList = new List<string>();

            CreateBulkFiles();

            using MySqlConnection connection = new MySqlConnection(_connectionString);
            connection.Open();

            var stm = "SELECT `Address` from Avatars";
            var cmd = new MySqlCommand(stm, connection);

            var rdr = cmd.ExecuteReader();
            List<string> avatars = new List<string>();
            List<string> agents = new List<string>();

            while (rdr.Read())
            {
                Console.WriteLine("{0}", rdr.GetString(0));
                avatars.Add(rdr.GetString(0).Replace("0x", string.Empty));
            }

            connection.Close();
            int shopOrderCount = 0;
            bool finalizeBaranking = false;

            // try
            // {
            var tipHash = _baseStore.IndexBlockHash(_baseChain.Id, _baseChain.Tip.Index);
            var tip = _baseStore.GetBlock((BlockHash)tipHash);
            var exec = _baseChain.EvaluateBlock(tip);
            var ev = exec.Last();
            var inputState = new Account(blockChainStates.GetAccountState(ev.InputContext.PreviousState));
            var outputState = new Account(blockChainStates.GetAccountState(ev.OutputState));
            var avatarCount = 0;
            AvatarState avatarState;
            int interval = 10000000;
            int intervalCount = 0;
            var sheets = outputState.GetSheets(
                sheetTypes: new[]
                {
                    typeof(RuneSheet),
                });
            var arenaSheet = outputState.GetSheet<ArenaSheet>();
            var arenaData = arenaSheet.GetRoundByBlockIndex(tip.Index);

            Console.WriteLine("2");
            var prevArenaEndIndex = arenaData.StartBlockIndex - 1;
            var prevArenaData = arenaSheet.GetRoundByBlockIndex(prevArenaEndIndex);
            var finalizeBarankingTip = prevArenaEndIndex;
            fbBARDbName = $"{fbBARDbName}_{prevArenaData.ChampionshipId}_{prevArenaData.Round}";

            connection.Open();
            var preBarQuery = $"SELECT `BlockIndex` FROM data_provider.{fbBARDbName} limit 1";
            var preBarCmd = new MySqlCommand(preBarQuery, connection);

            var dataReader = preBarCmd.ExecuteReader();
            long prevBarDbTip = 0;
            Console.WriteLine("3");
            while (dataReader.Read())
            {
                Console.WriteLine("{0}", dataReader.GetInt64(0));
                prevBarDbTip = dataReader.GetInt64(0);
            }

            connection.Close();
            Console.WriteLine("4");
            if (prevBarDbTip != 0 && prevBarDbTip < finalizeBarankingTip)
            {
                finalizeBaranking = true;
            }

            if (finalizeBaranking)
            {
                try
                {
                    Console.WriteLine($"Finalize {fbBARDbName} Table!");
                    var fbTipHash = _baseStore.IndexBlockHash(_baseChain.Id, finalizeBarankingTip);
                    var fbTip = _baseStore.GetBlock((BlockHash)fbTipHash!);
                    var fbExec = _baseChain.EvaluateBlock(fbTip);
                    var fbEv = fbExec.Last();
                    var fbOutputState = new Account(blockChainStates.GetAccountState(ev.OutputState));
                    var fbArenaSheet = fbOutputState.GetSheet<ArenaSheet>();
                    var fbArenaData = fbArenaSheet.GetRoundByBlockIndex(fbTip.Index);
                    List<string> fbAgents = new List<string>();
                    var fbavatarCount = 0;

                    Console.WriteLine("5");

                    foreach (var fbAvatar in avatars)
                    {
                        try
                        {
                            fbavatarCount++;
                            Console.WriteLine("Migrating {0}/{1}", fbavatarCount, avatars.Count);
                            AvatarState fbAvatarState;
                            var fbAvatarAddress = new Address(fbAvatar);
                            try
                            {
                                fbAvatarState = fbOutputState.GetAvatarStateV2(fbAvatarAddress);
                            }
                            catch (Exception ex)
                            {
                                fbAvatarState = fbOutputState.GetAvatarState(fbAvatarAddress);
                            }

                            var fbAvatarLevel = fbAvatarState.level;

                            var fbArenaScoreAdr =
                            ArenaScore.DeriveAddress(fbAvatarAddress, fbArenaData.ChampionshipId, fbArenaData.Round);
                            var fbArenaInformationAdr =
                                ArenaInformation.DeriveAddress(fbAvatarAddress, fbArenaData.ChampionshipId, fbArenaData.Round);
                            fbOutputState.TryGetArenaInformation(fbArenaInformationAdr, out var fbCurrentArenaInformation);
                            fbOutputState.TryGetArenaScore(fbArenaScoreAdr, out var fbOutputArenaScore);
                            if (fbCurrentArenaInformation != null && fbOutputArenaScore != null)
                            {
                                _fbBarBulkFile.WriteLine(
                                    $"{fbTip.Index};" +
                                    $"{fbAvatarState.agentAddress.ToString()};" +
                                    $"{fbAvatarAddress.ToString()};" +
                                    $"{fbAvatarLevel};" +
                                    $"{fbArenaData.ChampionshipId};" +
                                    $"{fbArenaData.Round};" +
                                    $"{fbArenaData.ArenaType.ToString()};" +
                                    $"{fbOutputArenaScore.Score};" +
                                    $"{fbCurrentArenaInformation.Win};" +
                                    $"{fbCurrentArenaInformation.Win};" +
                                    $"{fbCurrentArenaInformation.Lose};" +
                                    $"{fbCurrentArenaInformation.Ticket};" +
                                    $"{fbCurrentArenaInformation.PurchasedTicketCount};" +
                                    $"{fbCurrentArenaInformation.TicketResetCount};" +
                                    $"{fbArenaData.EntranceFee};" +
                                    $"{fbArenaData.TicketPrice};" +
                                    $"{fbArenaData.AdditionalTicketPrice};" +
                                    $"{fbArenaData.RequiredMedalCount};" +
                                    $"{fbArenaData.StartBlockIndex};" +
                                    $"{fbArenaData.EndBlockIndex};" +
                                    $"{0};" +
                                    $"{fbTip.Timestamp.UtcDateTime:yyyy-MM-dd}"
                                );
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                            Console.WriteLine(ex.StackTrace);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    Console.WriteLine(ex.StackTrace);
                }

                _fbBarBulkFile.Flush();
                _fbBarBulkFile.Close();

                Console.WriteLine("6");

                var fbstm23 = $"RENAME TABLE {fbBARDbName} TO {fbBARDbName}_Dump; CREATE TABLE {fbBARDbName} LIKE {fbBARDbName}_Dump;";
                var fbcmd23 = new MySqlCommand(fbstm23, connection);
                connection.Open();
                fbcmd23.CommandTimeout = 300;
                fbcmd23.ExecuteScalar();
                connection.Close();
                Console.WriteLine($"Move {fbBARDbName} Complete!");

                foreach (var path in _fbBarFiles)
                {
                    BulkInsert(fbBARDbName, path);
                }

                var fbstm34 = $"DROP TABLE {fbBARDbName}_Dump;";
                var fbcmd34 = new MySqlCommand(fbstm34, connection);
                connection.Open();
                fbcmd34.CommandTimeout = 300;
                fbcmd34.ExecuteScalar();
                connection.Close();
                Console.WriteLine($"Delete {fbBARDbName}_Dump Complete!");
                Console.WriteLine($"Finalize {fbBARDbName} Tables Complete!");
            }

            BARDbName = $"{BARDbName}_{arenaData.ChampionshipId}_{arenaData.Round}";
            Console.WriteLine("1");
            connection.Open();
            var stm33 =
                $@"CREATE TABLE IF NOT EXISTS `data_provider`.`{BARDbName}` (
                    `BlockIndex` bigint NOT NULL,
                    `AgentAddress` varchar(100) NOT NULL,
                    `AvatarAddress` varchar(100) NOT NULL,
                    `AvatarLevel` int NOT NULL,
                    `ChampionshipId` int NOT NULL,
                    `Round` int NOT NULL,
                    `ArenaType` varchar(100) NOT NULL,
                    `Score` int NOT NULL,
                    `WinCount` int NOT NULL,
                    `MedalCount` int NOT NULL,
                    `LossCount` int NOT NULL,
                    `Ticket` int NOT NULL,
                    `PurchasedTicketCount` int NOT NULL,
                    `TicketResetCount` int NOT NULL,
                    `EntranceFee` bigint NOT NULL,
                    `TicketPrice` bigint NOT NULL,
                    `AdditionalTicketPrice` bigint NOT NULL,
                    `RequiredMedalCount` int NOT NULL,
                    `StartBlockIndex` bigint NOT NULL,
                    `EndBlockIndex` bigint NOT NULL,
                    `Ranking` int NOT NULL,
                    `Timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    KEY `fk_BattleArenaRanking_Agent1_idx` (`AgentAddress`),
                    KEY `fk_BattleArenaRanking_AvatarAddress1_idx` (`AvatarAddress`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";

            var cmd33 = new MySqlCommand(stm33, connection);
            cmd33.CommandTimeout = 300;
            cmd33.ExecuteScalar();
            connection.Close();

            foreach (var avatar in avatars)
            {
                try
                {
                    intervalCount++;
                    avatarCount++;
                    Console.WriteLine("Interval Count {0}", intervalCount);
                    Console.WriteLine("Migrating {0}/{1}", avatarCount, avatars.Count);
                    var avatarAddress = new Address(avatar);
                    try
                    {
                        avatarState = outputState.GetAvatarStateV2(avatarAddress);
                    }
                    catch (Exception ex)
                    {
                        avatarState = outputState.GetAvatarState(avatarAddress);
                    }

                    var avatarLevel = avatarState.level;

                    var runeSheet = sheets.GetSheet<RuneSheet>();
                    foreach (var runeType in runeSheet.Values)
                    {
#pragma warning disable CS0618
                        var runeCurrency = Currency.Legacy(runeType.Ticker, 0, minters: null);
#pragma warning restore CS0618
                        var outputRuneBalance = outputState.GetBalance(
                            avatarAddress,
                            runeCurrency);
                        if (Convert.ToDecimal(outputRuneBalance.GetQuantityString()) > 0)
                        {
                            _urBulkFile.WriteLine(
                                $"{tip.Index};" +
                                $"{avatarState.agentAddress.ToString()};" +
                                $"{avatarAddress.ToString()};" +
                                $"{runeType.Ticker};" +
                                $"{Convert.ToDecimal(outputRuneBalance.GetQuantityString())};" +
                                $"{tip.Timestamp.UtcDateTime:yyyy-MM-dd}"
                            );
                        }
                    }

                    var arenaScoreAdr =
                        ArenaScore.DeriveAddress(avatarAddress, arenaData.ChampionshipId, arenaData.Round);
                    var arenaInformationAdr =
                        ArenaInformation.DeriveAddress(avatarAddress, arenaData.ChampionshipId, arenaData.Round);
                    outputState.TryGetArenaInformation(arenaInformationAdr, out var currentArenaInformation);
                    outputState.TryGetArenaScore(arenaScoreAdr, out var outputArenaScore);
                    if (currentArenaInformation != null && outputArenaScore != null)
                    {
                        _barBulkFile.WriteLine(
                            $"{tip.Index};" +
                            $"{avatarState.agentAddress.ToString()};" +
                            $"{avatarAddress.ToString()};" +
                            $"{avatarLevel};" +
                            $"{arenaData.ChampionshipId};" +
                            $"{arenaData.Round};" +
                            $"{arenaData.ArenaType.ToString()};" +
                            $"{outputArenaScore.Score};" +
                            $"{currentArenaInformation.Win};" +
                            $"{currentArenaInformation.Win};" +
                            $"{currentArenaInformation.Lose};" +
                            $"{currentArenaInformation.Ticket};" +
                            $"{currentArenaInformation.PurchasedTicketCount};" +
                            $"{currentArenaInformation.TicketResetCount};" +
                            $"{arenaData.EntranceFee};" +
                            $"{arenaData.TicketPrice};" +
                            $"{arenaData.AdditionalTicketPrice};" +
                            $"{arenaData.RequiredMedalCount};" +
                            $"{arenaData.StartBlockIndex};" +
                            $"{arenaData.EndBlockIndex};" +
                            $"{0};" +
                            $"{tip.Timestamp.UtcDateTime:yyyy-MM-dd}"
                        );
                    }

                    Address orderReceiptAddress = OrderDigestListState.DeriveAddress(avatarAddress);
                    var orderReceiptList = outputState.TryGetState(orderReceiptAddress, out Dictionary receiptDict)
                        ? new OrderDigestListState(receiptDict)
                        : new OrderDigestListState(orderReceiptAddress);
                    var userEquipments = avatarState.inventory.Equipments;
                    var userCostumes = avatarState.inventory.Costumes;
                    var userMaterials = avatarState.inventory.Materials;
                    var materialItemSheet = outputState.GetSheet<MaterialItemSheet>();
                    var hourglassRow = materialItemSheet
                        .First(pair => pair.Value.ItemSubType == ItemSubType.Hourglass)
                        .Value;
                    var apStoneRow = materialItemSheet
                        .First(pair => pair.Value.ItemSubType == ItemSubType.ApStone)
                        .Value;
                    var userConsumables = avatarState.inventory.Consumables;

                    Console.WriteLine("Migrating Complete {0}/{1}", avatarCount, avatars.Count);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    Console.WriteLine(ex.StackTrace);
                }

                if (intervalCount == interval)
                {
                    FlushBulkFiles();
                    foreach (var path in _agentFiles)
                    {
                        BulkInsert(AgentDbName, path);
                    }

                    foreach (var path in _avatarFiles)
                    {
                        BulkInsert(AvatarDbName, path);
                    }

                    foreach (var path in _barFiles)
                    {
                        BulkInsert(BARDbName, path);
                    }

                    _agentFiles.RemoveAt(0);
                    _avatarFiles.RemoveAt(0);
                    CreateBulkFiles();
                    intervalCount = 0;
                }
            }

            FlushBulkFiles();
            DateTimeOffset postDataPrep = DateTimeOffset.Now;
            Console.WriteLine("Data Preparation Complete! Time Elapsed: {0}", postDataPrep - start);

            var stm23 = $"RENAME TABLE {BARDbName} TO {BARDbName}_Dump; CREATE TABLE {BARDbName} LIKE {BARDbName}_Dump;";
            var cmd23 = new MySqlCommand(stm23, connection);
            foreach (var path in _agentFiles)
            {
                BulkInsert(AgentDbName, path);
            }

            foreach (var path in _avatarFiles)
            {
                BulkInsert(AvatarDbName, path);
            }

            var startMove = DateTimeOffset.Now;
            var endMove = DateTimeOffset.Now;

            startMove = DateTimeOffset.Now;
            connection.Open();
            cmd23.CommandTimeout = 300;
            cmd23.ExecuteScalar();
            connection.Close();
            endMove = DateTimeOffset.Now;
            Console.WriteLine("Move BattleArenaRanking Complete! Time Elapsed: {0}", endMove - startMove);
            foreach (var path in _barFiles)
            {
                BulkInsert(BARDbName, path);
            }

            var stm34 = $"DROP TABLE {BARDbName}_Dump;";
            var cmd34 = new MySqlCommand(stm34, connection);
            var startDelete = DateTimeOffset.Now;
            var endDelete = DateTimeOffset.Now;
            startDelete = DateTimeOffset.Now;
            endDelete = DateTimeOffset.Now;
            Console.WriteLine("Delete Equipments_Dump Complete! Time Elapsed: {0}", endDelete - startDelete);
            startDelete = DateTimeOffset.Now;
            connection.Open();
            cmd34.CommandTimeout = 300;
            cmd34.ExecuteScalar();
            connection.Close();
            endDelete = DateTimeOffset.Now;
            Console.WriteLine("Delete BattleArenaRanking_Dump Complete! Time Elapsed: {0}", endDelete - startDelete);

            DateTimeOffset end = DateTimeOffset.UtcNow;
            Console.WriteLine("Migration Complete! Time Elapsed: {0}", end - start);
            Console.WriteLine("Shop Count for {0} avatars: {1}", avatars.Count, shopOrderCount);
        }

        private void FlushBulkFiles()
        {
            _agentBulkFile.Flush();
            _agentBulkFile.Close();

            _avatarBulkFile.Flush();
            _avatarBulkFile.Close();

            _barBulkFile.Flush();
            _barBulkFile.Close();

            _urBulkFile.Flush();
            _urBulkFile.Close();
        }

        private void CreateBulkFiles()
        {
            string agentFilePath = Path.GetTempFileName();
            _agentBulkFile = new StreamWriter(agentFilePath);

            string avatarFilePath = Path.GetTempFileName();
            _avatarBulkFile = new StreamWriter(avatarFilePath);

            string barFilePath = Path.GetTempFileName();
            _barBulkFile = new StreamWriter(barFilePath);

            string urFilePath = Path.GetTempFileName();
            _urBulkFile = new StreamWriter(urFilePath);

            string fbBarFilePath = Path.GetTempFileName();
            _fbBarBulkFile = new StreamWriter(fbBarFilePath);

            _agentFiles.Add(agentFilePath);
            _avatarFiles.Add(avatarFilePath);
            _barFiles.Add(barFilePath);
            _fbBarFiles.Add(fbBarFilePath);
        }

        private void BulkInsert(
            string tableName,
            string filePath)
        {
            using MySqlConnection connection = new MySqlConnection(_connectionString);
            try
            {
                DateTimeOffset start = DateTimeOffset.Now;
                Console.WriteLine($"Start bulk insert to {tableName}.");
                MySqlBulkLoader loader = new MySqlBulkLoader(connection)
                {
                    TableName = tableName,
                    FileName = filePath,
                    Timeout = 0,
                    LineTerminator = "\n",
                    FieldTerminator = ";",
                    Local = true,
                    ConflictOption = MySqlBulkLoaderConflictOption.Ignore,
                };

                loader.Load();
                Console.WriteLine($"Bulk load to {tableName} complete.");
                DateTimeOffset end = DateTimeOffset.Now;
                Console.WriteLine("Time elapsed: {0}", end - start);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine($"Bulk load to {tableName} failed. Retry bulk insert");
                DateTimeOffset start = DateTimeOffset.Now;
                Console.WriteLine($"Start bulk insert to {tableName}.");
                MySqlBulkLoader loader = new MySqlBulkLoader(connection)
                {
                    TableName = tableName,
                    FileName = filePath,
                    Timeout = 0,
                    LineTerminator = "\n",
                    FieldTerminator = ";",
                    Local = true,
                    ConflictOption = MySqlBulkLoaderConflictOption.Ignore,
                };

                loader.Load();
                Console.WriteLine($"Bulk load to {tableName} complete.");
                DateTimeOffset end = DateTimeOffset.Now;
                Console.WriteLine("Time elapsed: {0}", end - start);
            }
        }
    }
}
