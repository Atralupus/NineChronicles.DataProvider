namespace NineChronicles.DataProvider
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Bencodex.Types;
    using Lib9c.Model.Order;
    using Lib9c.Renderer;
    using Libplanet;
    using Libplanet.Assets;
    using Microsoft.Extensions.Hosting;
    using Nekoyume;
    using Nekoyume.Action;
    using Nekoyume.Arena;
    using Nekoyume.Battle;
    using Nekoyume.Extensions;
    using Nekoyume.Helper;
    using Nekoyume.Model.Arena;
    using Nekoyume.Model.EnumType;
    using Nekoyume.Model.Item;
    using Nekoyume.Model.State;
    using Nekoyume.TableData;
    using Nekoyume.TableData.Crystal;
    using NineChronicles.DataProvider.Store;
    using NineChronicles.DataProvider.Store.Models;
    using NineChronicles.Headless;
    using Serilog;
    using static Lib9c.SerializeKeys;

    public class RenderSubscriber : BackgroundService
    {
        private const int DefaultInsertInterval = 30;
        private readonly int _blockInsertInterval;
        private readonly string _blockIndexFilePath;
        private readonly BlockRenderer _blockRenderer;
        private readonly ActionRenderer _actionRenderer;
        private readonly ExceptionRenderer _exceptionRenderer;
        private readonly NodeStatusRenderer _nodeStatusRenderer;
        private readonly List<AgentModel> _agentList = new List<AgentModel>();
        private readonly List<AvatarModel> _avatarList = new List<AvatarModel>();
        private readonly List<HackAndSlashModel> _hasList = new List<HackAndSlashModel>();
        private readonly List<CombinationConsumableModel> _ccList = new List<CombinationConsumableModel>();
        private readonly List<CombinationEquipmentModel> _ceList = new List<CombinationEquipmentModel>();
        private readonly List<EquipmentModel> _eqList = new List<EquipmentModel>();
        private readonly List<ItemEnhancementModel> _ieList = new List<ItemEnhancementModel>();
        private readonly List<ShopHistoryEquipmentModel> _buyShopEquipmentsList = new List<ShopHistoryEquipmentModel>();
        private readonly List<ShopHistoryCostumeModel> _buyShopCostumesList = new List<ShopHistoryCostumeModel>();
        private readonly List<ShopHistoryMaterialModel> _buyShopMaterialsList = new List<ShopHistoryMaterialModel>();
        private readonly List<ShopHistoryConsumableModel> _buyShopConsumablesList = new List<ShopHistoryConsumableModel>();
        private readonly List<StakeModel> _stakeList = new List<StakeModel>();
        private readonly List<ClaimStakeRewardModel> _claimStakeList = new List<ClaimStakeRewardModel>();
        private readonly List<MigrateMonsterCollectionModel> _mmcList = new List<MigrateMonsterCollectionModel>();
        private readonly List<GrindingModel> _grindList = new List<GrindingModel>();
        private readonly List<ItemEnhancementFailModel> _itemEnhancementFailList = new List<ItemEnhancementFailModel>();
        private readonly List<UnlockEquipmentRecipeModel> _unlockEquipmentRecipeList = new List<UnlockEquipmentRecipeModel>();
        private readonly List<UnlockWorldModel> _unlockWorldList = new List<UnlockWorldModel>();
        private readonly List<ReplaceCombinationEquipmentMaterialModel> _replaceCombinationEquipmentMaterialList = new List<ReplaceCombinationEquipmentMaterialModel>();
        private readonly List<HasRandomBuffModel> _hasRandomBuffList = new List<HasRandomBuffModel>();
        private readonly List<HasWithRandomBuffModel> _hasWithRandomBuffList = new List<HasWithRandomBuffModel>();
        private readonly List<JoinArenaModel> _joinArenaList = new List<JoinArenaModel>();
        private readonly List<BattleArenaModel> _battleArenaList = new List<BattleArenaModel>();
        private readonly List<BlockModel> _blockList = new List<BlockModel>();
        private readonly List<TransactionModel> _transactionList = new List<TransactionModel>();
        private readonly List<HackAndSlashSweepModel> _hasSweepList = new List<HackAndSlashSweepModel>();
        private readonly List<string> _agents;
        private int _renderedBlockCount;
        private DateTimeOffset _blockTimeOffset;
        private Address _miner;

        public RenderSubscriber(
            NineChroniclesNodeService nodeService,
            MySqlStore mySqlStore
        )
        {
            _blockRenderer = nodeService.BlockRenderer;
            _actionRenderer = nodeService.ActionRenderer;
            _exceptionRenderer = nodeService.ExceptionRenderer;
            _nodeStatusRenderer = nodeService.NodeStatusRenderer;
            MySqlStore = mySqlStore;
            _renderedBlockCount = 0;
            _agents = new List<string>();
            string dataPath = Environment.GetEnvironmentVariable("NC_BlockIndexFilePath")
                              ?? Path.GetTempPath();
            if (!Directory.Exists(dataPath))
            {
                dataPath = Path.GetTempPath();
            }

            _blockIndexFilePath = Path.Combine(dataPath, "blockIndex.txt");

            try
            {
                _blockInsertInterval = Convert.ToInt32(Environment.GetEnvironmentVariable("NC_BlockInsertInterval"));
                if (_blockInsertInterval < 1)
                {
                    _blockInsertInterval = DefaultInsertInterval;
                }
            }
            catch (Exception)
            {
                _blockInsertInterval = DefaultInsertInterval;
            }
        }

        internal MySqlStore MySqlStore { get; }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _blockRenderer.BlockSubject.Subscribe(b =>
            {
                var block = b.NewTip;
                _blockTimeOffset = block.Timestamp.UtcDateTime;
                _miner = block.Miner;
                _blockList.Add(new BlockModel()
                {
                    Index = block.Index,
                    Hash = block.Hash.ToString(),
                    Miner = block.Miner.ToString(),
                    Difficulty = block.Difficulty,
                    Nonce = block.Nonce.ToString(),
                    PreviousHash = block.PreviousHash.ToString(),
                    ProtocolVersion = block.ProtocolVersion,
                    PublicKey = block.PublicKey!.ToString(),
                    StateRootHash = block.StateRootHash.ToString(),
                    TotalDifficulty = (long)block.TotalDifficulty,
                    TxCount = block.Transactions.Count(),
                    TxHash = block.TxHash.ToString(),
                    TimeStamp = block.Timestamp.UtcDateTime,
                });

                foreach (var transaction in block.Transactions)
                {
                    var actionType = transaction.Actions.Select(action => action.ToString()!.Split('.')
                        .LastOrDefault()?.Replace(">", string.Empty));
                    _transactionList.Add(new TransactionModel()
                    {
                        BlockIndex = block.Index,
                        BlockHash = block.Hash.ToString(),
                        TxId = transaction.Id.ToString(),
                        Signer = transaction.Signer.ToString(),
                        ActionType = actionType.FirstOrDefault(),
                        Nonce = transaction.Nonce,
                        PublicKey = transaction.PublicKey.ToString(),
                        UpdatedAddressesCount = transaction.UpdatedAddresses.Count(),
                        Date = transaction.Timestamp.UtcDateTime,
                        TimeStamp = transaction.Timestamp.UtcDateTime,
                    });
                }

                _renderedBlockCount++;
                Log.Debug($"Rendered Block Count: #{_renderedBlockCount} at Block #{block.Index}");

                if (_renderedBlockCount == _blockInsertInterval)
                {
                    var start = DateTimeOffset.Now;
                    Log.Debug("Storing Data");
                    MySqlStore.StoreAgentList(_agentList.GroupBy(i => i.Address).Select(i => i.FirstOrDefault()).ToList());
                    MySqlStore.StoreAvatarList(_avatarList.GroupBy(i => i.Address).Select(i => i.FirstOrDefault()).ToList());
                    MySqlStore.StoreHackAndSlashList(_hasList.GroupBy(i => i.Id).Select(i => i.FirstOrDefault()).ToList());
                    MySqlStore.StoreCombinationConsumableList(_ccList.GroupBy(i => i.Id).Select(i => i.FirstOrDefault()).ToList());
                    MySqlStore.StoreCombinationEquipmentList(_ceList.GroupBy(i => i.Id).Select(i => i.FirstOrDefault()).ToList());
                    MySqlStore.StoreItemEnhancementList(_ieList.GroupBy(i => i.Id).Select(i => i.FirstOrDefault()).ToList());
                    MySqlStore.StoreShopHistoryEquipmentList(_buyShopEquipmentsList.GroupBy(i => i.OrderId).Select(i => i.FirstOrDefault()).ToList());
                    MySqlStore.StoreShopHistoryCostumeList(_buyShopCostumesList.GroupBy(i => i.OrderId).Select(i => i.FirstOrDefault()).ToList());
                    MySqlStore.StoreShopHistoryMaterialList(_buyShopMaterialsList.GroupBy(i => i.OrderId).Select(i => i.FirstOrDefault()).ToList());
                    MySqlStore.StoreShopHistoryConsumableList(_buyShopConsumablesList.GroupBy(i => i.OrderId).Select(i => i.FirstOrDefault()).ToList());
                    MySqlStore.ProcessEquipmentList(_eqList.GroupBy(i => i.ItemId).Select(i => i.FirstOrDefault()).ToList());
                    MySqlStore.StoreStakingList(_stakeList);
                    MySqlStore.StoreClaimStakeRewardList(_claimStakeList);
                    MySqlStore.StoreMigrateMonsterCollectionList(_mmcList);
                    MySqlStore.StoreGrindList(_grindList);
                    MySqlStore.StoreItemEnhancementFailList(_itemEnhancementFailList);
                    MySqlStore.StoreUnlockEquipmentRecipeList(_unlockEquipmentRecipeList);
                    MySqlStore.StoreUnlockWorldList(_unlockWorldList);
                    MySqlStore.StoreReplaceCombinationEquipmentMaterialList(_replaceCombinationEquipmentMaterialList);
                    MySqlStore.StoreHasRandomBuffList(_hasRandomBuffList);
                    MySqlStore.StoreHasWithRandomBuffList(_hasWithRandomBuffList);
                    MySqlStore.StoreJoinArenaList(_joinArenaList);
                    MySqlStore.StoreBattleArenaList(_battleArenaList);
                    MySqlStore.StoreBlockList(_blockList);
                    MySqlStore.StoreTransactionList(_transactionList);
                    MySqlStore.StoreHackAndSlashSweepList(_hasSweepList);
                    _renderedBlockCount = 0;
                    _agents.Clear();
                    _agentList.Clear();
                    _avatarList.Clear();
                    _hasList.Clear();
                    _ccList.Clear();
                    _ceList.Clear();
                    _ieList.Clear();
                    _buyShopEquipmentsList.Clear();
                    _buyShopCostumesList.Clear();
                    _buyShopMaterialsList.Clear();
                    _buyShopConsumablesList.Clear();
                    _eqList.Clear();
                    _stakeList.Clear();
                    _claimStakeList.Clear();
                    _mmcList.Clear();
                    _grindList.Clear();
                    _itemEnhancementFailList.Clear();
                    _unlockEquipmentRecipeList.Clear();
                    _unlockWorldList.Clear();
                    _replaceCombinationEquipmentMaterialList.Clear();
                    _hasRandomBuffList.Clear();
                    _hasWithRandomBuffList.Clear();
                    _joinArenaList.Clear();
                    _battleArenaList.Clear();
                    _blockList.Clear();
                    _transactionList.Clear();
                    _hasSweepList.Clear();
                    var end = DateTimeOffset.Now;
                    long blockIndex = b.OldTip.Index;
                    StreamWriter blockIndexFile = new StreamWriter(_blockIndexFilePath);
                    blockIndexFile.Write(blockIndex);
                    blockIndexFile.Flush();
                    blockIndexFile.Close();
                    Log.Debug($"Storing Data Complete. Time Taken: {(end - start).Milliseconds} ms.");
                }
            });

            _actionRenderer.EveryRender<ActionBase>()
                .Subscribe(
                    ev =>
                    {
                        try
                        {
                            if (ev.Exception != null)
                            {
                                return;
                            }

                            ProcessAgentAvatarData(ev);

                            if (ev.Action is HackAndSlash has)
                            {
                                var start = DateTimeOffset.UtcNow;
                                AvatarState avatarState = ev.OutputStates.GetAvatarStateV2(has.avatarAddress);
                                bool isClear = avatarState.stageMap.ContainsKey(has.stageId);
                                _hasList.Add(new HackAndSlashModel()
                                {
                                    Id = has.Id.ToString(),
                                    AgentAddress = ev.Signer.ToString(),
                                    AvatarAddress = has.avatarAddress.ToString(),
                                    StageId = has.stageId,
                                    Cleared = isClear,
                                    Mimisbrunnr = has.stageId > 10000000,
                                    BlockIndex = ev.BlockIndex,
                                });
                                if (has.stageBuffId.HasValue)
                                {
                                    _hasWithRandomBuffList.Add(new HasWithRandomBuffModel()
                                    {
                                        Id = has.Id.ToString(),
                                        BlockIndex = ev.BlockIndex,
                                        AgentAddress = ev.Signer.ToString(),
                                        AvatarAddress = has.avatarAddress.ToString(),
                                        StageId = has.stageId,
                                        BuffId = (int)has.stageBuffId,
                                        Cleared = isClear,
                                        TimeStamp = _blockTimeOffset,
                                    });
                                }

                                var end = DateTimeOffset.UtcNow;
                                Log.Debug("Stored HackAndSlash action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                            }

                            if (ev.Action is HackAndSlashSweep hasSweep)
                            {
                                var start = DateTimeOffset.UtcNow;
                                AvatarState avatarState = ev.OutputStates.GetAvatarStateV2(hasSweep.avatarAddress);
                                bool isClear = avatarState.stageMap.ContainsKey(hasSweep.stageId);
                                _hasSweepList.Add(new HackAndSlashSweepModel()
                                {
                                    Id = hasSweep.Id.ToString(),
                                    AgentAddress = ev.Signer.ToString(),
                                    AvatarAddress = hasSweep.avatarAddress.ToString(),
                                    WorldId = hasSweep.worldId,
                                    StageId = hasSweep.stageId,
                                    ApStoneCount = hasSweep.actionPoint,
                                    ActionPoint = hasSweep.actionPoint,
                                    CostumesCount = hasSweep.costumes.Count,
                                    EquipmentsCount = hasSweep.equipments.Count,
                                    Cleared = isClear,
                                    Mimisbrunnr = hasSweep.stageId > 10000000,
                                    BlockIndex = ev.BlockIndex,
                                    Timestamp = _blockTimeOffset,
                                });

                                var end = DateTimeOffset.UtcNow;
                                Log.Debug("Stored HackAndSlashSweep action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                            }

                            if (ev.Action is CombinationConsumable combinationConsumable)
                            {
                                var start = DateTimeOffset.UtcNow;
                                _ccList.Add(new CombinationConsumableModel()
                                {
                                    Id = combinationConsumable.Id.ToString(),
                                    AgentAddress = ev.Signer.ToString(),
                                    AvatarAddress = combinationConsumable.avatarAddress.ToString(),
                                    RecipeId = combinationConsumable.recipeId,
                                    SlotIndex = combinationConsumable.slotIndex,
                                    BlockIndex = ev.BlockIndex,
                                });

                                var end = DateTimeOffset.UtcNow;
                                Log.Debug("Stored CombinationConsumable action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                            }

                            if (ev.Action is CombinationEquipment combinationEquipment)
                            {
                                var start = DateTimeOffset.UtcNow;
                                var previousStates = ev.PreviousStates;
                                _ceList.Add(new CombinationEquipmentModel()
                                {
                                    Id = combinationEquipment.Id.ToString(),
                                    AgentAddress = ev.Signer.ToString(),
                                    AvatarAddress = combinationEquipment.avatarAddress.ToString(),
                                    RecipeId = combinationEquipment.recipeId,
                                    SlotIndex = combinationEquipment.slotIndex,
                                    SubRecipeId = combinationEquipment.subRecipeId ?? 0,
                                    BlockIndex = ev.BlockIndex,
                                });
                                if (combinationEquipment.payByCrystal)
                                {
                                    Currency crystalCurrency = CrystalCalculator.CRYSTAL;
                                    var prevCrystalBalance = previousStates.GetBalance(
                                        ev.Signer,
                                        crystalCurrency);
                                    var outputCrystalBalance = ev.OutputStates.GetBalance(
                                        ev.Signer,
                                        crystalCurrency);
                                    var burntCrystal = prevCrystalBalance - outputCrystalBalance;
                                    var requiredFungibleItems = new Dictionary<int, int>();
                                    Dictionary<Type, (Address, ISheet)> sheets = previousStates.GetSheets(
                                        sheetTypes: new[]
                                        {
                                            typeof(EquipmentItemRecipeSheet),
                                            typeof(EquipmentItemSheet),
                                            typeof(MaterialItemSheet),
                                            typeof(EquipmentItemSubRecipeSheetV2),
                                            typeof(EquipmentItemOptionSheet),
                                            typeof(SkillSheet),
                                            typeof(CrystalMaterialCostSheet),
                                            typeof(CrystalFluctuationSheet),
                                        });
                                    var materialItemSheet = sheets.GetSheet<MaterialItemSheet>();
                                    var equipmentItemRecipeSheet = sheets.GetSheet<EquipmentItemRecipeSheet>();
                                    equipmentItemRecipeSheet.TryGetValue(
                                        combinationEquipment.recipeId,
                                        out var recipeRow);
                                    materialItemSheet.TryGetValue(recipeRow!.MaterialId, out var materialRow);
                                    if (requiredFungibleItems.ContainsKey(materialRow!.Id))
                                    {
                                        requiredFungibleItems[materialRow.Id] += recipeRow.MaterialCount;
                                    }
                                    else
                                    {
                                        requiredFungibleItems[materialRow.Id] = recipeRow.MaterialCount;
                                    }

                                    if (combinationEquipment.subRecipeId.HasValue)
                                    {
                                        var equipmentItemSubRecipeSheetV2 = sheets.GetSheet<EquipmentItemSubRecipeSheetV2>();
                                        equipmentItemSubRecipeSheetV2.TryGetValue(combinationEquipment.subRecipeId.Value, out var subRecipeRow);

                                        // Validate SubRecipe Material
                                        for (var i = subRecipeRow!.Materials.Count; i > 0; i--)
                                        {
                                            var materialInfo = subRecipeRow.Materials[i - 1];
                                            materialItemSheet.TryGetValue(materialInfo.Id, out materialRow);

                                            if (requiredFungibleItems.ContainsKey(materialRow!.Id))
                                            {
                                                requiredFungibleItems[materialRow.Id] += materialInfo.Count;
                                            }
                                            else
                                            {
                                                requiredFungibleItems[materialRow.Id] = materialInfo.Count;
                                            }
                                        }
                                    }

                                    var inventory = ev.PreviousStates
                                        .GetAvatarStateV2(combinationEquipment.avatarAddress).inventory;
                                    foreach (var pair in requiredFungibleItems.OrderBy(pair => pair.Key))
                                    {
                                        var itemId = pair.Key;
                                        var requiredCount = pair.Value;
                                        if (materialItemSheet.TryGetValue(itemId, out materialRow))
                                        {
                                            int itemCount = inventory.TryGetItem(itemId, out Inventory.Item item)
                                                ? item.count
                                                : 0;
                                            if (itemCount < requiredCount && combinationEquipment.payByCrystal)
                                            {
                                                _replaceCombinationEquipmentMaterialList.Add(
                                                    new ReplaceCombinationEquipmentMaterialModel()
                                                    {
                                                        Id = combinationEquipment.Id.ToString(),
                                                        BlockIndex = ev.BlockIndex,
                                                        AgentAddress = ev.Signer.ToString(),
                                                        AvatarAddress =
                                                            combinationEquipment.avatarAddress.ToString(),
                                                        ReplacedMaterialId = itemId,
                                                        ReplacedMaterialCount = requiredCount - itemCount,
                                                        BurntCrystal =
                                                            Convert.ToDecimal(burntCrystal.GetQuantityString()),
                                                        TimeStamp = _blockTimeOffset,
                                                    });
                                            }
                                        }
                                    }
                                }

                                var end = DateTimeOffset.UtcNow;
                                Log.Debug(
                                    "Stored CombinationEquipment action in block #{index}. Time Taken: {time} ms.",
                                    ev.BlockIndex,
                                    (end - start).Milliseconds);
                                start = DateTimeOffset.UtcNow;

                                var slotState = ev.OutputStates.GetCombinationSlotState(
                                    combinationEquipment.avatarAddress,
                                    combinationEquipment.slotIndex);

                                if (slotState?.Result.itemUsable.ItemType is ItemType.Equipment)
                                {
                                    ProcessEquipmentData(
                                        ev.Signer,
                                        combinationEquipment.avatarAddress,
                                        (Equipment)slotState.Result.itemUsable);
                                }

                                end = DateTimeOffset.UtcNow;
                                Log.Debug(
                                    "Stored avatar {address}'s equipment in block #{index}. Time Taken: {time} ms.",
                                    combinationEquipment.avatarAddress,
                                    ev.BlockIndex,
                                    (end - start).Milliseconds);
                            }

                            if (ev.Action is ItemEnhancement itemEnhancement)
                            {
                                var start = DateTimeOffset.UtcNow;
                                AvatarState avatarState = ev.OutputStates.GetAvatarStateV2(itemEnhancement.avatarAddress);
                                var previousStates = ev.PreviousStates;
                                AvatarState prevAvatarState = previousStates.GetAvatarStateV2(itemEnhancement.avatarAddress);

                                int prevEquipmentLevel = 0;
                                if (prevAvatarState.inventory.TryGetNonFungibleItem(itemEnhancement.itemId, out ItemUsable prevEnhancementItem)
                                    && prevEnhancementItem is Equipment prevEnhancementEquipment)
                                {
                                       prevEquipmentLevel = prevEnhancementEquipment.level;
                                }

                                int outputEquipmentLevel = 0;
                                if (avatarState.inventory.TryGetNonFungibleItem(itemEnhancement.itemId, out ItemUsable outputEnhancementItem)
                                    && outputEnhancementItem is Equipment outputEnhancementEquipment)
                                {
                                    outputEquipmentLevel = outputEnhancementEquipment.level;
                                }

                                if (prevEquipmentLevel == outputEquipmentLevel)
                                {
                                    Currency crystalCurrency = CrystalCalculator.CRYSTAL;
                                    Currency ncgCurrency = ev.OutputStates.GetGoldCurrency();
                                    var prevCrystalBalance = previousStates.GetBalance(
                                        ev.Signer,
                                        crystalCurrency);
                                    var outputCrystalBalance = ev.OutputStates.GetBalance(
                                        ev.Signer,
                                        crystalCurrency);
                                    var prevNCGBalance = previousStates.GetBalance(
                                        ev.Signer,
                                        ncgCurrency);
                                    var outputNCGBalance = ev.OutputStates.GetBalance(
                                        ev.Signer,
                                        ncgCurrency);
                                    var gainedCrystal = outputCrystalBalance - prevCrystalBalance;
                                    var burntNCG = prevNCGBalance - outputNCGBalance;
                                    _itemEnhancementFailList.Add(new ItemEnhancementFailModel()
                                    {
                                        Id = itemEnhancement.Id.ToString(),
                                        BlockIndex = ev.BlockIndex,
                                        AgentAddress = ev.Signer.ToString(),
                                        AvatarAddress = itemEnhancement.avatarAddress.ToString(),
                                        EquipmentItemId = itemEnhancement.itemId.ToString(),
                                        MaterialItemId = itemEnhancement.materialId.ToString(),
                                        EquipmentLevel = outputEquipmentLevel,
                                        GainedCrystal = Convert.ToDecimal(gainedCrystal.GetQuantityString()),
                                        BurntNCG = Convert.ToDecimal(burntNCG.GetQuantityString()),
                                        TimeStamp = _blockTimeOffset,
                                    });
                                }

                                _ieList.Add(new ItemEnhancementModel()
                                {
                                    Id = itemEnhancement.Id.ToString(),
                                    AgentAddress = ev.Signer.ToString(),
                                    AvatarAddress = itemEnhancement.avatarAddress.ToString(),
                                    ItemId = itemEnhancement.itemId.ToString(),
                                    MaterialId = itemEnhancement.materialId.ToString(),
                                    SlotIndex = itemEnhancement.slotIndex,
                                    BlockIndex = ev.BlockIndex,
                                });

                                var end = DateTimeOffset.UtcNow;
                                Log.Debug("Stored ItemEnhancement action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                                start = DateTimeOffset.UtcNow;

                                var slotState = ev.OutputStates.GetCombinationSlotState(
                                    itemEnhancement.avatarAddress,
                                    itemEnhancement.slotIndex);

                                if (slotState?.Result.itemUsable.ItemType is ItemType.Equipment)
                                {
                                    ProcessEquipmentData(
                                        ev.Signer,
                                        itemEnhancement.avatarAddress,
                                        (Equipment)slotState.Result.itemUsable);
                                }

                                end = DateTimeOffset.UtcNow;
                                Log.Debug(
                                    "Stored avatar {address}'s equipment in block #{index}. Time Taken: {time} ms.",
                                    itemEnhancement.avatarAddress,
                                    ev.BlockIndex,
                                    (end - start).Milliseconds);
                            }

                            if (ev.Action is Buy buy)
                            {
                                var start = DateTimeOffset.UtcNow;
                                AvatarState avatarState = ev.OutputStates.GetAvatarStateV2(buy.buyerAvatarAddress);
                                var buyerInventory = avatarState.inventory;
                                foreach (var purchaseInfo in buy.purchaseInfos)
                                {
                                    var state = ev.OutputStates.GetState(
                                    Addresses.GetItemAddress(purchaseInfo.TradableId));
                                    ITradableItem orderItem =
                                        (ITradableItem)ItemFactory.Deserialize((Dictionary)state!);
                                    Order order =
                                        OrderFactory.Deserialize(
                                            (Dictionary)ev.OutputStates.GetState(
                                                Order.DeriveAddress(purchaseInfo.OrderId))!);
                                    int itemCount = order is FungibleOrder fungibleOrder
                                        ? fungibleOrder.ItemCount
                                        : 1;
                                    if (orderItem.ItemType == ItemType.Equipment)
                                    {
                                        Equipment equipment = (Equipment)orderItem;
                                        _buyShopEquipmentsList.Add(new ShopHistoryEquipmentModel()
                                        {
                                            OrderId = purchaseInfo.OrderId.ToString(),
                                            TxId = string.Empty,
                                            BlockIndex = ev.BlockIndex,
                                            BlockHash = string.Empty,
                                            ItemId = equipment.ItemId.ToString(),
                                            SellerAvatarAddress = purchaseInfo.SellerAvatarAddress.ToString(),
                                            BuyerAvatarAddress = buy.buyerAvatarAddress.ToString(),
                                            Price = decimal.Parse(purchaseInfo.Price.ToString().Split(" ").FirstOrDefault()!),
                                            ItemType = equipment.ItemType.ToString(),
                                            ItemSubType = equipment.ItemSubType.ToString(),
                                            Id = equipment.Id,
                                            BuffSkillCount = equipment.BuffSkills.Count,
                                            ElementalType = equipment.ElementalType.ToString(),
                                            Grade = equipment.Grade,
                                            SetId = equipment.SetId,
                                            SkillsCount = equipment.Skills.Count,
                                            SpineResourcePath = equipment.SpineResourcePath,
                                            RequiredBlockIndex = equipment.RequiredBlockIndex,
                                            NonFungibleId = equipment.NonFungibleId.ToString(),
                                            TradableId = equipment.TradableId.ToString(),
                                            UniqueStatType = equipment.UniqueStatType.ToString(),
                                            ItemCount = itemCount,
                                            TimeStamp = _blockTimeOffset,
                                        });
                                    }

                                    if (orderItem.ItemType == ItemType.Costume)
                                    {
                                        Costume costume = (Costume)orderItem;
                                        _buyShopCostumesList.Add(new ShopHistoryCostumeModel()
                                        {
                                            OrderId = purchaseInfo.OrderId.ToString(),
                                            TxId = string.Empty,
                                            BlockIndex = ev.BlockIndex,
                                            BlockHash = string.Empty,
                                            ItemId = costume.ItemId.ToString(),
                                            SellerAvatarAddress = purchaseInfo.SellerAvatarAddress.ToString(),
                                            BuyerAvatarAddress = buy.buyerAvatarAddress.ToString(),
                                            Price = decimal.Parse(purchaseInfo.Price.ToString().Split(" ").FirstOrDefault()!),
                                            ItemType = costume.ItemType.ToString(),
                                            ItemSubType = costume.ItemSubType.ToString(),
                                            Id = costume.Id,
                                            ElementalType = costume.ElementalType.ToString(),
                                            Grade = costume.Grade,
                                            Equipped = costume.Equipped,
                                            SpineResourcePath = costume.SpineResourcePath,
                                            RequiredBlockIndex = costume.RequiredBlockIndex,
                                            NonFungibleId = costume.NonFungibleId.ToString(),
                                            TradableId = costume.TradableId.ToString(),
                                            ItemCount = itemCount,
                                            TimeStamp = _blockTimeOffset,
                                        });
                                    }

                                    if (orderItem.ItemType == ItemType.Material)
                                    {
                                        Material material = (Material)orderItem;
                                        _buyShopMaterialsList.Add(new ShopHistoryMaterialModel()
                                        {
                                            OrderId = purchaseInfo.OrderId.ToString(),
                                            TxId = string.Empty,
                                            BlockIndex = ev.BlockIndex,
                                            BlockHash = string.Empty,
                                            ItemId = material.ItemId.ToString(),
                                            SellerAvatarAddress = purchaseInfo.SellerAvatarAddress.ToString(),
                                            BuyerAvatarAddress = buy.buyerAvatarAddress.ToString(),
                                            Price = decimal.Parse(purchaseInfo.Price.ToString().Split(" ").FirstOrDefault()!),
                                            ItemType = material.ItemType.ToString(),
                                            ItemSubType = material.ItemSubType.ToString(),
                                            Id = material.Id,
                                            ElementalType = material.ElementalType.ToString(),
                                            Grade = material.Grade,
                                            ItemCount = itemCount,
                                            TimeStamp = _blockTimeOffset,
                                        });
                                    }

                                    if (orderItem.ItemType == ItemType.Consumable)
                                    {
                                        Consumable consumable = (Consumable)orderItem;
                                        _buyShopConsumablesList.Add(new ShopHistoryConsumableModel()
                                        {
                                            OrderId = purchaseInfo.OrderId.ToString(),
                                            TxId = string.Empty,
                                            BlockIndex = ev.BlockIndex,
                                            BlockHash = string.Empty,
                                            ItemId = consumable.ItemId.ToString(),
                                            SellerAvatarAddress = purchaseInfo.SellerAvatarAddress.ToString(),
                                            BuyerAvatarAddress = buy.buyerAvatarAddress.ToString(),
                                            Price = decimal.Parse(purchaseInfo.Price.ToString().Split(" ").FirstOrDefault()!),
                                            ItemType = consumable.ItemType.ToString(),
                                            ItemSubType = consumable.ItemSubType.ToString(),
                                            Id = consumable.Id,
                                            BuffSkillCount = consumable.BuffSkills.Count,
                                            ElementalType = consumable.ElementalType.ToString(),
                                            Grade = consumable.Grade,
                                            SkillsCount = consumable.Skills.Count,
                                            RequiredBlockIndex = consumable.RequiredBlockIndex,
                                            NonFungibleId = consumable.NonFungibleId.ToString(),
                                            TradableId = consumable.TradableId.ToString(),
                                            MainStat = consumable.MainStat.ToString(),
                                            ItemCount = itemCount,
                                            TimeStamp = _blockTimeOffset,
                                        });
                                    }

                                    if (purchaseInfo.ItemSubType == ItemSubType.Armor
                                        || purchaseInfo.ItemSubType == ItemSubType.Belt
                                        || purchaseInfo.ItemSubType == ItemSubType.Necklace
                                        || purchaseInfo.ItemSubType == ItemSubType.Ring
                                        || purchaseInfo.ItemSubType == ItemSubType.Weapon)
                                    {
                                        var sellerState = ev.OutputStates.GetAvatarStateV2(purchaseInfo.SellerAvatarAddress);
                                        var sellerInventory = sellerState.inventory;

                                        if (buyerInventory.Equipments == null || sellerInventory.Equipments == null)
                                        {
                                            continue;
                                        }

                                        Equipment? equipment = buyerInventory.Equipments.SingleOrDefault(i =>
                                            i.TradableId == purchaseInfo.TradableId) ?? sellerInventory.Equipments.SingleOrDefault(i =>
                                            i.TradableId == purchaseInfo.TradableId);

                                        if (equipment is { } equipmentNotNull)
                                        {
                                            ProcessEquipmentData(
                                                ev.Signer,
                                                buy.buyerAvatarAddress,
                                                equipmentNotNull);
                                        }
                                    }
                                }

                                var end = DateTimeOffset.UtcNow;
                                Log.Debug(
                                    "Stored avatar {address}'s equipment in block #{index}. Time Taken: {time} ms.",
                                    buy.buyerAvatarAddress,
                                    ev.BlockIndex,
                                    (end - start).Milliseconds);
                            }

                            if (ev.Action is Stake stake)
                            {
                                var start = DateTimeOffset.UtcNow;
                                ev.OutputStates.TryGetStakeState(ev.Signer, out StakeState stakeState);
                                var prevStakeStartBlockIndex =
                                    !ev.PreviousStates.TryGetStakeState(ev.Signer, out StakeState prevStakeState)
                                        ? 0 : prevStakeState.StartedBlockIndex;
                                var newStakeStartBlockIndex = stakeState.StartedBlockIndex;
                                var currency = ev.OutputStates.GetGoldCurrency();
                                var balance = ev.OutputStates.GetBalance(ev.Signer, currency);
                                var stakeStateAddress = StakeState.DeriveAddress(ev.Signer);
                                var previousAmount = ev.PreviousStates.GetBalance(stakeStateAddress, currency);
                                var newAmount = ev.OutputStates.GetBalance(stakeStateAddress, currency);
                                _stakeList.Add(new StakeModel()
                                {
                                    BlockIndex = ev.BlockIndex,
                                    AgentAddress = ev.Signer.ToString(),
                                    PreviousAmount = Convert.ToDecimal(previousAmount.GetQuantityString()),
                                    NewAmount = Convert.ToDecimal(newAmount.GetQuantityString()),
                                    RemainingNCG = Convert.ToDecimal(balance.GetQuantityString()),
                                    PrevStakeStartBlockIndex = prevStakeStartBlockIndex,
                                    NewStakeStartBlockIndex = newStakeStartBlockIndex,
                                    TimeStamp = _blockTimeOffset,
                                });
                                var end = DateTimeOffset.UtcNow;
                                Log.Debug("Stored Stake action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                            }

                            if (ev.Action is ClaimStakeReward claimStakeReward)
                            {
                                var start = DateTimeOffset.UtcNow;
                                var plainValue = (Bencodex.Types.Dictionary)claimStakeReward.PlainValue;
                                var avatarAddress = plainValue[AvatarAddressKey].ToAddress();
                                var id = claimStakeReward.Id;
                                ev.PreviousStates.TryGetStakeState(ev.Signer, out StakeState prevStakeState);

                                var claimStakeStartBlockIndex = prevStakeState.StartedBlockIndex;
                                var claimStakeEndBlockIndex = prevStakeState.ReceivedBlockIndex;
                                var currency = ev.OutputStates.GetGoldCurrency();
                                var stakeStateAddress = StakeState.DeriveAddress(ev.Signer);
                                var stakedAmount = ev.OutputStates.GetBalance(stakeStateAddress, currency);

                                var sheets = ev.PreviousStates.GetSheets(new[]
                                {
                                    typeof(StakeRegularRewardSheet),
                                    typeof(ConsumableItemSheet),
                                    typeof(CostumeItemSheet),
                                    typeof(EquipmentItemSheet),
                                    typeof(MaterialItemSheet),
                                });
                                StakeRegularRewardSheet stakeRegularRewardSheet = sheets.GetSheet<StakeRegularRewardSheet>();
                                int level = stakeRegularRewardSheet.FindLevelByStakedAmount(ev.Signer, stakedAmount);
                                var rewards = stakeRegularRewardSheet[level].Rewards;
                                var accumulatedRewards = prevStakeState.CalculateAccumulatedRewards(ev.BlockIndex);
                                int hourGlassCount = 0;
                                int apPotionCount = 0;
                                foreach (var reward in rewards)
                                {
                                    var (quantity, _) = stakedAmount.DivRem(currency * reward.Rate);
                                    if (quantity < 1)
                                    {
                                        // If the quantity is zero, it doesn't add the item into inventory.
                                        continue;
                                    }

                                    if (reward.ItemId == 400000)
                                    {
                                        hourGlassCount += (int)quantity * accumulatedRewards;
                                    }

                                    if (reward.ItemId == 500000)
                                    {
                                        apPotionCount += (int)quantity * accumulatedRewards;
                                    }
                                }

                                if (ev.PreviousStates.TryGetSheet<StakeRegularFixedRewardSheet>(
                                        out var stakeRegularFixedRewardSheet))
                                {
                                    var fixedRewards = stakeRegularFixedRewardSheet[level].Rewards;
                                    foreach (var reward in fixedRewards)
                                    {
                                        if (reward.ItemId == 400000)
                                        {
                                            hourGlassCount += reward.Count * accumulatedRewards;
                                        }

                                        if (reward.ItemId == 500000)
                                        {
                                            apPotionCount += reward.Count * accumulatedRewards;
                                        }
                                    }
                                }

                                _claimStakeList.Add(new ClaimStakeRewardModel()
                                {
                                    Id = id.ToString(),
                                    BlockIndex = ev.BlockIndex,
                                    AgentAddress = ev.Signer.ToString(),
                                    ClaimRewardAvatarAddress = avatarAddress.ToString(),
                                    HourGlassCount = hourGlassCount,
                                    ApPotionCount = apPotionCount,
                                    ClaimStakeStartBlockIndex = claimStakeStartBlockIndex,
                                    ClaimStakeEndBlockIndex = claimStakeEndBlockIndex,
                                    TimeStamp = _blockTimeOffset,
                                });
                                var end = DateTimeOffset.UtcNow;
                                Log.Debug("Stored ClaimStakeReward action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                            }

                            if (ev.Action is MigrateMonsterCollection mc)
                            {
                                var start = DateTimeOffset.UtcNow;
                                ev.OutputStates.TryGetStakeState(ev.Signer, out StakeState stakeState);
                                var agentState = ev.PreviousStates.GetAgentState(ev.Signer);
                                Address collectionAddress = MonsterCollectionState.DeriveAddress(ev.Signer, agentState.MonsterCollectionRound);
                                ev.PreviousStates.TryGetState(collectionAddress, out Dictionary stateDict);
                                var monsterCollectionState = new MonsterCollectionState(stateDict);
                                var currency = ev.OutputStates.GetGoldCurrency();
                                var migrationAmount = ev.PreviousStates.GetBalance(monsterCollectionState.address, currency);
                                var migrationStartBlockIndex = ev.BlockIndex;
                                var stakeStartBlockIndex = stakeState.StartedBlockIndex;
                                _mmcList.Add(new MigrateMonsterCollectionModel()
                                {
                                    BlockIndex = ev.BlockIndex,
                                    AgentAddress = ev.Signer.ToString(),
                                    MigrationAmount = Convert.ToDecimal(migrationAmount.GetQuantityString()),
                                    MigrationStartBlockIndex = migrationStartBlockIndex,
                                    StakeStartBlockIndex = stakeStartBlockIndex,
                                    TimeStamp = _blockTimeOffset,
                                });
                                var end = DateTimeOffset.UtcNow;
                                Log.Debug("Stored MigrateMonsterCollection action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                            }

                            if (ev.Action is Grinding grind)
                            {
                                var start = DateTimeOffset.UtcNow;

                                AvatarState prevAvatarState = ev.PreviousStates.GetAvatarStateV2(grind.AvatarAddress);
                                AgentState agentState = ev.PreviousStates.GetAgentState(ev.Signer);
                                var previousStates = ev.PreviousStates;
                                Address monsterCollectionAddress = MonsterCollectionState.DeriveAddress(
                                    ev.Signer,
                                    agentState.MonsterCollectionRound
                                );
                                Dictionary<Type, (Address, ISheet)> sheets = previousStates.GetSheets(sheetTypes: new[]
                                {
                                    typeof(CrystalEquipmentGrindingSheet),
                                    typeof(CrystalMonsterCollectionMultiplierSheet),
                                    typeof(MaterialItemSheet),
                                    typeof(StakeRegularRewardSheet),
                                });

                                List<Equipment> equipmentList = new List<Equipment>();
                                foreach (var equipmentId in grind.EquipmentIds)
                                {
                                    if (prevAvatarState.inventory.TryGetNonFungibleItem(equipmentId, out Equipment equipment))
                                    {
                                        equipmentList.Add(equipment);
                                    }
                                }

                                Currency currency = previousStates.GetGoldCurrency();
                                FungibleAssetValue stakedAmount = 0 * currency;
                                if (previousStates.TryGetStakeState(ev.Signer, out StakeState stakeState))
                                {
                                    stakedAmount = previousStates.GetBalance(stakeState.address, currency);
                                }
                                else
                                {
                                    if (previousStates.TryGetState(monsterCollectionAddress, out Dictionary _))
                                    {
                                        stakedAmount = previousStates.GetBalance(monsterCollectionAddress, currency);
                                    }
                                }

                                FungibleAssetValue crystal = CrystalCalculator.CalculateCrystal(
                                    ev.Signer,
                                    equipmentList,
                                    stakedAmount,
                                    false,
                                    sheets.GetSheet<CrystalEquipmentGrindingSheet>(),
                                    sheets.GetSheet<CrystalMonsterCollectionMultiplierSheet>(),
                                    sheets.GetSheet<StakeRegularRewardSheet>()
                                );

                                foreach (var equipment in equipmentList)
                                {
                                    _grindList.Add(new GrindingModel()
                                    {
                                        Id = grind.Id.ToString(),
                                        AgentAddress = ev.Signer.ToString(),
                                        AvatarAddress = grind.AvatarAddress.ToString(),
                                        EquipmentItemId = equipment.ItemId.ToString(),
                                        EquipmentId = equipment.Id,
                                        EquipmentLevel = equipment.level,
                                        Crystal = Convert.ToDecimal(crystal.GetQuantityString()),
                                        BlockIndex = ev.BlockIndex,
                                        TimeStamp = _blockTimeOffset,
                                    });
                                }

                                var end = DateTimeOffset.UtcNow;
                                Log.Debug("Stored Grinding action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                            }

                            if (ev.Action is UnlockEquipmentRecipe unlockEquipmentRecipe)
                            {
                                var start = DateTimeOffset.UtcNow;
                                var previousStates = ev.PreviousStates;
                                Currency crystalCurrency = CrystalCalculator.CRYSTAL;
                                var prevCrystalBalance = previousStates.GetBalance(
                                    ev.Signer,
                                    crystalCurrency);
                                var outputCrystalBalance = ev.OutputStates.GetBalance(
                                    ev.Signer,
                                    crystalCurrency);
                                var burntCrystal = prevCrystalBalance - outputCrystalBalance;
                                foreach (var recipeId in unlockEquipmentRecipe.RecipeIds)
                                {
                                    _unlockEquipmentRecipeList.Add(new UnlockEquipmentRecipeModel()
                                    {
                                        Id = unlockEquipmentRecipe.Id.ToString(),
                                        BlockIndex = ev.BlockIndex,
                                        AgentAddress = ev.Signer.ToString(),
                                        AvatarAddress = unlockEquipmentRecipe.AvatarAddress.ToString(),
                                        UnlockEquipmentRecipeId = recipeId,
                                        BurntCrystal = Convert.ToDecimal(burntCrystal.GetQuantityString()),
                                        TimeStamp = _blockTimeOffset,
                                    });
                                }

                                var end = DateTimeOffset.UtcNow;
                                Log.Debug("Stored UnlockEquipmentRecipe action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                            }

                            if (ev.Action is UnlockWorld unlockWorld)
                            {
                                var start = DateTimeOffset.UtcNow;
                                var previousStates = ev.PreviousStates;
                                Currency crystalCurrency = CrystalCalculator.CRYSTAL;
                                var prevCrystalBalance = previousStates.GetBalance(
                                    ev.Signer,
                                    crystalCurrency);
                                var outputCrystalBalance = ev.OutputStates.GetBalance(
                                    ev.Signer,
                                    crystalCurrency);
                                var burntCrystal = prevCrystalBalance - outputCrystalBalance;
                                foreach (var worldId in unlockWorld.WorldIds)
                                {
                                    _unlockWorldList.Add(new UnlockWorldModel()
                                    {
                                        Id = unlockWorld.Id.ToString(),
                                        BlockIndex = ev.BlockIndex,
                                        AgentAddress = ev.Signer.ToString(),
                                        AvatarAddress = unlockWorld.AvatarAddress.ToString(),
                                        UnlockWorldId = worldId,
                                        BurntCrystal = Convert.ToDecimal(burntCrystal.GetQuantityString()),
                                        TimeStamp = _blockTimeOffset,
                                    });
                                }

                                var end = DateTimeOffset.UtcNow;
                                Log.Debug("Stored UnlockWorld action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                            }

                            if (ev.Action is HackAndSlashRandomBuff hasRandomBuff)
                            {
                                var start = DateTimeOffset.UtcNow;
                                var previousStates = ev.PreviousStates;
                                AvatarState prevAvatarState = previousStates.GetAvatarStateV2(hasRandomBuff.AvatarAddress);
                                prevAvatarState.worldInformation.TryGetLastClearedStageId(out var currentStageId);
                                Currency crystalCurrency = CrystalCalculator.CRYSTAL;
                                var prevCrystalBalance = previousStates.GetBalance(
                                    ev.Signer,
                                    crystalCurrency);
                                var outputCrystalBalance = ev.OutputStates.GetBalance(
                                    ev.Signer,
                                    crystalCurrency);
                                var burntCrystal = prevCrystalBalance - outputCrystalBalance;
                                _hasRandomBuffList.Add(new HasRandomBuffModel()
                                {
                                    Id = hasRandomBuff.Id.ToString(),
                                    BlockIndex = ev.BlockIndex,
                                    AgentAddress = ev.Signer.ToString(),
                                    AvatarAddress = hasRandomBuff.AvatarAddress.ToString(),
                                    HasStageId = currentStageId,
                                    GachaCount = !hasRandomBuff.AdvancedGacha ? 5 : 10,
                                    BurntCrystal = Convert.ToDecimal(burntCrystal.GetQuantityString()),
                                    TimeStamp = _blockTimeOffset,
                                });

                                var end = DateTimeOffset.UtcNow;
                                Log.Debug("Stored HasRandomBuff action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                            }

                            if (ev.Action is JoinArena joinArena)
                            {
                                var start = DateTimeOffset.UtcNow;
                                AvatarState avatarState = ev.OutputStates.GetAvatarStateV2(joinArena.avatarAddress);
                                var previousStates = ev.PreviousStates;
                                Currency crystalCurrency = CrystalCalculator.CRYSTAL;
                                var prevCrystalBalance = previousStates.GetBalance(
                                    ev.Signer,
                                    crystalCurrency);
                                var outputCrystalBalance = ev.OutputStates.GetBalance(
                                    ev.Signer,
                                    crystalCurrency);
                                var burntCrystal = prevCrystalBalance - outputCrystalBalance;
                                _joinArenaList.Add(new JoinArenaModel()
                                {
                                    Id = joinArena.Id.ToString(),
                                    BlockIndex = ev.BlockIndex,
                                    AgentAddress = ev.Signer.ToString(),
                                    AvatarAddress = joinArena.avatarAddress.ToString(),
                                    AvatarLevel = avatarState.level,
                                    ArenaRound = joinArena.round,
                                    ChampionshipId = joinArena.championshipId,
                                    BurntCrystal = Convert.ToDecimal(burntCrystal.GetQuantityString()),
                                    TimeStamp = _blockTimeOffset,
                                });

                                var end = DateTimeOffset.UtcNow;
                                Log.Debug("Stored JoinArena action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                            }

                            if (ev.Action is BattleArena battleArena)
                            {
                                var start = DateTimeOffset.UtcNow;
                                AvatarState avatarState = ev.OutputStates.GetAvatarStateV2(battleArena.myAvatarAddress);
                                var previousStates = ev.PreviousStates;
                                var myArenaScoreAdr =
                                    ArenaScore.DeriveAddress(battleArena.myAvatarAddress, battleArena.championshipId, battleArena.round);
                                previousStates.TryGetArenaScore(myArenaScoreAdr, out var previousArenaScore);
                                ev.OutputStates.TryGetArenaScore(myArenaScoreAdr, out var currentArenaScore);
                                Currency ncgCurrency = ev.OutputStates.GetGoldCurrency();
                                var prevNCGBalance = previousStates.GetBalance(
                                    ev.Signer,
                                    ncgCurrency);
                                var outputNCGBalance = ev.OutputStates.GetBalance(
                                    ev.Signer,
                                    ncgCurrency);
                                var burntNCG = prevNCGBalance - outputNCGBalance;
                                int ticketCount = battleArena.ticket;
                                var sheets = previousStates.GetSheets(
                                    containArenaSimulatorSheets: true,
                                    sheetTypes: new[] { typeof(ArenaSheet), typeof(ItemRequirementSheet), typeof(EquipmentItemRecipeSheet), typeof(EquipmentItemSubRecipeSheetV2), typeof(EquipmentItemOptionSheet), typeof(MaterialItemSheet), }
                                );
                                var arenaSheet = ev.OutputStates.GetSheet<ArenaSheet>();
                                var arenaData = arenaSheet.GetRoundByBlockIndex(ev.BlockIndex);
                                var arenaInformationAdr =
                                    ArenaInformation.DeriveAddress(battleArena.myAvatarAddress, battleArena.championshipId, battleArena.round);
                                previousStates.TryGetArenaInformation(arenaInformationAdr, out var previousArenaInformation);
                                ev.OutputStates.TryGetArenaInformation(arenaInformationAdr, out var currentArenaInformation);
                                var winCount = currentArenaInformation.Win - previousArenaInformation.Win;
                                var medalCount = 0;
                                if (arenaData.ArenaType != ArenaType.OffSeason &&
                                    winCount > 0)
                                {
                                    var materialSheet = sheets.GetSheet<MaterialItemSheet>();
                                    var medal = ArenaHelper.GetMedal(battleArena.championshipId, battleArena.round, materialSheet);
                                    if (medal != null)
                                    {
                                        medalCount += winCount;
                                    }
                                }

                                _battleArenaList.Add(new BattleArenaModel()
                                {
                                    Id = battleArena.Id.ToString(),
                                    BlockIndex = ev.BlockIndex,
                                    AgentAddress = ev.Signer.ToString(),
                                    AvatarAddress = battleArena.myAvatarAddress.ToString(),
                                    AvatarLevel = avatarState.level,
                                    EnemyAvatarAddress = battleArena.enemyAvatarAddress.ToString(),
                                    ChampionshipId = battleArena.championshipId,
                                    Round = battleArena.round,
                                    TicketCount = ticketCount,
                                    BurntNCG = Convert.ToDecimal(burntNCG.GetQuantityString()),
                                    Victory = currentArenaScore.Score > previousArenaScore.Score,
                                    MedalCount = medalCount,
                                    TimeStamp = _blockTimeOffset,
                                });

                                var end = DateTimeOffset.UtcNow;
                                Log.Debug("Stored BattleArena action in block #{index}. Time Taken: {time} ms.", ev.BlockIndex, (end - start).Milliseconds);
                            }
                        }
                        catch (Exception ex)
                        {
                            Log.Error("RenderSubscriber: {message}", ex.Message);
                        }
                    });

            _actionRenderer.EveryUnrender<ActionBase>()
                .Subscribe(
                    ev =>
                    {
                        try
                        {
                            if (ev.Exception != null)
                            {
                                return;
                            }

                            if (ev.Action is HackAndSlash has)
                            {
                                MySqlStore.DeleteHackAndSlash(has.Id);
                                Log.Debug("Deleted HackAndSlash action in block #{index}", ev.BlockIndex);
                            }

                            if (ev.Action is CombinationConsumable combinationConsumable)
                            {
                                MySqlStore.DeleteCombinationConsumable(combinationConsumable.Id);
                                Log.Debug("Deleted CombinationConsumable action in block #{index}", ev.BlockIndex);
                            }

                            if (ev.Action is CombinationEquipment combinationEquipment)
                            {
                                MySqlStore.DeleteCombinationEquipment(combinationEquipment.Id);
                                Log.Debug("Deleted CombinationEquipment action in block #{index}", ev.BlockIndex);
                                var slotState = ev.OutputStates.GetCombinationSlotState(
                                    combinationEquipment.avatarAddress,
                                    combinationEquipment.slotIndex);

                                if (slotState?.Result.itemUsable.ItemType is ItemType.Equipment)
                                {
                                    ProcessEquipmentData(
                                        ev.Signer,
                                        combinationEquipment.avatarAddress,
                                        (Equipment)slotState.Result.itemUsable);
                                }

                                Log.Debug(
                                    "Reverted avatar {address}'s equipments in block #{index}",
                                    combinationEquipment.avatarAddress,
                                    ev.BlockIndex);
                            }

                            if (ev.Action is ItemEnhancement itemEnhancement)
                            {
                                MySqlStore.DeleteItemEnhancement(itemEnhancement.Id);
                                Log.Debug("Deleted ItemEnhancement action in block #{index}", ev.BlockIndex);
                                var slotState = ev.OutputStates.GetCombinationSlotState(
                                    itemEnhancement.avatarAddress,
                                    itemEnhancement.slotIndex);

                                if (slotState?.Result.itemUsable.ItemType is ItemType.Equipment)
                                {
                                    ProcessEquipmentData(
                                        ev.Signer,
                                        itemEnhancement.avatarAddress,
                                        (Equipment)slotState.Result.itemUsable);
                                }

                                Log.Debug(
                                    "Reverted avatar {address}'s equipments in block #{index}",
                                    itemEnhancement.avatarAddress,
                                    ev.BlockIndex);
                            }

                            if (ev.Action is Buy buy)
                            {
                                var buyerInventory = ev.OutputStates.GetAvatarStateV2(buy.buyerAvatarAddress).inventory;

                                foreach (var purchaseInfo in buy.purchaseInfos)
                                {
                                    if (purchaseInfo.ItemSubType == ItemSubType.Armor
                                        || purchaseInfo.ItemSubType == ItemSubType.Belt
                                        || purchaseInfo.ItemSubType == ItemSubType.Necklace
                                        || purchaseInfo.ItemSubType == ItemSubType.Ring
                                        || purchaseInfo.ItemSubType == ItemSubType.Weapon)
                                    {
                                        AvatarState sellerState = ev.OutputStates.GetAvatarStateV2(purchaseInfo.SellerAvatarAddress);
                                        var sellerInventory = sellerState.inventory;
                                        var previousStates = ev.PreviousStates;
                                        var characterSheet = previousStates.GetSheet<CharacterSheet>();
                                        var avatarLevel = sellerState.level;
                                        var avatarArmorId = sellerState.GetArmorId();
                                        var avatarTitleCostume = sellerState.inventory.Costumes.FirstOrDefault(costume => costume.ItemSubType == ItemSubType.Title && costume.equipped);
                                        int? avatarTitleId = null;
                                        if (avatarTitleCostume != null)
                                        {
                                            avatarTitleId = avatarTitleCostume.Id;
                                        }

                                        var avatarCp = CPHelper.GetCP(sellerState, characterSheet);
                                        string avatarName = sellerState.name;

                                        if (buyerInventory.Equipments == null || sellerInventory.Equipments == null)
                                        {
                                            continue;
                                        }

                                        MySqlStore.StoreAgent(ev.Signer);
                                        MySqlStore.StoreAvatar(
                                            purchaseInfo.SellerAvatarAddress,
                                            purchaseInfo.SellerAgentAddress,
                                            avatarName,
                                            avatarLevel,
                                            avatarTitleId,
                                            avatarArmorId,
                                            avatarCp);
                                        Equipment? equipment = buyerInventory.Equipments.SingleOrDefault(i =>
                                            i.TradableId == purchaseInfo.TradableId) ?? sellerInventory.Equipments.SingleOrDefault(i =>
                                            i.TradableId == purchaseInfo.TradableId);

                                        if (equipment is { } equipmentNotNull)
                                        {
                                            ProcessEquipmentData(
                                                purchaseInfo.SellerAvatarAddress,
                                                purchaseInfo.SellerAgentAddress,
                                                equipmentNotNull);
                                        }
                                    }
                                }

                                Log.Debug(
                                    "Reverted avatar {address}'s equipment in block #{index}",
                                    buy.buyerAvatarAddress,
                                    ev.BlockIndex);
                            }
                        }
                        catch (Exception ex)
                        {
                            Log.Error("RenderSubscriber: {message}", ex.Message);
                        }
                    });
            return Task.CompletedTask;
        }

        private void ProcessEquipmentData(
            Address agentAddress,
            Address avatarAddress,
            Equipment equipment)
        {
            var cp = CPHelper.GetCP(equipment);
            _eqList.Add(new EquipmentModel()
            {
                ItemId = equipment.ItemId.ToString(),
                AgentAddress = agentAddress.ToString(),
                AvatarAddress = avatarAddress.ToString(),
                EquipmentId = equipment.Id,
                Cp = cp,
                Level = equipment.level,
                ItemSubType = equipment.ItemSubType.ToString(),
            });
        }

        private void ProcessAgentAvatarData(ActionBase.ActionEvaluation<ActionBase> ev)
        {
            if (!_agents.Contains(ev.Signer.ToString()))
            {
                _agents.Add(ev.Signer.ToString());
                _agentList.Add(new AgentModel()
                {
                    Address = ev.Signer.ToString(),
                });

                if (ev.Signer != _miner)
                {
                    var agentState = ev.OutputStates.GetAgentState(ev.Signer);
                    if (agentState is { } ag)
                    {
                        var avatarAddresses = ag.avatarAddresses;
                        foreach (var avatarAddress in avatarAddresses)
                        {
                            try
                            {
                                AvatarState avatarState;
                                try
                                {
                                    avatarState = ev.OutputStates.GetAvatarStateV2(avatarAddress.Value);
                                }
                                catch (Exception)
                                {
                                    avatarState = ev.OutputStates.GetAvatarState(avatarAddress.Value);
                                }

                                if (avatarState == null)
                                {
                                    continue;
                                }

                                var previousStates = ev.PreviousStates;
                                var characterSheet = previousStates.GetSheet<CharacterSheet>();
                                var avatarLevel = avatarState.level;
                                var avatarArmorId = avatarState.GetArmorId();
                                Costume? avatarTitleCostume;
                                try
                                {
                                    avatarTitleCostume =
                                        avatarState.inventory.Costumes.FirstOrDefault(costume =>
                                            costume.ItemSubType == ItemSubType.Title &&
                                            costume.equipped);
                                }
                                catch (Exception)
                                {
                                    avatarTitleCostume = null;
                                }

                                int? avatarTitleId = null;
                                if (avatarTitleCostume != null)
                                {
                                    avatarTitleId = avatarTitleCostume.Id;
                                }

                                var avatarCp = CPHelper.GetCP(avatarState, characterSheet);
                                string avatarName = avatarState.name;

                                Log.Debug(
                                    "AvatarName: {0}, AvatarLevel: {1}, ArmorId: {2}, TitleId: {3}, CP: {4}",
                                    avatarName,
                                    avatarLevel,
                                    avatarArmorId,
                                    avatarTitleId,
                                    avatarCp);
                                _avatarList.Add(new AvatarModel()
                                {
                                    Address = avatarAddress.Value.ToString(),
                                    AgentAddress = ev.Signer.ToString(),
                                    Name = avatarName,
                                    AvatarLevel = avatarLevel,
                                    TitleId = avatarTitleId,
                                    ArmorId = avatarArmorId,
                                    Cp = avatarCp,
                                    Timestamp = _blockTimeOffset,
                                });
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.Message);
                            }
                        }
                    }
                }
            }
        }
    }
}
