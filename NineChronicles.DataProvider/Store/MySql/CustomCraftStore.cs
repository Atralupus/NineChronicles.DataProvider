namespace NineChronicles.DataProvider.Store
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.EntityFrameworkCore;
    using NineChronicles.DataProvider.Store.Models.CustomCraft;
    using Serilog;

    public partial class MySqlStore
    {
        public async partial Task StoreCustomEquipmentCraftList(List<CustomEquipmentCraftModel> customEquipmentCraftList)
        {
            NineChroniclesContext? ctx = null;
            try
            {
                ctx = await _dbContextFactory.CreateDbContextAsync();

                // This is for count update
                var iconCraftCountDict = new Dictionary<(string, int), int>();

                // Add new CustomCraft data
                foreach (var craftData in customEquipmentCraftList)
                {
                    if (await ctx.CustomEquipmentCraft.FirstOrDefaultAsync(c => c.Id == craftData.Id) is null)
                    {
                        iconCraftCountDict[(craftData.ItemSubType!, craftData.IconId)]++;
                        await ctx.CustomEquipmentCraft.AddAsync(craftData);
                    }
                }

                // Upsert CustomCraft count
                foreach (var ((itemSubType, iconId), count) in iconCraftCountDict)
                {
                    var countData = await ctx.CustomEquipmentCraftCount.FirstOrDefaultAsync(c => c.IconId == iconId);
                    if (countData is null)
                    {
                        await ctx.CustomEquipmentCraftCount.AddAsync(new CustomEquipmentCraftCountModel
                        {
                            IconId = iconId,
                            ItemSubType = itemSubType,
                            Count = count,
                        });
                    }
                    else
                    {
                        countData.Count += count;
                        ctx.Update(countData);
                    }
                }

                await ctx.SaveChangesAsync();
            }
            catch (Exception e)
            {
                Log.Debug(e.Message);
                Log.Debug(e.StackTrace);
            }
            finally
            {
                if (ctx is not null)
                {
                    await ctx.DisposeAsync();
                }
            }
        }
    }
}
