namespace NineChronicles.DataProvider.Store.Models
{
    using System.ComponentModel.DataAnnotations;

    public class AuraSummonFailModel
    {
        [Key]
        public string? Id { get; set; }

        public string? AgentAddress { get; set; }

        public AgentModel? Agent { get; set; }

        public string? AvatarAddress { get; set; }

        public AvatarModel? Avatar { get; set; }

        public int GroupId { get; set; }

        public int SummonCount { get; set; }

        public long BlockIndex { get; set; }

        public string? Exception { get; set; }
    }
}
