using Newtonsoft.Json;
using System;

namespace CosmosOptimization
{
    class Models
    {

    }
    public class User
    {
        public string id { get; set; }

        public string username { get; set; }

        public string firstName { get; set; }

        public string lastName { get; set; }

        public string country { get; set; }

        public string phoneNumber { get; set; }

        public string loyaltyTier { get; set; }

        public string emailAddress { get; set; }

        public DateTime memberSince { get; set; }

        public string memberSinceYear { get; set; }

        public string documentType { get; set; }
    }

    public class ProductReview
    {
        public string id { get; set; }

        public string username { get; set; }

        public bool verifiedPurchase { get; set; }

        public string product { get; set; }

        public string review { get; set; }

        public decimal rating { get; set; }

        public DateTime reviewDate { get; set; }

        public string documentType { get; set; }
    }

    public class ProductReviewDetailed
    {
        public string id { get; set; }

        public string partitionKey { get; set; }

        public string username { get; set; }

        public bool verifiedPurchase { get; set; }

        public string product { get; set; }

        public string review { get; set; }

        public decimal rating { get; set; }

        public DateTime reviewDate { get; set; }

        public string documentType { get; set; }

        public string prop1 { get; set; }

        public string prop2 { get; set; }

        public string prop3 { get; set; }

        public string prop4 { get; set; }

        public string prop5 { get; set; }

        public string prop6 { get; set; }

        public string prop7 { get; set; }

        public string prop8 { get; set; }

        public string prop9 { get; set; }

        public string prop10 { get; set; }

        public string prop11 { get; set; }

        public string prop12 { get; set; }

    }

    public class UserTaskItem
    {
        public UserTaskItem()
        {
        }

        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        [JsonProperty(PropertyName = "_partitionKey", NullValueHandling = NullValueHandling.Ignore)]
        public string PartitionKey { get; set; }

        [JsonProperty(PropertyName = "status")]
        public string Status { get; set; }

    }
}
