using Bogus;
using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Threading.Tasks;

namespace CosmosOptimization
{
    class Program
    {
        // The Azure Cosmos DB endpoint for running this sample.
        private static readonly string EndpointUri = ConfigurationManager.AppSettings["EndPointUri"];

        // The primary key for the Azure Cosmos account.
        private static readonly string PrimaryKey = ConfigurationManager.AppSettings["PrimaryKey"];

        // The Cosmos client instance
        private CosmosClient cosmosClient;
        private Microsoft.Azure.Cosmos.Database database;
        private Container container;

        private string databaseId = "DemoDB";
        private string containerId = "UserReviews_v1"; //database partitioned by id

       // <Main>
        public static async Task Main(string[] args)
        {
            try
            {
                Program p = new Program();
                p.Go().Wait();
            }
            catch (CosmosException de)
            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine("{0} error occurred: {1}", de.StatusCode, de);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: {0}", e);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }

        public async Task Go()
        {
            Setup();

            while (true)
            {
                PrintPrompt();

                var c = Console.ReadKey(true);
                switch (c.Key)
                {
                    case ConsoleKey.D1:
                        await Issue1_Latency();
                        break;
                    case ConsoleKey.D2:
                        await Issue2_HighRU();
                        break;
                    case ConsoleKey.D3:
                        await Issue3_QueryVsPointRead();
                        break;
                    case ConsoleKey.D4:
                        await Issue4_TuneIndexingPolicy();
                        break;
                    case ConsoleKey.Escape:
                        Console.WriteLine("Exiting...");
                        return;
                    default:
                        Console.WriteLine("Select choice");
                        break;
                }
            }
        }

        private void PrintPrompt()
        {
            //Console.WriteLine("Summary:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine(String.Format("Endpoint: {0}", EndpointUri));
            //Console.WriteLine(String.Format("Collection : {0}.{1}", DatabaseName, CollectionName));
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("");
            Console.WriteLine("Press for demo scenario:\n");

            Console.WriteLine("1 - Issue 1: Query is taking longer than expected"); //TODO Move this out
            Console.WriteLine("2 - Isuse 2: Query is consuming high RU"); //TODO Move this out
            Console.WriteLine("3 - Issue 3: Using query instead of point reads"); //TODO Move this out
            Console.WriteLine("4 - Issue 4: Tuning indexing policy"); //TODO Move this out

            Console.WriteLine("--------------------------------------------------------------------- ");
        }


        public void Setup()
        {
            var cosmosClientOptions = new CosmosClientOptions()
            {
                ApplicationRegion = "West US 2",
                //ApplicationRegion = "East US 2",

            };
            // Create a new instance of the Cosmos Client
            this.cosmosClient = new CosmosClient(EndpointUri, PrimaryKey, cosmosClientOptions);
            this.database = this.cosmosClient.GetDatabase(databaseId);
            this.container = this.database.GetContainer(containerId);

        }

        // Issue #1: Query is taking longer than expected
        private async Task Issue1_Latency()
        {
            Console.WriteLine("Running issue 1 demo:\n");

            var username = "Curt28";
            var sqlQueryText = "SELECT * FROM c WHERE c.username = '" + username + "'";

            var containerName = "UserReviews_v1";
            await RunQuery(sqlQueryText, containerName);

        }

        // Issue #2: Query is consuming more RU than expected
        private async Task Issue2_HighRU()
        {
            //Run query against container 2
            var username = "Curt28";
            var sqlQueryText = "SELECT * FROM c WHERE c.username = '" + username + "'";

            var containerPartitionedById = "UserReviews_v1";
            var containerPartitionedByUsername = "UserReviews_v2";

            await RunQuery(sqlQueryText, containerPartitionedById);
            await RunQuery(sqlQueryText, containerPartitionedByUsername);

        }

        // Issue #3: Using queries instead of point reads
        private async Task Issue3_QueryVsPointRead() { 

            var username = "Curt28";
            var id = "Curt28";

            var containerName = "UserReviews_v2";
            var sqlQueryText = "SELECT * FROM c WHERE c.username = '" + username + "'" + " AND c.documentType = 'user'";

            // Run a query to get 1 document  
            await RunQuery(sqlQueryText, containerName);

            // Do a point read instead
            await RunPointRead(id, username, containerName);

        }

        // Issue #5: Higher RU than expected on writes
        private async Task Issue4_TuneIndexingPolicy()
        {
            var database = this.cosmosClient.GetDatabase("IndexingDemo");
            var productReview = GenerateProductReviewDetailed();

            //Write data to container with default indexing policy
            var container1 = database.GetContainer("DefaultIndexPolicy");
            Console.WriteLine("Write item to container {0} with default indexing policy", container1);

                ItemResponse<ProductReviewDetailed> prItemResponse = await container1.CreateItemAsync<ProductReviewDetailed>(productReview, new PartitionKey(productReview.username));
            Console.WriteLine("\tConsumed {0} RUs", prItemResponse.RequestCharge);
            Console.WriteLine("\n");

            //Write data to container with tuned indexing policy
            var container2 = database.GetContainer("TunedIndexPolicy");
            Console.WriteLine("Write item to container {0} with default indexing policy", container2);

            ItemResponse<ProductReviewDetailed> prItemResponse2 = await container2.CreateItemAsync<ProductReviewDetailed>(productReview, new PartitionKey(productReview.username));

            Console.WriteLine("\tConsumed {0} RUs", prItemResponse2.RequestCharge);
            Console.WriteLine("\n\n\n");
        }

        private ProductReviewDetailed GenerateProductReviewDetailed()
    {
            //Against 1 collection with indexing policy
            var productReviewDetailed = new Faker<ProductReviewDetailed>()
            .StrictMode(true)
            //Generate website event
            .RuleFor(o => o.id, f => Guid.NewGuid().ToString()) //id
            .RuleFor(o => o.username, f => f.Internet.UserName())
            .RuleFor(o => o.verifiedPurchase, f => f.Random.Bool())
            .RuleFor(o => o.product, f => f.Commerce.Product())
            .RuleFor(o => o.review, f => f.Rant.Review())
            .RuleFor(o => o.rating, f => Math.Round(f.Random.Decimal(0, 5), 1))
            .RuleFor(o => o.reviewDate, f => f.Date.Between(DateTime.Now, DateTime.Now.AddDays(-1000)))
            .RuleFor(o => o.documentType, f => "review")
            .RuleFor(o => o.prop1, f => f.Commerce.ProductAdjective())
            .RuleFor(o => o.prop2, f => f.Lorem.Sentence())
            .RuleFor(o => o.prop3, f => f.Lorem.Sentence())
            .RuleFor(o => o.prop4, f => f.Lorem.Sentence())
            .RuleFor(o => o.prop5, f => f.Commerce.ProductAdjective())
            .RuleFor(o => o.prop6, f => f.Commerce.ProductAdjective())
            .RuleFor(o => o.prop7, f => f.Commerce.ProductAdjective())
            .RuleFor(o => o.prop8, f => f.Commerce.ProductAdjective())
            .RuleFor(o => o.prop9, f => f.Commerce.ProductAdjective())
            .RuleFor(o => o.prop10, f => f.Commerce.ProductAdjective())
            .RuleFor(o => o.prop11, f => f.Commerce.ProductAdjective())
            .RuleFor(o => o.prop12, f => f.Commerce.ProductAdjective())
            .Generate(1)[0];

            return productReviewDetailed;
        }

        //Helper method to run query
        private async Task RunQuery(string sqlQueryText, string containerName)
        {

            Console.WriteLine("Running query: {0} against container {1}", sqlQueryText, containerName);

            double totalRequestCharge = 0;
            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);

            // Time the query
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            // Run query against Cosmos DB
            var container = this.database.GetContainer(containerName);

            FeedIterator<dynamic> queryResultSetIterator = container.GetItemQueryIterator<dynamic>(queryDefinition);
            List<dynamic> reviews = new List<dynamic>();

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<dynamic> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                totalRequestCharge += currentResultSet.RequestCharge;
                foreach (var item in currentResultSet)
                {
                    reviews.Add(item);
                }
            }

            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;

            //Print results
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);
            Console.WriteLine("\tQuery returned {0} results", reviews.Count);
            Console.WriteLine("\tTotal time: {0}", elapsedTime);
            Console.WriteLine("\tTotal Request Units consumed: {0}\n", totalRequestCharge);
            Console.WriteLine("\n\n\n");

        }

        //Helper method to run point read
        private async Task RunPointRead(string id, string partitionKeyValue, string containerName)
        {
            Console.Write("Running point read:\n"); 
            var container = this.database.GetContainer(containerName);
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            //Perform point read
            ItemResponse<User> userProfileResponse = await container.ReadItemAsync<User>(id, new PartitionKey(partitionKeyValue));
            var result = userProfileResponse.Resource;

            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;

            //Print results
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);

            Console.WriteLine("\tPoint read returned {0} results", "1");
            Console.WriteLine("\tTotal time: {0}", elapsedTime);
            Console.WriteLine("\tTotal Request Units consumed: {0}\n", userProfileResponse.RequestCharge);
            Console.WriteLine("\n\n\n");

        }

    }
}
