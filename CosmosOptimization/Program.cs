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
                        await Issue4_TuneMaxItemCount();
                        break;
                    case ConsoleKey.Escape:
                        Console.WriteLine("Exiting...");
                        return;
                    case ConsoleKey.D5:
                        await Issue5_TuneCrossPartitionQuery();
                        break;
                    case ConsoleKey.D6:
                        await Issue6_TuneIndexingPolicy();
                        break;
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
            Console.WriteLine("4 - Issue 4: Tuning MaxItemCount per page"); //TODO Move this out

            Console.WriteLine("5 - Issue 5: Tuning cross-partition query"); //TODO Move this out
            Console.WriteLine("6 - Issue 6: Tuning indexing policy"); //TODO Move this out

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

        // Issue #4: Tuning Max Item Count on queries
        private async Task Issue4_TuneMaxItemCount() {
            var username = "Curt28";
            var containerName = "UserReviews_v2";
            var sqlQueryText = "SELECT * FROM c WHERE c.username = '" + username + "'";
            await RunQuery(sqlQueryText, containerName, maxItemCountPerPage : 50, useQueryOptions:true); //default of 100 maxitemcount per page
            await RunQuery(sqlQueryText, containerName, maxItemCountPerPage : -1, useQueryOptions: true); // set to dynamic page size

        }

        // For cross partition query, tune max degree of parallelism
        private async Task Issue5_TuneCrossPartitionQuery() {
            var containerName = "UserReviews_v2";
            var sqlQueryText = "SELECT * FROM c WHERE c.rating >= 4.7";
            await RunQuery(sqlQueryText, containerName, maxItemCountPerPage: -1, maxConcurrency : -1, useQueryOptions: true); // Setting to -1 lets Cosmos DB SDK set it to # of partitions

            await RunQuery(sqlQueryText, containerName, maxItemCountPerPage: -1, maxConcurrency : 0, useQueryOptions: true); // By default, Cosmos sets maxConcurrency = 0 
        }



        // Issue #5: Higher RU than expected on writes
        private async Task Issue6_TuneIndexingPolicy()
        {
            var database = this.cosmosClient.GetDatabase("IndexingDemo");
            var productReview = GenerateProductReviewDetailed();

            //Write data to container with default indexing policy
            var container1 = database.GetContainer("DefaultIndexPolicy");
            Console.BackgroundColor = ConsoleColor.Yellow;
            Console.ForegroundColor = ConsoleColor.Blue ;
            Console.WriteLine("Write item to container {0} with default indexing policy", container1.Id);

            Console.ResetColor();

            ItemResponse<ProductReviewDetailed> prItemResponse = await container1.CreateItemAsync<ProductReviewDetailed>(productReview, new PartitionKey(productReview.username));
            Console.WriteLine("\tConsumed {0} RUs", prItemResponse.RequestCharge);
            Console.WriteLine("\n");

            //Write data to container with tuned indexing policy
            var container2 = database.GetContainer("TunedIndexPolicy");
            Console.BackgroundColor = ConsoleColor.DarkGreen;
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine("Write item to container {0} with tuned indexing policy", container2.Id);

            Console.ResetColor();
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
        private async Task RunQuery(string sqlQueryText, string containerName, int maxItemCountPerPage = 100, int maxConcurrency = -1, bool useQueryOptions = false) //100 or 1MB, whichever comes first
        {
            Console.BackgroundColor = ConsoleColor.Blue;

            Console.WriteLine("Running query: {0} against container {1}\n", sqlQueryText, containerName);

            if (useQueryOptions)
            {
                Console.WriteLine("Using MaxConcurrency: {0}", maxConcurrency);
                Console.WriteLine("Using MaxItemCountPerPage: {0}", maxItemCountPerPage);
            }
            Console.ResetColor();

            double totalRequestCharge = 0;
            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);

            // Time the query
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            // Run query against Cosmos DB
            var container = this.database.GetContainer(containerName);

            QueryRequestOptions requestOptions;
            if (useQueryOptions) {
                requestOptions = new QueryRequestOptions()
                {
                    MaxItemCount = maxItemCountPerPage,
                    MaxConcurrency = maxConcurrency,
                };
            } else
            {
                requestOptions = new QueryRequestOptions(); //use all default query options
            }

            FeedIterator<dynamic> queryResultSetIterator = container.GetItemQueryIterator<dynamic>(queryDefinition,requestOptions: requestOptions);
            List<dynamic> reviews = new List<dynamic>();

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<dynamic> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                totalRequestCharge += currentResultSet.RequestCharge;
                //Console.WriteLine("another page");
                foreach (var item in currentResultSet)
                {
                    reviews.Add(item);
                }
                if (useQueryOptions)
                {
                    Console.WriteLine("Result count: {0}", reviews.Count);
                }

            }

            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;

            //Print results
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);

            Console.ForegroundColor = ConsoleColor.Green;

            Console.WriteLine("\tQuery returned {0} results", reviews.Count);
            Console.WriteLine("\tTotal time: {0}", elapsedTime);
            Console.WriteLine("\tTotal Request Units consumed: {0}\n", totalRequestCharge);
            Console.WriteLine("\n\n\n");
            Console.ResetColor();

        }

        //Helper method to run point read
        private async Task RunPointRead(string id, string partitionKeyValue, string containerName)
        {
            Console.BackgroundColor = ConsoleColor.Blue;

            Console.Write("Running point read: await container.ReadItemAsync<User>(id, partitionKeyValue)\n");
            Console.ResetColor();

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
            Console.ForegroundColor = ConsoleColor.Green;

            Console.WriteLine("\tPoint read returned {0} results", "1");
            Console.WriteLine("\tTotal time: {0}", elapsedTime);
            Console.WriteLine("\tTotal Request Units consumed: {0}\n", userProfileResponse.RequestCharge);
            Console.WriteLine("\n\n\n");
            Console.ResetColor();
        }
    }
}
