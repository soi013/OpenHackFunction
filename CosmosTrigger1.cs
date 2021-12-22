using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos;
using System.Linq;

namespace Contoso.Function
{
    public static class CosmosTrigger1
    {
        private const string outputCollection = "PopularMovies";
        private const string inputCollection = "MergedOrders";
        private const string workCollection = "CountedProductId";
        private const string targetDataBase = "contoso-movies";
        private const string endpointUrl = "https://contoso-db.documents.azure.com:443/";
        private const string authorizationKey = "tGkmlAsfIe6XPu8ZIU9Z76j75sT6VU5gBYhypIZWMqYwJMjAJT8UMGXgn169oIo0bcGws0jUg0FDae389Mwdrg==";

        [FunctionName("CosmosTrigger1")]
        public static async Task RunAsync([CosmosDBTrigger(
            databaseName: targetDataBase,
            collectionName: inputCollection,
            ConnectionStringSetting = "contosodb_DOCUMENTDB",
            LeaseCollectionName = "leases",
            CreateLeaseCollectionIfNotExists = true)]
            IReadOnlyList<Document> input,
            [CosmosDB(
            databaseName: targetDataBase,
            collectionName: outputCollection,
            ConnectionStringSetting = "contosodb_DOCUMENTDB")]
            IAsyncCollector<MovieRankItem> myDestinationCollection,
            // out dynamic document,            
            ILogger log)
        {
            if (input == null || input.Count <= 0)
            {
                // document = null;
                return;
            }

            log.LogInformation("Documents modified " + input.Count);
            log.LogInformation("First document Id " + input[0].Id);

            await QueryItems(log, myDestinationCollection);

            string queueMessage = input[0].Id;
            // document = new { Description = queueMessage, id = Guid.NewGuid() };

            log.LogInformation($"Description={queueMessage}");
        }
        private static async Task QueryItems(ILogger log, IAsyncCollector<MovieRankItem> myDestinationCollection)
        {
            //MergedOrdersの中から、人気順でTop10のProduct IDを取得するクエリ
            var sqlQueryText1 = @"
SELECT count(1) AS ProductIdcount,c.ProductId
FROM MergedOrders m
JOIN c IN m.Details
GROUP BY c.ProductId";

            //10個のProductIDからItemsの結果を取得してランクを付けるクエリ
            var sqlQueryText2 = "SELECT * FROM c WHERE c.LastName = 'Andersen'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText1);

            CosmosClient inputCosmosClient = new CosmosClient(endpointUrl, authorizationKey);
            var inputContainer = inputCosmosClient.GetContainer(targetDataBase, inputCollection);

            CosmosClient workCosmosClient = new CosmosClient(endpointUrl, authorizationKey);
            var workContainer = workCosmosClient.GetContainer(targetDataBase, workCollection);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText1);
            var feedIterator = inputContainer.GetItemQueryIterator<WorkItem>(queryDefinition);

            var workList = new List<WorkItem>();

            while (feedIterator.HasMoreResults)
            {
                FeedResponse<WorkItem> response = await feedIterator.ReadNextAsync();
                foreach (var item in response)
                {
                    log.LogInformation($"items = {item}");
                    workList.Add(item);
                    //なぜか書き込めない。
                    // var responseUpsert = await workContainer.UpsertItemAsync(item, new Microsoft.Azure.Cosmos.PartitionKey(item.ProductId));
                    // log.LogInformation($"response = {responseUpsert}");
                }
            }

            var topMovies = workList.OrderByDescending(x => x.ProductIdcount)
            .Take(10)
            .Select((x, i) => new MovieRankItem
            (
                ProductId: x.ProductId,
                ProductIdcount: x.ProductIdcount,
                MovieTitle: "HOGE",
                Rank: i
            ));

            foreach (var item in topMovies)
                await myDestinationCollection.AddAsync(item);
        }
    }

    public record WorkItem(string ProductId, int ProductIdcount);
    public record MovieRankItem(string ProductId, int ProductIdcount, string MovieTitle, int Rank);
}