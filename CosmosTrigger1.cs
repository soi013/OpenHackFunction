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
        private const string subInputCollection = "Item";
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
            var sqlQueryWork = @"
SELECT count(1) AS ProductIdcount,c.ProductId
FROM MergedOrders m
JOIN c IN m.Details
GROUP BY c.ProductId";

            Console.WriteLine("Running query: {0}\n", sqlQueryWork);

            CosmosClient inputCosmosClient = new CosmosClient(endpointUrl, authorizationKey);
            var inputContainer = inputCosmosClient.GetContainer(targetDataBase, inputCollection);
            List<WorkItem> workList = await GetItemsFromContainer<WorkItem>(log, sqlQueryWork, inputContainer);

            var guid = Guid.NewGuid();
            var calcTime = DateTime.UtcNow;
            var subInputContainer = inputCosmosClient.GetContainer(targetDataBase, subInputCollection);

            var topWorkItems = workList
                            .OrderByDescending(x => x.ProductIdcount)
                            .Take(10)
                            .ToArray();

            string targetIds = string.Join(",", topWorkItems.Select(x => x.ProductId));
            string querySub = @$"
SELECT TOP 10 c.ItemId,c.ProductName,c.Category, c.CategoryId From c
WHERE c.ItemId in ({targetIds})";

            var movies = await GetItemsFromContainer<MovieItem>(log, querySub, subInputContainer);

            var topMovies = topWorkItems
            .Join(movies, x => x.ProductId, y => y.ItemId,
                (x, y) =>
                 new MovieRankItem(x.ProductId, x.ProductIdcount, y.ProductName, 0, guid, calcTime))
            .Select((a, i) => a with { Rank = i + 1 });

            foreach (var item in topMovies)
            {
                log.LogInformation($"add = {item}");
                await myDestinationCollection.AddAsync(item);
            }
        }

        private static async Task<List<T>> GetItemsFromContainer<T>(ILogger log, string queryText, Container sourceContainer)
        {
            QueryDefinition queryDefinition = new QueryDefinition(queryText);
            var feedIterator = sourceContainer.GetItemQueryIterator<T>(queryDefinition);

            var workList = new List<T>();

            while (feedIterator.HasMoreResults)
            {
                FeedResponse<T> response = await feedIterator.ReadNextAsync();
                foreach (var item in response)
                {
                    log.LogInformation($"items = {item}");
                    workList.Add(item);
                }
            }

            return workList;
        }
    }

    public record WorkItem(string ProductId, int ProductIdcount);
    public record MovieItem(string ItemId, string ProductName, string CategoryId, string Category);
    public record MovieRankItem(string ProductId, int ProductIdcount, string MovieTitle, int Rank, Guid Guid, DateTime CalcTime);
}