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
        private const string targetDataBase = "contoso-movies";
        private const string endpointUrl = "https://contoso-db.documents.azure.com:443/";
        private const string authorizationKey = "tGkmlAsfIe6XPu8ZIU9Z76j75sT6VU5gBYhypIZWMqYwJMjAJT8UMGXgn169oIo0bcGws0jUg0FDae389Mwdrg==";

        [FunctionName("CosmosTrigger1")]
        public static void Run([CosmosDBTrigger(
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
            out dynamic document,
            ILogger log)
        {
            if (input == null || input.Count <= 0)
            {
                document = null;
                return;
            }

            log.LogInformation("Documents modified " + input.Count);
            log.LogInformation("First document Id " + input[0].Id);

            CosmosClient cosmosClient = new CosmosClient(endpointUrl, authorizationKey);
            QueryItems(cosmosClient, log);

            string queueMessage = input[0].Id;
            document = new { Description = queueMessage, id = Guid.NewGuid() };

            log.LogInformation($"Description={queueMessage}");
        }
        private static async Task QueryItems(CosmosClient cosmosClient, ILogger log)
        {
            //MergedOrdersの中から、人気順でTop10のProduct IDを取得するクエリ
            var sqlQueryText1 = "SELECT TOP 10 * FROM c";

            //10個のProductIDからItemsの結果を取得してランクを付けるクエリ
            var sqlQueryText2 = "SELECT * FROM c WHERE c.LastName = 'Andersen'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText1);

            var container = cosmosClient.GetContainer(targetDataBase, inputCollection);

            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText1);
            FeedIterator<dynamic> feedIterator = container.GetItemQueryIterator<dynamic>(queryDefinition);
            while (feedIterator.HasMoreResults)
            {
                FeedResponse<dynamic> response = await feedIterator.ReadNextAsync();
                foreach (var item in response)
                {
                    log.LogInformation($"{item}");
                }
            }
        }
    }
}