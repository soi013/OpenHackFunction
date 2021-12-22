using System;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace Contoso.Function
{
    public static class CosmosTrigger1
    {
        [FunctionName("CosmosTrigger1")]
        public static void Run([CosmosDBTrigger(
            databaseName: "contoso-movies",
            collectionName: "MergedOrders",
            ConnectionStringSetting = "contosodb_DOCUMENTDB",
            LeaseCollectionName = "leases",
            CreateLeaseCollectionIfNotExists = true)]
            IReadOnlyList<Document> input,
            [CosmosDB(
            databaseName: "contoso-movies",
            collectionName: "PopularMovies",
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

            string queueMessage = input[0].Id;
            document = new { Description = queueMessage, id = Guid.NewGuid() };

            log.LogInformation($"Description={queueMessage}");
        }
    }
}