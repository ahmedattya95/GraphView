using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Configuration;

namespace GraphViewProgram
{
    public class Program
    {

        public static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                throw new GraphView.GraphViewException("args in Main() Error");
            }

            List<string> result = GraphView.GraphTraversal.ExecuteQueryByDeserilization();

            foreach (var r in result)
            {
                Console.WriteLine(r);
            }

            SaveOutput(result, args[0]);
        }

        private static void SaveOutput(List<string> result, string outputContainerSas)
        {
            string outputFile = $"output-{Environment.GetEnvironmentVariable("AZ_BATCH_JOB_ID")}-{Environment.GetEnvironmentVariable("AZ_BATCH_TASK_ID")}";
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(outputFile))
            {
                file.WriteLine("------------------------------------------");
                foreach (var row in result)
                {
                    file.WriteLine(row);
                }

                // Write out some task information using some of the node's environment variables
                file.WriteLine($"Task: {Environment.GetEnvironmentVariable("AZ_BATCH_TASK_ID")}" +
                               $", Node: {Environment.GetEnvironmentVariable("AZ_BATCH_NODE_ID")}");
                file.WriteLine("------------------------------------------");
            }

            // Upload the output file to blob container in Azure Storage
            UploadFileToContainer(outputFile, outputContainerSas);
        }

        /// <summary>
        /// Uploads the specified file to the container represented by the specified
        /// container shared access signature (SAS).
        /// </summary>
        /// <param name="filePath">The path of the file to upload to the Storage container.</param>
        /// <param name="containerSas">The shared access signature granting write access to the specified container.</param>
        private static void UploadFileToContainer(string filePath, string containerSas)
        {
            string blobName = Path.GetFileName(filePath);

            // Obtain a reference to the container using the SAS URI.
            CloudBlobContainer container = new CloudBlobContainer(new Uri(containerSas));

            // Upload the file (as a new blob) to the container
            try
            {
                CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
                blob.UploadFromFile(filePath);

                Console.WriteLine("Write operation succeeded for SAS URL " + containerSas);
                Console.WriteLine();
            }
            catch (StorageException e)
            {

                Console.WriteLine("Write operation failed for SAS URL " + containerSas);
                Console.WriteLine("Additional error information: " + e.Message);
                Console.WriteLine();

                // Indicate that a failure has occurred so that when the Batch service sets the
                // CloudTask.ExecutionInformation.ExitCode for the task that executed this application,
                // it properly indicates that there was a problem with the task.
                Environment.ExitCode = -1;
            }
        }


        private static void LoadModernGraphData()
        {
            string endpoint = ConfigurationManager.AppSettings["DocDBEndPoint"];
            string authKey = ConfigurationManager.AppSettings["DocDBKey"];
            string databaseId = ConfigurationManager.AppSettings["DocDBDatabase"];
            string collectionId = ConfigurationManager.AppSettings["DocDBCollection"];
            bool TestUseReverseEdge = true;
            string TestPartitionByKey = "name";
            int TestSpilledEdgeThresholdViagraphAPI = 1;

            GraphView.GraphViewConnection connection =  GraphView.GraphViewConnection.ResetGraphAPICollection(endpoint, authKey, databaseId, collectionId,
                TestUseReverseEdge, TestSpilledEdgeThresholdViagraphAPI, TestPartitionByKey);

            using (GraphView.GraphViewCommand graphCommand = new GraphView.GraphViewCommand(connection))
            {
                graphCommand.g().AddV("person").Property("id", "dummy").Property("name", "marko").Property("age", 29).Next();
                graphCommand.g().AddV("person").Property("id", "特殊符号").Property("name", "vadas").Property("age", 27).Next();
                graphCommand.g().AddV("software").Property("id", "这是一个中文ID").Property("name", "lop").Property("lang", "java").Next();
                graphCommand.g().AddV("person").Property("id", "引号").Property("name", "josh").Property("age", 32).Next();
                graphCommand.g().AddV("software").Property("id", "中文English").Property("name", "ripple").Property("lang", "java").Next();
                graphCommand.g().AddV("person").Property("name", "peter").Property("age", 35).Next();  // Auto generate document id
                graphCommand.g().V().Has("name", "marko").AddE("knows").Property("weight", 0.5d).To(graphCommand.g().V().Has("name", "vadas")).Next();
                graphCommand.g().V().Has("name", "marko").AddE("knows").Property("weight", 1.0d).To(graphCommand.g().V().Has("name", "josh")).Next();
                graphCommand.g().V().Has("name", "marko").AddE("created").Property("weight", 0.4d).To(graphCommand.g().V().Has("name", "lop")).Next();
                graphCommand.g().V().Has("name", "josh").AddE("created").Property("weight", 1.0d).To(graphCommand.g().V().Has("name", "ripple")).Next();
                graphCommand.g().V().Has("name", "josh").AddE("created").Property("weight", 0.4d).To(graphCommand.g().V().Has("name", "lop")).Next();
                graphCommand.g().V().Has("name", "peter").AddE("created").Property("weight", 0.2d).To(graphCommand.g().V().Has("name", "lop")).Next();

            }
        }
    }
}