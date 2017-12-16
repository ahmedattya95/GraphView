﻿using System.Linq;

namespace StartAzureBatch
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Azure.Batch;
    using Microsoft.Azure.Batch.Auth;
    using Microsoft.Azure.Batch.Common;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using GraphView;

    public class StartAzureBatch
    {
        // Batch account credentials
        private readonly string batchAccountName;
        private readonly string batchAccountKey;
        private readonly string batchAccountUrl;

        // Storage account credentials
        private readonly string storageConnectionString;

        // CosmosDB account credentials
        private readonly string docDBEndPoint;
        private readonly string docDBKey;
        private readonly string docDBDatabaseId;
        private readonly string docDBCollectionId;
        private readonly bool useReverseEdge;
        private readonly string partitionByKey;
        private readonly int spilledEdgeThresholdViagraphAPI;

        private readonly string poolId;
        private readonly string jobId;

        // number of tasks.
        private readonly int parallelism;

        // use container to upload program
        private readonly string denpendencyPath;
        private readonly string exeName;

        // local path that stores downloaded output
        private readonly string outputPath;

        public StartAzureBatch()
        {
            batchAccountName = "";
            batchAccountKey = "";
            batchAccountUrl = "";

            storageConnectionString = $"DefaultEndpointsProtocol=https;AccountName={""};AccountKey={""}";

            docDBEndPoint = "";
            docDBKey = "";
            docDBDatabaseId = "GroupMatch";
            docDBCollectionId = "Modern";
            useReverseEdge = true;
            partitionByKey = "name";
            spilledEdgeThresholdViagraphAPI = 1;

            poolId = "GraphViewPool";
            jobId = "GraphViewJob";

            parallelism = 3;

            denpendencyPath = "...\\GraphView\\GraphViewProgram\\bin\\Debug\\";
            exeName = "Program.exe";

            outputPath = "...\\Desktop";
        }

        public static void Main(string[] args)
        {
            try
            {
                // Call the asynchronous version of the Main() method. This is done so that we can await various
                // calls to async methods within the "Main" method of this console application.
                MainAsyncUseContainer().Wait();
            }
            catch (AggregateException ae)
            {
                Console.WriteLine();
                Console.WriteLine("One or more exceptions occurred.");
                Console.WriteLine();

                PrintAggregateException(ae);
            }
            finally
            {
                Console.WriteLine();
                Console.WriteLine("Sample complete, hit ENTER to exit...");
                Console.ReadLine();
            }
        }

        private static async Task MainAsyncUseContainer()
        {
            StartAzureBatch client = new StartAzureBatch();

            Console.WriteLine("Query start: {0}\n", DateTime.Now);
            Stopwatch timer = new Stopwatch();
            timer.Start();

            // Retrieve the storage account
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(client.storageConnectionString);

            // Create the blob client, for use in obtaining references to blob storage containers
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            const string outputContainerName = "output";
            const string appContainerName = "application";
            const string serializationContainerName = "serialization";
            await CreateContainerIfNotExistAsync(blobClient, appContainerName);
            await CreateContainerIfNotExistAsync(blobClient, outputContainerName);
            await CreateContainerIfNotExistAsync(blobClient, serializationContainerName);

            // Paths to the executable and its dependencies that will be executed by the tasks
            List<string> applicationFilePaths = new List<string>
            {
                client.denpendencyPath + client.exeName, // Program.exe
                client.denpendencyPath + "Microsoft.WindowsAzure.Storage.dll",
                client.denpendencyPath + "DocumentDB.Spatial.Sql.dll",
                client.denpendencyPath + "GraphView.dll",
                client.denpendencyPath + "JsonServer.dll",
                client.denpendencyPath + "Microsoft.Azure.Documents.Client.dll",
                client.denpendencyPath + "Microsoft.Azure.Documents.ServiceInterop.dll",
                client.denpendencyPath + "Microsoft.SqlServer.TransactSql.ScriptDom.dll", // 120
                client.denpendencyPath + "Newtonsoft.Json.dll",
                client.denpendencyPath + client.exeName + ".config", // "Program.exe.config"
            };

            Console.WriteLine("start compile query");
            // compile query, store results in XML files
            client.CompileQuery();
            Console.WriteLine("compile query finished");

            client.MakePartitionPlan();

            List<string> serializationDataPath = new List<string>
            {
                GraphViewSerializer.CommandFile,
                GraphViewSerializer.ContainerFile,
                GraphViewSerializer.OperatorsFile,
                GraphViewSerializer.SideEffectFile,
                GraphViewSerializer.PartitionPlanFile
            };

            // Upload the application and its dependencies to Azure Storage.
            List<ResourceFile> resourceFiles = await UploadFilesToContainerAsync(blobClient, appContainerName, applicationFilePaths);
            // Upload the serialization results (XML files) and its partition config to Azure Storage.
            resourceFiles.AddRange(await UploadFilesToContainerAsync(blobClient, serializationContainerName, serializationDataPath));

            // Obtain a shared access signature that provides write access to the output container to which
            // the tasks will upload their output.
            string outputContainerSasUrl = GetContainerSasUrl(blobClient, outputContainerName, SharedAccessBlobPermissions.Write);

            // Create a BatchClient. We'll now be interacting with the Batch service in addition to Storage
            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(client.batchAccountUrl, client.batchAccountName, client.batchAccountKey);
            using (BatchClient batchClient = BatchClient.Open(cred))
            {
                // Create the pool that will contain the compute nodes that will execute the tasks.
                // The ResourceFile collection that we pass in is used for configuring the pool's StartTask
                // which is executed each time a node first joins the pool (or is rebooted or reimaged).
                await client.CreatePoolIfNotExistAsync(batchClient, resourceFiles);

                // Create the job that will run the tasks.
                await client.CreateJobAsync(batchClient);

                // Add the tasks to the job. We need to supply a container shared access signature for the
                // tasks so that they can upload their output to Azure Storage.
                await client.AddTasksAsync(batchClient, resourceFiles, outputContainerSasUrl);

                // Monitor task success/failure, specifying a maximum amount of time to wait for the tasks to complete
                await MonitorTasks(batchClient, client.jobId, TimeSpan.FromMinutes(15));

                // Download the task output files from the output Storage container to a local directory
                await client.DownloadAndAggregateOutputAsync(blobClient, outputContainerName);

                // Clean up Storage resources
                await DeleteContainerAsync(blobClient, outputContainerName);

                // Print out some timing info
                timer.Stop();
                Console.WriteLine();
                Console.WriteLine("Sample end: {0}", DateTime.Now);
                Console.WriteLine("Elapsed time: {0}", timer.Elapsed);

                // use when debuging. print stdout and stderr
                // client.PrintTaskOutput(batchClient);

                // Clean up Batch resources (if the user so chooses)
                Console.WriteLine();
                Console.Write("Delete job? [yes] no: ");
                string response = Console.ReadLine().ToLower();
                if (response != "n" && response != "no")
                {
                    await batchClient.JobOperations.DeleteJobAsync(client.jobId);
                }
            }
        }

        private void CompileQuery()
        {
            GraphViewConnection connection = new GraphViewConnection(
                this.docDBEndPoint, this.docDBKey, this.docDBDatabaseId, this.docDBCollectionId,
                GraphType.GraphAPIOnly, this.useReverseEdge, this.spilledEdgeThresholdViagraphAPI, this.partitionByKey);
            GraphViewCommand command = new GraphViewCommand(connection);

            GraphTraversal traversal = command.g().V();

            traversal.CompileQuery();
        }

        private void MakePartitionPlan()
        {
            List<PartitionPlan> plans = new List<PartitionPlan>();
            // temporary solution, very simple. just can run very simple query like g.V().out()

            if (this.parallelism == 1)
            {
                plans.Add(new PartitionPlan("name", PartitionMethod.CompareFirstChar,
                    new Tuple<string, string, PartitionBetweenType>("a", "z", PartitionBetweenType.IncludeBoth)));
            }

            string left = "a";
            string right;
            Debug.Assert(this.parallelism <= 26);
            int span = 26 / this.parallelism;
            for (int i = 0; i < this.parallelism; i++)
            {
                PartitionPlan plan;
                if (i == this.parallelism - 1)
                {
                    plan = new PartitionPlan("name", PartitionMethod.CompareFirstChar,
                        new Tuple<string, string, PartitionBetweenType>(left, "z", PartitionBetweenType.IncludeBoth));
                    plans.Add(plan);
                    break;
                }

                right = "" + (char)(left.First() + span);
                plan = new PartitionPlan("name", PartitionMethod.CompareFirstChar, 
                    new Tuple<string, string, PartitionBetweenType>(left, right, PartitionBetweenType.IncludeLeft));
                plans.Add(plan);
                left = right;
            }

            GraphViewSerializer.SerializePatitionPlan(plans);
        }

        private void PrintTaskOutput(BatchClient batchClient)
        {
            for (int i = 0; i < this.parallelism; i++)
            {
                CloudTask task = batchClient.JobOperations.GetTask(this.jobId, i.ToString());
                string stdOut = task.GetNodeFile(Constants.StandardOutFileName).ReadAsString();
                string stdErr = task.GetNodeFile(Constants.StandardErrorFileName).ReadAsString();
                Console.WriteLine("---- stdout.txt ----taskId: " + i);
                Console.WriteLine(stdOut);
                Console.WriteLine("---- stderr.txt ----taskId: " + i);
                Console.WriteLine(stdErr);
                Console.WriteLine("------------------------------------");
            }
        }

        /// <summary>
        /// Creates a container with the specified name in Blob storage, unless a container with that name already exists.
        /// </summary>
        /// <param name="blobClient">A <see cref="Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient"/>.</param>
        /// <param name="containerName">The name for the new container.</param>
        /// <returns>A <see cref="System.Threading.Tasks.Task"/> object that represents the asynchronous operation.</returns>
        private static async Task CreateContainerIfNotExistAsync(CloudBlobClient blobClient, string containerName)
        {
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            if (await container.CreateIfNotExistsAsync())
            {
                Console.WriteLine("Container [{0}] created.", containerName);
            }
            else
            {
                Console.WriteLine("Container [{0}] exists, skipping creation.", containerName);
            }
        }

        /// <summary>
        /// Returns a shared access signature (SAS) URL providing the specified permissions to the specified container.
        /// </summary>
        /// <param name="blobClient">A <see cref="Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient"/>.</param>
        /// <param name="containerName">The name of the container for which a SAS URL should be obtained.</param>
        /// <param name="permissions">The permissions granted by the SAS URL.</param>
        /// <returns>A SAS URL providing the specified access to the container.</returns>
        /// <remarks>The SAS URL provided is valid for 2 hours from the time this method is called. The container must
        /// already exist within Azure Storage.</remarks>
        private static string GetContainerSasUrl(CloudBlobClient blobClient, string containerName, SharedAccessBlobPermissions permissions)
        {
            // Set the expiry time and permissions for the container access signature. In this case, no start time is specified,
            // so the shared access signature becomes valid immediately
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = permissions
            };
            
            // Generate the shared access signature on the container, setting the constraints directly on the signature
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            string sasContainerToken = container.GetSharedAccessSignature(sasConstraints);

            // Return the URL string for the container, including the SAS token
            return String.Format("{0}{1}", container.Uri, sasContainerToken);
        }

        /// <summary>
        /// Uploads the specified files to the specified Blob container, returning a corresponding
        /// collection of <see cref="ResourceFile"/> objects appropriate for assigning to a task's
        /// <see cref="CloudTask.ResourceFiles"/> property.
        /// </summary>
        /// <param name="blobClient">A <see cref="Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient"/>.</param>
        /// <param name="inputContainerName">The name of the blob storage container to which the files should be uploaded.</param>
        /// <param name="filePaths">A collection of paths of the files to be uploaded to the container.</param>
        /// <returns>A collection of <see cref="ResourceFile"/> objects.</returns>
        private static async Task<List<ResourceFile>> UploadFilesToContainerAsync(CloudBlobClient blobClient, string inputContainerName, List<string> filePaths)
        {
            List<ResourceFile> resourceFiles = new List<ResourceFile>();

            foreach (string filePath in filePaths)
            {
                resourceFiles.Add(await UploadFileToContainerAsync(blobClient, inputContainerName, filePath));
            }

            return resourceFiles;
        }

        /// <summary>
        /// Uploads the specified file to the specified Blob container.
        /// </summary>
        /// <param name="filePath">The full path to the file to upload to Storage.</param>
        /// <param name="blobClient">A <see cref="Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient"/>.</param>
        /// <param name="containerName">The name of the blob storage container to which the file should be uploaded.</param>
        /// <returns>A <see cref="Microsoft.Azure.Batch.ResourceFile"/> instance representing the file within blob storage.</returns>
        private static async Task<ResourceFile> UploadFileToContainerAsync(CloudBlobClient blobClient, string containerName, string filePath)
        {
            Console.WriteLine("Uploading file {0} to container [{1}]...", filePath, containerName);

            string blobName = Path.GetFileName(filePath);

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            CloudBlockBlob blobData = container.GetBlockBlobReference(blobName);
            await blobData.UploadFromFileAsync(filePath);
            
            // Set the expiry time and permissions for the blob shared access signature. In this case, no start time is specified,
            // so the shared access signature becomes valid immediately
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Read
            };

            // Construct the SAS URL for blob
            string sasBlobToken = blobData.GetSharedAccessSignature(sasConstraints);
            string blobSasUri = String.Format("{0}{1}", blobData.Uri, sasBlobToken);

            return new ResourceFile(blobSasUri, blobName);
        }

        /// <summary>
        /// Downloads all files from the specified blob storage container to the specified directory.
        /// </summary>
        /// <param name="blobClient">A <see cref="CloudBlobClient"/>.</param>
        /// <param name="containerName">The name of the blob storage container containing the files to download.</param>
        /// <param name="directoryPath">The full path of the local directory to which the files should be downloaded.</param>
        /// <returns>A <see cref="System.Threading.Tasks.Task"/> object that represents the asynchronous operation.</returns>
        private static async Task DownloadBlobsFromContainerAsync(CloudBlobClient blobClient, string containerName, string directoryPath)
        {
            Console.WriteLine("Downloading all files from container [{0}]...", containerName);

            // Retrieve a reference to a previously created container
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            // Get a flat listing of all the block blobs in the specified container
            foreach (IListBlobItem item in container.ListBlobs(prefix: null, useFlatBlobListing: true))
            {
                // Retrieve reference to the current blob
                CloudBlob blob = (CloudBlob)item;

                // Save blob contents to a file in the specified folder
                string localOutputFile = Path.Combine(directoryPath, blob.Name);
                await blob.DownloadToFileAsync(localOutputFile, FileMode.Create);
            }

            Console.WriteLine("All files downloaded to {0}", directoryPath);
        }

        /// <summary>
        /// Deletes the container with the specified name from Blob storage, unless a container with that name does not exist.
        /// </summary>
        /// <param name="blobClient">A <see cref="Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient"/>.</param>
        /// <param name="containerName">The name of the container to delete.</param>
        /// <returns>A <see cref="System.Threading.Tasks.Task"/> object that represents the asynchronous operation.</returns>
        private static async Task DeleteContainerAsync(CloudBlobClient blobClient, string containerName)
        {
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            if (await container.DeleteIfExistsAsync())
            {
                Console.WriteLine("Container [{0}] deleted.", containerName);
            }
            else
            {
                Console.WriteLine("Container [{0}] does not exist, skipping deletion.", containerName);
            }
        }

        /// <summary>
        /// Processes all exceptions inside an <see cref="AggregateException"/> and writes each inner exception to the console.
        /// </summary>
        /// <param name="aggregateException">The <see cref="AggregateException"/> to process.</param>
        public static void PrintAggregateException(AggregateException aggregateException)
        {
            // Flatten the aggregate and iterate over its inner exceptions, printing each
            foreach (Exception exception in aggregateException.Flatten().InnerExceptions)
            {
                Console.WriteLine(exception.ToString());
                Console.WriteLine();
            }
        }

        /// <summary>
        /// Creates a <see cref="CloudPool"/> with the specified id and configures its StartTask with the
        /// specified <see cref="ResourceFile"/> collection.
        /// </summary>
        /// <param name="batchClient">A <see cref="BatchClient"/>.</param>
        /// <param name="poolId">The id of the <see cref="CloudPool"/> to create.</param>
        /// <param name="resourceFiles">A collection of <see cref="ResourceFile"/> objects representing blobs within
        /// a Storage account container. The StartTask will download these files from Storage prior to execution.</param>
        /// <returns>A <see cref="System.Threading.Tasks.Task"/> object that represents the asynchronous operation.</returns>
        private async Task CreatePoolIfNotExistAsync(BatchClient batchClient, List<ResourceFile> resourceFiles)
        {
            CloudPool pool = null;
            try
            {
                Console.WriteLine("Creating pool [{0}]...", poolId);

                pool = batchClient.PoolOperations.CreatePool(
                    poolId: poolId,
                    targetLowPriorityComputeNodes: 0,
                    targetDedicatedComputeNodes: this.parallelism,
                    virtualMachineSize: "small",
                    cloudServiceConfiguration: new CloudServiceConfiguration(osFamily: "4"));   // Windows Server 2012 R2

                pool.InterComputeNodeCommunicationEnabled = true;
                pool.MaxTasksPerComputeNode = 1;

                await pool.CommitAsync();
            }
            catch (BatchException be)
            {
                // Swallow the specific error code PoolExists since that is expected if the pool already exists
                if (be.RequestInformation?.BatchError != null && be.RequestInformation.BatchError.Code == BatchErrorCodeStrings.PoolExists)
                {
                    Console.WriteLine("The pool {0} already existed when we tried to create it", poolId);
                    pool = batchClient.PoolOperations.GetPool(poolId);
                    Console.WriteLine("TargetDedicatedComputeNodes: " + pool.TargetDedicatedComputeNodes);
                    Console.WriteLine("TargetLowPriorityComputeNodes :" + pool.TargetLowPriorityComputeNodes);
                }
                else
                {
                    throw; // Any other exception is unexpected
                }
            }
        }

        /// <summary>
        /// Creates a job in the specified pool.
        /// </summary>
        /// <param name="batchClient">A <see cref="BatchClient"/>.</param>
        /// <param name="jobId">The id of the job to be created.</param>
        /// <param name="poolId">The id of the <see cref="CloudPool"/> in which to create the job.</param>
        /// <returns>A <see cref="System.Threading.Tasks.Task"/> object that represents the asynchronous operation.</returns>
        private async Task CreateJobAsync(BatchClient batchClient)
        {
            Console.WriteLine("Creating job [{0}]...", this.jobId);

            CloudJob job = batchClient.JobOperations.CreateJob();
            job.Id = this.jobId;
            job.PoolInformation = new PoolInformation { PoolId = this.poolId };

            await job.CommitAsync();
        }

        /// <summary>
        /// Creates tasks to process each of the specified input files, and submits them to the
        /// specified job for execution.
        /// </summary>
        /// <param name="batchClient">A <see cref="BatchClient"/>.</param>
        /// <param name="jobId">The id of the job to which the tasks should be added.</param>
        /// <param name="applicationFiles">A collection of <see cref="ResourceFile"/> objects representing the program 
        /// (with dependencies and serialization data) to be executed on the compute nodes.</param>
        /// <param name="outputContainerSasUrl">The shared access signature URL for the container within Azure Storage that
        /// will receive the output files created by the tasks.</param>
        /// <returns>A collection of the submitted tasks.</returns>
        private async Task<List<CloudTask>> AddTasksAsync(BatchClient batchClient, List<ResourceFile> applicationFiles, string outputContainerSasUrl)
        {
            Console.WriteLine("Adding task to job [{0}]...", jobId);

            // Create a collection to hold the tasks that we'll be adding to the job
            List<CloudTask> tasks = new List<CloudTask>();

            for (int i = 0; i < this.parallelism; i++)
            {
                string taskCommandLine = $"cmd /c %AZ_BATCH_TASK_WORKING_DIR%\\{this.exeName} \"{outputContainerSasUrl}\"";
                CloudTask task = new CloudTask(i.ToString(), taskCommandLine);
                task.ResourceFiles = new List<ResourceFile>(applicationFiles);

                tasks.Add(task);
            }

            // Add the tasks as a collection opposed to a separate AddTask call for each. Bulk task submission
            // helps to ensure efficient underlying API calls to the Batch service.
            await batchClient.JobOperations.AddTaskAsync(jobId, tasks);

            return tasks;
        }

        private async Task DownloadAndAggregateOutputAsync(CloudBlobClient blobClient, string containerName)
        {
            Console.WriteLine("Downloading all files from container [{0}]...", containerName);

            // Retrieve a reference to a previously created container
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            string outputFile = Path.Combine(this.outputPath, $"output-{this.jobId}");
            // if file exists, clear it; otherwise create a empty file.
            File.WriteAllText(outputFile, String.Empty);
            using (StreamWriter file = new StreamWriter(outputFile))
            {
                // Get a flat listing of all the block blobs in the specified container
                foreach (IListBlobItem item in container.ListBlobs(prefix: null, useFlatBlobListing: true))
                {
                    // Retrieve reference to the current blob
                    CloudBlob blob = (CloudBlob)item;

                    // Save blob contents to a file in the specified folder
                    string localOutputFile = Path.Combine(this.outputPath, blob.Name);
                    await blob.DownloadToFileAsync(localOutputFile, FileMode.Create);

                    // write result to aggregate file
                    string text = File.ReadAllText(localOutputFile);
                    file.Write(text);
                }
            }

            Console.WriteLine("All files downloaded to {0}", this.outputPath);
            Console.WriteLine("Aggregation File create in {0}. The content is as follows:", outputFile);
            Console.Write(File.ReadAllText(outputFile));
        }

        /// <summary>
        /// Monitors the specified tasks for completion and returns a value indicating whether all tasks completed successfully
        /// within the timeout period.
        /// </summary>
        /// <param name="batchClient">A <see cref="BatchClient"/>.</param>
        /// <param name="jobId">The id of the job containing the tasks that should be monitored.</param>
        /// <param name="timeout">The period of time to wait for the tasks to reach the completed state.</param>
        /// <returns><c>true</c> if all tasks in the specified job completed with an exit code of 0 within the specified timeout period, otherwise <c>false</c>.</returns>
        private static async Task<bool> MonitorTasks(BatchClient batchClient, string jobId, TimeSpan timeout)
        {
            bool allTasksSuccessful = true;
            const string successMessage = "All tasks reached state Completed.";
            const string failureMessage = "One or more tasks failed to reach the Completed state within the timeout period.";

            // Obtain the collection of tasks currently managed by the job. Note that we use a detail level to
            // specify that only the "id" property of each task should be populated. Using a detail level for
            // all list operations helps to lower response time from the Batch service.
            ODATADetailLevel detail = new ODATADetailLevel(selectClause: "id");
            List<CloudTask> tasks = await batchClient.JobOperations.ListTasks(jobId, detail).ToListAsync();

            Console.WriteLine("Awaiting task completion, timeout in {0}...", timeout.ToString());

            // We use a TaskStateMonitor to monitor the state of our tasks. In this case, we will wait for all tasks to
            // reach the Completed state.
            TaskStateMonitor taskStateMonitor = batchClient.Utilities.CreateTaskStateMonitor();
            try
            {
                await taskStateMonitor.WhenAll(tasks, TaskState.Completed, timeout);
            }
            catch (TimeoutException)
            {
                Console.WriteLine(failureMessage);
                await batchClient.JobOperations.TerminateJobAsync(jobId, failureMessage);
                return false;
            }

            await batchClient.JobOperations.TerminateJobAsync(jobId, successMessage);

            // All tasks have reached the "Completed" state, however, this does not guarantee all tasks completed successfully.
            // Here we further check each task's ExecutionInfo property to ensure that it did not encounter a scheduling error
            // or return a non-zero exit code.

            // Update the detail level to populate only the task id and executionInfo properties.
            // We refresh the tasks below, and need only this information for each task.
            detail.SelectClause = "id, executionInfo";

            foreach (CloudTask task in tasks)
            {
                // Populate the task's properties with the latest info from the Batch service
                await task.RefreshAsync(detail);

                if (task.ExecutionInformation.Result == TaskExecutionResult.Failure)
                {
                    // A task with failure information set indicates there was a problem with the task. It is important to note that
                    // the task's state can be "Completed," yet still have encountered a failure.

                    allTasksSuccessful = false;

                    Console.WriteLine("WARNING: Task [{0}] encountered a failure: {1}", task.Id, task.ExecutionInformation.FailureInformation.Message);
                    if (task.ExecutionInformation.ExitCode != 0)
                    {
                        // A non-zero exit code may indicate that the application executed by the task encountered an error
                        // during execution. As not every application returns non-zero on failure by default (e.g. robocopy),
                        // your implementation of error checking may differ from this example.

                        Console.WriteLine("WARNING: Task [{0}] returned a non-zero exit code - this may indicate task execution or completion failure.", task.Id);
                    }
                }
            }

            if (allTasksSuccessful)
            {
                Console.WriteLine("Success! All tasks completed successfully within the specified timeout period.");
            }

            return allTasksSuccessful;
        }

        private void DeletePool()
        {
            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(this.batchAccountUrl, this.batchAccountName, this.batchAccountKey);
            using (BatchClient batchClient = BatchClient.Open(cred))
            {
                batchClient.PoolOperations.DeletePool(this.poolId);
            }
        }

        private void DeleteJob()
        {
            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(this.batchAccountUrl, this.batchAccountName, this.batchAccountKey);
            using (BatchClient batchClient = BatchClient.Open(cred))
            {
                batchClient.JobOperations.DeleteJob(this.jobId);
            }
        }

        private void DeletePoolAndJob()
        {
            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(this.batchAccountUrl, this.batchAccountName, this.batchAccountKey);
            using (BatchClient batchClient = BatchClient.Open(cred))
            {
                batchClient.JobOperations.DeleteJob(this.jobId);
                batchClient.PoolOperations.DeletePool(this.poolId);
            }
        }
    }
}
