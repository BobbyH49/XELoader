namespace XELoader
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine($"\r\nStarted at {DateTimeOffset.Now}!");

            // Populate a dictionary with all of the parameters
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].StartsWith("-") && i + 1 < args.Length)
                {
                    string paramName = args[i].Substring(1); // Remove the leading '-'
                    string paramValue = args[i + 1];
                    parameters[paramName] = paramValue;
                }
            }

            // Create a new XEReader object with all of the parameters provided
            XEReader xer = new XEReader(parameters);

            Console.WriteLine("\r\nThe following parameters have been set!");
            Console.WriteLine($"SqlInstanceName: {xer.SqlInstanceName}");
            Console.WriteLine($"DatabaseName: {xer.DatabaseName}");
            Console.WriteLine($"Authentication: {xer.Authentication}");
            Console.WriteLine($"TrustCertificate: {xer.TrustCertificate.ToString()}");
            Console.WriteLine($"Username: {xer.Username}");
            Console.WriteLine($"SchemaName: {xer.SchemaName}");
            Console.WriteLine($"TableSuffix: {xer.TableSuffix}");
            Console.WriteLine($"XEFolder: {xer.XEFolder}");
            Console.WriteLine($"XEFilePattern: {xer.XEFilePattern}");
            Console.WriteLine($"StartTime: {xer.StartTime.ToString()}");
            Console.WriteLine($"EndTime: {xer.EndTime.ToString()}");
            Console.WriteLine($"TimeZoneOffset: {xer.TimeZoneOffset}");
            Console.WriteLine($"StartTimeOffset: {xer.StartTimeOffset.ToString()}");
            Console.WriteLine($"EndTimeOffset: {xer.EndTimeOffset.ToString()}");
            Console.WriteLine($"BatchSize: {xer.BatchSize.ToString()}");
            Console.WriteLine($"ParallelThreads: {xer.ParallelThreads.ToString()}");

            string sqlCommand;
            string successMessage;
            string failedMessage;
            Int16 result = 0;
            Int32 commandTimeout = 60;

            // Create the schema
            sqlCommand = $"CREATE SCHEMA {xer.SchemaName};";
            successMessage = $"\r\nSchema {xer.SchemaName} created successfully!";
            failedMessage = $"Failed to create schema {xer.SchemaName}!";
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Create the main batches table
            sqlCommand = $"CREATE TABLE {xer.FullBatchesTableName} (\r\n\tEventSequence bigint\r\n\t, EventType nvarchar(20)\r\n\t, Timestamp datetimeoffset\r\n\t, ClientHostname nvarchar(500)\r\n\t, ClientAppName nvarchar(1000)\r\n\t, DatabaseName nvarchar(200)\r\n\t, TextDataHashId int\r\n\t, TextData nvarchar(max)\r\n\t, NormTextHashId int\r\n\t, NormText nvarchar(max)\r\n\t, Successful tinyint\r\n\t, Failed tinyint\r\n\t, Aborted tinyint\r\n\t, Duration bigint\r\n\t, CpuTime bigint\r\n\t, LogicalReads bigint\r\n\t, PhysicalReads bigint\r\n\t, Writes bigint\r\n\t, [Rowcount] bigint\r\n\t, constraint PKC_{xer.BatchesTableName}\r\n\t\tprimary key clustered (EventSequence asc)\r\n);";
            successMessage = $"\r\nTable {xer.FullBatchesTableName} created successfully!";
            failedMessage = $"Failed to create table {xer.FullBatchesTableName}!";
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Load events into the batches table
            result = xer.LoadEvents(xer);

            // Create an index to be used to populate the unique batches table
            sqlCommand = $"CREATE NONCLUSTERED INDEX IX_{xer.BatchesTableName}_NormTextHashId ON {xer.FullBatchesTableName} (NormTextHashId ASC);";
            successMessage = $"\r\nNonclustered index IX_{xer.BatchesTableName}_NormTextHashId created successfully on {xer.FullBatchesTableName}!";
            failedMessage = $"Failed to create nonclustered index IX_{xer.BatchesTableName}_NormTextHashId on {xer.FullBatchesTableName}!";
            commandTimeout = 300;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Create the unique batches table 
            sqlCommand = $"CREATE TABLE {xer.FullUniqueBatchesTableName} (\r\n\tNormTextHashId int\r\n\t, OrigText nvarchar(max)\r\n\t, NormText nvarchar(max)\r\n\t, constraint PKC_{xer.UniqueBatchesTableName}\r\n\t\tprimary key clustered (NormTextHashId asc)\r\n);";
            successMessage = $"\r\nTable {xer.FullUniqueBatchesTableName} created successfully!";
            failedMessage = $"Failed to create table {xer.FullUniqueBatchesTableName}!";
            commandTimeout = 60;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Load the unique batches table
            sqlCommand = $"DECLARE @EventSequences TABLE (EventSequence BIGINT PRIMARY KEY);\r\n\r\nINSERT INTO @EventSequences\r\nSELECT EventSequence = MIN(EventSequence)\r\nFROM {xer.FullBatchesTableName}\r\nGROUP BY NormTextHashId;\r\n\r\nINSERT INTO {xer.FullUniqueBatchesTableName} (NormTextHashId, OrigText, NormText)\r\nSELECT NormTextHashId, OrigText = TextData, NormText\r\nFROM {xer.FullBatchesTableName}\r\nWHERE EventSequence IN (\r\n\tSELECT EventSequence  FROM @EventSequences\r\n);";
            successMessage = $"\r\nPopulated {xer.FullUniqueBatchesTableName} successfully!";
            failedMessage = $"Failed to populate {xer.FullUniqueBatchesTableName}!";
            commandTimeout = 300;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Create an index to be used to populate the summary tables
            sqlCommand = $"CREATE NONCLUSTERED COLUMNSTORE INDEX NCI_{xer.BatchesTableName} ON {xer.FullBatchesTableName} (NormTextHashId, TextDataHashId, Successful, Failed, Aborted, Duration, CpuTime, LogicalReads, PhysicalReads, Writes, [Rowcount]);";
            successMessage = $"\r\nNonclustered columnstore index NCI_{xer.BatchesTableName} created successfully on {xer.FullBatchesTableName}!";
            failedMessage = $"Failed to create nonclustered columnstore index NCI_{xer.BatchesTableName} on {xer.FullBatchesTableName}!";
            commandTimeout = 600;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Create the summary table
            sqlCommand = $"CREATE TABLE {xer.FullBatchSummaryTableName} (\r\n\tNormTextHashId INT\r\n\t, AllExecutions INT\r\n\t, DistinctExecutions INT\r\n\t, SuccessfulExecutions INT\r\n\t, FailedExecutions INT\r\n\t, AbortedExecutions INT\r\n\t, CONSTRAINT PKC_{xer.BatchSummaryTableName}\r\n\t\tPRIMARY KEY CLUSTERED (NormTextHashId ASC)\r\n);";
            successMessage = $"\r\nTable {xer.BatchSummaryTableName} created successfully!";
            failedMessage = $"Failed to create table {xer.BatchSummaryTableName}!";
            commandTimeout = 60;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Load the Summary table
            sqlCommand = $"INSERT INTO {xer.FullBatchSummaryTableName} (NormTextHashId, AllExecutions, DistinctExecutions, SuccessfulExecutions, FailedExecutions, AbortedExecutions)\r\nSELECT\r\n\tNormTextHashId\r\n\t, AllExecutions = COUNT(*)\r\n\t, DistinctExecutions = COUNT(DISTINCT TextDataHashId)\r\n\t, SuccessfulExecutions = SUM(Successful)\r\n\t, FailedExecutions = SUM(Failed)\r\n\t, AbortedExecutions = SUM(Aborted)\r\nFROM {xer.FullBatchesTableName}\r\nGROUP BY\r\n\tNormTextHashId;";
            successMessage = $"\r\nPopulated {xer.BatchSummaryTableName} successfully!";
            failedMessage = $"Failed to populate {xer.BatchSummaryTableName}!";
            commandTimeout = 600;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Create the summary table for Duration
            sqlCommand = $"CREATE TABLE {xer.FullBatchDurationSummaryTableName} (\r\n\tNormTextHashId INT\r\n\t, MinDuration BIGINT\r\n\t, MeanDuration BIGINT\r\n\t, MaxDuration BIGINT\r\n\t, TotalDuration BIGINT\r\n\t, FirstQuartileDuration BIGINT\r\n\t, MedianDuration BIGINT\r\n\t, ThirdQuartileDuration BIGINT\r\n\t, Percentile90Duration BIGINT\r\n\t, Percentile95Duration BIGINT\r\n\t, Percentile99Duration BIGINT\r\n\t, CONSTRAINT PKC_{xer.BatchDurationSummaryTableName}\r\n\t\tPRIMARY KEY CLUSTERED (NormTextHashId ASC)\r\n);";
            successMessage = $"\r\nTable {xer.BatchDurationSummaryTableName} created successfully!";
            failedMessage = $"Failed to create table {xer.BatchDurationSummaryTableName}!";
            commandTimeout = 60;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Load the Duration Summary table
            sqlCommand = $"INSERT INTO {xer.FullBatchDurationSummaryTableName} (NormTextHashId, MinDuration, MeanDuration, MaxDuration, TotalDuration, FirstQuartileDuration, MedianDuration, ThirdQuartileDuration, Percentile90Duration, Percentile95Duration, Percentile99Duration)\r\nSELECT\r\n\tNormTextHashId\r\n\t, MinDuration = MIN(Duration)\r\n\t, MeanDuration = AVG(Duration)\r\n\t, MaxDuration = MAX(Duration)\r\n\t, TotalDuration = SUM(Duration)\r\n\t, FirstQuartileDuration = CASE WHEN MAX(ExecutionCount) <= 3 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.25 THEN Duration ELSE 0 END) END\r\n\t, MedianDuration = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.5 THEN Duration ELSE 0 END) END\r\n\t, ThirdQuartileDuration = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.75 THEN Duration ELSE 0 END) END\r\n\t, Percentile90Duration = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.9 THEN Duration ELSE 0 END) END\r\n\t, Percentile95Duration = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.95 THEN Duration ELSE 0 END) END\r\n\t, Percentile99Duration = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.99 THEN Duration ELSE 0 END) END\r\nFROM (\r\n\tSELECT\r\n\t\tNormTextHashId\r\n\t\t, Duration\r\n\t\t, DurationExecutionNumber = ROW_NUMBER() OVER(PARTITION BY NormTextHashId ORDER BY Duration ASC)\r\n\t\t, ExecutionCount = COUNT(*) OVER(PARTITION BY NormTextHashId)\r\n\tFROM {xer.FullBatchesTableName}\r\n) SummaryData\r\nGROUP BY NormTextHashId\r\nORDER BY NormTextHashId;";
            successMessage = $"\r\nPopulated {xer.BatchDurationSummaryTableName} successfully!";
            failedMessage = $"Failed to populate {xer.BatchDurationSummaryTableName}!";
            commandTimeout = 600;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Create the summary table for CpuTime
            sqlCommand = $"CREATE TABLE {xer.FullBatchCpuTimeSummaryTableName} (\r\n\tNormTextHashId INT\r\n\t, MinCpuTime BIGINT\r\n\t, MeanCpuTime BIGINT\r\n\t, MaxCpuTime BIGINT\r\n\t, TotalCpuTime BIGINT\r\n\t, FirstQuartileCpuTime BIGINT\r\n\t, MedianCpuTime BIGINT\r\n\t, ThirdQuartileCpuTime BIGINT\r\n\t, Percentile90CpuTime BIGINT\r\n\t, Percentile95CpuTime BIGINT\r\n\t, Percentile99CpuTime BIGINT\r\n\t, CONSTRAINT PKC_{xer.BatchCpuTimeSummaryTableName}\r\n\t\tPRIMARY KEY CLUSTERED (NormTextHashId ASC)\r\n);";
            successMessage = $"\r\nTable {xer.BatchCpuTimeSummaryTableName} created successfully!";
            failedMessage = $"Failed to create table {xer.BatchCpuTimeSummaryTableName}!";
            commandTimeout = 60;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Load the CpuTime Summary table
            sqlCommand = $"INSERT INTO {xer.FullBatchCpuTimeSummaryTableName} (NormTextHashId, MinCpuTime, MeanCpuTime, MaxCpuTime, TotalCpuTime, FirstQuartileCpuTime, MedianCpuTime, ThirdQuartileCpuTime, Percentile90CpuTime, Percentile95CpuTime, Percentile99CpuTime)\r\nSELECT\r\n\tNormTextHashId\r\n\t, MinCpuTime = MIN(CpuTime)\r\n\t, MeanCpuTime = AVG(CpuTime)\r\n\t, MaxCpuTime = MAX(CpuTime)\r\n\t, TotalCpuTime = SUM(CpuTime)\r\n\t, FirstQuartileCpuTime = CASE WHEN MAX(ExecutionCount) <= 3 THEN MIN(CpuTime) ELSE MAX(CASE WHEN CpuTimeExecutionNumber*1.0/ExecutionCount <= 0.25 THEN CpuTime ELSE 0 END) END\r\n\t, MedianCpuTime = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(CpuTime) ELSE MAX(CASE WHEN CpuTimeExecutionNumber*1.0/ExecutionCount <= 0.5 THEN CpuTime ELSE 0 END) END\r\n\t, ThirdQuartileCpuTime = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(CpuTime) ELSE MAX(CASE WHEN CpuTimeExecutionNumber*1.0/ExecutionCount <= 0.75 THEN CpuTime ELSE 0 END) END\r\n\t, Percentile90CpuTime = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(CpuTime) ELSE MAX(CASE WHEN CpuTimeExecutionNumber*1.0/ExecutionCount <= 0.9 THEN CpuTime ELSE 0 END) END\r\n\t, Percentile95CpuTime = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(CpuTime) ELSE MAX(CASE WHEN CpuTimeExecutionNumber*1.0/ExecutionCount <= 0.95 THEN CpuTime ELSE 0 END) END\r\n\t, Percentile99CpuTime = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(CpuTime) ELSE MAX(CASE WHEN CpuTimeExecutionNumber*1.0/ExecutionCount <= 0.99 THEN CpuTime ELSE 0 END) END\r\nFROM (\r\n\tSELECT\r\n\t\tNormTextHashId\r\n\t\t, CpuTime\r\n\t\t, CpuTimeExecutionNumber = ROW_NUMBER() OVER(PARTITION BY NormTextHashId ORDER BY CpuTime ASC)\r\n\t\t, ExecutionCount = COUNT(*) OVER(PARTITION BY NormTextHashId)\r\n\tFROM {xer.FullBatchesTableName}\r\n) SummaryData\r\nGROUP BY NormTextHashId\r\nORDER BY NormTextHashId;";
            successMessage = $"\r\nPopulated {xer.BatchCpuTimeSummaryTableName} successfully!";
            failedMessage = $"Failed to populate {xer.BatchCpuTimeSummaryTableName}!";
            commandTimeout = 600;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Create the summary table for LogicalReads
            sqlCommand = $"CREATE TABLE {xer.FullBatchLogicalReadsSummaryTableName} (\r\n\tNormTextHashId INT\r\n\t, MinLogicalReads BIGINT\r\n\t, MeanLogicalReads BIGINT\r\n\t, MaxLogicalReads BIGINT\r\n\t, TotalLogicalReads BIGINT\r\n\t, FirstQuartileLogicalReads BIGINT\r\n\t, MedianLogicalReads BIGINT\r\n\t, ThirdQuartileLogicalReads BIGINT\r\n\t, Percentile90LogicalReads BIGINT\r\n\t, Percentile95LogicalReads BIGINT\r\n\t, Percentile99LogicalReads BIGINT\r\n\t, CONSTRAINT PKC_{xer.BatchLogicalReadsSummaryTableName}\r\n\t\tPRIMARY KEY CLUSTERED (NormTextHashId ASC)\r\n);";
            successMessage = $"\r\nTable {xer.BatchLogicalReadsSummaryTableName} created successfully!";
            failedMessage = $"Failed to create table {xer.BatchLogicalReadsSummaryTableName}!";
            commandTimeout = 60;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Load the LogicalReads Summary table
            sqlCommand = $"INSERT INTO {xer.FullBatchLogicalReadsSummaryTableName} (NormTextHashId, MinLogicalReads, MeanLogicalReads, MaxLogicalReads, TotalLogicalReads, FirstQuartileLogicalReads, MedianLogicalReads, ThirdQuartileLogicalReads, Percentile90LogicalReads, Percentile95LogicalReads, Percentile99LogicalReads)\r\nSELECT\r\n\tNormTextHashId\r\n\t, MinLogicalReads = MIN(LogicalReads)\r\n\t, MeanLogicalReads = AVG(LogicalReads)\r\n\t, MaxLogicalReads = MAX(LogicalReads)\r\n\t, TotalLogicalReads = SUM(LogicalReads)\r\n\t, FirstQuartileLogicalReads = CASE WHEN MAX(ExecutionCount) <= 3 THEN MIN(LogicalReads) ELSE MAX(CASE WHEN LogicalReadsExecutionNumber*1.0/ExecutionCount <= 0.25 THEN LogicalReads ELSE 0 END) END\r\n\t, MedianLogicalReads = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(LogicalReads) ELSE MAX(CASE WHEN LogicalReadsExecutionNumber*1.0/ExecutionCount <= 0.5 THEN LogicalReads ELSE 0 END) END\r\n\t, ThirdQuartileLogicalReads = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(LogicalReads) ELSE MAX(CASE WHEN LogicalReadsExecutionNumber*1.0/ExecutionCount <= 0.75 THEN LogicalReads ELSE 0 END) END\r\n\t, Percentile90LogicalReads = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(LogicalReads) ELSE MAX(CASE WHEN LogicalReadsExecutionNumber*1.0/ExecutionCount <= 0.9 THEN LogicalReads ELSE 0 END) END\r\n\t, Percentile95LogicalReads = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(LogicalReads) ELSE MAX(CASE WHEN LogicalReadsExecutionNumber*1.0/ExecutionCount <= 0.95 THEN LogicalReads ELSE 0 END) END\r\n\t, Percentile99LogicalReads = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(LogicalReads) ELSE MAX(CASE WHEN LogicalReadsExecutionNumber*1.0/ExecutionCount <= 0.99 THEN LogicalReads ELSE 0 END) END\r\nFROM (\r\n\tSELECT\r\n\t\tNormTextHashId\r\n\t\t, LogicalReads\r\n\t\t, LogicalReadsExecutionNumber = ROW_NUMBER() OVER(PARTITION BY NormTextHashId ORDER BY LogicalReads ASC)\r\n\t\t, ExecutionCount = COUNT(*) OVER(PARTITION BY NormTextHashId)\r\n\tFROM {xer.FullBatchesTableName}\r\n) SummaryData\r\nGROUP BY NormTextHashId\r\nORDER BY NormTextHashId;";
            successMessage = $"\r\nPopulated {xer.BatchLogicalReadsSummaryTableName} successfully!";
            failedMessage = $"Failed to populate {xer.BatchLogicalReadsSummaryTableName}!";
            commandTimeout = 600;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Create the summary table for PhysicalReads
            sqlCommand = $"CREATE TABLE {xer.FullBatchPhysicalReadsSummaryTableName} (\r\n\tNormTextHashId INT\r\n\t, MinPhysicalReads BIGINT\r\n\t, MeanPhysicalReads BIGINT\r\n\t, MaxPhysicalReads BIGINT\r\n\t, TotalPhysicalReads BIGINT\r\n\t, FirstQuartilePhysicalReads BIGINT\r\n\t, MedianPhysicalReads BIGINT\r\n\t, ThirdQuartilePhysicalReads BIGINT\r\n\t, Percentile90PhysicalReads BIGINT\r\n\t, Percentile95PhysicalReads BIGINT\r\n\t, Percentile99PhysicalReads BIGINT\r\n\t, CONSTRAINT PKC_{xer.BatchPhysicalReadsSummaryTableName}\r\n\t\tPRIMARY KEY CLUSTERED (NormTextHashId ASC)\r\n);";
            successMessage = $"\r\nTable {xer.BatchPhysicalReadsSummaryTableName} created successfully!";
            failedMessage = $"Failed to create table {xer.BatchPhysicalReadsSummaryTableName}!";
            commandTimeout = 60;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Load the PhysicalReads Summary table
            sqlCommand = $"INSERT INTO {xer.FullBatchPhysicalReadsSummaryTableName} (NormTextHashId, MinPhysicalReads, MeanPhysicalReads, MaxPhysicalReads, TotalPhysicalReads, FirstQuartilePhysicalReads, MedianPhysicalReads, ThirdQuartilePhysicalReads, Percentile90PhysicalReads, Percentile95PhysicalReads, Percentile99PhysicalReads)\r\nSELECT\r\n\tNormTextHashId\r\n\t, MinPhysicalReads = MIN(PhysicalReads)\r\n\t, MeanPhysicalReads = AVG(PhysicalReads)\r\n\t, MaxPhysicalReads = MAX(PhysicalReads)\r\n\t, TotalPhysicalReads = SUM(PhysicalReads)\r\n\t, FirstQuartilePhysicalReads = CASE WHEN MAX(ExecutionCount) <= 3 THEN MIN(PhysicalReads) ELSE MAX(CASE WHEN PhysicalReadsExecutionNumber*1.0/ExecutionCount <= 0.25 THEN PhysicalReads ELSE 0 END) END\r\n\t, MedianPhysicalReads = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(PhysicalReads) ELSE MAX(CASE WHEN PhysicalReadsExecutionNumber*1.0/ExecutionCount <= 0.5 THEN PhysicalReads ELSE 0 END) END\r\n\t, ThirdQuartilePhysicalReads = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(PhysicalReads) ELSE MAX(CASE WHEN PhysicalReadsExecutionNumber*1.0/ExecutionCount <= 0.75 THEN PhysicalReads ELSE 0 END) END\r\n\t, Percentile90PhysicalReads = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(PhysicalReads) ELSE MAX(CASE WHEN PhysicalReadsExecutionNumber*1.0/ExecutionCount <= 0.9 THEN PhysicalReads ELSE 0 END) END\r\n\t, Percentile95PhysicalReads = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(PhysicalReads) ELSE MAX(CASE WHEN PhysicalReadsExecutionNumber*1.0/ExecutionCount <= 0.95 THEN PhysicalReads ELSE 0 END) END\r\n\t, Percentile99PhysicalReads = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(PhysicalReads) ELSE MAX(CASE WHEN PhysicalReadsExecutionNumber*1.0/ExecutionCount <= 0.99 THEN PhysicalReads ELSE 0 END) END\r\nFROM (\r\n\tSELECT\r\n\t\tNormTextHashId\r\n\t\t, PhysicalReads\r\n\t\t, PhysicalReadsExecutionNumber = ROW_NUMBER() OVER(PARTITION BY NormTextHashId ORDER BY PhysicalReads ASC)\r\n\t\t, ExecutionCount = COUNT(*) OVER(PARTITION BY NormTextHashId)\r\n\tFROM {xer.FullBatchesTableName}\r\n) SummaryData\r\nGROUP BY NormTextHashId\r\nORDER BY NormTextHashId;";
            successMessage = $"\r\nPopulated {xer.BatchPhysicalReadsSummaryTableName} successfully!";
            failedMessage = $"Failed to populate {xer.BatchPhysicalReadsSummaryTableName}!";
            commandTimeout = 600;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Create the summary table for Writes
            sqlCommand = $"CREATE TABLE {xer.FullBatchWritesSummaryTableName} (\r\n\tNormTextHashId INT\r\n\t, MinWrites BIGINT\r\n\t, MeanWrites BIGINT\r\n\t, MaxWrites BIGINT\r\n\t, TotalWrites BIGINT\r\n\t, FirstQuartileWrites BIGINT\r\n\t, MedianWrites BIGINT\r\n\t, ThirdQuartileWrites BIGINT\r\n\t, Percentile90Writes BIGINT\r\n\t, Percentile95Writes BIGINT\r\n\t, Percentile99Writes BIGINT\r\n\t, CONSTRAINT PKC_{xer.BatchWritesSummaryTableName}\r\n\t\tPRIMARY KEY CLUSTERED (NormTextHashId ASC)\r\n);";
            successMessage = $"\r\nTable {xer.BatchWritesSummaryTableName} created successfully!";
            failedMessage = $"Failed to create table {xer.BatchWritesSummaryTableName}!";
            commandTimeout = 60;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Load the Writes Summary table
            sqlCommand = $"INSERT INTO {xer.FullBatchWritesSummaryTableName} (NormTextHashId, MinWrites, MeanWrites, MaxWrites, TotalWrites, FirstQuartileWrites, MedianWrites, ThirdQuartileWrites, Percentile90Writes, Percentile95Writes, Percentile99Writes)\r\nSELECT\r\n\tNormTextHashId\r\n\t, MinWrites = MIN(Writes)\r\n\t, MeanWrites = AVG(Writes)\r\n\t, MaxWrites = MAX(Writes)\r\n\t, TotalWrites = SUM(Writes)\r\n\t, FirstQuartileWrites = CASE WHEN MAX(ExecutionCount) <= 3 THEN MIN(Writes) ELSE MAX(CASE WHEN WritesExecutionNumber*1.0/ExecutionCount <= 0.25 THEN Writes ELSE 0 END) END\r\n\t, MedianWrites = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Writes) ELSE MAX(CASE WHEN WritesExecutionNumber*1.0/ExecutionCount <= 0.5 THEN Writes ELSE 0 END) END\r\n\t, ThirdQuartileWrites = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Writes) ELSE MAX(CASE WHEN WritesExecutionNumber*1.0/ExecutionCount <= 0.75 THEN Writes ELSE 0 END) END\r\n\t, Percentile90Writes = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Writes) ELSE MAX(CASE WHEN WritesExecutionNumber*1.0/ExecutionCount <= 0.9 THEN Writes ELSE 0 END) END\r\n\t, Percentile95Writes = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Writes) ELSE MAX(CASE WHEN WritesExecutionNumber*1.0/ExecutionCount <= 0.95 THEN Writes ELSE 0 END) END\r\n\t, Percentile99Writes = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Writes) ELSE MAX(CASE WHEN WritesExecutionNumber*1.0/ExecutionCount <= 0.99 THEN Writes ELSE 0 END) END\r\nFROM (\r\n\tSELECT\r\n\t\tNormTextHashId\r\n\t\t, Writes\r\n\t\t, WritesExecutionNumber = ROW_NUMBER() OVER(PARTITION BY NormTextHashId ORDER BY Writes ASC)\r\n\t\t, ExecutionCount = COUNT(*) OVER(PARTITION BY NormTextHashId)\r\n\tFROM {xer.FullBatchesTableName}\r\n) SummaryData\r\nGROUP BY NormTextHashId\r\nORDER BY NormTextHashId;";
            successMessage = $"\r\nPopulated {xer.BatchWritesSummaryTableName} successfully!";
            failedMessage = $"Failed to populate {xer.BatchWritesSummaryTableName}!";
            commandTimeout = 600;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Create the summary table for Rowcount
            sqlCommand = $"CREATE TABLE {xer.FullBatchRowcountSummaryTableName} (\r\n\tNormTextHashId INT\r\n\t, MinRowcount BIGINT\r\n\t, MeanRowcount BIGINT\r\n\t, MaxRowcount BIGINT\r\n\t, TotalRowcount BIGINT\r\n\t, FirstQuartileRowcount BIGINT\r\n\t, MedianRowcount BIGINT\r\n\t, ThirdQuartileRowcount BIGINT\r\n\t, Percentile90Rowcount BIGINT\r\n\t, Percentile95Rowcount BIGINT\r\n\t, Percentile99Rowcount BIGINT\r\n\t, CONSTRAINT PKC_{xer.BatchRowcountSummaryTableName}\r\n\t\tPRIMARY KEY CLUSTERED (NormTextHashId ASC)\r\n);";
            successMessage = $"\r\nTable {xer.BatchRowcountSummaryTableName} created successfully!";
            failedMessage = $"Failed to create table {xer.BatchRowcountSummaryTableName}!";
            commandTimeout = 60;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            // Load the Rowcount Summary table
            sqlCommand = $"INSERT INTO {xer.FullBatchRowcountSummaryTableName} (NormTextHashId, MinRowcount, MeanRowcount, MaxRowcount, TotalRowcount, FirstQuartileRowcount, MedianRowcount, ThirdQuartileRowcount, Percentile90Rowcount, Percentile95Rowcount, Percentile99Rowcount)\r\nSELECT\r\n\tNormTextHashId\r\n\t, MinRowcount = MIN([Rowcount])\r\n\t, MeanRowcount = AVG([Rowcount])\r\n\t, MaxRowcount = MAX([Rowcount])\r\n\t, TotalRowcount = SUM([Rowcount])\r\n\t, FirstQuartileRowcount = CASE WHEN MAX(ExecutionCount) <= 3 THEN MIN([Rowcount]) ELSE MAX(CASE WHEN RowcountExecutionNumber*1.0/ExecutionCount <= 0.25 THEN [Rowcount] ELSE 0 END) END\r\n\t, MedianRowcount = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN([Rowcount]) ELSE MAX(CASE WHEN RowcountExecutionNumber*1.0/ExecutionCount <= 0.5 THEN [Rowcount] ELSE 0 END) END\r\n\t, ThirdQuartileRowcount = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN([Rowcount]) ELSE MAX(CASE WHEN RowcountExecutionNumber*1.0/ExecutionCount <= 0.75 THEN [Rowcount] ELSE 0 END) END\r\n\t, Percentile90Rowcount = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN([Rowcount]) ELSE MAX(CASE WHEN RowcountExecutionNumber*1.0/ExecutionCount <= 0.9 THEN [Rowcount] ELSE 0 END) END\r\n\t, Percentile95Rowcount = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN([Rowcount]) ELSE MAX(CASE WHEN RowcountExecutionNumber*1.0/ExecutionCount <= 0.95 THEN [Rowcount] ELSE 0 END) END\r\n\t, Percentile99Rowcount = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN([Rowcount]) ELSE MAX(CASE WHEN RowcountExecutionNumber*1.0/ExecutionCount <= 0.99 THEN [Rowcount] ELSE 0 END) END\r\nFROM (\r\n\tSELECT\r\n\t\tNormTextHashId\r\n\t\t, [Rowcount]\r\n\t\t, RowcountExecutionNumber = ROW_NUMBER() OVER(PARTITION BY NormTextHashId ORDER BY [Rowcount] ASC)\r\n\t\t, ExecutionCount = COUNT(*) OVER(PARTITION BY NormTextHashId)\r\n\tFROM {xer.FullBatchesTableName}\r\n) SummaryData\r\nGROUP BY NormTextHashId\r\nORDER BY NormTextHashId;";
            successMessage = $"\r\nPopulated {xer.BatchRowcountSummaryTableName} successfully!";
            failedMessage = $"Failed to populate {xer.BatchRowcountSummaryTableName}!";
            commandTimeout = 600;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            Console.WriteLine($"Ended at {DateTimeOffset.Now}!");
        }
    }
}
