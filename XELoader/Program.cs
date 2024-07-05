﻿namespace XELoader
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine($"\r\nStarted at {DateTimeOffset.Now}!");

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
            Console.WriteLine($"BatchSize: {xer.BatchSize.ToString()}");
            Console.WriteLine($"ParallelThreads: {xer.ParallelThreads.ToString()}");

            string sqlCommand;
            string successMessage;
            string failedMessage;
            Int16 result = 0;
            Int32 commandTimeout = 20;

            sqlCommand = $"CREATE SCHEMA {xer.SchemaName};";
            successMessage = $"\r\nSchema {xer.SchemaName} created successfully!";
            failedMessage = $"Failed to create schema {xer.SchemaName}!";
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            sqlCommand = $"CREATE TABLE {xer.FullBatchesTableName} (\r\n\tEventSequence bigint\r\n\t, EventType nvarchar(20)\r\n\t, Timestamp datetimeoffset\r\n\t, ClientHostname nvarchar(500)\r\n\t, ClientAppName nvarchar(1000)\r\n\t, DatabaseName nvarchar(200)\r\n\t, HashId int\r\n\t, TextData nvarchar(max)\r\n\t, NormText nvarchar(max)\r\n\t, Result nvarchar(10)\r\n\t, Duration bigint\r\n\t, CpuTime bigint\r\n\t, LogicalReads bigint\r\n\t, PhysicalReads bigint\r\n\t, Writes bigint\r\n\t, [RowCount] bigint\r\n\t, constraint PK_{xer.BatchesTableName}\r\n\t\tprimary key clustered (EventSequence asc)\r\n);";
            successMessage = $"\r\nTable {xer.FullBatchesTableName} created successfully!";
            failedMessage = $"Failed to create table {xer.FullBatchesTableName}!";
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            sqlCommand = $"CREATE NONCLUSTERED INDEX IX_{xer.BatchesTableName}_HashId ON {xer.FullBatchesTableName} (HashId ASC) INCLUDE (Result, Duration, CpuTime, LogicalReads, PhysicalReads, Writes, [RowCount]);";
            successMessage = $"\r\nNonclustered index IX_{xer.BatchesTableName}_HashId created successfully on {xer.FullBatchesTableName}!";
            failedMessage = $"Failed to create nonclustered index IX_{xer.BatchesTableName}_HashId on {xer.FullBatchesTableName}!";
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            sqlCommand = $"CREATE TABLE {xer.FullUniqueBatchesTableName} (\r\n\tHashId int\r\n\t, OrigText nvarchar(max)\r\n\t, NormText nvarchar(max)\r\n\t, constraint PK_{xer.UniqueBatchesTableName}\r\n\t\tprimary key clustered (HashId asc)\r\n);";
            successMessage = $"\r\nTable {xer.FullUniqueBatchesTableName} created successfully!";
            failedMessage = $"Failed to create table {xer.FullUniqueBatchesTableName}!";
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            sqlCommand = $"CREATE TABLE {xer.FullBatchDurationSummaryTableName} (\r\n\tHashId INT\r\n\t, ExecutionCount INT\r\n\t, SuccessfulExecutions INT\r\n\t, FailedExecutions INT\r\n\t, AbortedExecutions INT\r\n\t, MinDuration BIGINT\r\n\t, MeanDuration BIGINT\r\n\t, MaxDuration BIGINT\r\n\t, TotalDuration BIGINT\r\n\t, FirstQuartileDuration BIGINT\r\n\t, MedianDuration BIGINT\r\n\t, ThirdQuartileDuration BIGINT\r\n\t, Percentile90Duration BIGINT\r\n\t, Percentile95Duration BIGINT\r\n\t, Percentile99Duration BIGINT\r\n\t, CONSTRAINT PK_{xer.BatchDurationSummaryTableName}\r\n\t\tPRIMARY KEY CLUSTERED (HashId ASC)\r\n);";
            successMessage = $"\r\nTable {xer.BatchDurationSummaryTableName} created successfully!";
            failedMessage = $"Failed to create table {xer.BatchDurationSummaryTableName}!";
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            result = xer.LoadEvents(xer);

            sqlCommand = $"INSERT INTO {xer.FullUniqueBatchesTableName} (HashId, OrigText, NormText)\r\nSELECT HashId, OrigText = TextData, NormText\r\nFROM {xer.FullBatchesTableName}\r\nWHERE EventSequence IN (\r\n\tSELECT EventSequence = MIN(EventSequence)\r\n\tFROM XELoader.tblBatches20240522\r\n\tGROUP BY HashId\r\n);";
            successMessage = $"\r\nPopulated {xer.FullUniqueBatchesTableName} successfully!";
            failedMessage = $"Failed to populate {xer.FullUniqueBatchesTableName}!";
            commandTimeout = 300;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            sqlCommand = $"ALTER TABLE {xer.FullBatchesTableName} DROP COLUMN NormText;";
            successMessage = $"\r\nDropped NormText column from {xer.FullBatchesTableName}!";
            failedMessage = $"Failed to drop NormText column from {xer.FullBatchesTableName}!";
            commandTimeout = 20;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

//            sqlCommand = $"INSERT INTO {xer.FullBatchDurationSummaryTableName} (HashId, ExecutionCount, SuccessfulExecutions, FailedExecutions, AbortedExecutions, MinDuration, MeanDuration, MaxDuration, TotalDuration, FirstQuartileDuration, MedianDuration, ThirdQuartileDuration, Percentile90Duration, Percentile95Duration, Percentile99Duration)\r\nSELECT\r\n\tHashId\r\n\t, ExecutionCount = MAX(ExecutionCount)\r\n\t, SuccessfulExecutions = MAX(SuccessfulExecutions)\r\n\t, FailedExecutions = MAX(FailedExecutions)\r\n\t, AbortedExecutions = MAX(AbortedExecutions)\r\n\t, MinDuration = MIN(duration)\r\n\t, MeanDuration = AVG(duration)\r\n\t, MaxDuration = MAX(duration)\r\n\t, TotalDuration = SUM(duration)\r\n\t, FirstQuartileDuration = CASE WHEN MAX(ExecutionCount) <= 3 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.25 THEN Duration ELSE 0 END) END\r\n\t, MedianDuration = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.5 THEN Duration ELSE 0 END) END\r\n\t, ThirdQuartileDuration = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.75 THEN Duration ELSE 0 END) END\r\n\t, Percentile90Duration = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.9 THEN Duration ELSE 0 END) END\r\n\t, Percentile95Duration = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.95 THEN Duration ELSE 0 END) END\r\n\t, Percentile99Duration = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.99 THEN Duration ELSE 0 END) END\r\nFROM (\r\n\tSELECT\r\n\t\tHashId\r\n\t\t, Duration\r\n\t\t, DurationExecutionNumber = ROW_NUMBER() OVER(PARTITION BY HashId ORDER BY Duration ASC)\r\n\t\t, ExecutionCount = COUNT(*) OVER(PARTITION BY HashId)\r\n\t\t, SuccessfulExecutions = SUM(CASE WHEN Result = N'OK' THEN 1 ELSE 0 END) OVER(PARTITION BY HashId)\r\n\t\t, FailedExecutions = SUM(CASE WHEN Result = N'Error' THEN 1 ELSE 0 END) OVER(PARTITION BY HashId)\r\n\t\t, AbortedExecutions = SUM(CASE WHEN Result = N'Abort' THEN 1 ELSE 0 END) OVER(PARTITION BY HashId)\r\n\tFROM XELoader.tblBatches20240522\r\n) SummaryData\r\nGROUP BY HashId\r\nORDER BY HashId;";
            sqlCommand = $"SET NOCOUNT ON;\r\n\r\nDECLARE\r\n\t@BatchSize INT = 20;\r\n\r\nDECLARE\r\n\t@Rowcount INT = @BatchSize\r\n\t, @RowsCompleted INT = 0;\r\n\r\nWHILE @Rowcount = @BatchSize\r\nBEGIN\r\n\tINSERT INTO {xer.FullBatchDurationSummaryTableName} (HashId, ExecutionCount, SuccessfulExecutions, FailedExecutions, AbortedExecutions, MinDuration, MeanDuration, MaxDuration, TotalDuration, FirstQuartileDuration, MedianDuration, ThirdQuartileDuration, Percentile90Duration, Percentile95Duration, Percentile99Duration)\r\n\tSELECT\r\n\t\tHashId\r\n\t\t, ExecutionCount = MAX(ExecutionCount)\r\n\t\t, SuccessfulExecutions = MAX(SuccessfulExecutions)\r\n\t\t, FailedExecutions = MAX(FailedExecutions)\r\n\t\t, AbortedExecutions = MAX(AbortedExecutions)\r\n\t\t, MinDuration = MIN(duration)\r\n\t\t, MeanDuration = AVG(duration)\r\n\t\t, MaxDuration = MAX(duration)\r\n\t\t, TotalDuration = SUM(duration)\r\n\t\t, FirstQuartileDuration = CASE WHEN MAX(ExecutionCount) <= 3 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.25 THEN Duration ELSE 0 END) END\r\n\t\t, MedianDuration = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.5 THEN Duration ELSE 0 END) END\r\n\t\t, ThirdQuartileDuration = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.75 THEN Duration ELSE 0 END) END\r\n\t\t, Percentile90Duration = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.9 THEN Duration ELSE 0 END) END\r\n\t\t, Percentile95Duration = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.95 THEN Duration ELSE 0 END) END\r\n\t\t, Percentile99Duration = CASE WHEN MAX(ExecutionCount) = 1 THEN MIN(Duration) ELSE MAX(CASE WHEN DurationExecutionNumber*1.0/ExecutionCount <= 0.99 THEN Duration ELSE 0 END) END\r\n\tFROM (\r\n\t\tSELECT\r\n\t\t\tu.HashId\r\n\t\t\t, Duration\r\n\t\t\t, DurationExecutionNumber = ROW_NUMBER() OVER(PARTITION BY u.HashId ORDER BY Duration ASC)\r\n\t\t\t, ExecutionCount = COUNT(*) OVER(PARTITION BY u.HashId)\r\n\t\t\t, SuccessfulExecutions = SUM(CASE WHEN Result = N'OK' THEN 1 ELSE 0 END) OVER(PARTITION BY u.HashId)\r\n\t\t\t, FailedExecutions = SUM(CASE WHEN Result = N'Error' THEN 1 ELSE 0 END) OVER(PARTITION BY u.HashId)\r\n\t\t\t, AbortedExecutions = SUM(CASE WHEN Result = N'Abort' THEN 1 ELSE 0 END) OVER(PARTITION BY u.HashId)\r\n\t\tFROM XELoader.tblBatches20240522 b\r\n\t\tINNER JOIN (\r\n\t\t\tSELECT HashId\r\n\t\t\tFROM XELoader.tblUniqueBatches20240522\r\n\t\t\tORDER BY HashId ASC\r\n\t\t\tOFFSET @RowsCompleted ROWS FETCH NEXT @BatchSize ROWS ONLY\r\n\t\t) u ON u.HashId = b.HashId\r\n\t) SummaryData\r\n\tGROUP BY HashId\r\n\tORDER BY HashId;\r\n\r\n\tSET @Rowcount = @@ROWCOUNT;\r\n\tSET @RowsCompleted += @Rowcount;\r\nEND";
            successMessage = $"\r\nPopulated {xer.BatchDurationSummaryTableName} successfully!";
            failedMessage = $"Failed to populate {xer.BatchDurationSummaryTableName}!";
            commandTimeout = 600;
            result = xer.ExecuteSqlCommand(xer.SqlConnectionStr, sqlCommand, successMessage, failedMessage, commandTimeout);

            Console.WriteLine($"Ended at {DateTimeOffset.Now}!");
        }
    }
}
