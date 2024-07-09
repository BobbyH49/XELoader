﻿using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.SqlServer.XEvent.Linq;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using System.Data;

namespace XELoader
{
    class XEReader
    {
        private string sqlInstanceName;
        private string databaseName;
        private string authentication;
        private bool trustCertificate;
        private string username = "";
        private string password = "";
        private string schemaName;
        private string tableSuffix;
        private string xeFolder;
        private string xeFilePattern;
        private DateTime startTime;
        private DateTime endTime;
        private string timeZoneOffset = "+00:00";
        private Int32 batchSize;
        private Int16 parallelThreads;

        public XEReader(Dictionary<string, string> paramList)
        {
            // Initialize the XEReader object with all of the parameters
            this.sqlInstanceName = paramList["SqlInstanceName"];

            this.databaseName = paramList["DatabaseName"];

            if (paramList.ContainsKey("Authentication"))
                this.authentication = paramList["Authentication"];
            else
                this.authentication = "Windows";

            if (paramList.ContainsKey("TrustCertificate"))
                bool.TryParse(paramList["TrustCertificate"], out this.trustCertificate);
            else
                trustCertificate = true;

            if (paramList.ContainsKey("Username"))
                this.username = paramList["Username"];

            if (paramList.ContainsKey("Password"))
                this.password = paramList["Password"];

            if (paramList.ContainsKey("SchemaName"))
                this.schemaName = paramList["SchemaName"];
            else
                this.schemaName = "XELoader";

            this.tableSuffix = paramList["TableSuffix"];

            this.xeFolder = paramList["XEFolder"];

            if (paramList.ContainsKey("XEFilePattern"))
                this.xeFilePattern = paramList["XEFilePattern"];
            else
                this.xeFilePattern = "*.xel";

            if (paramList.ContainsKey("StartTime"))
                DateTime.TryParse(paramList["StartTime"], out this.startTime);
            else
                DateTime.TryParse("1900-01-01", out this.startTime);

            if (paramList.ContainsKey("EndTime"))
                DateTime.TryParse(paramList["EndTime"], out this.endTime);
            else
                DateTime.TryParse("9999-12-31", out this.endTime);

            if (paramList.ContainsKey("TimeZoneOffset"))
                this.timeZoneOffset = paramList["TimeZoneOffset"];

            if (paramList.ContainsKey("BatchSize"))
                Int32.TryParse(paramList["BatchSize"], out this.batchSize);
            else
                this.batchSize = 10000;

            if (paramList.ContainsKey("ParallelThreads"))
                Int16.TryParse(paramList["ParallelThreads"], out this.parallelThreads);
            else
                this.parallelThreads = 1;
        }
        public Int16 ExecuteSqlCommand(string sqlConnectionStr, string sqlCommand, string successMessage, string failedMessage, Int32 commandTimeout)
        {
            Int16 result;

            // Execute a simple SQL Query and then return a success or failed message
            using (SqlConnection sqlConnection = GetConnection(sqlConnectionStr))
            {
                result = ExecuteSqlQuery(sqlConnection, sqlCommand, commandTimeout);

                if (result == 0)
                {
                    Console.WriteLine($"{successMessage} - {DateTimeOffset.Now}");
                }
                else
                {
                    Console.WriteLine($"{failedMessage} - {DateTimeOffset.Now}");
                }
            }

            return result;
        }

        public Int16 LoadEvents(XEReader xer)
        {
            Int16 result;

            // Get the list of files and then store in a queue to be dequeued by each thread
            FileInfo[] eventFiles = new DirectoryInfo(xer.XEFolder).GetFiles(xer.XEFilePattern);
            Int32 eventFilesLength = eventFiles.Length;
            FileInfo eventFile;
            Int32 eventFileId = 0;
            XEFile xeFile;
            Queue<XEFile> xeFileQueue = new Queue<XEFile>(eventFilesLength);

            Console.WriteLine($"\r\n{eventFilesLength} files to process!");

            // Enqueue each file to the queue
            for (Int32 i = 0; i < eventFilesLength; i++)
            {
                eventFile = eventFiles[i];
                eventFileId = i + 1;
                xeFile = new XEFile(eventFileId, eventFile, xer);
                xeFileQueue.Enqueue(xeFile);
            }

            Thread[] threads = new Thread[xer.ParallelThreads];

            // Create threads based on the ParallelThreads parameter and then start each with a pointer to the queue
            for (int i = 0; i < xer.ParallelThreads; i++)
            {
                threads[i] = new Thread(ProcessEventFiles);
                threads[i].Start(xeFileQueue);
            }

            // Join each thread to wait for completion
            for (int i = 0; i < ParallelThreads; i++)
            {
                threads[i].Join();
            }

            result = 0;
            return result;
        }

        static void ProcessEventFiles(object? xeFileQueueObj)
        {
            if (xeFileQueueObj != null)
            {
                Queue<XEFile> xeFileQueue = (Queue<XEFile>)xeFileQueueObj;

                Int64 eventsRead;
                Int64 eventsProcessed;
                Int32 batchCount;

                string statement;
                string object_name;
                string batch_text;

                // Create a DataTable to store the data that will be uploaded to the database
                DataTable dt = new DataTable();
                DataColumn eventSequence = new DataColumn("EventSequence", typeof(Int64));
                dt.Columns.Add(eventSequence);
                DataColumn eventType = new DataColumn("EventType", typeof(string));
                eventType.MaxLength = 20;
                dt.Columns.Add(eventType);
                DataColumn timestamp = new DataColumn("Timestamp", typeof(DateTimeOffset));
                dt.Columns.Add(timestamp);
                DataColumn clientHostname = new DataColumn("ClientHostname", typeof(string));
                clientHostname.MaxLength = 500;
                dt.Columns.Add(clientHostname);
                DataColumn clientAppName = new DataColumn("ClientAppName", typeof(string));
                clientAppName.MaxLength = 1000;
                dt.Columns.Add(clientAppName);
                DataColumn databaseName = new DataColumn("DatabaseName", typeof(string));
                databaseName.MaxLength = 200;
                dt.Columns.Add(databaseName);
                DataColumn hashId = new DataColumn("HashId", typeof(Int32));
                dt.Columns.Add(hashId);
                DataColumn textData = new DataColumn("TextData", typeof(string));
                textData.MaxLength = -1;
                dt.Columns.Add(textData);
                DataColumn normText = new DataColumn("NormText", typeof(string));
                normText.MaxLength = -1;
                dt.Columns.Add(normText);
                DataColumn successful = new DataColumn("Successful", typeof(UInt16));
                dt.Columns.Add(successful);
                DataColumn failed = new DataColumn("Failed", typeof(UInt16));
                dt.Columns.Add(failed);
                DataColumn aborted = new DataColumn("Aborted", typeof(UInt16));
                dt.Columns.Add(aborted);
                DataColumn duration = new DataColumn("Duration", typeof(UInt64));
                dt.Columns.Add(duration);
                DataColumn cpuTime = new DataColumn("CpuTime", typeof(UInt64));
                dt.Columns.Add(cpuTime);
                DataColumn logicalReads = new DataColumn("LogicalReads", typeof(UInt64));
                dt.Columns.Add(logicalReads);
                DataColumn physicalReads = new DataColumn("PhysicalReads", typeof(UInt64));
                dt.Columns.Add(physicalReads);
                DataColumn writes = new DataColumn("Writes", typeof(UInt64));
                dt.Columns.Add(writes);
                DataColumn rowcount = new DataColumn("Rowcount", typeof(UInt64));
                dt.Columns.Add(rowcount);

                DataRow row;
                XEFile? xeFile;

                // While there are files remaining in the queue
                while (xeFileQueue.Count > 0)
                {
                    xeFile = null;

                    // Lock the queue to block other threads while dequeuing the file
                    lock (xeFileQueue)
                    {
                        xeFileQueue.TryDequeue(out xeFile);
                    }

                    // If the thread has retrieved a file then process
                    if (xeFile != null)
                    {
                        Console.WriteLine($"\r\nProcessing file {xeFile.FileNumber.ToString()} - {xeFile.File.Name} at {DateTimeOffset.Now}!");

                        eventsRead = 0;
                        eventsProcessed = 0;
                        batchCount = 0;

                        // Open the xel file for reading
                        using (var events = new QueryableXEventData(xeFile.File.FullName))
                        {
                            // Open a new SQL Connection
                            using (SqlConnection sqlConnection = GetConnection(xeFile.SqlConnectionStr))
                            {
                                // Setup the connection to the batches table
                                sqlConnection.Open();
                                SqlBulkCopy bcp = new SqlBulkCopy(sqlConnection);
                                bcp.DestinationTableName = $"{xeFile.FullBatchesTableName}";

                                // Process each event one by one
                                foreach (var xe in events)
                                {
                                    eventsRead++;

                                    // Only process rpc_completed and sql_batch_completed events
                                    if ((xe.Name == "rpc_completed") || (xe.Name == "sql_batch_completed"))
                                    {
                                        // Only process events that have a Timestamp that fall within the StartTime and EndTime boundaries
                                        if ((xe.Timestamp >= xeFile.StartTimeOffset) && (xe.Timestamp <= xeFile.EndTimeOffset))
                                        {
                                            // Create a new row in the DataTable and populate
                                            row = dt.NewRow();
                                            dt.Rows.Add(row);
                                            row["EventSequence"] = xe.Actions["event_sequence"].Value;
                                            row["EventType"] = xe.Name;
                                            row["Timestamp"] = xe.Timestamp;
                                            row["ClientHostname"] = xe.Actions["client_hostname"].Value.ToString();
                                            row["ClientAppName"] = xe.Actions["client_app_name"].Value.ToString();
                                            row["DatabaseName"] = xe.Actions["database_name"].Value.ToString();
                                            row["HashId"] = 0;
                                            // HashId, NormText and TextData are treated differently depending on whether the event is rpc_completed or sql_batch_completed
                                            if (xe.Name == "rpc_completed")
                                            {
                                                statement = xe.Fields["statement"].Value.ToString() ?? "";
                                                object_name = xe.Fields["object_name"].Value.ToString() ?? "";
                                                row["TextData"] = statement;
                                                if ((!object_name.StartsWith("sp_execute")) && (object_name != "sp_prepare"))
                                                {
                                                    row["HashId"] = object_name.GetHashCode();
                                                    row["NormText"] = object_name;
                                                }
                                                else
                                                {
                                                    row["HashId"] = GetNormText(statement).GetHashCode();
                                                    row["NormText"] = GetNormText(statement);
                                                }
                                            }
                                            else
                                            {
                                                batch_text = xe.Fields["batch_text"].Value.ToString() ?? "";
                                                row["HashId"] = GetNormText(batch_text).GetHashCode();
                                                row["TextData"] = batch_text;
                                                row["NormText"] = GetNormText(batch_text);
                                            }

                                            if (xe.Fields["result"].Value.ToString() == "OK")
                                            {
                                                row["Successful"] = 1;
                                                row["Failed"] = 0;
                                                row["Aborted"] = 0;

                                            }
                                            else if (xe.Fields["result"].Value.ToString() == "Error")
                                            {
                                                row["Successful"] = 0;
                                                row["Failed"] = 1;
                                                row["Aborted"] = 0;

                                            }
                                            else if (xe.Fields["result"].Value.ToString() == "Abort")
                                            {
                                                row["Successful"] = 0;
                                                row["Failed"] = 0;
                                                row["Aborted"] = 1;

                                            }
                                            else
                                            {
                                                row["Successful"] = 0;
                                                row["Failed"] = 0;
                                                row["Aborted"] = 0;

                                            }
                                            row["Duration"] = xe.Fields["duration"].Value;
                                            row["CpuTime"] = xe.Fields["cpu_time"].Value;
                                            row["LogicalReads"] = xe.Fields["logical_reads"].Value;
                                            row["PhysicalReads"] = xe.Fields["physical_reads"].Value;
                                            row["Writes"] = xe.Fields["writes"].Value;
                                            row["Rowcount"] = xe.Fields["row_count"].Value;

                                            eventsProcessed++;

                                            // Flush the DataTable to the database in batches based on BatchSize parameter
                                            if ((eventsProcessed != 0) && (eventsProcessed % xeFile.BatchSize == 0))
                                            {
                                                bcp.WriteToServer(dt);
                                                dt.Rows.Clear();
                                                batchCount++;
                                            }
                                        }
                                    }
                                }
                                // Flush the final DataTable to the database if not empty
                                if (eventsProcessed != 0)
                                {
                                    bcp.WriteToServer(dt); // write last batch
                                    dt.Rows.Clear();
                                    batchCount++;
                                }
                            }
                        }

                        Console.WriteLine($"\r\nProcessed file {xeFile.FileNumber.ToString()} - {xeFile.File.Name} at {DateTimeOffset.Now}!");
                        Console.WriteLine($"\tEvents read = {eventsRead.ToString()}");
                        Console.WriteLine($"\tEvents processed = {eventsProcessed.ToString()}");
                        Console.WriteLine($"\tBatches processed = {batchCount.ToString()}");
                    }
                }
            }
        }

        private static string GetNormText(string origText)
        {
            // Parses the TextData column and normalizes by removing literals and whitespace
            // This version is set to SQL Server 2022
            var parser = new TSql160Parser(false);
            IList<ParseError> errors;

            string parsedText = "";
            string normText = "";
            string pattern = @"\s+"; // Matches one or more whitespace characters
            string replacement = " "; // Replace with a single space

            var result = parser.Parse(new StringReader(origText), out errors);

            if (result is TSqlScript script)
            {
                var isDynamic = false;

                foreach (var batch in script.ScriptTokenStream)
                {
                    // If dynamic sql is being used and the token is text
                    if ((isDynamic == true) && ((batch.TokenType.ToString() == "AsciiStringLiteral") || (batch.TokenType.ToString() == "UnicodeStringLiteral")))
                    {
                        // Keep the text and then set the dynamic flag to false
                        parsedText += batch.Text;
                        isDynamic = false;
                    }
                    // If dynamic sql is not being used and the token is text
                    else if ((batch.TokenType.ToString() == "AsciiStringLiteral") || (batch.TokenType.ToString() == "UnicodeStringLiteral"))
                    {
                        // Replace text with static value
                        parsedText += "{STR}";

                    }
                    // If the token is an integer
                    else if (batch.TokenType.ToString().StartsWith("Integer"))
                    {
                        // Replace with a static value
                        parsedText += "{##}";
                    }
                    // If the token is a comment
                    else if ((batch.TokenType.ToString() == "SingleLineComment") || (batch.TokenType.ToString() == "MultilineComment"))
                    {
                        // Do nothing
                        parsedText += "";
                    }
                    // If the current statement starts with sp_executesql or sp_prepare
                    else if (((batch.Text ?? "").StartsWith("sp_executesql")) || ((batch.Text ?? "") == "sp_prepare"))
                    {
                        // Set the dynamic flag to true and keep the text
                        isDynamic = true;
                        parsedText += batch.Text;
                    }
                    else
                    {
                        // For anything else keep the text
                        parsedText += batch.Text;
                    }
                }
                // Replace any remaining whitespace
                normText = Regex.Replace(parsedText, pattern, replacement).Trim();
            }
            return normText;
        }

        private static SqlConnection GetConnection(string sqlConnectionStr)
        {
            // Setup the connection
            return new SqlConnection(sqlConnectionStr);
        }

        private Int16 ExecuteSqlQuery(SqlConnection sqlConnection, string query, Int32 commandTimeout)
        {
            Int16 result = 0;

            // Try to execute a simple SQL Query or display the error message
            try
            {
                sqlConnection.Open();
                using (SqlCommand command = new SqlCommand(query, sqlConnection))
                {
                    command.CommandTimeout = commandTimeout;
                    command.ExecuteNonQuery();
                    result = 0;
                }
            }
            catch (Exception ex)
            {
                result = -1;
                Console.WriteLine($"\r\nERROR: {ex.Message}");
            }

            return result;
        }

        public string SqlInstanceName
        {
            get
            {
                return sqlInstanceName;
            }
        }
        public string DatabaseName
        {
            get
            {
                return databaseName;
            }
        }
        public string Authentication
        {
            get
            {
                return authentication;
            }
        }
        public bool TrustCertificate
        {
            get
            {
                return trustCertificate;
            }
        }
        public string Username
        {
            get
            {
                return username;
            }
        }
        public string SqlConnectionStr
        {
            get
            {
                string connectionStr = $"Data Source={sqlInstanceName};Initial Catalog={databaseName};";

                if (authentication.ToUpper() == "SQL")
                    connectionStr += $"User ID={username};Password={password};TrustServerCertificate={TrustCertificate.ToString()};";
                else
                    connectionStr += $"Integrated Security=SSPI;TrustServerCertificate={TrustCertificate.ToString()};";

                return connectionStr;
            }
        }
        public string SchemaName
        {
            get
            {
                return schemaName;
            }
        }
        public string TableSuffix
        {
            get
            {
                return tableSuffix;
            }
        }
        public string BatchesTableName
        {
            get
            {
                string batchesTableName = $"tblBatches{tableSuffix}";
                return batchesTableName;
            }
        }
        public string FullBatchesTableName
        {
            get
            {
                string fullBatchesTableName = $"{schemaName}.{BatchesTableName}";
                return fullBatchesTableName;
            }
        }
        public string UniqueBatchesTableName
        {
            get
            {
                string uniqueBatchesTableName = $"tblUniqueBatches{tableSuffix}";
                return uniqueBatchesTableName;
            }
        }
        public string FullUniqueBatchesTableName
        {
            get
            {
                string fullUniqueBatchesTableName = $"{schemaName}.{UniqueBatchesTableName}";
                return fullUniqueBatchesTableName;
            }
        }
        public string BatchDurationSummaryTableName
        {
            get
            {
                string batchDurationSummaryTableName = $"tblBatchDurationSummary{tableSuffix}";
                return batchDurationSummaryTableName;
            }
        }
        public string FullBatchDurationSummaryTableName
        {
            get
            {
                string FullBatchDurationSummaryTableName = $"{schemaName}.{BatchDurationSummaryTableName}";
                return FullBatchDurationSummaryTableName;
            }
        }
        public string XEFolder
        {
            get
            {
                return xeFolder;
            }
        }
        public string XEFilePattern
        {
            get
            {
                return xeFilePattern;
            }
        }
        public DateTime StartTime
        {
            get
            {
                return startTime;
            }
        }
        public DateTimeOffset StartTimeOffset
        {
            get
            {
                Int16 timeZoneHours;
                Int16 timeZoneMinutes;

                Int16.TryParse(timeZoneOffset.Substring(0, 3), out timeZoneHours);
                Int16.TryParse(timeZoneOffset.Substring(4, 2), out timeZoneMinutes);

                TimeSpan tzDiff = new TimeSpan(timeZoneHours, timeZoneMinutes, 0);

                DateTimeOffset startTimeOffset = new DateTimeOffset(startTime.Ticks, tzDiff);

                return startTimeOffset;
            }
        }
        public DateTime EndTime
        {
            get
            {
                return endTime;
            }
        }
        public DateTimeOffset EndTimeOffset
        {
            get
            {
                Int16 timeZoneHours;
                Int16 timeZoneMinutes;

                Int16.TryParse(timeZoneOffset.Substring(0, 3), out timeZoneHours);
                Int16.TryParse(timeZoneOffset.Substring(4, 2), out timeZoneMinutes);

                TimeSpan tzDiff = new TimeSpan(timeZoneHours, timeZoneMinutes, 0);

                DateTimeOffset endTimeOffset = new DateTimeOffset(endTime.Ticks, tzDiff);

                return endTimeOffset;
            }
        }
        public string TimeZoneOffset
        {
            get
            {
                return timeZoneOffset;
            }
        }
        public Int32 BatchSize
        {
            get
            {
                return batchSize;
            }
        }
        public Int16 ParallelThreads
        {
            get
            {
                return parallelThreads;
            }
        }
    }
}
