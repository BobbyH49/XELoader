using Microsoft.SqlServer.TransactSql.ScriptDom;
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

            using (SqlConnection sqlConnection = GetConnection(sqlConnectionStr))
            {
                result = ExecuteSqlQuery(sqlConnection, sqlCommand, commandTimeout);

                if (result == 0)
                {
                    Console.WriteLine(successMessage);
                }
                else
                {
                    Console.WriteLine(failedMessage);
                }
            }

            return result;
        }

        public Int16 LoadEvents(XEReader xer)
        {
            Int16 result;

            FileInfo[] eventFiles = new DirectoryInfo(xer.XEFolder).GetFiles(xer.XEFilePattern);
            Int32 eventFilesLength = eventFiles.Length;
            FileInfo eventFile;
            Int32 eventFileId = 0;
            XEFile xeFile;
            Queue<XEFile> xeFileQueue = new Queue<XEFile>(eventFilesLength);

            Console.WriteLine($"\r\n{eventFilesLength} files to process!");

            for (Int32 i = 0; i < eventFilesLength; i++)
            {
                eventFile = eventFiles[i];
                eventFileId = i + 1;
                xeFile = new XEFile(eventFileId, eventFile, xer);
                xeFileQueue.Enqueue(xeFile);
            }

            Thread[] threads = new Thread[xer.ParallelThreads];

            for (int i = 0; i < xer.ParallelThreads; i++)
            {
                threads[i] = new Thread(ProcessEventFiles);
                threads[i].Start(xeFileQueue);
            }

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
                DataColumn result = new DataColumn("Result", typeof(string));
                result.MaxLength = 10;
                dt.Columns.Add(result);
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

                while (xeFileQueue.Count > 0)
                {
                    xeFile = null;

                    lock (xeFileQueue)
                    {
                        xeFileQueue.TryDequeue(out xeFile);
                    }

                    if (xeFile != null)
                    {
                        Console.WriteLine($"\r\nProcessing file {xeFile.FileNumber.ToString()} - {xeFile.File.Name}!");

                        eventsRead = 0;
                        eventsProcessed = 0;
                        batchCount = 0;

                        using (var events = new QueryableXEventData(xeFile.File.FullName))
                        {
                            using (SqlConnection sqlConnection = GetConnection(xeFile.SqlConnectionStr))
                            {
                                sqlConnection.Open();
                                SqlBulkCopy bcp = new SqlBulkCopy(sqlConnection);
                                bcp.DestinationTableName = $"{xeFile.FullBatchesTableName}";

                                foreach (var xe in events)
                                {
                                    eventsRead++;

                                    if ((xe.Name == "rpc_completed") || (xe.Name == "sql_batch_completed"))
                                    {
                                        if ((xe.Timestamp >= xeFile.StartTimeOffset) && (xe.Timestamp <= xeFile.EndTimeOffset))
                                        {
                                            row = dt.NewRow();
                                            dt.Rows.Add(row);
                                            row["EventSequence"] = xe.Actions["event_sequence"].Value;
                                            row["EventType"] = xe.Name;
                                            row["Timestamp"] = xe.Timestamp;
                                            row["ClientHostname"] = xe.Actions["client_hostname"].Value.ToString();
                                            row["ClientAppName"] = xe.Actions["client_app_name"].Value.ToString();
                                            row["DatabaseName"] = xe.Actions["database_name"].Value.ToString();
                                            row["HashId"] = 0;
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
                                            row["Result"] = xe.Fields["result"].Value.ToString();
                                            row["Duration"] = xe.Fields["duration"].Value;
                                            row["CpuTime"] = xe.Fields["cpu_time"].Value;
                                            row["LogicalReads"] = xe.Fields["logical_reads"].Value;
                                            row["PhysicalReads"] = xe.Fields["physical_reads"].Value;
                                            row["Writes"] = xe.Fields["writes"].Value;
                                            row["Rowcount"] = xe.Fields["row_count"].Value;

                                            eventsProcessed++;

                                            if ((eventsProcessed != 0) && (eventsProcessed % xeFile.BatchSize == 0))
                                            {
                                                bcp.WriteToServer(dt);
                                                dt.Rows.Clear();
                                                batchCount++;
                                            }
                                        }
                                    }
                                }
                                if (eventsProcessed != 0)
                                {
                                    bcp.WriteToServer(dt); // write last batch
                                    dt.Rows.Clear();
                                    batchCount++;
                                }
                            }
                        }

                        Console.WriteLine($"\r\nProcessed file {xeFile.FileNumber.ToString()} - {xeFile.File.Name}!");
                        Console.WriteLine($"\tEvents read = {eventsRead.ToString()}");
                        Console.WriteLine($"\tEvents processed = {eventsProcessed.ToString()}");
                        Console.WriteLine($"\tBatches processed = {batchCount.ToString()}");
                    }
                }
            }
        }

        private static string GetNormText(string origText)
        {
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
                    if ((isDynamic == true) && ((batch.TokenType.ToString() == "AsciiStringLiteral") || (batch.TokenType.ToString() == "UnicodeStringLiteral")))
                    {
                        parsedText += batch.Text;
                        isDynamic = false;
                    }
                    else if ((batch.TokenType.ToString() == "AsciiStringLiteral") || (batch.TokenType.ToString() == "UnicodeStringLiteral"))
                    {
                        parsedText += "{STR}";

                    }
                    else if (batch.TokenType.ToString().StartsWith("Integer"))
                    {
                        parsedText += "{##}";
                    }
                    else if ((batch.TokenType.ToString() == "SingleLineComment") || (batch.TokenType.ToString() == "MultilineComment"))
                    {
                        parsedText += "";
                    }
                    else if (((batch.Text ?? "").StartsWith("sp_executesql")) || ((batch.Text ?? "") == "sp_prepare"))
                    {
                        isDynamic = true;
                        parsedText += batch.Text;
                    }
                    else
                    {
                        parsedText += batch.Text;
                    }
                }
                normText = Regex.Replace(parsedText, pattern, replacement).Trim();
            }
            return normText;
        }

        private static SqlConnection GetConnection(string sqlConnectionStr)
        {
            return new SqlConnection(sqlConnectionStr);
        }

        private Int16 ExecuteSqlQuery(SqlConnection sqlConnection, string query, Int32 commandTimeout)
        {
            Int16 result = 0;

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
