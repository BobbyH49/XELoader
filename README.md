# XELoader

### Load Extended Events into a SQL Server Database and aggregate over a number of metrics

* This is a current WIP which can load multiple xel files into a SQL Server Database by throttling Batch Size and Parallel Threads
* Once the tblBatches table is loaded it will generate a tblUniqueBatches and several summary tables to provide full stats on all SQL Server batch metrics
* It uses the SQL Server 2022 TSQLParser but is backward compatible with other SQL Server versions
* Currently only processes rpc_completed and sql_batch_completed events but can be extended
* Also includes a SQL Server script and Excel Template to generate results for a report that will give you daily charts along with individual statistics about each batch.

### XELoader parameters

* SqlInstanceName - Your default or named SQL Instance (e.g. MyServer\MyInstance)
* DatabaseName - Any existing database on your instance
* Authentication - Windows or SQL (default is Windows)
* TrustCertificate - True or False (default is True)
* Username - Only required for SQL Authentication
* Password - Only required for SQL Authentication
* SchemaName - Can be new or existing
* TableSuffix - Will be appended to the new tables (tblBatches, tblUniqueBatches, tblBatchDurationSummary)
* XEFolder - Directory where xel files are located
* XEFilePattern - Default is *.xel
* StartTime - Filters the timestamp with a default of 01/01/1900 (e.g. UK format is dd/mm/yyyy hh:mm:ss or US format is mm/dd/yyyy hh:mm:ss)
* EndTime  - Filters the timestamp with a default of 31/12/9999 (e.g. UK format is dd/mm/yyyy hh:mm:ss or US format is mm/dd/yyyy hh:mm:ss)
* TimeZoneOffset - Timezone to use if filtering on StartTime and EndTime (format is +/-hh:mm)
* FilterConnectionResets - Do not load sp_reset_connection batches (default is False)
* BatchSize - Throttle the BatchSize but can cause timeouts (try starting with a size that allows for around 20-25 batches per file)
* ParallelThreads - 1 or many threads but can cause CPU or Memory issues (try starting with 4)
