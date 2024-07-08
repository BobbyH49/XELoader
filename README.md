# XELoader

### Load Extended Events into a SQL Server Database and aggregate over a number of metrics

* This is a current WIP which can load multiple xel files into a SQL Server Database by throttling Batch Size and Parallel Threads
* Once the tblBatches table is loaded it will generate a tblUniqueBatches and a tblBatchDurationSummary table which gives full statistics about the duration of each unique batch
* It uses the SQL Server 2022 TSQLParser but is backward compatible with other SQL Server versions
* Currently only processes rpc_completed and sql_batch_completed events but can be extended
* Statistics are currently only based on Duration but this can be extended to do all other metrics in the same way
* There is very little error handling at the moment but can easily be included
* This project is to show what you can do as an alternative to using RML Utilities by Microsoft, and can easily be extended

XELoader
	- SqlInstanceName - Your default or named SQL Instance (e.g. MyServer\MyInstance)
	- DatabaseName - Any existing database on your instance
	- Authentication - Windows or SQL (default is Windows)
	- TrustCertificate - True or False (default is True)
	- Username - Only required for SQL Authentication
	- Password - Only required for SQL Authentication
	- SchemaName - Can be new or existing
	- TableSuffix - Will be appended to the new tables (tblBatches, tblUniqueBatches, tblBatchDurationSummary)
	- XEFolder - Directory where xel files are located
	- XEFilePattern - Default is *.xel
	- StartTime - Filters the timestamp with a default of 01/01/1900 (e.g. UK format is dd/mm/yyyy hh:mm:ss or US format is mm/dd/yyyy hh:mm:ss)
	- EndTime  - Filters the timestamp with a default of 31/12/9999 (e.g. UK format is dd/mm/yyyy hh:mm:ss or US format is mm/dd/yyyy hh:mm:ss)
	- TimeZoneOffset - Timezone to use if filtering on StartTime and EndTime (format is +/-hh:mm)
	- BatchSize - Throttle the BatchSize but can cause timeouts (try starting with a size that allows for around 20-25 batches per file)
	- ParallelThreads - 1 or many threads but can cause CPU or Memory issues (try starting with 4)

