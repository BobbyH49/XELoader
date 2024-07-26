-- Provide the number of VCores for the server you are monitoring
-- Find and Replace the word "Suffix" with your own suffix for the tables generated, including tblHourlyReport which will be created by this script

DECLARE
	@VCores INT = 20;

SELECT
	DateTime = DATEADD(mi, DATEPART(mi, Timestamp), DATEADD(hh, DATEPART(hh, Timestamp), CAST(CAST(Timestamp AS DATE) AS DATETIME)))
	, DatabaseName
	, NormTextHashId
	, ExecutionCount = COUNT(*)
	, Duration_Microseconds = SUM(Duration)
	, CPU_Microseconds = SUM(CpuTime)
	, CpuPct = CAST(100.0 * SUM(CpuTime) / 1000000 / 60 / @Vcores AS DECIMAL(7, 4))
	, LogicalReadsPages = SUM(LogicalReads)
	, PhysicalReadsPages = SUM(PhysicalReads)
	, WritesPages = SUM(Writes)
	, AvgRowcount = AVG([Rowcount])
INTO PeopleWeb.tblHourlyReportSuffix
FROM PeopleWeb.tblBatchesSuffix
GROUP BY
	DATEADD(mi, DATEPART(mi, Timestamp), DATEADD(hh, DATEPART(hh, Timestamp), CAST(CAST(Timestamp AS DATE) AS DATETIME)))
	, DatabaseName
	, NormTextHashId
ORDER BY
	DateTime
	, NormTextHashId;


-- Add the subset of NormText that you want to filter by
SELECT
	hr.DateTime
	, ub.DatabaseName
	, ub.NormText
	, hr.ExecutionCount
	, hr.Duration_Microseconds
	, hr.CPU_Microseconds
	, hr.CpuPct
	, hr.LogicalReadsPages
	, hr.PhysicalReadsPages
	, hr.WritesPages
	, hr.AvgRowcount
FROM PeopleWeb.tblHourlyReportSuffix hr
JOIN PeopleWeb.tblUniqueBatchesSuffix ub ON ub.NormTextHashId = hr.NormTextHashId
--WHERE ub.NormText in (
--	N''
--)
ORDER BY DateTime ASC, hr.NormTextHashId ASC;

-- Add the subset of NormText that you want to filter by
SELECT
	ub.DatabaseName
	, ub.NormText
	, bs.AllExecutions
	, bss.MinDuration
	, bss.MeanDuration
	, bss.MaxDuration
	, TotalDurationSeconds = bss.TotalDuration / 1000000
	, bss.FirstQuartileDuration
	, bss.MedianDuration
	, bss.ThirdQuartileDuration
	, bss.Percentile90Duration
	, bss.Percentile95Duration
	, bss.Percentile99Duration
FROM PeopleWeb.tblBatchDurationSummarySuffix bss
JOIN PeopleWeb.tblBatchSummarySuffix bs on bs.NormTextHashId = bss.NormTextHashId
JOIN PeopleWeb.tblUniqueBatchesSuffix ub on ub.NormTextHashId = bss.NormTextHashId
--WHERE ub.NormText in (
--	N''
--)
ORDER BY TotalDurationSeconds DESC;

-- Add the subset of NormText that you want to filter by
SELECT
	ub.DatabaseName
	, ub.NormText
	, bs.AllExecutions
	, bss.MinCpuTime
	, bss.MeanCpuTime
	, bss.MaxCpuTime
	, TotalCpuTimeSeconds = bss.TotalCpuTime / 1000000
	, bss.FirstQuartileCpuTime
	, bss.MedianCpuTime
	, bss.ThirdQuartileCpuTime
	, bss.Percentile90CpuTime
	, bss.Percentile95CpuTime
	, bss.Percentile99CpuTime
FROM PeopleWeb.tblBatchCpuTimeSummarySuffix bss
JOIN PeopleWeb.tblBatchSummarySuffix bs on bs.NormTextHashId = bss.NormTextHashId
JOIN PeopleWeb.tblUniqueBatchesSuffix ub on ub.NormTextHashId = bss.NormTextHashId
--WHERE ub.NormText in (
--	N''
--)
ORDER BY TotalCpuTimeSeconds DESC;

-- Add the subset of NormText that you want to filter by
SELECT
	ub.DatabaseName
	, ub.NormText
	, bs.AllExecutions
	, bss.MinLogicalReads
	, bss.MeanLogicalReads
	, bss.MaxLogicalReads
	, TotalLogicalReadsMB = bss.TotalLogicalReads / 128
	, bss.FirstQuartileLogicalReads
	, bss.MedianLogicalReads
	, bss.ThirdQuartileLogicalReads
	, bss.Percentile90LogicalReads
	, bss.Percentile95LogicalReads
	, bss.Percentile99LogicalReads
FROM PeopleWeb.tblBatchLogicalReadsSummarySuffix bss
JOIN PeopleWeb.tblBatchSummarySuffix bs on bs.NormTextHashId = bss.NormTextHashId
JOIN PeopleWeb.tblUniqueBatchesSuffix ub on ub.NormTextHashId = bss.NormTextHashId
--WHERE ub.NormText in (
--	N''
--)
ORDER BY TotalLogicalReadsMB DESC;

-- Add the subset of NormText that you want to filter by
SELECT
	ub.DatabaseName
	, ub.NormText
	, bs.AllExecutions
	, bss.MinPhysicalReads
	, bss.MeanPhysicalReads
	, bss.MaxPhysicalReads
	, TotalPhysicalReadsMB = bss.TotalPhysicalReads / 128
	, bss.FirstQuartilePhysicalReads
	, bss.MedianPhysicalReads
	, bss.ThirdQuartilePhysicalReads
	, bss.Percentile90PhysicalReads
	, bss.Percentile95PhysicalReads
	, bss.Percentile99PhysicalReads
FROM PeopleWeb.tblBatchPhysicalReadsSummarySuffix bss
JOIN PeopleWeb.tblBatchSummarySuffix bs on bs.NormTextHashId = bss.NormTextHashId
JOIN PeopleWeb.tblUniqueBatchesSuffix ub on ub.NormTextHashId = bss.NormTextHashId
--WHERE ub.NormText in (
--	N''
--)
ORDER BY TotalPhysicalReadsMB DESC;

-- Add the subset of NormText that you want to filter by
SELECT
	ub.DatabaseName
	, ub.NormText
	, bs.AllExecutions
	, bss.MinWrites
	, bss.MeanWrites
	, bss.MaxWrites
	, TotalWritesMB = bss.TotalWrites / 128
	, bss.FirstQuartileWrites
	, bss.MedianWrites
	, bss.ThirdQuartileWrites
	, bss.Percentile90Writes
	, bss.Percentile95Writes
	, bss.Percentile99Writes
FROM PeopleWeb.tblBatchWritesSummarySuffix bss
JOIN PeopleWeb.tblBatchSummarySuffix bs on bs.NormTextHashId = bss.NormTextHashId
JOIN PeopleWeb.tblUniqueBatchesSuffix ub on ub.NormTextHashId = bss.NormTextHashId
--WHERE ub.NormText in (
--	N''
--)
ORDER BY TotalWritesMB DESC;

-- Add the subset of NormText that you want to filter by
SELECT
	ub.DatabaseName
	, ub.NormText
	, bs.AllExecutions
	, bss.MinRowcount
	, bss.MeanRowcount
	, bss.MaxRowcount
	, TotalRowcount = bss.TotalRowcount
	, bss.FirstQuartileRowcount
	, bss.MedianRowcount
	, bss.ThirdQuartileRowcount
	, bss.Percentile90Rowcount
	, bss.Percentile95Rowcount
	, bss.Percentile99Rowcount
FROM PeopleWeb.tblBatchRowcountSummarySuffix bss
JOIN PeopleWeb.tblBatchSummarySuffix bs on bs.NormTextHashId = bss.NormTextHashId
JOIN PeopleWeb.tblUniqueBatchesSuffix ub on ub.NormTextHashId = bss.NormTextHashId
--WHERE ub.NormText in (
--	N''
--)
ORDER BY TotalRowcount DESC;
