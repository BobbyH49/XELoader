-- Provide the number of VCores for the server you are monitoring
-- Add your Replace the suffix of 20240716 with your own suffix for the tables generated including tblHourlyReport which will be created by this script

DECLARE
	@VCores INT = 24;

SELECT
	DateTime = DATEADD(mi, DATEPART(mi, Timestamp), DATEADD(hh, DATEPART(hh, Timestamp), CAST(CAST(Timestamp AS DATE) AS DATETIME)))
	, NormTextHashId
	, ExecutionCount = COUNT(*)
	, Duration_Microseconds = SUM(Duration)
	, CPU_Microseconds = SUM(CpuTime)
	, CpuPct = CAST(100.0 * SUM(CpuTime) / 1000000 / 60 / @Vcores AS DECIMAL(7, 4))
	, LogicalReadsPages = SUM(LogicalReads)
	, PhysicalReadsPages = SUM(PhysicalReads)
	, WritesPages = SUM(Writes)
	, AvgRowcount = AVG([Rowcount])
INTO XELoader.tblHourlyReport20240716
FROM XELoader.tblBatches20240716
GROUP BY
	DATEADD(mi, DATEPART(mi, Timestamp), DATEADD(hh, DATEPART(hh, Timestamp), CAST(CAST(Timestamp AS DATE) AS DATETIME)))
	, NormTextHashId
ORDER BY
	DateTime
	, NormTextHashId;


-- Add the subset of NormText that you want to filter by
SELECT
	hr.DateTime
	, ub.NormText
	, hr.ExecutionCount
	, hr.Duration_Microseconds
	, hr.CPU_Microseconds
	, hr.CpuPct
	, hr.LogicalReadsPages
	, hr.PhysicalReadsPages
	, hr.WritesPages
	, hr.AvgRowcount
FROM XELoader.tblHourlyReport20240716 hr
JOIN XELoader.tblUniqueBatches20240716 ub ON ub.NormTextHashId = hr.NormTextHashId
--WHERE ub.NormText in (
--	N''
--	, N''
--)
ORDER BY DateTime ASC, hr.NormTextHashId ASC;

-- Add the subset of NormText that you want to filter by
SELECT
	ub.NormText
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
FROM XELoader.tblBatchDurationSummary20240716 bss
JOIN XELoader.tblBatchSummary20240716 bs on bs.NormTextHashId = bss.NormTextHashId
JOIN XELoader.tblUniqueBatches20240716 ub on ub.NormTextHashId = bss.NormTextHashId
--WHERE ub.NormText in (
--	N''
--	, N''
--)
ORDER BY ub.NormText ASC;

-- Add the subset of NormText that you want to filter by
SELECT
	ub.NormText
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
FROM XELoader.tblBatchCpuTimeSummary20240716 bss
JOIN XELoader.tblBatchSummary20240716 bs on bs.NormTextHashId = bss.NormTextHashId
JOIN XELoader.tblUniqueBatches20240716 ub on ub.NormTextHashId = bss.NormTextHashId
--WHERE ub.NormText in (
--	N''
--	, N''
--)
ORDER BY ub.NormText ASC;

-- Add the subset of NormText that you want to filter by
SELECT
	ub.NormText
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
FROM XELoader.tblBatchLogicalReadsSummary20240716 bss
JOIN XELoader.tblBatchSummary20240716 bs on bs.NormTextHashId = bss.NormTextHashId
JOIN XELoader.tblUniqueBatches20240716 ub on ub.NormTextHashId = bss.NormTextHashId
--WHERE ub.NormText in (
--	N''
--	, N''
--)
ORDER BY ub.NormText ASC;

-- Add the subset of NormText that you want to filter by
SELECT
	ub.NormText
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
FROM XELoader.tblBatchPhysicalReadsSummary20240716 bss
JOIN XELoader.tblBatchSummary20240716 bs on bs.NormTextHashId = bss.NormTextHashId
JOIN XELoader.tblUniqueBatches20240716 ub on ub.NormTextHashId = bss.NormTextHashId
--WHERE ub.NormText in (
--	N''
--	, N''
--)
ORDER BY ub.NormText ASC;

-- Add the subset of NormText that you want to filter by
SELECT
	ub.NormText
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
FROM XELoader.tblBatchWritesSummary20240716 bss
JOIN XELoader.tblBatchSummary20240716 bs on bs.NormTextHashId = bss.NormTextHashId
JOIN XELoader.tblUniqueBatches20240716 ub on ub.NormTextHashId = bss.NormTextHashId
--WHERE ub.NormText in (
--	N''
--	, N''
--)
ORDER BY ub.NormText ASC;

-- Add the subset of NormText that you want to filter by
SELECT
	ub.NormText
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
FROM XELoader.tblBatchRowcountSummary20240716 bss
JOIN XELoader.tblBatchSummary20240716 bs on bs.NormTextHashId = bss.NormTextHashId
JOIN XELoader.tblUniqueBatches20240716 ub on ub.NormTextHashId = bss.NormTextHashId
--WHERE ub.NormText in (
--	N''
--	, N''
--)
ORDER BY ub.NormText ASC;
