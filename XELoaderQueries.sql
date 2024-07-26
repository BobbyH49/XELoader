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
WHERE ub.NormText in (
	N'usp_Process_GetUserByQueryId'
	, N'usp_API_GetQueryResultByQueryName'
	, N'usp_Detail_GetListWithAdvancedSearch'
	, N'usp_V4_MobileAPI_GetMainDashboardData'
	, N'usp_DocumentCategory_GetForEmployee'
	, N'usp_MenuAccessForHiringLeadAndJobApprovers_GetList'
	, N'usp_V4_MobileAPI_GetPlannerDetailData'
	, N'usp_DocumentCategory_GetAllByCompanyId'
	, N'usp_QueryDefDBTypeFilter_GetById'
	, N'usp_Query_Get_Active'
	, N'usp_Query_GetAggregateFunction'
	, N'usp_Process_GetList'
	, N'usp_LogbookScreenHeader_GetLogbookScreenList'
	, N'usp_API_GetQueryResult'
	, N'usp_Detail_GetCompanyInfoById'
	, N'usp_Applicant_GetApplicantList'
	, N'usp_V4_MobileAPI_GetAllCompanyDirectoryEmployeeDetails'
	, N'usp_APIKey_GetByUniqueAPIKey'
	, N'usp_Query_UpdateFinalQuery'
)
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
WHERE ub.NormText in (
	N'usp_Process_GetUserByQueryId'
	, N'usp_API_GetQueryResultByQueryName'
	, N'usp_Detail_GetListWithAdvancedSearch'
	, N'usp_V4_MobileAPI_GetMainDashboardData'
	, N'usp_DocumentCategory_GetForEmployee'
	, N'usp_MenuAccessForHiringLeadAndJobApprovers_GetList'
	, N'usp_V4_MobileAPI_GetPlannerDetailData'
	, N'usp_DocumentCategory_GetAllByCompanyId'
	, N'usp_QueryDefDBTypeFilter_GetById'
	, N'usp_Query_Get_Active'
	, N'usp_Query_GetAggregateFunction'
	, N'usp_Process_GetList'
	, N'usp_LogbookScreenHeader_GetLogbookScreenList'
	, N'usp_API_GetQueryResult'
	, N'usp_Detail_GetCompanyInfoById'
	, N'usp_Applicant_GetApplicantList'
	, N'usp_V4_MobileAPI_GetAllCompanyDirectoryEmployeeDetails'
	, N'usp_APIKey_GetByUniqueAPIKey'
	, N'usp_Query_UpdateFinalQuery'
)
ORDER BY ub.NormText ASC;

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
WHERE ub.NormText in (
	N'usp_Process_GetUserByQueryId'
	, N'usp_API_GetQueryResultByQueryName'
	, N'usp_Detail_GetListWithAdvancedSearch'
	, N'usp_V4_MobileAPI_GetMainDashboardData'
	, N'usp_DocumentCategory_GetForEmployee'
	, N'usp_MenuAccessForHiringLeadAndJobApprovers_GetList'
	, N'usp_V4_MobileAPI_GetPlannerDetailData'
	, N'usp_DocumentCategory_GetAllByCompanyId'
	, N'usp_QueryDefDBTypeFilter_GetById'
	, N'usp_Query_Get_Active'
	, N'usp_Query_GetAggregateFunction'
	, N'usp_Process_GetList'
	, N'usp_LogbookScreenHeader_GetLogbookScreenList'
	, N'usp_API_GetQueryResult'
	, N'usp_Detail_GetCompanyInfoById'
	, N'usp_Applicant_GetApplicantList'
	, N'usp_V4_MobileAPI_GetAllCompanyDirectoryEmployeeDetails'
	, N'usp_APIKey_GetByUniqueAPIKey'
	, N'usp_Query_UpdateFinalQuery'
)
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
WHERE ub.NormText in (
	N'usp_Process_GetUserByQueryId'
	, N'usp_API_GetQueryResultByQueryName'
	, N'usp_Detail_GetListWithAdvancedSearch'
	, N'usp_V4_MobileAPI_GetMainDashboardData'
	, N'usp_DocumentCategory_GetForEmployee'
	, N'usp_MenuAccessForHiringLeadAndJobApprovers_GetList'
	, N'usp_V4_MobileAPI_GetPlannerDetailData'
	, N'usp_DocumentCategory_GetAllByCompanyId'
	, N'usp_QueryDefDBTypeFilter_GetById'
	, N'usp_Query_Get_Active'
	, N'usp_Query_GetAggregateFunction'
	, N'usp_Process_GetList'
	, N'usp_LogbookScreenHeader_GetLogbookScreenList'
	, N'usp_API_GetQueryResult'
	, N'usp_Detail_GetCompanyInfoById'
	, N'usp_Applicant_GetApplicantList'
	, N'usp_V4_MobileAPI_GetAllCompanyDirectoryEmployeeDetails'
	, N'usp_APIKey_GetByUniqueAPIKey'
	, N'usp_Query_UpdateFinalQuery'
)
ORDER BY ub.NormText ASC;

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
WHERE ub.NormText in (
	N'usp_Process_GetUserByQueryId'
	, N'usp_API_GetQueryResultByQueryName'
	, N'usp_Detail_GetListWithAdvancedSearch'
	, N'usp_V4_MobileAPI_GetMainDashboardData'
	, N'usp_DocumentCategory_GetForEmployee'
	, N'usp_MenuAccessForHiringLeadAndJobApprovers_GetList'
	, N'usp_V4_MobileAPI_GetPlannerDetailData'
	, N'usp_DocumentCategory_GetAllByCompanyId'
	, N'usp_QueryDefDBTypeFilter_GetById'
	, N'usp_Query_Get_Active'
	, N'usp_Query_GetAggregateFunction'
	, N'usp_Process_GetList'
	, N'usp_LogbookScreenHeader_GetLogbookScreenList'
	, N'usp_API_GetQueryResult'
	, N'usp_Detail_GetCompanyInfoById'
	, N'usp_Applicant_GetApplicantList'
	, N'usp_V4_MobileAPI_GetAllCompanyDirectoryEmployeeDetails'
	, N'usp_APIKey_GetByUniqueAPIKey'
	, N'usp_Query_UpdateFinalQuery'
)
ORDER BY ub.NormText ASC;

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
WHERE ub.NormText in (
	N'usp_Process_GetUserByQueryId'
	, N'usp_API_GetQueryResultByQueryName'
	, N'usp_Detail_GetListWithAdvancedSearch'
	, N'usp_V4_MobileAPI_GetMainDashboardData'
	, N'usp_DocumentCategory_GetForEmployee'
	, N'usp_MenuAccessForHiringLeadAndJobApprovers_GetList'
	, N'usp_V4_MobileAPI_GetPlannerDetailData'
	, N'usp_DocumentCategory_GetAllByCompanyId'
	, N'usp_QueryDefDBTypeFilter_GetById'
	, N'usp_Query_Get_Active'
	, N'usp_Query_GetAggregateFunction'
	, N'usp_Process_GetList'
	, N'usp_LogbookScreenHeader_GetLogbookScreenList'
	, N'usp_API_GetQueryResult'
	, N'usp_Detail_GetCompanyInfoById'
	, N'usp_Applicant_GetApplicantList'
	, N'usp_V4_MobileAPI_GetAllCompanyDirectoryEmployeeDetails'
	, N'usp_APIKey_GetByUniqueAPIKey'
	, N'usp_Query_UpdateFinalQuery'
)
ORDER BY ub.NormText ASC;

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
WHERE ub.NormText in (
	N'usp_Process_GetUserByQueryId'
	, N'usp_API_GetQueryResultByQueryName'
	, N'usp_Detail_GetListWithAdvancedSearch'
	, N'usp_V4_MobileAPI_GetMainDashboardData'
	, N'usp_DocumentCategory_GetForEmployee'
	, N'usp_MenuAccessForHiringLeadAndJobApprovers_GetList'
	, N'usp_V4_MobileAPI_GetPlannerDetailData'
	, N'usp_DocumentCategory_GetAllByCompanyId'
	, N'usp_QueryDefDBTypeFilter_GetById'
	, N'usp_Query_Get_Active'
	, N'usp_Query_GetAggregateFunction'
	, N'usp_Process_GetList'
	, N'usp_LogbookScreenHeader_GetLogbookScreenList'
	, N'usp_API_GetQueryResult'
	, N'usp_Detail_GetCompanyInfoById'
	, N'usp_Applicant_GetApplicantList'
	, N'usp_V4_MobileAPI_GetAllCompanyDirectoryEmployeeDetails'
	, N'usp_APIKey_GetByUniqueAPIKey'
	, N'usp_Query_UpdateFinalQuery'
)
ORDER BY ub.NormText ASC;
