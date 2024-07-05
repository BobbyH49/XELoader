namespace XELoader
{
    class XEFile
    {
        private Int32 fileNumber;
        private FileInfo file;
        private DateTimeOffset startTimeOffset;
        private DateTimeOffset endTimeOffset;
        private Int32 batchSize;
        private string sqlConnectionStr;
        private string fullBatchesTableName;

        public XEFile(Int32 fileNumber, FileInfo file, XEReader xer)
        {
            this.fileNumber = fileNumber;
            this.file = file;
            this.startTimeOffset = xer.StartTimeOffset;
            this.endTimeOffset = xer.EndTimeOffset;
            this.batchSize = xer.BatchSize;
            this.sqlConnectionStr = xer.SqlConnectionStr;
            this.fullBatchesTableName = xer.FullBatchesTableName;
        }
        public Int32 FileNumber
        {
            get
            {
                return fileNumber;
            }
        }

        public FileInfo File
        {
            get
            {
                return file;
            }
        }
        public DateTimeOffset StartTimeOffset
        {
            get
            {
                return startTimeOffset;
            }
        }
        public DateTimeOffset EndTimeOffset
        {
            get
            {
                return endTimeOffset;
            }
        }
        public Int32 BatchSize
        {
            get
            {
                return batchSize;
            }
        }
        public string SqlConnectionStr
        {
            get
            {
                return sqlConnectionStr;
            }
        }
        public string FullBatchesTableName
        {
            get
            {
                return fullBatchesTableName;
            }
        }
    }
}
