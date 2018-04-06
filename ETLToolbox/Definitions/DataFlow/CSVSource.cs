using ALE.ETLToolbox;
using CsvHelper;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLToolbox {
    public class CSVSource : IDisposable {

        public CsvReader CsvReader { get; set; }
        public int SourceCommentRows { get; set; } = 0;
        public bool TrimFields { get; set; } = true;
        public bool TrimHeaders { get; set; } = true;
        public string Delimiter { get; set; } = ",";
        public char Quote { get; set; } = '"';
        public bool AllowComments { get; set; } = true;
        public char Comment { get; set; } = '/';
        public bool SkipEmptyRecords = true;
        public bool IgnoreBlankLines = true;
        string FileName { get; set; }
        public string[] FieldHeaders {
            get {
                return CsvReader.FieldHeaders.Select(header => header.Trim()).ToArray();
            }
        }
        public bool IsHeaderRead => CsvReader.FieldHeaders != null;
        
        public CSVSource() { }

        StreamReader StreamReader { get; set; }

        public CSVSource(string fileName) {
            FileName = fileName;
        }

        public void Open() {
            StreamReader = new StreamReader(FileName, Encoding.UTF8);
            SkipSourceCommentRows();
            CsvReader = new CsvReader(StreamReader);
            ConfigureCSVReader();
        }
        private void SkipSourceCommentRows() {
            for (int i = 0; i < SourceCommentRows; i++)
                StreamReader.ReadLine();
        }

        public async void Read(ITargetBlock<string[]> target) {
            while (CsvReader.Read()) {
                string[] line = new string[CsvReader.CurrentRecord.Length];
                for (int idx = 0; idx < CsvReader.CurrentRecord.Length; idx++)
                    line[idx] = CsvReader.GetField(idx);
                await target.SendAsync(line);
            }
        }
        private void ConfigureCSVReader() {
            CsvReader.Configuration.Delimiter = Delimiter;
            CsvReader.Configuration.Quote = Quote;
            CsvReader.Configuration.AllowComments = AllowComments;
            CsvReader.Configuration.Comment = Comment;
            CsvReader.Configuration.SkipEmptyRecords = SkipEmptyRecords;
            CsvReader.Configuration.IgnoreBlankLines = IgnoreBlankLines;
            CsvReader.Configuration.TrimHeaders = TrimHeaders;
            CsvReader.Configuration.TrimFields = TrimFields;
            CsvReader.Configuration.Encoding = Encoding.UTF8;
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing) {
            if (!disposedValue) {
                if (disposing) {
                    if (CsvReader != null)
                        CsvReader.Dispose();
                    CsvReader = null;
                    if (StreamReader != null) StreamReader.Dispose();
                    StreamReader = null;
                }
                disposedValue = true;
            }
        }
        public void Close() {
            Dispose();

        }
        public void Dispose() {
            Dispose(true);
        }
        #endregion

    }
}
