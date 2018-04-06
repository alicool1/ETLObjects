using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ALE.ETLToolbox {
    public class InMemoryTable : IDataReader, IDisposable {
        public int? EstimatedBatchSize { get; set; }
        public DataColumnMappingCollection ColumnMapping {
            get {
                var mapping = new DataColumnMappingCollection();
                foreach (var col in Columns)
                    mapping.Add(new DataColumnMapping(col.SourceColumn, col.DataSetColumn));
                return mapping;
            }
        }
        public bool HasIdentityColumn { get; set; }
        public List<string[]> Rows { get; set; }
        public List<InMemoryColumn> Columns { get; set; }
        public string[] CurrentRow { get; set; }
        int ReadIndex { get; set; }

        public InMemoryTable() {
            Columns = new List<InMemoryColumn>();
            Rows = new List<string[]>();
        }

        public InMemoryTable(int estimatedBatchSize) {
            Columns = new List<InMemoryColumn>();
            Rows = new List<string[]>(estimatedBatchSize);
        }

        public string[] NewRow() => new string[Columns.Count];
        public object this[string name] => Rows[GetOrdinal(name)];
        public object this[int i] => Rows[i];
        public int Depth => 0;
        public int FieldCount => Rows.Count;
        public bool IsClosed => Rows.Count == 0;
        public int RecordsAffected => Rows.Count;
        public bool GetBoolean(int i) => Convert.ToBoolean(CurrentRow[i]);
        public byte GetByte(int i) => Convert.ToByte(CurrentRow[i]);
        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => 0;
        public char GetChar(int i) => Convert.ToChar(CurrentRow[i]);
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) {
            string value = Convert.ToString(CurrentRow[i]);
            buffer = value.Substring(bufferoffset, length).ToCharArray();
            return buffer.Length;

        }
        public DateTime GetDateTime(int i) => Convert.ToDateTime(CurrentRow[i]);
        public IDataReader GetData(int i) => null;
        public string GetDataTypeName(int i) => Columns[i].NETDataType.Name;
        public decimal GetDecimal(int i) => Convert.ToDecimal(CurrentRow[i]);
        public double GetDouble(int i) => Convert.ToDouble(CurrentRow[i]);
        public Type GetFieldType(int i) => Columns[i].NETDataType;
        public float GetFloat(int i) => float.Parse(Convert.ToString(CurrentRow[i]));
        public Guid GetGuid(int i) => Guid.Parse(Convert.ToString(CurrentRow[i]));
        public short GetInt16(int i) => Convert.ToInt16(CurrentRow[i]);
        public int GetInt32(int i) => Convert.ToInt32(CurrentRow[i]);
        public long GetInt64(int i) => Convert.ToInt64(CurrentRow[i]);
        public string GetName(int i) => Columns[i].Name;
        public int GetOrdinal(string name) => this.Columns.FindIndex(col => col.Name == name);
        public DataTable GetSchemaTable() {
            throw new NotImplementedException();
        }
        public string GetString(int i) => Convert.ToString(CurrentRow[i]);
        public object GetValue(int i) => CurrentRow[i];

        public int GetValues(object[] values) {
            values = CurrentRow;
            return values.Length;
        }

        public bool IsDBNull(int i) {
            if (Columns[i].AllowNulls)
                return CurrentRow[i] == null;
            else
                return false;
        }

        public bool NextResult() {
            return Rows?.Count > (ReadIndex + 1);
        }

        public bool Read() {
            if (Rows?.Count > ReadIndex) {
                CurrentRow = Rows[ReadIndex];
                ReadIndex++;
                return true;
            }
            else
                return false;
        }

        #region IDisposable Support
        private bool disposedValue = false;
        protected virtual void Dispose(bool disposing) {
            if (!disposedValue) {
                if (disposing) {
                    Rows = null;
                }

                disposedValue = true;
            }
        }

        public void Dispose() {
            Dispose(true);
        }

        public void Close() {
            Dispose();
        }
        #endregion
    }
}
