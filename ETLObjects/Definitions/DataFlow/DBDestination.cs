using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;
using System.Data;

namespace ETLObjects {
    public class DBDestination<DS> : IDataFlowDestination<DS> {
        public IConnectionManager Connection { get; set; }

        public string TableName_Target { get; set; }

        public DataColumnMappingCollection ColumnMapping { get; set; }

        private EnumerableToDataReader<DS> _enumerableToDataReader
                = new EnumerableToDataReader<DS>();
        public EnumerableToDataReader<DS>.ObjectMapping ObjectMappingMethod { get; set; }
        public int FieldCount { get; set; }

        private int _MaxBufferSize = 10000;
        public int MaxBufferSize
        {
            get
            {
                return _MaxBufferSize;
            }
            set
            {
                if (value > 0)
                {
                    _MaxBufferSize = value;
                }
            }
        }

        public void WriteBatch(InMemoryTable batchData) {
            new SqlTask($"Execute Bulk insert into {TableName_Target}") { ConnectionManager = Connection }.BulkInsert(batchData, batchData.ColumnMapping, TableName_Target);
        }

        public void WriteBatch(DS[] batchData)
        {
            _enumerableToDataReader.EnumarableList = batchData;
            new SqlTask($"Execute Bulk insert into {TableName_Target}") { ConnectionManager = Connection }.BulkInsert(_enumerableToDataReader, ColumnMapping, TableName_Target);
        }

        public void Open()
        {

            _enumerableToDataReader.ObjectMappingMethod = ObjectMappingMethod;
            _enumerableToDataReader.FieldCount = FieldCount;

        }

        public override string ToString()
        {
            return TableName_Target;
        }
    }

}
