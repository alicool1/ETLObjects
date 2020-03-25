using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;
using System.Data;
using System.Data.SqlClient;

namespace ETLObjects {
    public class SqlDestination<DS> : IDataFlowDestination<DS> {

        public SqlConnection SqlConnection { get; set; }

        public string ObjectName { get; set; }

        public DataColumnMappingCollection ColumnMapping { get; set; }

        private EnumerableToDataReader<DS> _enumerableToDataReader
                = new EnumerableToDataReader<DS>();
        public EnumerableToDataReader<DS>.ObjectMapping ObjectMappingMethod { get; set; }
        public int FieldCount { get; set; }

        public void WriteBatch(DS[] batchData)
        {
            _enumerableToDataReader.EnumarableList = batchData;
            _enumerableToDataReader.ObjectMappingMethod = ObjectMappingMethod;
            _enumerableToDataReader.FieldCount = FieldCount;

            new ExecuteSQLTask(SqlConnection).BulkInsert(_enumerableToDataReader, ColumnMapping, ObjectName);
        }
        public override string ToString()
        {
            return ObjectName;
        }

    }

}
