using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;
using MySql.Data.MySqlClient;

namespace ETLObjects {
    public class MySqlDestination<DS> : IDataFlowDestination<DS> {

        public string FieldTerminator { get; set; }
        public MySqlConnection MySqlConnection { get; set; }

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

            new ExecuteMySqlTask(MySqlConnection).BulkInsert(_enumerableToDataReader, ColumnMapping, ObjectName, FieldTerminator: "|");
        }
        public override string ToString()
        {
            return ObjectName;
        }

    }

}
