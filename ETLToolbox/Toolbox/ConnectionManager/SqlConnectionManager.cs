using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace ETLObjects {
    public class SqlConnectionManager : DbConnectionManager<SqlConnection, SqlCommand> {

        public SqlConnectionManager() :base(){ } 

        public SqlConnectionManager(ConnectionString connectionString) : base(connectionString) { }

        public override void BulkInsert(IDataReader data, IColumnMappingCollection columnMapping, string tableName) {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(DbConnection, SqlBulkCopyOptions.TableLock, null)) {
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.DestinationTableName = tableName;
                foreach (IColumnMapping colMap in columnMapping)
                    bulkCopy.ColumnMappings.Add(colMap.SourceColumn, colMap.DataSetColumn);
                bulkCopy.WriteToServer(data);
            }
        }

        public override IDbConnectionManager Clone() {
            SqlConnectionManager clone = new SqlConnectionManager(ConnectionString) {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }


    }
}
