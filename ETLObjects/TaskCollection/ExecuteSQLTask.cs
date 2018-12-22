using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace ETLObjects
{

    public class ExecuteSQLTask
    {
        public SqlConnection SqlConnection;

        public ExecuteSQLTask(SqlConnection SqlConnectionManager)
        {
            this.SqlConnection = SqlConnectionManager;
        }

        public int ExecuteNonQuery(string SQL)
        {
            SqlCommand command = new SqlCommand(SQL, SqlConnection);
            command.CommandTimeout = 0;
            return command.ExecuteNonQuery();
        }

        public object ExecuteScalar(string SQL)
        {
            SqlCommand command = new SqlCommand(SQL, SqlConnection);
            command.CommandTimeout = 0;
            return command.ExecuteScalar();
        }

        public void BulkInsert(IDataReader data, IColumnMappingCollection columnMapping, string tableName)
        {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(SqlConnection, SqlBulkCopyOptions.TableLock, null))
            {
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.DestinationTableName = tableName;
                if (columnMapping != null) foreach (IColumnMapping colMap in columnMapping)
                        bulkCopy.ColumnMappings.Add(colMap.SourceColumn, colMap.DataSetColumn);
                bulkCopy.WriteToServer(data);
            }
        }
    }
}
