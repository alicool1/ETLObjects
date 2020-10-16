using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySql.Data;
using System.Data;
using System.IO;

namespace ETLObjects
{

    public class ExecuteMySqlTask
    {
        public MySqlConnection MySqlConnection;

        public ExecuteMySqlTask(MySqlConnection MySqlConnectionManager)
        {
            this.MySqlConnection = MySqlConnectionManager;
        }

        public int ExecuteNonQuery(string SQL)
        {
            MySqlCommand command = new MySqlCommand(SQL, MySqlConnection);
            command.CommandTimeout = 0;
            return command.ExecuteNonQuery();
        }

        public object ExecuteScalar(string SQL)
        {
            MySqlCommand command = new MySqlCommand(SQL, MySqlConnection);
            command.CommandTimeout = 0;
            return command.ExecuteScalar();
        }

        public void BulkInsert(IDataReader data, IColumnMappingCollection columnMapping, string tableName)
        {

            string LineTerminator = Environment.NewLine;
            string FieldTerminator = ";";

            string tmpFile = Guid.NewGuid().ToString();
            StreamWriter sw = new StreamWriter(tmpFile);

            while (data.Read())
            {
                for (int i = 0; i < data.FieldCount; i++)
                {
                    var o = data.GetValue(i);
                    sw.Write((i > 0 ? FieldTerminator : string.Empty) + (o == null ? string.Empty : o.ToString()));
                }
                sw.Write(LineTerminator);
            }

            sw.Flush();
            sw.Close();

            MySqlBulkLoader bulkCopy = new MySqlBulkLoader(MySqlConnection);
            bulkCopy.Timeout = 0;
            bulkCopy.TableName = tableName;
            bulkCopy.FileName = tmpFile;
            bulkCopy.Local = true;
            bulkCopy.FieldTerminator = FieldTerminator;
            bulkCopy.LineTerminator = LineTerminator;

            //    if (columnMapping != null) foreach (IColumnMapping colMap in columnMapping)
            //            bulkCopy.Add(colMap.SourceColumn, colMap.DataSetColumn);
            bulkCopy.Load();
            File.Delete(tmpFile);
        }
    }
}
