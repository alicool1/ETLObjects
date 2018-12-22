using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace ETLObjects
{
    public class DropTableTask
    {
        private SqlConnection SqlConnection;

        public DropTableTask(SqlConnection SqlConnection)
        {
            this.SqlConnection = SqlConnection;
        }

        public void Execute(string SchemaName, string TableName)
        {
        
            string Sql = $"begin try drop TABLE [{SchemaName}].[{TableName}] ";
            Sql += Environment.NewLine + $" end try begin catch end catch";
            Run(Sql);
        }

        public void Execute(string ObjectName)
        {

            string Sql = $"begin try drop TABLE {ObjectName} ";
            Sql += Environment.NewLine + $" end try begin catch end catch";
            Run(Sql);
            
        }

        private void Run(string sql)
        {
            new ExecuteSQLTask(SqlConnection).ExecuteNonQuery(sql);
        }



    }
}
