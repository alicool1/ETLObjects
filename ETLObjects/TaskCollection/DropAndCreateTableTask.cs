using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace ETLObjects
{
    public class DropAndCreateTableTask
    {
        private SqlConnection SqlConnection;

        public DropAndCreateTableTask(SqlConnection SqlConnection)
        {
            this.SqlConnection = SqlConnection;
        }

        public void Execute(string SchemaName, string TableName, List<TableColumn> TableColumns)
        {
            new DropTableTask(SqlConnection).Execute(SchemaName, TableName);
            new CreateTableTask(SqlConnection).Execute(SchemaName, TableName, TableColumns);
        }

    }
}
