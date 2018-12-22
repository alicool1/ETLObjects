using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace ETLObjects
{
    public class CreateSchemaTask
    {
        private SqlConnection SqlConnection;

        public CreateSchemaTask(SqlConnection SqlConnection)
        {
            this.SqlConnection = SqlConnection;
        }

        public void Create(string SchemaName)
        {
            
            string Sql = $@"if not exists (select schema_name(schema_id) from sys.schemas where schema_name(schema_id) = '{SchemaName}')
begin
	exec sp_executesql N'create schema [{SchemaName}]'
end";

            new ExecuteSQLTask(SqlConnection).ExecuteNonQuery(Sql);
        }
    }
}
