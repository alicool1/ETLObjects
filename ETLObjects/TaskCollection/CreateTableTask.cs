using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;


namespace ETLObjects
{
    public class CreateTableTask
    {
        private SqlConnection SqlConnection;

        public CreateTableTask(SqlConnection SqlConnectionManager)
        {
            this.SqlConnection = SqlConnectionManager;
        }

        public void Execute(string SchemaName, string TableName, List<TableColumn> TableColumns)
        {
        
            string Sql = $"begin try CREATE TABLE [{SchemaName}].[{TableName}](";

            string ColumnsString = string.Empty;
            foreach (TableColumn tableColumn in TableColumns)
            {
                ColumnsString += Environment.NewLine;
                ColumnsString += $"[{tableColumn.Name}]";
                ColumnsString += $" {tableColumn.getDataBaseType()}";

                if (tableColumn.IsIdentity) ColumnsString += $" IDENTITY(1, 1)";
                if (tableColumn.IsPrimaryKeyColumn) ColumnsString += $" PRIMARY KEY";


                ColumnsString += $"{(tableColumn.IsNullable ? string.Empty : " NOT")} NULL,";
            }

            Sql += ColumnsString;

            Sql += Environment.NewLine + $") ON[PRIMARY] end try begin catch end catch";

            new ExecuteSQLTask(SqlConnection).ExecuteNonQuery(Sql);
        }



    }
}
