using System;


namespace ETLObjects {
    public class CreateSchemaTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "CREATESCHEMA";
        public override string TaskName => $"Create schema {SchemaName}";
        public override void Execute() => new SqlTask(this, Sql).ExecuteNonQuery();

        /* Public properties */
        public string SchemaName { get; set; }
        public string Sql => $@"if not exists (select schema_name(schema_id) from sys.schemas where schema_name(schema_id) = '{SchemaName}')
begin
	exec sp_executesql N'create schema [{SchemaName}]'
end";

        public CreateSchemaTask() {

        }
        public CreateSchemaTask(string schemaName) : this() {
            this.SchemaName = schemaName;
        }

        public static void Create(string schemaName) => new CreateSchemaTask(schemaName).Execute();


    }
}
