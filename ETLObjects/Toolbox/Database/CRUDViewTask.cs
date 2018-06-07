using System;


namespace ETLObjects {
    public class CRUDViewTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "CRUDVIEW";
        public override string TaskName => $"{CreateOrAlterSql} view {ViewName}";
        public override void Execute() {
            IsExisting = new SqlTask(this, CheckIfExistsSql) { TaskName = $"Check if view {ViewName} exists", TaskHash = this.TaskHash }.ExecuteScalarAsBool();
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string ViewName { get; set; }
        public string Definition { get; set; }
        public string Sql => $@"{CreateOrAlterSql} view {ViewName}
as
{Definition}
";

        public CRUDViewTask() {

        }
        public CRUDViewTask(string viewName, string definition) : this() {
            this.ViewName = viewName;
            this.Definition = definition;
        }

        public static void CreateOrAlter(string viewName, string definition) => new CRUDViewTask(viewName, definition).Execute();

        string CheckIfExistsSql => $@"if exists (select * from sys.objects where type = 'V' and object_id = object_id('{ViewName}')) select 1; 
else select 0;";
        bool IsExisting { get; set; }
        string CreateOrAlterSql => IsExisting ? "Alter" : "Create";

    }
}
