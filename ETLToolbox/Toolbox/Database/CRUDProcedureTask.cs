using System;
using System.Collections.Generic;
using System.Linq;

namespace ALE.ETLToolbox {
    public class CRUDProcedureTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "CRUDPROC";
        public override string TaskName => $"{CreateOrAlterSql} procedure {ProcedureName}";
        public override void Execute() {
            IsExisting = new SqlTask(this, CheckIfExistsSql) { TaskName = $"Check if procedure {ProcedureName} exists", TaskHash = this.TaskHash }.ExecuteScalarAsBool();
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string ProcedureName { get; set; }
        public string ProcedureDefinition { get; set; }
        public IList<ProcedureParameter> ProcedureParameters { get; set; }
        public string Sql => $@"{CreateOrAlterSql} procedure {ProcedureName}
{ParameterDefinition}
as
begin
set nocount on

{ProcedureDefinition}

end --End of Procedure
        ";

        public CRUDProcedureTask() {
            
        }
        public CRUDProcedureTask(string procedureName, string procedureDefinition) : this() {
            this.ProcedureName = procedureName;
            this.ProcedureDefinition = procedureDefinition;
        }

        public CRUDProcedureTask(string procedureName, string procedureDefinition, IList<ProcedureParameter> procedureParameter) : this(procedureName, procedureDefinition) {
            this.ProcedureParameters = procedureParameter;
        }

        public static void CreateOrAlter(string procedureName, string procedureDefinition) => new CRUDProcedureTask(procedureName, procedureDefinition).Execute();
        public static void CreateOrAlter(string procedureName, string procedureDefinition, IList<ProcedureParameter> procedureParameter) 
            => new CRUDProcedureTask(procedureName, procedureDefinition, procedureParameter).Execute();

        string CheckIfExistsSql => $@"if exists (select * from sys.objects where type = 'P' and object_id = object_id('{ProcedureName}')) select 1; 
else select 0;";
        bool IsExisting { get; set; }
        string CreateOrAlterSql => IsExisting ? "Alter" : "Create";
        string ParameterDefinition => ProcedureParameters?.Count > 0 ?
                String.Join("," + Environment.NewLine, ProcedureParameters.Select(par => par.Sql))
                : String.Empty;

    }
}
