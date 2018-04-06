using System;

namespace ALE.ETLToolbox {
    public class ProcedureParameter {
        public string Name { get; set; }
        public string DataType { get; set; }
        public string DefaultValue { get; set; }
        public bool HasDefaultValue => !String.IsNullOrWhiteSpace(DefaultValue);
        public bool ReadOnly { get; set; }
        public bool Out { get; set; }
        public string Sql {
            get {
                string sql = $@"@{Name} {DataType}";
                if (HasDefaultValue)
                    sql += $" = {DefaultValue}";
                if (Out)
                    sql += " OUT";
                if (ReadOnly)
                    sql += " READONLY";
                return sql;
            }
        }

        public ProcedureParameter() {
        }

        public ProcedureParameter(string name, string dataType) : this() {
            Name = name;
            DataType = dataType;
        }

        public ProcedureParameter(string name, string dataType, string defaultValue) : this(name, dataType) {
            DefaultValue = defaultValue;
        }

    }
}
