using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ALE.ETLToolbox {
    public class TableColumn : ITableColumn {
        public string Name { get; set; }
        public string DataType { get; set; }
        public bool AllowNulls { get; set; }
        public bool IsIdentity { get; set; }
        public int? IdentitySeed { get; set; }
        public int? IdentityIncrement { get; set; }
        public bool IsPrimaryKey { get; set; }
        public string DefaultValue { get; set; }
        public string DefaultConstraintName { get; set; }
        public string Collation { get; set; }
        public string ComputedColumn { get; set; }


        public TableColumn() { }
        public TableColumn(string name, string dataType) : this() {
            Name = name;
            DataType = dataType;
        }

        public TableColumn(string name, string dataType, bool allowNulls) : this(name, dataType) {
            AllowNulls = allowNulls;
        }

        public TableColumn(string name, string dataType, bool allowNulls, bool isPrimaryKey) : this(name, dataType, allowNulls) {
            IsPrimaryKey = isPrimaryKey;
        }

        public TableColumn(string name, string dataType, bool allowNulls, bool isPrimaryKey, bool isIdentity) : this(name, dataType, allowNulls, isPrimaryKey) {
            IsIdentity = isIdentity;
        }
    }
}
