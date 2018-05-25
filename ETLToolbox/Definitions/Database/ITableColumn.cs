using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects {
    public interface ITableColumn {
        string Name { get; }
        string DataType { get; }
        bool AllowNulls { get; }
        bool IsIdentity { get; }
        int? IdentitySeed { get; }
        int? IdentityIncrement { get; }
        bool IsPrimaryKey { get; }
        string DefaultValue { get; }
        string DefaultConstraintName { get; }
        string Collation { get; }
        string ComputedColumn { get; }

    }
}
