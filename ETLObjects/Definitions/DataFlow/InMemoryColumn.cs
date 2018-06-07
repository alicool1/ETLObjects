using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects {
    public class InMemoryColumn : IColumnMapping {
        public System.Type NETDataType => Type.GetType(DataTypeConverter.GetObjectTypeString(DataType));

        public string DataType { get; set; }
        public string Name { get; set; }

        public bool AllowNulls { get; set; }

        string _DataSetColumn;
        public string DataSetColumn {
            get {
                return _DataSetColumn ?? Name;
            }

            set {
                _DataSetColumn = value;
            }
        }

        string _SourceColumn;
        public string SourceColumn {
            get {
                return _SourceColumn ?? Name;
            }
            set {
                _SourceColumn = value;
            }
        }

        public InMemoryColumn() { }

        public InMemoryColumn(ITableColumn tableColumn) : this() {
            this.DataType = tableColumn.DataType;
            this.Name = tableColumn.Name;
            this.AllowNulls = tableColumn.AllowNulls;
        }
    }

}
