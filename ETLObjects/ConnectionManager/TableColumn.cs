using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public class TableColumn
    {
       

        private void SetDefaults()
        {
            this.IsIdentity = false;
            this.IsNullable = true;
            this.IsPrimaryKeyColumn = false;
            this.Size = 0;
            this.SqlDbType = SqlDbType.Int;
            this.Precision = 0;
            this.Scale = 0;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="Name">gets or sets the Name of the TableColumn</param>
        /// <param name="SqlDbType">gets or sets the SqlDbType of the TableColumn</param>
        /// <param name="Size">gets or sets the size (in bytes) of the data within the column</param>
        public TableColumn(string Name, SqlDbType SqlDbType, int Size)
        {
            SetDefaults();
            this.Name = Name;
            this.SqlDbType = SqlDbType;
            this.Size = Size;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="Name">gets or sets the Name of the TableColumn</param>
        /// <param name="SqlDbType">gets or sets the SqlDbType of the TableColumn</param>
        /// <param name="Size">gets or sets the size (in bytes) of the data within the column</param>
        /// <param name="IsNullable">gets or sets the vlaue that indicate wheter the TableColumn accepts NULL-values</param>
        public TableColumn(string Name, SqlDbType SqlDbType, int Size, bool IsNullable)
        {
            SetDefaults();
            this.Name = Name;
            this.SqlDbType = SqlDbType;
            this.Size = Size;
            this.IsNullable = IsNullable;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="Name">gets or sets the Name of the TableColumn</param>
        /// <param name="SqlDbType">gets or sets the SqlDbType of the TableColumn</param>
        /// <param name="Precision"></param>
        /// <param name="Scale"></param>
        /// <param name="IsNullable">gets or sets the vlaue that indicate wheter the TableColumn accepts NULL-values</param>
        public TableColumn(string Name, SqlDbType SqlDbType, int Precision, int Scale, bool IsNullable)
        {
            SetDefaults();
            this.Name = Name;
            this.SqlDbType = SqlDbType;
            this.Precision = Precision;
            this.Scale = Scale;
            this.IsNullable = IsNullable;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="Name">gets or sets the Name of the TableColumn</param>
        /// <param name="SqlDbType">gets or sets the SqlDbType of the TableColumn</param>
        /// <param name="IsNullable">gets or sets the vlaue that indicate wheter the TableColumn accepts NULL-values</param>
        public TableColumn(string Name, SqlDbType SqlDbType, bool IsNullable)
        {
            SetDefaults();
            this.Name = Name;
            this.SqlDbType = SqlDbType;
            this.IsNullable = IsNullable;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="Name">gets or sets the Name of the TableColumn</param>
        /// <param name="SqlDbType">gets or sets the SqlDbType of the TableColumn</param>
        /// <param name="IsNullable">gets or sets the vlaue that indicate wheter the TableColumn accepts NULL-values</param>
        /// <param name="IsIdentity"></param>
        /// <param name="IsPrimaryKeyColumn"></param>
        public TableColumn(string Name, SqlDbType SqlDbType, bool IsNullable, bool IsIdentity, bool IsPrimaryKeyColumn)
        {
            SetDefaults();
            this.Name = Name;
            this.SqlDbType = SqlDbType;
            this.IsNullable = IsNullable;
            this.IsIdentity = IsIdentity;
            this.IsPrimaryKeyColumn = IsPrimaryKeyColumn;
        }


        private string _Name;
        public string Name
        {
            get
            {
                return _Name;
            }
            set
            {
                _Name = value;
            }
        }


        private bool _IsPrimaryKeyColumn;
        public bool IsPrimaryKeyColumn
        {
            get
            {
                return _IsPrimaryKeyColumn;
            }
            set
            {
                _IsPrimaryKeyColumn = value;
            }
        }

        private bool _IsIdentity;
        public bool IsIdentity
        {
            get
            {
                return _IsIdentity;
            }
            set
            {
                _IsIdentity = value;
            }
        }

        private bool _IsNullable;
        public bool IsNullable
        {
            get
            {
                return _IsNullable;
            }
            set
            {
                _IsNullable = value;
            }
        }

        private int _Size;
        public int Size
        {
            get
            {
                return _Size;
            }
            set
            {
                _Size = (value < 0) ? 0 : value;
            }
        }


        private int _Precision;
        public int Precision
        {
            get
            {
                return _Precision;
            }
            set
            {
                _Precision = (value < 0) ? 0 : value;
            }
        }

        private int _Scale;
        public int Scale
        {
            get
            {
                return _Scale;
            }
            set
            {
                _Scale = (value < 0) ? 0 : value;
            }
        }


        private SqlDbType _SqlDbType;
        public SqlDbType SqlDbType
        {
            get
            {
                return _SqlDbType;
            }
            set
            {
                _SqlDbType = value;
            }
        }


        public string getDataBaseType()
        {
            if (SqlDbType == SqlDbType.BigInt) return "[bigint]";
            if (SqlDbType == SqlDbType.Bit) return "[bit]";
            if (SqlDbType == SqlDbType.Char) return $"[char]({Size})";
            if (SqlDbType == SqlDbType.Date) return "[date]";
            if (SqlDbType == SqlDbType.DateTime) return "[datetime]";
            if (SqlDbType == SqlDbType.DateTime2) return "[datetime2]";
            if (SqlDbType == SqlDbType.DateTimeOffset) return "[datetimeoffset]";
            if (SqlDbType == SqlDbType.Decimal) return $"[decimal]({Precision},{Scale})";
            if (SqlDbType == SqlDbType.VarBinary) return $"[varbinary]({Size})";
            if (SqlDbType == SqlDbType.Float) return "[float]";
            if (SqlDbType == SqlDbType.Binary) return "[image]";
            if (SqlDbType == SqlDbType.Int) return "[int]";
            if (SqlDbType == SqlDbType.Money) return "[money]";
            if (SqlDbType == SqlDbType.NChar) return $"[nchar]({Size})";
            if (SqlDbType == SqlDbType.NText) return "[ntext]";
            if (SqlDbType == SqlDbType.Decimal) return $"[numeric]({Precision},{Scale})";
            if (SqlDbType == SqlDbType.NVarChar) return $"[nvarchar]({Size})";
            if (SqlDbType == SqlDbType.Real) return "[real]";
            if (SqlDbType == SqlDbType.Timestamp) return "[rowversion]";
            if (SqlDbType == SqlDbType.DateTime) return "[smalldatetime]";
            if (SqlDbType == SqlDbType.SmallInt) return "[smallint]";
            if (SqlDbType == SqlDbType.SmallMoney) return "[smallmoney]";
            if (SqlDbType == SqlDbType.Variant) return "[sql_variant]";
            if (SqlDbType == SqlDbType.Text) return "[text]";
            if (SqlDbType == SqlDbType.Time) return "[time]";
            if (SqlDbType == SqlDbType.Timestamp) return "[timestamp]";
            if (SqlDbType == SqlDbType.TinyInt) return "[tinyint]";
            if (SqlDbType == SqlDbType.UniqueIdentifier) return "[uniqueidentifier]";
            if (SqlDbType == SqlDbType.VarBinary) return $"[varbinary]({Size})";
            if (SqlDbType == SqlDbType.VarChar) return $"[varchar]({Size})";
            if (SqlDbType == SqlDbType.Xml) return "[xml]";

            return null;
        }
    }
}
