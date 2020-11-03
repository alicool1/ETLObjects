using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Reflection;

namespace ETLObjects
{
    public class EnumerableToDataReader<DS> : IDataReader
    {
        protected object[] Values;
        protected bool Eof { get; set; }
        protected DS CurrentRecord { get; set; }
        protected int CurrentIndex { get; set; }

        /// <summary>
        /// Delegate-Typ für Methoden, die ein Klassenobjekt vom Typ DS in einen Value-Object-Array eines Datareaders umwandeln kann. 
        /// </summary>
        /// <param name="record"></param>
        /// <param name="fieldCount"></param>
        /// <returns></returns>
        public delegate object[] ObjectMapping(DS record);

        /// <summary>
        /// Enthält die Methode, die das Value-Objekt-Feld  aus einem Klassenobjekt vom Typen DS auffüllt.
        /// </summary>
        public ObjectMapping ObjectMappingMethod { get; set; }

        public object this[string name]
        {
            get { throw new NotImplementedException(); }
        }

        public object this[int i]
        {
            get { throw new NotImplementedException(); }
        }

        private int _fieldCount;

        private IEnumerator<DS> _enumerator = null;
        private IEnumerable<DS> _enumarable = null;

        public IEnumerable<DS> EnumarableList
        {
            get
            {
                return _enumarable;
            }
            set
            {
                _enumarable = value;
                _enumerator = _enumarable.GetEnumerator();
            }
        }


        public EnumerableToDataReader(IEnumerable<DS> list, short fieldCount)
        {
            _enumarable = list;
            _enumerator = list.GetEnumerator();

            if (fieldCount > 0) _fieldCount = fieldCount;
            else throw new ArgumentException("Das FieldCount-Argument darf nicht 0 sein.");

            Values = new object[FieldCount];
        }

        public void SetFieldCount(int value)
        {
            _fieldCount = value;
        }

        public EnumerableToDataReader()
        {

        }



        public void Close()
        {
            Array.Clear(Values, 0, Values.Length);
        }

        public int Depth
        {
            get { return 0; }
        }

        public Decimal GetDecimal(int i) { throw new NotImplementedException(); }
        public DateTime GetDateTime(int i) { throw new NotImplementedException(); }
        public Double GetDouble(int i) { throw new NotImplementedException(); }
        public Int16 GetInt16(int i) { throw new NotImplementedException(); }
        public Int32 GetInt32(int i) { throw new NotImplementedException(); }
        public Int64 GetInt64(int i) { throw new NotImplementedException(); }
        public Boolean IsDBNull(int i) { throw new NotImplementedException(); }
        public float GetFloat(int i) { throw new NotImplementedException(); }
        public Guid GetGuid(int i) { throw new NotImplementedException(); }
        public Char GetChar(int i) { throw new NotImplementedException(); }
        public Byte GetByte(int i) { throw new NotImplementedException(); }
        public Boolean GetBoolean(int i) { throw new NotImplementedException(); }
        public int GetOrdinal(string Feld) {

            FieldInfo[] f = EnumarableList.GetType().GetElementType().GetFields();

            for (int ix = 0; ix < f.Count(); ix++) if (f[ix].Name == Feld) return ix;

            throw new Exception($"Feld {Feld} nicht in EnumarableList bei GetOrdinal() in EnumerableToDataReader gefunden.");

        }
        public Type GetFieldType(int i) { throw new NotImplementedException(); }
        public Int64 GetChars(int i, long dataIndex, char[] buffer, int bufferIndex, int length) { throw new NotImplementedException(); }
        public Int64 GetBytes(int i, long dataIndex, byte[] buffer, int bufferIndex, int length) { throw new NotImplementedException(); }

        public void Dispose(bool i) { throw new NotImplementedException(); }
        public void Dispose()
        {

        }



        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public bool IsClosed
        {
            get { return Eof; }
        }

        public bool NextResult()
        {
            return false;
        }

        public bool Read()
        {

            if (_enumerator.MoveNext())
            {
                CurrentRecord = (DS)_enumerator.Current;
                GetValues(Values);
                return true;
            }
            else return false;
        }

        private void Fill(object[] values)
        {
            if (ObjectMappingMethod == null)
                throw new NullReferenceException("ObjectMappingMethod is null.");

            values = ObjectMappingMethod(CurrentRecord);
            _fieldCount = values.Count();

            if (Values == null) Values = new object[_fieldCount];
            Array.Copy(values, Values, _fieldCount);
        }

        public int RecordsAffected
        {
            get { return -1; }
        }

        public int FieldCount
        {
            get { return _fieldCount; }
            set { _fieldCount = value; }
        }

        public IDataReader GetData(int i)
        {
            if (i == 0)
                return this;

            return null;
        }

        public string GetDataTypeName(int i)
        {
            return "String";
        }

        public string GetName(int i)
        {
            return Values[i].ToString();
        }

        public string GetString(int i)
        {
            return Values[i].ToString();
        }

        public object GetValue(int i)
        {
            return Values[i];
        }

        public int GetValues(object[] values)
        {
            Fill(values);

            return this.FieldCount;
        }
    }
}
