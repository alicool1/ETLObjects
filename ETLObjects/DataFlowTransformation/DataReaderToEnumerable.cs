using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using CsvHelper;
using MySql.Data.MySqlClient;

namespace ETLObjects
{
    public class DataReaderToEnumerable<DS> : IEnumerable<DS>
    {
        /// <summary>
        /// SQL-DataReader of IDataRecords to be convert to IEnumerable
        /// </summary>
        public SqlDataReader SqlDataReader { get; set; }

        public MySqlDataReader MySqlDataReader { get; set; }

        /// <summary>
        /// Csv-DataReader of IDataRecords to be convert to IEnumerable
        /// </summary>
        public CsvReader CsvReader  { get; set; }

    /// <summary>
    /// Delegate-Typ für Methoden, die einen IDataRecord (eines Datareaders) in ein IEnumerable Klassenobjekt vom Typen DS umwandeln kann.
    /// </summary>
    /// <param name="record"></param>
    /// <returns></returns>
    public delegate DS DataMapping(IDataRecord record);

        /// <summary>
        /// Enthält die Datenadapter-Methode, die das IDataRecord in eine Objekt vom Typen DS umwandelt
        /// </summary>
        public DataMapping DataMappingMethod { get; set; }

        /// <summary>
        /// zur Iteration ueber eine generische Auflistung
        /// </summary>
        /// <returns></returns>
        IEnumerator<DS> IEnumerable<DS>.GetEnumerator()
        {


            //if (SqlDataReader == null) throw new ArgumentException("Die SqlDataReader-Eigenschaft wurde nicht gesetzt.");

            if (SqlDataReader != null)
            {
                var x = (from IDataRecord record in SqlDataReader select DataMappingMethod(record));
                IEnumerator<DS> enusm = x.GetEnumerator();
                return enusm;
            }
            else if (MySqlDataReader != null)
            {
                var x = (from IDataRecord record in MySqlDataReader select DataMappingMethod(record));
                IEnumerator<DS> enusm = x.GetEnumerator();
                return enusm;
            }
            throw new ArgumentException("Die DataReader-Eigenschaft wurde nicht gesetzt.");

            //if (CsvReader == null) throw new ArgumentException("Die CsvReader-Eigenschaft wurde nicht gesetzt.");

            //var x = (from ICsvReader record in CsvReader select DataMappingMethod(record));
            //IEnumerator<DS> enusm = x.GetEnumerator();
            //return enusm;
        }

        /// <summary>
        /// zur Iteration ueber eine nicht-generische Auflistung
        /// </summary>
        /// <returns></returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<DS>)this).GetEnumerator();
        }
    }
}
