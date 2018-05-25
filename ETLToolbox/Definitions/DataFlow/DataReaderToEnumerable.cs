using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using CsvHelper;

namespace ETLObjects
{
    public class DataReaderToEnumerable<DS> : IEnumerable<DS>
    {
        /// <summary>
        /// Enthält den DataReader (bisher nur SQL) des IDataRecords in ein IEnumerable umgewandelt werden soll.
        /// </summary>
        public SqlDataReader SqlDataReader { get; set; }

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
            if (SqlDataReader == null) throw new ArgumentException("Die SqlDataReader-Eigenschaft wurde nicht gesetzt.");

            var x = (from IDataRecord record in SqlDataReader select DataMappingMethod(record));
            IEnumerator<DS> enusm = x.GetEnumerator();
            return enusm;

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
