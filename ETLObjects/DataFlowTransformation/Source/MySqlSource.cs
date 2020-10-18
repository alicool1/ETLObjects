using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using MySql.Data.MySqlClient;

namespace ETLObjects {
    public class MySqlSource<DS> : IDataFlowSource<DS>
    {

        public MySqlConnection MySqlConnection { get; set; }

        public MySqlSource(MySqlConnection MySqlConnection, string MySqlQueryString)
        {
            this.MySqlConnection = MySqlConnection;
            this.MySqlQueryString = MySqlQueryString;
        }

        /// <summary>
        /// SQL query that returns data for DataFlowTask
        /// </summary>
        public string MySqlQueryString { get; set; }

        public IEnumerable<DS> EnumerableDataSource
        {
            get { return _dataReaderToEnumerable; }
        }
        public DataReaderToEnumerable<DS>.DataMapping DataMappingMethod { get; set; }


        private DataReaderToEnumerable<DS> _dataReaderToEnumerable = new DataReaderToEnumerable<DS>();

        private MySqlDataReader _reader;

        public async Task Read(ITargetBlock<DS> target)
        {
            if (DataMappingMethod == null) throw new InvalidOperationException("Fehler: Die DataMappingMethod Eigengschaft wurde nicht gesetzt!");

            MySqlCommand command = new MySqlCommand(MySqlQueryString, MySqlConnection);
            command.CommandTimeout = 0;
            _reader = command.ExecuteReader();

            _dataReaderToEnumerable.MySqlDataReader = _reader;
            _dataReaderToEnumerable.DataMappingMethod = DataMappingMethod;

            foreach (DS dataSet in _dataReaderToEnumerable)
            {
                await target.SendAsync(dataSet);
            }
        }


    }

    
        

}
