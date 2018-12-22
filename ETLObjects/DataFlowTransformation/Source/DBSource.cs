using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Data.SqlClient;

namespace ETLObjects {
    public class DBSource<DS> : IDataFlowSource<DS>
    {

        public SqlConnection SqlConnection { get; set; }

        public DBSource(SqlConnection SqlConnection, string SqlQueryString)
        {
            this.SqlConnection = SqlConnection;
            this.SqlQueryString = SqlQueryString;
        }

        /// <summary>
        /// SQL query that returns data for DataFlowTask
        /// </summary>
        public string SqlQueryString { get; set; }

        public IEnumerable<DS> EnumerableDataSource
        {
            get { return _dataReaderToEnumerable; }
        }
        public DataReaderToEnumerable<DS>.DataMapping DataMappingMethod { get; set; }


        private DataReaderToEnumerable<DS> _dataReaderToEnumerable = new DataReaderToEnumerable<DS>();

        private SqlDataReader _reader;

        public async void Read(ITargetBlock<DS> target)
        {
            if (DataMappingMethod == null) throw new InvalidOperationException("Fehler: Die DataMappingMethod Eigengschaft wurde nicht gesetzt!");

            SqlCommand command = new SqlCommand(SqlQueryString, SqlConnection);
            command.CommandTimeout = 0;
            _reader = command.ExecuteReader();

            _dataReaderToEnumerable.SqlDataReader = _reader;
            _dataReaderToEnumerable.DataMappingMethod = DataMappingMethod;

            foreach (DS dataSet in _dataReaderToEnumerable)
            {
                await target.SendAsync(dataSet);
            }
        }


    }

    
        

}
