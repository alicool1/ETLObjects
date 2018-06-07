using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Data.SqlClient;

namespace ETLObjects {
    public class DBSource<DS> : IDataFlowSource<DS>, IDisposable
    {

        public IConnectionManager Connection { get; set; }


        public DBSource(string ServerName, string InitialCatalog, string SqlQueryString)
        {
            this.SqlQueryString = SqlQueryString;
            this.InitialCatalog = InitialCatalog;
            this.ServerName = ServerName;
        }

        /// <summary>
        /// SQL query that returns data for DataFlowTask
        /// </summary>
        public string SqlQueryString { get; set; }

        public string ServerName { get; set; }

        /// <summary>
        /// SQL Server DB name
        /// </summary>
        public string InitialCatalog { get; set; }
        public int ConnectionTimeOut { get; set; }

        public IEnumerable<DS> EnumerableDataSource
        {
            get { return _dataReaderToEnumerable; }
        }
        public DataReaderToEnumerable<DS>.DataMapping DataMappingMethod { get; set; }

        private string _conectionString = "Data Source={0};Initial Catalog={1};Integrated Security=True;Connection Timeout={2}";
        private SqlConnection _con = new SqlConnection();

        private DataReaderToEnumerable<DS> _dataReaderToEnumerable = new DataReaderToEnumerable<DS>();

        private SqlDataReader _reader;

        public async void Read(ITargetBlock<DS> target)
        {
            foreach (DS dataSet in _dataReaderToEnumerable)
            {
                await target.SendAsync(dataSet);
            }
        }

        public override string ToString()
        {
            return string.Format("{0} - {1} ..", InitialCatalog , SqlQueryString.Substring(0, 15));
        }

        public void Open()
        {
            if (string.IsNullOrEmpty(ServerName)) throw new InvalidOperationException("Fehler: Die SqlServer Eigengschaft wurde nicht gesetzt!");
            if (string.IsNullOrEmpty(InitialCatalog)) throw new InvalidOperationException("Fehler: Die InitialCatalog (SQL Server Datenbank) Eigengschaft wurde nicht gesetzt!");
            if (string.IsNullOrEmpty(SqlQueryString)) throw new InvalidOperationException("Fehler: Die SqlQueryString Eigengschaft wurde nicht gesetzt!");
            if (DataMappingMethod == null) throw new InvalidOperationException("Fehler: Die DataMappingMethod Eigengschaft wurde nicht gesetzt!");

            _con = new SqlConnection();
            _con.ConnectionString = string.Format(_conectionString, ServerName, InitialCatalog, ConnectionTimeOut.ToString());
            _con.Open();

            SqlCommand command = new SqlCommand(SqlQueryString, _con);
            command.CommandTimeout = 0;
            _reader = command.ExecuteReader();

            _dataReaderToEnumerable.SqlDataReader = _reader;
            _dataReaderToEnumerable.DataMappingMethod = DataMappingMethod;
        }

        #region IDisposable Support
        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _con?.Close();
                    _con = null;
                }
                disposedValue = true;
            }
        }

        public void Close() => Dispose();

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }

    
        

}
