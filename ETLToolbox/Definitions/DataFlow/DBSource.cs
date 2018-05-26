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

        public IConnectionManager Connection { get; set; }


        public DBSource(string ServerName, string InitialCatalog, string SqlQueryString)
        {
            this.SqlQueryString = SqlQueryString;
            this.InitialCatalog = InitialCatalog;
            this.ServerName = ServerName;
        }

        /// <summary>
        /// SQL-Abfrage, die die Daten für den DataFlowTask liefert.
        /// </summary>
        public string SqlQueryString { get; set; }

        public string ServerName { get; set; }
        /// <summary>
        /// SQL-Server Datenbankname
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


        /// <summary>
        /// Initialisierung SQL-Verbindung und SQLDataReader öffnen, Erzeugung IEnumerable für foreach-Schleife
        /// </summary>
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
        private bool disposedValue = false; // Dient zur Erkennung redundanter Aufrufe.

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: verwalteten Zustand (verwaltete Objekte) entsorgen.
                }

                // TODO: nicht verwaltete Ressourcen (nicht verwaltete Objekte) freigeben und Finalizer weiter unten überschreiben.
                // TODO: große Felder auf Null setzen.

                disposedValue = true;
            }
        }

        // TODO: Finalizer nur überschreiben, wenn Dispose(bool disposing) weiter oben Code für die Freigabe nicht verwalteter Ressourcen enthält.
        // ~DBSource() {
        //   // Ändern Sie diesen Code nicht. Fügen Sie Bereinigungscode in Dispose(bool disposing) weiter oben ein.
        //   Dispose(false);
        // }

        // Dieser Code wird hinzugefügt, um das Dispose-Muster richtig zu implementieren.
        public void Dispose()
        {
            // Ändern Sie diesen Code nicht. Fügen Sie Bereinigungscode in Dispose(bool disposing) weiter oben ein.
            Dispose(true);
            // TODO: Auskommentierung der folgenden Zeile aufheben, wenn der Finalizer weiter oben überschrieben wird.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }

    
        

}
