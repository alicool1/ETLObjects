using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace ETLObjects
{
    public class SqlConnectionManager : IConnectionManager, IDisposable
    {
        /// <summary>
        /// getNewSqlConnection opens and returns a new SqlConnection 
        /// by the class attriubtes ServerName and InitialCatalog
        /// </summary>
        /// <returns>SqlConnection</returns>
        public SqlConnection getNewSqlConnection()
        {
            if (SqlConnection_Collection == null) SqlConnection_Collection = new List<SqlConnection>();

            SqlConnectionStringBuilder sqlConnectionStringBuilder = new SqlConnectionStringBuilder();
            sqlConnectionStringBuilder.DataSource = ServerName;
            sqlConnectionStringBuilder.InitialCatalog = InitialCatalog;
            sqlConnectionStringBuilder.IntegratedSecurity = true;

            SqlConnection NewSqlConnection = new SqlConnection();
            NewSqlConnection.ConnectionString = sqlConnectionStringBuilder.ConnectionString;

            SqlConnection_Collection.Add(NewSqlConnection);

            NewSqlConnection.Open();

            return NewSqlConnection;

        }


        private List<SqlConnection> SqlConnection_Collection = null;

        public void Open()
        {
            // opens the default SqlConnection
            _SqlConnection.Open();
        }

        public string ServerName { get; set; }
        public string InitialCatalog { get; set; }

        private SqlConnection _SqlConnection;
        /// <summary>
        /// SqlConnection is the default SqlConnection of the SqlConnectionManager
        /// </summary>
        public SqlConnection SqlConnection
        {
            get {
                return _SqlConnection;
            }
            set {
                _SqlConnection = value;
            }
           
        }

        public SqlConnectionManager(string ServerName, string InitialCatalog)
        {
            this.ServerName = ServerName;
            this.InitialCatalog = InitialCatalog;
            // get a new SqlConnection for the default SqlConnection
            _SqlConnection = getNewSqlConnection();
        }

        public void Close()
        {
            _SqlConnection?.Close();
        }

        public void CloseAll()
        {
            foreach (SqlConnection con in SqlConnection_Collection)
            {
                con?.Close();
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // Dient zur Erkennung redundanter Aufrufe.

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Close();
                    CloseAll();
                }

                disposedValue = true;
            }
        }


        public void Dispose()
        {
            Dispose(true);
            // TODO: Auskommentierung der folgenden Zeile aufheben, wenn der Finalizer weiter oben überschrieben wird.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
