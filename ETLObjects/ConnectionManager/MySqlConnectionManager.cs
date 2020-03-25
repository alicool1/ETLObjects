using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace ETLObjects
{
    public class MySqlConnectionManager : IConnectionManager, IDisposable
    {
        /// <summary>
        /// getNewMySqlConnection opens and returns a new MySqlConnection 
        /// by the class attriubtes ServerName and InitialCatalog
        /// </summary>
        /// <returns>MySqlConnection</returns>
        public MySqlConnection getNewMySqlConnection()
        {
            if (MySqlConnection_Collection == null) MySqlConnection_Collection = new List<MySqlConnection>();

            MySqlConnectionStringBuilder mySqlConnectionStringBuilder = new MySqlConnectionStringBuilder();
            mySqlConnectionStringBuilder.Server = ServerName;
            mySqlConnectionStringBuilder.Database = InitialCatalog;
            mySqlConnectionStringBuilder.Password = Password;
            mySqlConnectionStringBuilder.UserID = UserID;


            MySqlConnection NewMySqlConnection = new MySqlConnection();
            NewMySqlConnection.ConnectionString = mySqlConnectionStringBuilder.ConnectionString;

            MySqlConnection_Collection.Add(NewMySqlConnection);

            NewMySqlConnection.Open();

            return NewMySqlConnection;

        }


        private List<MySqlConnection> MySqlConnection_Collection = null;

        public void Open()
        {
            // opens the default SqlConnection
            _MySqlConnection.Open();
        }

        public string ServerName { get; set; }
        public string Password { get; set; }
        public string UserID { get; set; }
        public string InitialCatalog { get; set; }

        private MySqlConnection _MySqlConnection;
        /// <summary>
        /// MySqlConnection is the default MySqlConnection of the MySqlConnectionManager
        /// </summary>
        public MySqlConnection MySqlConnection
        {
            get {
                return _MySqlConnection;
            }
            set
            {
                _MySqlConnection = value;
            }
        }

        public MySqlConnectionManager(string ServerName, string InitialCatalog, string UserID, string Password)
        {
            this.ServerName = ServerName;
            this.InitialCatalog = InitialCatalog;
            this.UserID = UserID;
            this.Password = Password;
            // get a new SqlConnection for the default SqlConnection
            _MySqlConnection = getNewMySqlConnection();

        }

        public void Close()
        {
            _MySqlConnection?.Close();
        }

        public void CloseAll()
        {
            foreach (MySqlConnection con in MySqlConnection_Collection)
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
