using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace ALE.ETLToolbox {
    public class SMOConnectionManager : IDbConnectionManager, IDisposable {
        public ConnectionString ConnectionString { get; set; }
        public bool IsConnectionOpen => SqlConnectionManager.DbConnection?.State == ConnectionState.Open;

        public SMOConnectionManager(ConnectionString connectionString) {
            RuntimePolicyHelper.SetNET20Compatibilty();
            ConnectionString = connectionString;
            SqlConnectionManager = new SqlConnectionManager(connectionString);
        }

        internal Server Server { get; set; }
        internal ServerConnection Context => Server.ConnectionContext;
        internal SqlConnectionManager SqlConnectionManager { get; set; }
        internal ServerConnection OpenedContext {
            get {
                if (!IsConnectionOpen)
                    Open();
                return Context;
            }
        }

        public void Open() {
            SqlConnectionManager = new SqlConnectionManager(ConnectionString);
            SqlConnectionManager.Open();
            Server = new Server(new ServerConnection(SqlConnectionManager.DbConnection));
            Context.StatementTimeout = 0;
        }

        public int ExecuteNonQuery(string command) {
            return OpenedContext.ExecuteNonQuery(command);
        }

        public object ExecuteScalar(string command) {
            return OpenedContext.ExecuteScalar(command);
        }

        public IDataReader ExecuteReader(string command) {
            return OpenedContext.ExecuteReader(command);
        }

        public void BulkInsert(IDataReader data, IColumnMappingCollection columnMapping, string tableName)
    => SqlConnectionManager.BulkInsert(data, columnMapping, tableName);

        private bool disposedValue = false; // To detect redundant calls
        protected void Dispose(bool disposing) {
            if (!disposedValue) {
                if (disposing) {
                    Server?.ConnectionContext?.Disconnect();
                    if (SqlConnectionManager != null)
                        SqlConnectionManager.Close();
                    SqlConnectionManager = null;
                    Server = null;
                }
                disposedValue = true;
            }
        }

        public void Dispose() => Dispose(true);
        public void Close() => Dispose();

        public IDbConnectionManager Clone() {
            SMOConnectionManager clone = new SMOConnectionManager(ConnectionString) { };
            return clone;
        }


    }



}
