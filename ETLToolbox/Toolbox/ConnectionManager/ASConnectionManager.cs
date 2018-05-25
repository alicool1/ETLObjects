using AS = Microsoft.AnalysisServices;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace ETLObjects {
    public class ASConnectionManager : ICubeConnectionManager, IDisposable {
        public ConnectionString ConnectionString { get; set; }
        public bool DoParallelProcessing { get; set; } = true;
        public bool IsConnectionOpen => Server?.Connected ?? false;
        public string ErrorMessages { get; set; } = "";
        public bool IgnoreErrors { get; set; }

        public ASConnectionManager() {
            Server = new AS.Server();
        }

        public ASConnectionManager(ConnectionString connectionString) : this() {
            this.ConnectionString = connectionString;
        }

        public void Open() {
            if (!IsConnectionOpen) {
                Server = new AS.Server();
                Server.Connect(ConnectionString.GetConnectionWithoutCatalog().Value);

            }
        }

        public void Process() {
            //Open();
            FindDatabase();
            //TODO ErrorConfiguration has no effect when capture log is enabled!!! The workaround is to do a "normal processing" (always sequential?)
            if (IgnoreErrors)
                Database?.Process(AS.ProcessType.ProcessFull, GetErrorConfiguration());
            else {
                EnabledCaptureLog();
                Database?.Process(AS.ProcessType.ProcessFull);
                ExecuteAndCheckErrorLog();
            }
        }

        public void DropIfExists() {
            //Open();
            FindDatabase();
            Database?.Drop(AS.DropOptions.IgnoreFailures);
        }
        
        internal AS.Server Server { get; set; }
        internal AS.Database Database { get; set; }
        internal AS.Cube Cube { get; set; }

        void FindDatabase() {
            Database = Server.Databases.FindByName(ConnectionString.CatalogName);
            if (Database == null)
                ErrorMessages += $"Can't find cube database with name {ConnectionString.CatalogName}" + Environment.NewLine;
        }

        void EnabledCaptureLog() {
            Server.CaptureXml = true;
        }

        void ExecuteAndCheckErrorLog() {
            AS.XmlaResultCollection resultCol = Server.ExecuteCaptureLog(true, DoParallelProcessing, true);
            if (resultCol.ContainsErrors) {
                ErrorMessages += $"Errors occured in cube {ConnectionString.CatalogName}:" + Environment.NewLine;
                foreach (AS.XmlaResult result in resultCol) {
                    foreach (object error in result.Messages) {
                        if (error.GetType() == typeof(AS.XmlaError))
                            ErrorMessages += "ERR: " + ((AS.XmlaError)error).Description + Environment.NewLine;
                        else if (error.GetType() == typeof(AS.XmlaWarning))
                            ErrorMessages += "WARN: " + ((AS.XmlaWarning)error).Description + Environment.NewLine;
                    }
                }
                throw new Exception(ErrorMessages);
            }
        }

        AS.ErrorConfiguration GetErrorConfiguration() {
            AS.ErrorConfiguration err = new AS.ErrorConfiguration();
            err.KeyErrorAction = AS.KeyErrorAction.DiscardRecord;
            err.KeyErrorLimitAction = AS.KeyErrorLimitAction.StopLogging;
            err.CalculationError = AS.ErrorOption.IgnoreError;
            err.KeyDuplicate = AS.ErrorOption.IgnoreError;
            err.KeyNotFound = AS.ErrorOption.IgnoreError;
            err.NullKeyConvertedToUnknown = AS.ErrorOption.IgnoreError;
            err.NullKeyNotAllowed = AS.ErrorOption.IgnoreError;
            err.KeyErrorLimit = -1;
            return err;

        }


        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected void Dispose(bool disposing) {
            if (!disposedValue) {
                if (disposing) {
                    if (Server != null)
                        Server.Dispose();
                    Server = null;
                }
                disposedValue = true;
            }
        }

        public void Dispose() => Dispose(true);
        public void Close() => Dispose();

        #endregion

        public ICubeConnectionManager Clone() {
            ASConnectionManager clone = new ASConnectionManager(ConnectionString) {
                DoParallelProcessing = this.DoParallelProcessing,
                IgnoreErrors = this.IgnoreErrors
            };
            return clone;
            
        }


    }
}
