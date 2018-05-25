using NLog;
using NLog.Config;
//using ALE.SchemaReference;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ETLObjects {
    public static class ControlFlow {
        public static string STAGE { get; set; }

        static IDbConnectionManager _currentDbConnection;
        public static IDbConnectionManager CurrentDbConnection {
            get {
                return _currentDbConnection;
            }
            set {
                _currentDbConnection = value;
                if (value != null)
                    SetLoggingDatabase(value);
            }
        }        
        public static AdomdConnectionManager CurrentAdomdConnection { get; set; }
        public static ASConnectionManager CurrentASConnection { get; set; }
        

        public static LoadProcess CurrentLoadProcess { get; internal set; }

        public static bool DisableAllLogging { get; set; }
        static ControlFlow() {
            NLog.Config.ConfigurationItemFactory.Default.LayoutRenderers.RegisterDefinition("etllog", typeof(ETLLogLayoutRenderer));
        }

        public static void SetLoggingDatabase(IConnectionManager connection) {
            var dbTarget = LogManager.Configuration?.ConfiguredNamedTargets?.Where(t => t.GetType() == typeof(NLog.Targets.DatabaseTarget)).FirstOrDefault() as NLog.Targets.DatabaseTarget;
            if (dbTarget != null)
                dbTarget.ConnectionString = connection.ConnectionString.Value; //?? CurrentDbConnection.ConnectionString.Value; //""; Parameter.DWHConnection?.Value;
        }

        public static void ClearSettings() {
            CurrentDbConnection = null;
            CurrentAdomdConnection = null;
            CurrentASConnection = null;
            CurrentLoadProcess = null;
            DisableAllLogging = false;
        }

    }
}
