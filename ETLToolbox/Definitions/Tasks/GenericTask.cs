using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ALE.ETLToolbox {
    public abstract class GenericTask : ITask {
        public virtual string TaskType { get; set; } = "N/A";
        public virtual string TaskName { get; set; } = "N/A";
        public virtual void Execute() {
            throw new Exception("Not implemented!");
        }

        public virtual IConnectionManager ConnectionManager { get; set; }
        //IConnectionManager _connectionManager;
        //public IConnectionManager ConnectionManager {
        //    get {
        //        if (_connectionManager == null && ControlFlow.CurrentDbConnection != null)
        //            return ControlFlow.CurrentDbConnection;
        //        else
        //            return _connectionManager;
        //    }
        //    set {
        //        _connectionManager = value;
        //    }
        //}
        internal virtual IDbConnectionManager DbConnectionManager {
            get {
                if (ConnectionManager == null) {
                    if (UseAdomdConnection && ControlFlow.CurrentAdomdConnection != null)
                        return (IDbConnectionManager)ControlFlow.CurrentAdomdConnection;
                    if (ControlFlow.CurrentDbConnection != null)
                        return (IDbConnectionManager)ControlFlow.CurrentDbConnection;
                    else
                        return null;
                }
                else
                    return (IDbConnectionManager)ConnectionManager;
            }
        }

        internal virtual ICubeConnectionManager ASConnectionManager {
            get {
                if (ConnectionManager == null) {
                    if (ControlFlow.CurrentASConnection != null)
                        return ControlFlow.CurrentASConnection as ICubeConnectionManager;
                    else
                        return null;
                }
                return ConnectionManager as ICubeConnectionManager;
            }
        }

        public bool _disableLogging;
        public virtual bool DisableLogging {
            get {
                if (ControlFlow.DisableAllLogging == false)
                    return _disableLogging;
                else
                    return ControlFlow.DisableAllLogging;
            }
            set {
                _disableLogging = value;
            }
        }

        private string _taskHash;
        public virtual string TaskHash {
            get {
                if (_taskHash == null)
                    return HashHelper.Encrypt_Char40(this);
                else
                    return _taskHash;
            }
            set {
                _taskHash = value;
            }
        }
        internal virtual bool HasName => !String.IsNullOrWhiteSpace(TaskName);
        internal virtual string NameAsComment => CommentStart + TaskName + CommentEnd + Environment.NewLine;
        private string CommentStart => DoXMLCommentStyle ? @"<!--" : "/*";
        private string CommentEnd => DoXMLCommentStyle ? @"-->" : "*/";        
        public virtual bool DoXMLCommentStyle { get; set; }
        internal virtual bool UseAdomdConnection { get; set; }        


    }
}
