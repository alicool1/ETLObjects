using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects {
    public class LoadProcess {
        public int? LoadProcessKey { get; set; }
        public DateTime StartDate { get; set; }
        public DateTime? TransferCompletedDate { get; set; }
        public DateTime? EndDate { get; set; }
        public string ProcessName { get; set; }
        public string StartMessage { get; set; }
        public bool IsRunning { get; set; }
        public string EndMessage { get; set; }
        public bool WasSuccessful { get; set; }
        public string AbortMessage { get; set; }
        public bool WasAborted { get; set; }
        public bool IsFinished { get; set; }
        public bool IsTransferCompleted { get; set; }
    }
}
