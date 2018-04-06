using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ALE.ETLToolbox {
    public class GetLoadProcessAsJSONTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "LOADPROCESS_GETJSON";
        public override string TaskName => $"Get load process list as JSON";

        public override void Execute() {
            //TODO umschreiben in eine Zeile?
            var read = new ReadLoadProcessTableTask() { ReadOption = ReadOptions.ReadAllProcesses};
            read.Execute();
            List<LoadProcess> logEntries = read.AllLoadProcesses;
            JSON = JsonConvert.SerializeObject(logEntries, new JsonSerializerSettings {
                Formatting = Formatting.Indented,
                ContractResolver = new CamelCasePropertyNamesContractResolver(),
                NullValueHandling = NullValueHandling.Ignore
            });

        }
       
        public string JSON { get; private set; }

        public GetLoadProcessAsJSONTask Create() {
            this.Execute();
            return this;
        }

        public GetLoadProcessAsJSONTask() {

        }
      
        public static string GetJSON() => new GetLoadProcessAsJSONTask().Create().JSON;

    }
}
