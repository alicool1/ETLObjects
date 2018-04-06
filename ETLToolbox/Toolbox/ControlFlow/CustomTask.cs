using System;

namespace ALE.ETLToolbox {
    public class CustomTask : GenericTask, ITask {
        #region
        public override string TaskType { get; set; } = "CUSTOM";
        public override string TaskName { get; set; }        
        public new void Execute() {
            throw new Exception("A custom task can't be used without an Action!");
        }
        #endregion

        public CustomTask(string name) {
            NLogger = NLog.LogManager.GetLogger("Default");
            this.TaskName = name;
        }


        public void Execute(Action task) {
            NLogStart();
            task.Invoke();
            NLogFinish();
        }

        public void Execute<t1>(Action<t1> task, t1 param1) {
            NLogStart();
            task.Invoke(param1);
            NLogFinish();
        }

        public void Execute<t1, t2>(Action<t1, t2> task, t1 param1, t2 param2) {
            NLogStart();
            task.Invoke(param1, param2);
            NLogFinish();
        }

        public static void Execute(string name, Action task) =>
           new CustomTask(name).Execute(task);

        public static void Execute<t1>(string name, Action<t1> task, t1 param1) =>
           new CustomTask(name).Execute<t1>(task, param1);

        public static void Execute<t1, t2>(string name, Action<t1, t2> task, t1 param1, t2 param2) =>
            new CustomTask(name).Execute<t1, t2>(task, param1, param2);

        NLog.Logger NLogger { get; set; }

        void NLogStart() {
            NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void NLogFinish() {
            NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }


    }
}