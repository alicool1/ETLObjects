using System;


namespace ALE.ETLToolbox {
    public class LogTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "LOG";
        public override string TaskName => $"Logs message";
        public override void Execute() {
            Info(Message);
        }

        /* Public properties */
        public string Message { get; set; }

        public LogTask() {
            NLogger = NLog.LogManager.GetLogger("Default");
        }

        public LogTask(string message) : this() {
            Message = message; 
        }
        //NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        public void Info() => NLogger?.Info(Message, TaskType, "LOG", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        public void Warn() => NLogger?.Warn(Message, TaskType, "LOG", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        public void Error() => NLogger?.Error(Message, TaskType, "LOG", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        public static void Info(string message) => new LogTask(message).Info();
        public static void Warn(string message) => new LogTask(message).Warn();
        public static void Error(string message) => new LogTask(message).Error();

        NLog.Logger NLogger { get; set; }
    }
}
