using System;

namespace ETLObjects {
    public class Package : GenericTask, ITask {
        public override string TaskType { get; set; } = "PACKAGE";
        public override string TaskName { get; set; } = "Package";
        public override void Execute() => new CustomTask(TaskName) { TaskType = this.TaskType, TaskHash = this.TaskHash }.Execute(Tasks);
        public Action Tasks { get; set; }

        public Package() { }

        public Package(string name) : this() {
            TaskName = name;
        }

        public Package(string name, Action tasks) : this(name) {
            this.Tasks = tasks;
        }

        public static void Execute(string name, Action tasks) => new Package(name, tasks).Execute();
    }
}
