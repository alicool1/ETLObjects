
using System;

namespace ETLObjects
{
    public interface ITask
    {
        string TaskName { get; }
        string TaskType { get; }
        string TaskHash { get; }
        IConnectionManager ConnectionManager { get; }
        bool DisableLogging { get; }
        void Execute();
    }
}
