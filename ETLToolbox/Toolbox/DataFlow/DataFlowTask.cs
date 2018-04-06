using ALE.ETLToolbox;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLToolbox
{
    public class DataFlowTask : GenericTask, ITask 
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DATAFLOW";
        public override string TaskName { get; set; }

        /* Public properties */
        public string FileName { get; set; }
        public string TableName { get; set; }
        public int BatchSize { get; set; }        
        //public ISource<string[]> Source { get; set; }                
        //public IBatchTransformation<string[], InMemoryTable> BatchTransformation { get; set; }
        public Func<string[][], InMemoryTable> BatchTransformFunction { get; set; }
        //public ITransformation<string[], string[]> RowTransformation { get; set; }
        public Func<string[],string[]> RowTransformFunction { get; set; }


        public CSVSource Source { get; set; }
        public DBDestination Destination { get; set; }
        BufferBlock<string[]> SourceBufferBlock { get; set; }
        BatchBlock<string[]> DestinationBatchBlock { get; set; }
        TransformBlock<string[], string[]> RowTransformBlock { get; set; }
        TransformBlock<string[][], InMemoryTable> BatchTransformBlock {get;set;}
        ActionBlock<InMemoryTable> DestinationBlock { get; set; }

        NLog.Logger NLogger { get; set; }

        public DataFlowTask()
        {
            NLogger = NLog.LogManager.GetLogger("Default");
        }

        public DataFlowTask(string name, string fileName, string tableName, int batchSize, Func<string[], string[]> rowTransformFunction, Func<string[][], InMemoryTable> batchTransformFunction) : this()
        {          
            TaskName = name;
            FileName = fileName;
            TableName = tableName;
            BatchSize = batchSize;
            BatchTransformFunction = batchTransformFunction;
            RowTransformFunction = rowTransformFunction;
        }
      
        public override void Execute()
        {
            SourceBufferBlock = new BufferBlock<string[]>();
            DestinationBatchBlock = new BatchBlock<string[]>(BatchSize);
            if (Source == null) Source = new CSVSource(FileName);
            using (Source) {
                Source.Open();
                Destination = new DBDestination() { Connection = DbConnectionManager, TableName = TableName };

                NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
                /* Pipeline:
                 * Source -> BufferBlock -> RowTransformation -> BatchBlock -> BatchTransformation -> Destination
                 * */
                RowTransformBlock = new TransformBlock<string[], string[]>(inp => RowTransformFunction.Invoke(inp));
                BatchTransformBlock = new TransformBlock<string[][], InMemoryTable>(inp => BatchTransformFunction.Invoke(inp));
                DestinationBlock = new ActionBlock<InMemoryTable>(outp => Destination.WriteBatch(outp));

                SourceBufferBlock.LinkTo(RowTransformBlock);
                RowTransformBlock.LinkTo(DestinationBatchBlock);
                DestinationBatchBlock.LinkTo(BatchTransformBlock);
                BatchTransformBlock.LinkTo(DestinationBlock);
                SourceBufferBlock.Completion.ContinueWith(t => { NLogger.Debug($"SoureBufferBlock DataFlow Completed: {TaskName}", TaskType, "RUN", TaskHash); RowTransformBlock.Complete(); });
                RowTransformBlock.Completion.ContinueWith(t => { NLogger.Debug($"RowTransformBlock DataFlow Completed: {TaskName}", TaskType, "RUN", TaskHash); DestinationBatchBlock.Complete(); });
                DestinationBatchBlock.Completion.ContinueWith(t => { NLogger.Debug($"DestinationBatchBlock DataFlow Completed: {TaskName}", TaskType, "RUN", TaskHash); BatchTransformBlock.Complete(); });
                BatchTransformBlock.Completion.ContinueWith(t => { NLogger.Debug($"BatchTransformBlock DataFlow Completed: {TaskName}", TaskType, "RUN", TaskHash); DestinationBlock.Complete(); });

                Source.Read(RowTransformBlock);
                SourceBufferBlock.Complete();
                DestinationBlock.Completion.Wait();

                NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            }
            
        }

        public static void Execute(string name, string fileName, string tableName, int batchSize, Func<string[], string[]> rowTransformFunction, Func<string[][], InMemoryTable> batchTransformFunction) => new DataFlowTask(name, fileName, tableName, batchSize, rowTransformFunction, batchTransformFunction).Execute();  
    }
}
