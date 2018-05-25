using ETLObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLObjects
{
    public class DataFlowTask<DS> : GenericTask, ITask
        
    {
        
        /* ITask Interface */
        public override string TaskType { get; set; } = "DATAFLOW";
        public override string TaskName { get; set; }

        /* Public properties */
        
        public string TableName_Target { get; set; }

        public int BatchSize { get; set; }        
        //public ISource<DS> Source { get; set; }                
        //public IBatchTransformation<DS, InMemoryTable> BatchTransformation { get; set; }
        public Func<DS[], InMemoryTable> BatchTransformFunction { get; set; }
        //public ITransformation<DS, DS> RowTransformation { get; set; }
        public Func<DS,DS> RowTransformFunction { get; set; }


        public IDataFlowSource<DS> DataFlowSource { get; set; }

        public IDataFlowDestination<DS> DataFlowDestination { get; set; }

        public DBSource<DS> DBSource { get; set; }
        public CSVSource<DS> CSVSource { get; set; }
        public DBDestination Destination { get; set; }
        BufferBlock<DS> SourceBufferBlock { get; set; }
        BatchBlock<DS> DestinationBatchBlock { get; set; }
        TransformBlock<DS, DS> RowTransformBlock { get; set; }
        TransformBlock<DS[], InMemoryTable> BatchTransformBlock {get;set;}
        ActionBlock<InMemoryTable> DestinationBlock { get; set; }

        NLog.Logger NLogger { get; set; }

        public DataFlowTask()
        {
            NLogger = NLog.LogManager.GetLogger("Default");
        }

        int MaxDegreeOfParallelism = 1;

        public DataFlowTask(string name, CSVSource<DS> CSVSource, string tableName_Target, int batchSize, Func<DS, DS> rowTransformFunction, Func<DS[], InMemoryTable> batchTransformFunction) : this()
        {
            
            TaskName = name;
            this.DataFlowSource = CSVSource;
            TableName_Target = tableName_Target;
            BatchSize = batchSize;
            BatchTransformFunction = batchTransformFunction;
            RowTransformFunction = rowTransformFunction;
        }

        public DataFlowTask(string name, DBSource<DS> DBSource, IDataFlowDestination<DS> DataFlowDestination, string tableName_Target, int batchSize, int MaxDegreeOfParallelism ,Func<DS, DS> rowTransformFunction) : this()
        {
            this.DataFlowSource = DBSource;
            this.MaxDegreeOfParallelism = MaxDegreeOfParallelism;
            TaskName = name;
            BatchSize = batchSize;
            TableName_Target = tableName_Target;
            RowTransformFunction = rowTransformFunction;
            this.DataFlowDestination = DataFlowDestination;
        }

        public void Execute_CSVSource()
        {
            SourceBufferBlock = new BufferBlock<DS>();
            DestinationBatchBlock = new BatchBlock<DS>(BatchSize);

            CSVSource = (CSVSource<DS>)DataFlowSource;

            using (CSVSource)
            {

                CSVSource.Open();
                Destination = new DBDestination() { Connection = DbConnectionManager, TableName_Target = TableName_Target };

                NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
                /* Pipeline:
                 * Source -> BufferBlock -> RowTransformation -> BatchBlock -> BatchTransformation -> Destination
                 * */
                RowTransformBlock = new TransformBlock<DS, DS>(inp => RowTransformFunction.Invoke(inp));
                BatchTransformBlock = new TransformBlock<DS[], InMemoryTable>(inp => BatchTransformFunction.Invoke(inp));
                DestinationBlock = new ActionBlock<InMemoryTable>(outp => Destination.WriteBatch(outp));

                SourceBufferBlock.LinkTo(RowTransformBlock);
                RowTransformBlock.LinkTo(DestinationBatchBlock);
                DestinationBatchBlock.LinkTo(BatchTransformBlock);
                BatchTransformBlock.LinkTo(DestinationBlock);
                SourceBufferBlock.Completion.ContinueWith(t => { NLogger.Debug($"SoureBufferBlock DataFlow Completed: {TaskName}", TaskType, "RUN", TaskHash); RowTransformBlock.Complete(); });
                RowTransformBlock.Completion.ContinueWith(t => { NLogger.Debug($"RowTransformBlock DataFlow Completed: {TaskName}", TaskType, "RUN", TaskHash); DestinationBatchBlock.Complete(); });
                DestinationBatchBlock.Completion.ContinueWith(t => { NLogger.Debug($"DestinationBatchBlock DataFlow Completed: {TaskName}", TaskType, "RUN", TaskHash); BatchTransformBlock.Complete(); });
                BatchTransformBlock.Completion.ContinueWith(t => { NLogger.Debug($"BatchTransformBlock DataFlow Completed: {TaskName}", TaskType, "RUN", TaskHash); DestinationBlock.Complete(); });

                CSVSource.Read(RowTransformBlock);


                SourceBufferBlock.Complete();
                DestinationBlock.Completion.Wait();

                NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            }
        }

        public async void LeseDBSource(ITargetBlock<DS> target)
        {
            foreach (DS dataSet in DBSource.EnumerableDataSource)
            {
                await target.SendAsync(dataSet);
            }
        }

        

        public void Execute_DBSource()
        { 
            DBSource = (DBSource<DS>)DataFlowSource;
            DBSource.Init();

            DataFlowDestination.Init();

            var RowTransformBlock = new TransformBlock<DS, DS>(RowTransformFunction
                , new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism });


            var bacthBlock = new BatchBlock<DS>(BatchSize);
            var DataFlowDestinationBlock = new ActionBlock<DS[]>(outp => DataFlowDestination.Insert(outp.ToList<DS>()));

            RowTransformBlock.LinkTo(bacthBlock);
            bacthBlock.LinkTo(DataFlowDestinationBlock);

            RowTransformBlock.Completion.ContinueWith(t => { bacthBlock.Complete(); });
            bacthBlock.Completion.ContinueWith(t => { DataFlowDestinationBlock.Complete(); });

            LeseDBSource(RowTransformBlock);

            RowTransformBlock.Complete();
            DataFlowDestinationBlock.Completion.Wait();
            RowTransformBlock.Complete();

        }

        public override void Execute()
        {
            if (DataFlowSource == null) throw new InvalidOperationException("Die DataFlowSource-Eigenschaft wurde nicht gesetzt.");
            else if (DataFlowSource.GetType() == typeof(CSVSource<DS>)) Execute_CSVSource();
            else if (DataFlowSource.GetType() == typeof(DBSource<DS>)) Execute_DBSource();
            else throw new Exception("unbekannter Quelltyp");
        }

        public static void Execute(string name, CSVSource<DS> CSVSource, string tableName_Target, int batchSize
            , Func<DS, DS> rowTransformFunction
            , Func<DS[], InMemoryTable> batchTransformFunction) => 
            new DataFlowTask<DS>(name, CSVSource, tableName_Target, batchSize, rowTransformFunction, batchTransformFunction).Execute();

        public static void Execute(string name, DBSource<DS> DBSource, IDataFlowDestination<DS> DataFlowDestination, string tableName_Target, int batchSize, int MaxDegreeOfParallelism
            , Func<DS, DS> rowTransformFunction) => 
            new DataFlowTask<DS>(name, DBSource, DataFlowDestination, tableName_Target, batchSize, MaxDegreeOfParallelism, rowTransformFunction).Execute();


    }
}
