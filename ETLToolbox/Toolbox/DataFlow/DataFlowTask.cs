using ETLObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Data.Common;
using System.Data;

namespace ETLObjects
{
    public class DataFlowTask<DS> : GenericTask, ITask
        
    {
        
        /* ITask Interface */
        public override string TaskType { get; set; } = "DATAFLOW";
        public override string TaskName { get; set; }

        /* Public properties */
        


        public int BatchSize { get; set; }        

        public Func<DS[], InMemoryTable> BatchTransformFunction { get; set; }

        public Func<DS,DS> RowTransformFunction { get; set; }


        public IDataFlowSource<DS> DataFlowSource { get; set; }

        public IDataFlowDestination<DS> DataFlowDestination { get; set; }

        BufferBlock<DS> SourceBufferBlock { get; set; }
        BatchBlock<DS> DestinationBatchBlock { get; set; }
        TransformBlock<DS, DS> RowTransformBlock { get; set; }
        TransformBlock<DS[], InMemoryTable> BatchTransformBlock {get;set;}
        ActionBlock<InMemoryTable> DestinationBlock { get; set; }

        public Graph g { get; set; }

        NLog.Logger NLogger { get; set; }

        public DataFlowTask()
        {
            NLogger = NLog.LogManager.GetLogger("Default");
        }

        int MaxDegreeOfParallelism = 1;

        public DataFlowTask(string name, IDataFlowSource<DS> DataFlowSource, IDataFlowDestination<DS> DataFlowDestination, int batchSize, Func<DS, DS> rowTransformFunction, Func<DS[], InMemoryTable> batchTransformFunction) : this()
        {
            
            TaskName = name;
            this.DataFlowSource = DataFlowSource;
            BatchSize = batchSize;
            BatchTransformFunction = batchTransformFunction;
            RowTransformFunction = rowTransformFunction;
            this.DataFlowDestination = DataFlowDestination;
        }

        public DataFlowTask(string name, IDataFlowSource<DS> DataFlowSource, IDataFlowDestination<DS> DataFlowDestination, int batchSize, int MaxDegreeOfParallelism ,Func<DS, DS> rowTransformFunction) : this()
        {
            this.DataFlowSource = DataFlowSource;
            this.MaxDegreeOfParallelism = MaxDegreeOfParallelism;
            TaskName = name;
            BatchSize = batchSize;
            RowTransformFunction = rowTransformFunction;
            this.DataFlowDestination = DataFlowDestination;
        }

        public DataFlowTask(string name, int batchSize, int MaxDegreeOfParallelism, Graph g) : this()
        {
            
            this.MaxDegreeOfParallelism = MaxDegreeOfParallelism;
            this.TaskName = name;
            this.BatchSize = batchSize;
            this.g = g;

        }

        public void Execute_Graph()
        {
            var sortedgraph = TopoSort.sortGraph(g);
            
            foreach (long i in sortedgraph.Keys)
            {
                Vertex v_source = g.getVertex(i, null);
                foreach (Edge e in v_source.edges)
                {
                    Vertex v_dest = e.dest;
                    long key_source = v_source.key;
                    long key_dest = v_dest.key;
                    if (v_source.BenutzerObjekt.GetType() == typeof(DBSource<DS>))
                    {
                        IDataFlowSource<DS> source = (IDataFlowSource<DS>)v_source.BenutzerObjekt;
                        
                    }
                }

            }
        }

        public void Execute_CSVSource()
        {
            SourceBufferBlock = new BufferBlock<DS>();
            DestinationBatchBlock = new BatchBlock<DS>(BatchSize);

            using (DataFlowSource)
            {


                DataFlowSource.Open();
                DataFlowDestination.Open();

                NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
                /* Pipeline:
                 * Source -> BufferBlock -> RowTransformation -> BatchBlock -> BatchTransformation -> Destination
                 * */
                RowTransformBlock = new TransformBlock<DS, DS>(inp => RowTransformFunction.Invoke(inp));
                BatchTransformBlock = new TransformBlock<DS[], InMemoryTable>(inp => BatchTransformFunction.Invoke(inp));
                DestinationBlock = new ActionBlock<InMemoryTable>(outp => DataFlowDestination.WriteBatch(outp));

                SourceBufferBlock.LinkTo(RowTransformBlock);
                RowTransformBlock.LinkTo(DestinationBatchBlock);
                DestinationBatchBlock.LinkTo(BatchTransformBlock);
                BatchTransformBlock.LinkTo(DestinationBlock);
                SourceBufferBlock.Completion.ContinueWith(t => { NLogger.Debug($"SoureBufferBlock DataFlow Completed: {TaskName}", TaskType, "RUN", TaskHash); RowTransformBlock.Complete(); });
                RowTransformBlock.Completion.ContinueWith(t => { NLogger.Debug($"RowTransformBlock DataFlow Completed: {TaskName}", TaskType, "RUN", TaskHash); DestinationBatchBlock.Complete(); });
                DestinationBatchBlock.Completion.ContinueWith(t => { NLogger.Debug($"DestinationBatchBlock DataFlow Completed: {TaskName}", TaskType, "RUN", TaskHash); BatchTransformBlock.Complete(); });
                BatchTransformBlock.Completion.ContinueWith(t => { NLogger.Debug($"BatchTransformBlock DataFlow Completed: {TaskName}", TaskType, "RUN", TaskHash); DestinationBlock.Complete(); });

                DataFlowSource.Read(RowTransformBlock);


                SourceBufferBlock.Complete();
                DestinationBlock.Completion.Wait();

                NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            }
        }

        public override void Execute()
        {
            if (g != null) { Execute_Graph(); return; }

            if (DataFlowSource == null) throw new InvalidOperationException("DataFlowSource is null.");
            else if (DataFlowDestination == null) throw new InvalidOperationException("DataFlowDestination is null.");


            using (DataFlowSource)
            {
                DataFlowSource.Open();
                DataFlowDestination.Open();

                var RowTransformBlock = new TransformBlock<DS, DS>(RowTransformFunction
                    , new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism });


                var bacthBlock = new BatchBlock<DS>(BatchSize);

                var DataFlowDestinationBlock = new ActionBlock<DS[]>(outp => DataFlowDestination.WriteBatch(outp));

                RowTransformBlock.LinkTo(bacthBlock);
                bacthBlock.LinkTo(DataFlowDestinationBlock);

                RowTransformBlock.Completion.ContinueWith(t => { bacthBlock.Complete(); });
                bacthBlock.Completion.ContinueWith(t => { DataFlowDestinationBlock.Complete(); });

                DataFlowSource.Read(RowTransformBlock);

                RowTransformBlock.Complete();
                DataFlowDestinationBlock.Completion.Wait();
                RowTransformBlock.Complete();
            }
        }


        public static void Execute(string name, IDataFlowSource<DS> DataFlowSource, IDataFlowDestination<DS> DataFlowDestination, int batchSize
            , Func<DS, DS> rowTransformFunction
            , Func<DS[], InMemoryTable> batchTransformFunction) => 
            new DataFlowTask<DS>(name, DataFlowSource, DataFlowDestination,batchSize, rowTransformFunction, batchTransformFunction).Execute();

        public static void Execute(string name, IDataFlowSource<DS> DataFlowSource, IDataFlowDestination<DS> DataFlowDestination, int batchSize, int MaxDegreeOfParallelism
            , Func<DS, DS> rowTransformFunction) => 
            new DataFlowTask<DS>(name, DataFlowSource, DataFlowDestination, batchSize, MaxDegreeOfParallelism, rowTransformFunction).Execute();

        public static void Execute(string name, int batchSize, int MaxDegreeOfParallelism, Graph g) =>
            new DataFlowTask<DS>(name, batchSize, MaxDegreeOfParallelism, g).Execute();


    }
}
