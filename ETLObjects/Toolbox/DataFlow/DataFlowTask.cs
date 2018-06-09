using ETLObjects;
using System;
using System.Collections;
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

        public Func<DS,DS> RowTransformFunction { get; set; }

        public IDataFlowSource<DS> DataFlowSource { get; set; }

        public IDataFlowDestination<DS> DataFlowDestination { get; set; }

        BufferBlock<DS> SourceBufferBlock { get; set; }
        BatchBlock<DS> DestinationBatchBlock { get; set; }
        TransformBlock<DS, DS> RowTransformBlock { get; set; }
        ActionBlock<InMemoryTable> DestinationBlock { get; set; }

        public Graph g { get; set; }

        NLog.Logger NLogger { get; set; }

        public DataFlowTask()
        {
            NLogger = NLog.LogManager.GetLogger("Default");
        }

        int MaxDegreeOfParallelism = 1;

        public DataFlowTask(string name, IDataFlowSource<DS> DataFlowSource, IDataFlowDestination<DS> DataFlowDestination, int batchSize, Func<DS, DS> rowTransformFunction) : this()
        {
            
            TaskName = name;
            this.DataFlowSource = DataFlowSource;
            BatchSize = batchSize;
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
            List<object> WatingForCompletitionCollection = new List<object>();
            List<object> ToCompleteCollection = new List<object>();
            Dictionary<IDataFlowSource<DS>, object> DataFlowReaderCollection = new Dictionary<IDataFlowSource<DS>, object>();

            //TODO All Checks should be done before any real calculations or so happens 

            // the algorithm for topological sorting is required 
            // for the chronological order in the DataFlow-Pipeline
            DoALotOfChecksAndSomeRealStuff(WatingForCompletitionCollection, ToCompleteCollection, DataFlowReaderCollection);

            DoDataFlowReaderCollection(DataFlowReaderCollection);

            DoTransformOrThrowAnExceptionBecauseThereIsMoreToCheck(ToCompleteCollection);

            WaitForCompletionOrEndWithException(WatingForCompletitionCollection);
        }

       

        private void DoALotOfChecksAndSomeRealStuff(List<object> WatingForCompletitionCollection, List<object> ToCompleteCollection, Dictionary<IDataFlowSource<DS>, object> DataFlowReaderCollection)
        {
            foreach (long i in TopoSort.SortGraph(g).Keys)
            {
                Vertex v_source = g.GetVertex(i, null);
                // loop over all edges of v_source 
                foreach (Edge e in v_source.edges)
                {
                    Vertex v_dest = e.dest;

                    // for case that object in vertex is missing
                    if (v_source.BenutzerObjekte == null || v_source.BenutzerObjekte[0] == null)
                        throw new Exception(string.Format("Vertex needs any object. For example an IDataFlowSource."));
                    // for case that v_source is type of IDataFlowSource
                    else if (new List<Type>(new Type[] { typeof(CSVSource<DS>), typeof(DBSource<DS>) }).Contains(v_source.BenutzerObjekte[0].GetType()))
                    {
                        IDataFlowSource<DS> source = (IDataFlowSource<DS>)v_source.BenutzerObjekte[0];

                        // for case that object in vertex is missing
                        if (v_dest.BenutzerObjekte == null || v_dest.BenutzerObjekte[0] == null)
                            throw new Exception(string.Format("Vertex needs any object. For example DBDestination."));
                        // for case that v_source is type of IDataFlowSource
                        // AND that v_dest is type of RowTransformFunction
                        else if (v_dest.BenutzerObjekte[0].GetType() == typeof(RowTransformation<DS>))
                        {
                            RowTransformation<DS> t = (RowTransformation<DS>)v_dest.BenutzerObjekte[0];
                            TransformBlock<DS, DS> t_b = new TransformBlock<DS, DS>(t.RowTransformFunction
                                , new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism });
                            ToCompleteCollection.Add(t_b);
                            v_dest.BenutzerObjekte.Add(t_b);
                            DataFlowReaderCollection.Add(source, t_b);
                        }
                        // for case that type is not implemented
                        else
                        {
                            throw new Exception(string.Format("Type {0} is not implemented.", v_dest.BenutzerObjekte[0].GetType()));
                        }
                    }
                    // for case that v_source is type of RowTransformFunction
                    else if (v_source.BenutzerObjekte[0].GetType() == typeof(RowTransformation<DS>))
                    {

                        // for case that v_source is type of RowTransformFunction
                        // AND that v_dest is type of IDataFlowDestination
                        if (new List<Type>(new Type[] { typeof(DBDestination<DS>) }).Contains(v_dest.BenutzerObjekte[0].GetType()))
                        {
                            IDataFlowDestination<DS> dest = (IDataFlowDestination<DS>)v_dest.BenutzerObjekte[0];
                            //using (dest) // TODO Schnittstelle fehlt
                            //{
                            dest.Open();

                            var bacthBlock = new BatchBlock<DS>(BatchSize);
                            var DataFlowDestinationBlock = new ActionBlock<DS[]>(outp => dest.WriteBatch(outp));

                            TransformBlock<DS, DS> t_b = (TransformBlock<DS, DS>)v_source.BenutzerObjekte[1];
                            t_b.LinkTo(bacthBlock);
                            bacthBlock.LinkTo(DataFlowDestinationBlock);

                            t_b.Completion.ContinueWith(t => { bacthBlock.Complete(); });
                            bacthBlock.Completion.ContinueWith(t => { DataFlowDestinationBlock.Complete(); });

                            WatingForCompletitionCollection.Add(DataFlowDestinationBlock);
                        }
                        // for case that v_source is type of RowTransformFunction
                        // AND that v_dest is type of RowTransformFunction
                        else if (v_dest.BenutzerObjekte[0].GetType() == typeof(RowTransformation<DS>))
                        {
                            TransformBlock<DS, DS> t_b_source = (TransformBlock<DS, DS>)v_source.BenutzerObjekte[1];

                            RowTransformation<DS> tr = (RowTransformation<DS>)v_dest.BenutzerObjekte[0];
                            TransformBlock<DS, DS> t_b_dest = new TransformBlock<DS, DS>(tr.RowTransformFunction
                                , new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism });
                            ToCompleteCollection.Add(t_b_dest);
                            v_dest.BenutzerObjekte.Add(t_b_dest);

                            t_b_source.LinkTo(t_b_dest);
                            t_b_source.Completion.ContinueWith(t => { t_b_dest.Complete(); });
                            WatingForCompletitionCollection.Add(t_b_dest);
                        }
                        // for case that v_source is type of RowTransformFunction
                        // AND that v_dest is type of RowTransformMany
                        else if (v_dest.BenutzerObjekte[0].GetType() == typeof(RowTransformationMany<DS>))
                        {
                           

                            TransformBlock<DS, DS> t_b_source = (TransformBlock<DS, DS>)v_source.BenutzerObjekte[1];

                            RowTransformationMany<DS> t_many = (RowTransformationMany<DS>)v_dest.BenutzerObjekte[0];
                            TransformManyBlock<DS, DS> broadcastBlock = new TransformManyBlock<DS, DS>(t_many.RowTransformManyFunction
                                , new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism });
                            ToCompleteCollection.Add(broadcastBlock);
                            v_dest.BenutzerObjekte.Add(broadcastBlock);

                            t_b_source.LinkTo(broadcastBlock);
                            t_b_source.Completion.ContinueWith(t => { broadcastBlock.Complete(); });
                            WatingForCompletitionCollection.Add(broadcastBlock);
                        }
                        // for case that v_source is type of RowTransformFunction
                        // AND that v_dest is type of Broadcast
                        else if (v_dest.BenutzerObjekte[0].GetType() == typeof(BroadCast<DS>))
                        {
                            TransformBlock<DS, DS> t_b_source = (TransformBlock<DS, DS>)v_source.BenutzerObjekte[1];
                            
                            BroadCast<DS> t_broadcast = (BroadCast<DS>)v_dest.BenutzerObjekte[0];
                            BroadcastBlock<DS> broadcastBlock = new BroadcastBlock<DS>(null
                                , new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism });
                            ToCompleteCollection.Add(broadcastBlock);
                            v_dest.BenutzerObjekte.Add(broadcastBlock);

                            t_b_source.LinkTo(broadcastBlock);
                            t_b_source.Completion.ContinueWith(t => { broadcastBlock.Complete(); });
                            WatingForCompletitionCollection.Add(broadcastBlock);
                        }
                        // for case that type is not implemented
                        else
                        {
                            throw new Exception(string.Format("Type {0} is not implemented.", v_dest.BenutzerObjekte[0].GetType()));

                        }
                    }
                    // for case that v_source is type of RowTransformMany
                    else if (v_source.BenutzerObjekte[0].GetType() == typeof(RowTransformationMany<DS>))
                    {
                        // for case that v_source is type of RowTransformMany
                        // AND that v_dest is type of IDataFlowDestination
                        if (new List<Type>(new Type[] { typeof(DBDestination<DS>) }).Contains(v_dest.BenutzerObjekte[0].GetType()))
                        {
                            IDataFlowDestination<DS> dest = (IDataFlowDestination<DS>)v_dest.BenutzerObjekte[0];
                            //using (dest) // TODO Schnittstelle fehlt
                            //{
                            dest.Open();
                            
                            var bacthBlock = new BatchBlock<DS>(BatchSize);
                            var DataFlowDestinationBlock = new ActionBlock<DS[]>(outp => dest.WriteBatch(outp));

                            TransformManyBlock<DS, DS> t_b = (TransformManyBlock<DS, DS>)v_source.BenutzerObjekte[1];
                            t_b.LinkTo(bacthBlock);
                            bacthBlock.LinkTo(DataFlowDestinationBlock);

                            t_b.Completion.ContinueWith(t => { bacthBlock.Complete(); });
                            bacthBlock.Completion.ContinueWith(t => { DataFlowDestinationBlock.Complete(); });
                            WatingForCompletitionCollection.Add(DataFlowDestinationBlock);
                        }
                        // for case that v_source is type of RowTransformMany
                        // AND that v_dest is type of RowTransformFunction
                        else if (v_dest.BenutzerObjekte[0].GetType() == typeof(RowTransformation<DS>))
                        {
                            TransformManyBlock<DS, DS> t_b_source = (TransformManyBlock<DS, DS>)v_source.BenutzerObjekte[1];

                            RowTransformation<DS> tr = (RowTransformation<DS>)v_dest.BenutzerObjekte[0];
                            TransformBlock<DS, DS> t_b_dest = new TransformBlock<DS, DS>(tr.RowTransformFunction
                                , new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism });
                            ToCompleteCollection.Add(t_b_dest);
                            v_dest.BenutzerObjekte.Add(t_b_dest);

                            t_b_source.LinkTo(t_b_dest);
                            t_b_source.Completion.ContinueWith(t => { t_b_dest.Complete(); });
                            WatingForCompletitionCollection.Add(t_b_dest);
                        }
                        // for case that type is not implemented
                        else
                        {
                            throw new Exception(string.Format("Type {0} is not implemented.", v_dest.BenutzerObjekte[0].GetType()));

                        }
                    }
                    // for case that v_source is type of Broadcast
                    else if (v_source.BenutzerObjekte[0].GetType() == typeof(BroadCast<DS>))
                    {
                        // for case that v_source is type of Broadcast
                        // AND that v_dest is type of IDataFlowDestination
                        if (new List<Type>(new Type[] { typeof(DBDestination<DS>) }).Contains(v_dest.BenutzerObjekte[0].GetType()))
                        {
                            IDataFlowDestination<DS> dest = (IDataFlowDestination<DS>)v_dest.BenutzerObjekte[0];
                            //using (dest) // TODO Schnittstelle fehlt
                            //{
                            dest.Open();

                            var bacthBlock = new BatchBlock<DS>(BatchSize);
                            var DataFlowDestinationBlock = new ActionBlock<DS[]>(outp => dest.WriteBatch(outp));

                            BroadcastBlock<DS> t_b = (BroadcastBlock<DS>)v_source.BenutzerObjekte[1];
                            t_b.LinkTo(bacthBlock);
                            bacthBlock.LinkTo(DataFlowDestinationBlock);

                            t_b.Completion.ContinueWith(t => { bacthBlock.Complete(); });
                            bacthBlock.Completion.ContinueWith(t => { DataFlowDestinationBlock.Complete(); });
                            WatingForCompletitionCollection.Add(DataFlowDestinationBlock);
                        }
                        // for case that v_source is type of Broadcast
                        // AND that v_dest is type of RowTransformation
                        else if (v_dest.BenutzerObjekte[0].GetType() == typeof(RowTransformation<DS>))
                        {
                            BroadcastBlock<DS> t_b_source = (BroadcastBlock<DS>)v_source.BenutzerObjekte[1];

                            RowTransformation<DS> tr = (RowTransformation<DS>)v_dest.BenutzerObjekte[0];
                            TransformBlock<DS, DS> t_b_dest = new TransformBlock<DS, DS>(tr.RowTransformFunction
                                , new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism });
                            ToCompleteCollection.Add(t_b_dest);
                            v_dest.BenutzerObjekte.Add(t_b_dest);

                            t_b_source.LinkTo(t_b_dest);
                            t_b_source.Completion.ContinueWith(t => { t_b_dest.Complete(); });
                            WatingForCompletitionCollection.Add(t_b_dest);
                        }
                        // for case that type is not implemented
                        else
                        {
                            throw new Exception(string.Format("Type {0} is not implemented.", v_dest.BenutzerObjekte[0].GetType()));

                        }
                    }
                    // for case that type is not implemented
                    else
                    {
                        throw new Exception(string.Format("Type {0} is not implemented.", v_source.BenutzerObjekte[0].GetType()));
                    }
                }
            }
        }

        private void DoTransformOrThrowAnExceptionBecauseThereIsMoreToCheck(List<object> ToCompleteCollection)
        {
            foreach (object o in ToCompleteCollection)
            {
                NLogger.Info(string.Format("Execute Graph: ToComplete: {0}", o.GetType().Name));


                // for case that type is TransformBlock
                if (o.GetType() == typeof(TransformBlock<DS, DS>))
                    ((TransformBlock<DS, DS>)o).Complete();
                else if (o.GetType() == typeof(TransformManyBlock<DS, DS>))
                    ((TransformManyBlock<DS, DS>)o).Complete();
                // for case that type is BroadcastBlock
                else if (o.GetType() == typeof(BroadcastBlock<DS>))
                    ((BroadcastBlock<DS>)o).Complete();
                // for case that type is not implemented
                else throw new Exception(string.Format("Type {0} in ToCompleteCollection is not implemented.", o.GetType()));
            }
        }

        private void WaitForCompletionOrEndWithException(List<object> WatingForCompletitionCollection)
        {
            foreach (object o in WatingForCompletitionCollection)
            {

                NLogger.Info(string.Format("Execute Graph: WatingForCompletition: {0}", o.GetType().Name));

                // for case that type is TransformBlock
                if (o.GetType() == typeof(TransformBlock<DS, DS>))
                    ((TransformBlock<DS, DS>)o).Completion.Wait();
                // for case that type is TransformManyBlock
                else if (o.GetType() == typeof(TransformManyBlock<DS, DS>))
                    ((TransformManyBlock<DS, DS>)o).Completion.Wait();
                // for case that type is ActionBlock
                else if (o.GetType() == typeof(ActionBlock<DS[]>))
                    ((ActionBlock<DS[]>)o).Completion.Wait();
                // for case that type is BroadcastBlock
                else if (o.GetType() == typeof(BroadcastBlock<DS>))
                    ((BroadcastBlock<DS>)o).Completion.Wait();
                // for case that type is not implemented
                else throw new Exception(string.Format("Type {0} in WatingForCompletitionCollection is not implemented.", o.GetType()));

            }
        }

        private void DoDataFlowReaderCollection(Dictionary<IDataFlowSource<DS>, object> DataFlowReaderCollection)
        {
            foreach(KeyValuePair<IDataFlowSource<DS>, object> kvp in DataFlowReaderCollection)
            {
                IDataFlowSource<DS> source = kvp.Key;
                if (kvp.Value.GetType() == typeof(TransformBlock<DS, DS>))
                {
                    using (source) { source.Open(); source.Read((TransformBlock<DS, DS>)kvp.Value); }
                }
                else throw new Exception(string.Format("Type {0} in DoDataFlowReaderCollection is not implemented.", kvp.Value.GetType()));

            }
        }

        public override void Execute()
        {
            if (g != null) { Execute_Graph(); return; }

            // test code
            if (DataFlowSource == null) throw new InvalidOperationException("DataFlowSource is null.");
            else if (DataFlowDestination == null) throw new InvalidOperationException("DataFlowDestination is null.");


            using (DataFlowSource)
            {
                DataFlowSource.Open();
                DataFlowDestination.Open();

                var RowTransformBlock = new TransformBlock<DS, DS>(RowTransformFunction
                    , new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism });


                var bacthBlock = new BatchBlock<DS>(BatchSize);
                var bacthBlock2 = new BatchBlock<DS>(BatchSize);
                var DataFlowDestinationBlock = new ActionBlock<DS[]>(outp => DataFlowDestination.WriteBatch(outp));


                DBDestination<string[]> destination2 = new DBDestination<string[]>();
                destination2.TableName_Target = "test.Staging2";
                destination2.FieldCount = 4;
                destination2.Connection = ControlFlow.CurrentDbConnection;
                IDataFlowDestination<DS> dest2 = (IDataFlowDestination<DS>)destination2;
                dest2.ObjectMappingMethod = DataFlowDestination.ObjectMappingMethod;
                dest2.Open();
                var DataFlowDestinationBlock2 = new ActionBlock<DS[]>(outp => dest2.WriteBatch(outp));


                //RowTransformBlock.LinkTo(bacthBlock);


                var broadcastBlock = new BroadcastBlock<DS>(RowTransformFunction,
                    new ExecutionDataflowBlockOptions {
                        MaxDegreeOfParallelism = this.MaxDegreeOfParallelism

                    });

                RowTransformBlock.LinkTo(broadcastBlock);
                broadcastBlock.LinkTo(bacthBlock2);
                broadcastBlock.LinkTo(bacthBlock);
                

                bacthBlock.LinkTo(DataFlowDestinationBlock);
                bacthBlock2.LinkTo(DataFlowDestinationBlock2);

                //RowTransformBlock.Completion.ContinueWith(t => { bacthBlock.Complete(); });
                RowTransformBlock.Completion.ContinueWith(t => { broadcastBlock.Complete(); });
                broadcastBlock.Completion.ContinueWith(t => { bacthBlock.Complete(); });
                broadcastBlock.Completion.ContinueWith(t => { bacthBlock2.Complete(); });

                bacthBlock.Completion.ContinueWith(t => { DataFlowDestinationBlock.Complete(); });
                bacthBlock2.Completion.ContinueWith(t => { DataFlowDestinationBlock2.Complete(); });

                DataFlowSource.Read(RowTransformBlock);

                RowTransformBlock.Complete();
                
                DataFlowDestinationBlock.Completion.Wait();
                DataFlowDestinationBlock2.Completion.Wait();

            }
        }


        public static void Execute(string name, IDataFlowSource<DS> DataFlowSource, IDataFlowDestination<DS> DataFlowDestination, int batchSize
            , Func<DS, DS> rowTransformFunction) => 
            new DataFlowTask<DS>(name, DataFlowSource, DataFlowDestination,batchSize, rowTransformFunction).Execute();

        public static void Execute(string name, IDataFlowSource<DS> DataFlowSource, IDataFlowDestination<DS> DataFlowDestination, int batchSize, int MaxDegreeOfParallelism
            , Func<DS, DS> rowTransformFunction) => 
            new DataFlowTask<DS>(name, DataFlowSource, DataFlowDestination, batchSize, MaxDegreeOfParallelism, rowTransformFunction).Execute();

        public static void Execute(string name, int batchSize, int MaxDegreeOfParallelism, Graph g) =>
            new DataFlowTask<DS>(name, batchSize, MaxDegreeOfParallelism, g).Execute();


    }
}
