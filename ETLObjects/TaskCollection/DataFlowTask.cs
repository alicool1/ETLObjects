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
    public class DataFlowTask<DS>
    {
        DataflowLinkOptions linkOptions = new DataflowLinkOptions { PropagateCompletion = true };
        /* Public properties */

        private int BatchSize { get; set; }

        private int MaxDegreeOfParallelism { get; set; } = 1;

        public Graph g { get; set; }

        public DataFlowTask()
        {

        }
    
        public DataFlowTask(string name, int batchSize, int MaxDegreeOfParallelism, Graph g) : this()
        {          
            this.MaxDegreeOfParallelism = MaxDegreeOfParallelism;
            this.BatchSize = batchSize;
            this.g = g;
        }

        public void Execute()
        {
            List<object> WatingForCompletitionCollection = new List<object>();
            List<object> ToCompleteCollection = new List<object>();
            Dictionary<IDataFlowSource<DS>, object> DataFlowReaderCollection = new Dictionary<IDataFlowSource<DS>, object>();

            // the algorithm for topological sorting is required 
            // for the chronological order in the DataFlow-Pipeline
            InterpreteGraph(ref WatingForCompletitionCollection, ref ToCompleteCollection, ref DataFlowReaderCollection);

            // begin to read all readers
            DataFlowReadersExecute(ref DataFlowReaderCollection);

            // complete all blocks
            DataFlowBlockComplete(ref ToCompleteCollection);

            // wait for completition
            WaitForCompletion(ref WatingForCompletitionCollection);
        }


        /// <summary>
        /// InterpreteGraph loops over all vertices and over all edges of each vertex.
        /// The edges are directed, so each edge has a source-vertex and a destination-vertex.
        /// The edges are beeing translated to TPL-DataFlow-Objects, so a TPL-DataFlow is going generated and executed during Run-Time. 
        /// </summary>
        /// <param name="WatingForCompletitionCollection"></param>
        /// <param name="ToCompleteCollection"></param>
        /// <param name="DataFlowReaderCollection"></param>
        private void InterpreteGraph(ref List<object> WatingForCompletitionCollection, ref List<object> ToCompleteCollection, ref Dictionary<IDataFlowSource<DS>, object> DataFlowReaderCollection)
        {
            // the algorithm for topological sorting is required 
            // for the chronological order in the TPL-DataFlow-Pipeline
            foreach (long i in TopoSort.SortGraph(g).Keys)
            {
                Vertex v_source = g.GetVertex(i, null);
                // loop over all edges of v_source 
                foreach (Edge e in v_source.edges)
                {
                    Vertex v_dest = e.dest;

                    // for case that object in vertex is missing
                    if (v_source.UserDefinedObjects == null || v_source.UserDefinedObjects[0] == null)
                    {
                        string CurrentMethodName = new System.Diagnostics.StackFrame(0, true).GetMethod().Name;
                        throw new Exception(string.Format("Error in {0}. Vertex needs any object. For example an IDataFlowSource.", CurrentMethodName));
                    }

                    // GeneratePipeline_Transformation_to_Transformation
                    // for the case that v_source AND v_dest are type of IDataFlowTransformation
                    else if (new List<Type>(new Type[] { typeof(RowTransformation<DS>), typeof(RowTransformationMany<DS>), typeof(BroadCast<DS>) }).Contains(v_source.UserDefinedObjects[0].GetType())
                        && new List<Type>(new Type[] { typeof(RowTransformation<DS>), typeof(RowTransformationMany<DS>), typeof(BroadCast<DS>) }).Contains(v_dest.UserDefinedObjects[0].GetType()))
                    {
                        GeneratePipeline_Transformation_to_Transformation(v_source, v_dest, ref ToCompleteCollection, ref WatingForCompletitionCollection);
                    }

                    // GeneratePipeline_DataFlowSource_to_Transformation
                    // for the case that v_source is type of IDataFlowSource AND v_dest is type of IDataFlowTransformation
                    else if (new List<Type>(new Type[] { typeof(CSVSource<DS>), typeof(SqlSource<DS>), typeof(MySqlSource<DS>), typeof(HttpSource<DS>) }).Contains(v_source.UserDefinedObjects[0].GetType())
                        && new List<Type>(new Type[] { typeof(RowTransformation<DS>), typeof(RowTransformationMany<DS>), typeof(BroadCast<DS>) }).Contains(v_dest.UserDefinedObjects[0].GetType()))
                    {
                        GeneratePipeline_DataFlowSource_to_Transformation(v_source, v_dest, ref ToCompleteCollection, ref WatingForCompletitionCollection, ref DataFlowReaderCollection);
                    }


                    // GeneratePipeline_Transformation_to_DataFlowDestination
                    // for the case that v_source is type of IDataFlowTransformation AND v_dest is type of IDataFlowDestination
                    else if (new List<Type>(new Type[] { typeof(RowTransformation<DS>), typeof(RowTransformationMany<DS>), typeof(BroadCast<DS>) }).Contains(v_source.UserDefinedObjects[0].GetType())
                        && new List<Type>(new Type[] { typeof(SqlDestination<DS>), typeof(ListDestination<DS>), typeof(MySqlDestination<DS>) }).Contains(v_dest.UserDefinedObjects[0].GetType()))
                    {
                        GeneratePipeline_Transformation_to_DataFlowDestination(v_source, v_dest, ref ToCompleteCollection, ref WatingForCompletitionCollection);
                    }

                    // GeneratePipeline_DataFlowSource_to_DataFlowDestination
                    // for the case that v_source is type of IDataFlowSource AND v_dest is type of IDataFlowDestination
                    else if (new List<Type>(new Type[] { typeof(CSVSource<DS>), typeof(SqlSource<DS>), typeof(MySqlSource<DS>), typeof(HttpSource<DS>) }).Contains(v_source.UserDefinedObjects[0].GetType())
                        && new List<Type>(new Type[] { typeof(SqlDestination<DS>), typeof(ListDestination<DS>), typeof(MySqlDestination<DS>) }).Contains(v_dest.UserDefinedObjects[0].GetType()))
                    {
                        GeneratePipeline_DataFlowSource_to_DataFlowDestination(v_source, v_dest, ref ToCompleteCollection, ref WatingForCompletitionCollection, ref DataFlowReaderCollection);
                    }

                    else
                    {
                        string CurrentMethodName = new System.Diagnostics.StackFrame(0, true).GetMethod().Name;
                        throw new Exception(string.Format("Not implemented Combination of Types in {0}.", CurrentMethodName));
                    }
                }
            }
        }

        /// <summary>
        /// GeneratePipeline_Transformation_to_Transformation generates a TPL-DataFlowPipeline between two vertices of a graph.
        /// v_source.UserDefinedObjects[0] and v_dest.UserDefinedObjects[0] has to be Type of IDataFlowTransformation - so its a pipeline between two transformations.
        /// </summary>
        /// <param name="v_source"></param>
        /// <param name="v_dest"></param>
        /// <param name="ToCompleteCollection"></param>
        /// <param name="WatingForCompletitionCollection"></param>
        private void GeneratePipeline_Transformation_to_Transformation(Vertex v_source, Vertex v_dest, ref List<object> ToCompleteCollection, ref List<object> WatingForCompletitionCollection)
        {
            var t_b_source = (IPropagatorBlock<DS, DS>)v_source.UserDefinedObjects[1];
            var t_b_dest = (IPropagatorBlock<DS, DS>)null;


            if (v_dest.UserDefinedObjects[0].GetType() == typeof(RowTransformation<DS>))
            {
                RowTransformation<DS> tr = (RowTransformation<DS>)v_dest.UserDefinedObjects[0];
                t_b_dest = new TransformBlock<DS, DS>(tr.RowTransformFunction
                , new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism });
            }
            else if (v_dest.UserDefinedObjects[0].GetType() == typeof(RowTransformationMany<DS>))
            {
                RowTransformationMany<DS> t_many = (RowTransformationMany<DS>)v_dest.UserDefinedObjects[0];
                t_b_dest = new TransformManyBlock<DS, DS>(t_many.RowTransformManyFunction
                    , new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism });

            }
            else if (v_dest.UserDefinedObjects[0].GetType() == typeof(BroadCast<DS>))
            {
                BroadCast<DS> t_broadcast = (BroadCast<DS>)v_dest.UserDefinedObjects[0];
                t_b_dest = new BroadcastBlock<DS>(t_broadcast.TransformFunction
                    , new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism });
            }
            else
            {
                string CurrentMethodName = new System.Diagnostics.StackFrame(0, true).GetMethod().Name;
                throw new Exception(string.Format("Not implemented Type {0} in {1}.", v_dest.UserDefinedObjects[0].GetType(), CurrentMethodName));
            }
            //ToCompleteCollection.Add(t_b_dest); // fuehrte zu Fehlern bei dotnet execute auf linux-system
            v_dest.UserDefinedObjects.Add(t_b_dest);

            t_b_source.LinkTo(t_b_dest, linkOptions);
            t_b_source.Completion.ContinueWith(t => { t_b_dest.Complete(); });

            WatingForCompletitionCollection.Add(t_b_dest);

        }


        /// <summary>
        /// GeneratePipeline_DataFlowSource_to_Transformation generates a TPL-DataFlowPipeline between two vertices of a graph.
        /// v_source.UserDefinedObjects[0] has to be Type of IDataFlowSource
        /// v_dest.UserDefinedObjects[0] has to be Type of IDataFlowTransformation
        /// </summary>
        /// <param name="v_source"></param>
        /// <param name="v_dest"></param>
        /// <param name="ToCompleteCollection"></param>
        /// <param name="WatingForCompletitionCollection"></param>
        private void GeneratePipeline_DataFlowSource_to_Transformation(Vertex v_source, Vertex v_dest, ref List<object> ToCompleteCollection, ref List<object> WatingForCompletitionCollection, ref Dictionary<IDataFlowSource<DS>, object> DataFlowReaderCollection)
        {
            
            var t_b_source = (IDataFlowSource<DS>)v_source.UserDefinedObjects[0];
            var t_b_dest = (IPropagatorBlock<DS, DS>)null;

            // for case that object in vertex is missing
            if (v_dest.UserDefinedObjects == null || v_dest.UserDefinedObjects[0] == null)
                throw new Exception(string.Format("Vertex needs any object. For example DBDestination."));
            // for case that v_dest is type of RowTransformFunction
            else if (v_dest.UserDefinedObjects[0].GetType() == typeof(RowTransformation<DS>))
            {
                RowTransformation<DS> t = (RowTransformation<DS>)v_dest.UserDefinedObjects[0];
                t_b_dest = new TransformBlock<DS, DS>(t.RowTransformFunction
                    , new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism });
            }
            // for case that v_dest is type of RowTransformationMany
            else if (v_dest.UserDefinedObjects[0].GetType() == typeof(RowTransformationMany<DS>))
            {
                RowTransformationMany<DS> t_many = (RowTransformationMany<DS>)v_dest.UserDefinedObjects[0];
                t_b_dest = new TransformManyBlock<DS, DS>(t_many.RowTransformManyFunction
                    , new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism });
            }
            // for case that v_dest is type of RowTransformationMany
            else if (v_dest.UserDefinedObjects[0].GetType() == typeof(BroadCast<DS>))
            {
                BroadCast<DS> t_broadcast = (BroadCast<DS>)v_dest.UserDefinedObjects[0];
                t_b_dest = new BroadcastBlock<DS>(t_broadcast.TransformFunction
                        , new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism });
            }
            // for case that type is not implemented
            else
            {
                string CurrentMethodName = new System.Diagnostics.StackFrame(0, true).GetMethod().Name;
                throw new Exception(string.Format("Not implemented Type {0} in {1}.", v_dest.UserDefinedObjects[0].GetType(), CurrentMethodName));
            }

            ToCompleteCollection.Add(t_b_dest);
            v_dest.UserDefinedObjects.Add(t_b_dest);
            DataFlowReaderCollection.Add(t_b_source, t_b_dest);
        }

        /// <summary>
        /// GeneratePipeline_Transformation_to_DataFlowDestination generates a TPL-DataFlowPipeline between two vertices of a graph.
        /// v_source.UserDefinedObjects[0] has to be Type of IDataFlowTransformation
        /// v_dest.UserDefinedObjects[0] has to be Type of IDataFlowDestination
        /// </summary>
        /// <param name="v_source"></param>
        /// <param name="v_dest"></param>
        /// <param name="ToCompleteCollection"></param>
        /// <param name="WatingForCompletitionCollection"></param>
        private void GeneratePipeline_Transformation_to_DataFlowDestination(Vertex v_source, Vertex v_dest, ref List<object> ToCompleteCollection, ref List<object> WatingForCompletitionCollection)
        {           
            var t_b_source = (IPropagatorBlock<DS, DS>)v_source.UserDefinedObjects[1];
            
            IDataFlowDestination<DS> dest = (IDataFlowDestination<DS>)v_dest.UserDefinedObjects[0];

            var bacthBlock = new BatchBlock<DS>(BatchSize);
            var DataFlowDestinationBlock = new ActionBlock<DS[]>(outp => dest.WriteBatch(outp));


            if (v_source.UserDefinedObjects[0].GetType() == typeof(RowTransformation<DS>))
            {
                t_b_source = (TransformBlock<DS, DS>)v_source.UserDefinedObjects[1];
            }
            else if (v_source.UserDefinedObjects[0].GetType() == typeof(RowTransformationMany<DS>))
            {
                t_b_source = (TransformManyBlock<DS, DS>)v_source.UserDefinedObjects[1];
            }
            else if (v_source.UserDefinedObjects[0].GetType() == typeof(BroadCast<DS>))
            {
                t_b_source = (BroadcastBlock<DS>)v_source.UserDefinedObjects[1];
            }
            else
            {
                string CurrentMethodName = new System.Diagnostics.StackFrame(0, true).GetMethod().Name;
                throw new Exception(string.Format("Not implemented Type {0} in {1}.", v_source.UserDefinedObjects[1].GetType(), CurrentMethodName));
            }

            t_b_source.LinkTo(bacthBlock, linkOptions);
            bacthBlock.LinkTo(DataFlowDestinationBlock, linkOptions);

            t_b_source.Completion.ContinueWith(t => { bacthBlock.Complete(); });
            bacthBlock.Completion.ContinueWith(t => { DataFlowDestinationBlock.Complete(); });

            WatingForCompletitionCollection.Add(DataFlowDestinationBlock);


        }

        /// <summary>
        /// GeneratePipeline_DataFlowSource_to_DataFlowDestination generates a TPL-DataFlowPipeline between two vertices of a graph.
        /// v_source.UserDefinedObjects[0] has to be Type of IDataFlowSource
        /// v_dest.UserDefinedObjects[0] has to be Type of IDataFlowDestination
        /// </summary>
        /// <param name="v_source"></param>
        /// <param name="v_dest"></param>
        /// <param name="ToCompleteCollection"></param>
        /// <param name="WatingForCompletitionCollection"></param>
        private void GeneratePipeline_DataFlowSource_to_DataFlowDestination(Vertex v_source, Vertex v_dest, ref List<object> ToCompleteCollection, ref List<object> WatingForCompletitionCollection, ref Dictionary<IDataFlowSource<DS>, object> DataFlowReaderCollection)
        {           
            IDataFlowSource<DS> t_b_source = (IDataFlowSource<DS>)v_source.UserDefinedObjects[0];
            IDataFlowDestination<DS> dest = (IDataFlowDestination<DS>)v_dest.UserDefinedObjects[0];

            TransformBlock<DS, DS> t_b_dummy = new TransformBlock<DS, DS>(DS => { return DS; }
                , new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = this.MaxDegreeOfParallelism });

            ToCompleteCollection.Add(t_b_dummy);
            v_dest.UserDefinedObjects.Add(t_b_dummy);
            DataFlowReaderCollection.Add(t_b_source, t_b_dummy);


            var bacthBlock = new BatchBlock<DS>(BatchSize);
            var DataFlowDestinationBlock = new ActionBlock<DS[]>(outp => dest.WriteBatch(outp));
            t_b_dummy.LinkTo(bacthBlock, linkOptions);
            bacthBlock.LinkTo(DataFlowDestinationBlock, linkOptions);

            t_b_dummy.Completion.ContinueWith(t => { bacthBlock.Complete(); });
            bacthBlock.Completion.ContinueWith(t => { DataFlowDestinationBlock.Complete(); });

            WatingForCompletitionCollection.Add(DataFlowDestinationBlock);


        }


        private void DataFlowBlockComplete(ref List<object> ToCompleteCollection)
        {
            foreach (object o in ToCompleteCollection)
            {               
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

        private void WaitForCompletion(ref List<object> WatingForCompletitionCollection)
        {          
            foreach (object o in WatingForCompletitionCollection)
            {
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

        private void DataFlowReadersExecute(ref Dictionary<IDataFlowSource<DS>, object> DataFlowReaderCollection)
        {
            foreach (KeyValuePair<IDataFlowSource<DS>, object> kvp in DataFlowReaderCollection)
            {
                IDataFlowSource<DS> source = kvp.Key;
                if (kvp.Value.GetType() == typeof(TransformBlock<DS, DS>))
                {
                    source.Read((TransformBlock<DS, DS>)kvp.Value); 
                }
                else if (kvp.Value.GetType() == typeof(TransformManyBlock<DS, DS>))
                {
                    source.Read((TransformManyBlock<DS, DS>)kvp.Value); 
                }
                else if (kvp.Value.GetType() == typeof(BroadcastBlock<DS>))
                {
                    source.Read((BroadcastBlock<DS>)kvp.Value);
                }
                else throw new Exception(string.Format("Type {0} in DoDataFlowReaderCollection is not implemented.", kvp.Value.GetType()));
            }
        }
        

        public static void Execute(string name, int batchSize, int MaxDegreeOfParallelism, Graph g) =>
            new DataFlowTask<DS>(name, batchSize, MaxDegreeOfParallelism, g).Execute();


    }
}
