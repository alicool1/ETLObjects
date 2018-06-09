using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLObjects
{
    public class RowTransformationMany<DS> : IDataFlowTransformation<DS>
    {
        public RowTransformationMany(Func<DS, DS[]> rowTransformManyFunction)
        {
            this.RowTransformManyFunction = rowTransformManyFunction;
        }
        public Func<DS, DS[]> RowTransformManyFunction { get; set; }

        public override string ToString()
        {
            return RowTransformManyFunction.Method.Name;
        }
    }
}
