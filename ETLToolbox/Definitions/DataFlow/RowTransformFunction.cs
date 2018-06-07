using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public class RowTransformFunction<DS> : IDataFlowTransformation<DS>
    {
        public RowTransformFunction(Func<DS, DS> rowTransformFunction)
        {
            this.rowTransformFunction = rowTransformFunction;
        }
        public Func<DS, DS> rowTransformFunction { get; set; }

        public override string ToString()
        {
            return rowTransformFunction.Method.Name;
        }
    }
}
