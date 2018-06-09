using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public class RowTransformation<DS> : IDataFlowTransformation<DS>
    {
        public RowTransformation(Func<DS, DS> rowTransformFunction)
        {
            this.RowTransformFunction = rowTransformFunction;
        }
        public Func<DS, DS> RowTransformFunction { get; set; }

        public override string ToString()
        {
            return RowTransformFunction.Method.Name;
        }
    }
}
