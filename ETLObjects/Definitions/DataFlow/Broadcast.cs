using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public class Broadcast<DS> : IDataFlowTransformation<DS>
    {
        public Broadcast(Func<DS, DS> cloneTransformFunction)
        {
            this.CloneTransformFunction = cloneTransformFunction;
        }
        public Func<DS, DS> CloneTransformFunction { get; set; }

        public override string ToString()
        {
            return CloneTransformFunction.Method.Name;
        }
    }
}
