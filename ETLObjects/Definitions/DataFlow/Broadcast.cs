using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public class BroadCast<DS> : IDataFlowTransformation<DS>
    {
        public BroadCast()
        {

        }
        public BroadCast(Func<DS, DS> cloneTransformFunction)
        {
            this.CloneTransformFunction = cloneTransformFunction;
        }
        public Func<DS, DS> CloneTransformFunction { get; set; }

        public override string ToString()
        {
            return "BroadCast";
        }
    }
}
