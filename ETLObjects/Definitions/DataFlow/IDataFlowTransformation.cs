using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public interface IDataFlowTransformation<DS>
    {
        Func<DS, DS> rowTransformFunction { get; set; }
    }
}
