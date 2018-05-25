using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public interface IDataFlowSource<DS>
    {
        IEnumerable<DS> EnumerableDataSource { get; }

        void Init();
    }

    
}
