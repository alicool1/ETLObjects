using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ALE.ETLToolbox
{
    public interface IDataFlowSource<DS>
    {
        IEnumerable<DS> EnumerableDataSource { get; }

        void Init();
    }

    
}
