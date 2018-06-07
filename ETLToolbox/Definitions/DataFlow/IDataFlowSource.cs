using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLObjects
{
    public interface IDataFlowSource<DS> : IDisposable
    {
        IEnumerable<DS> EnumerableDataSource { get; }

        void Open();

        void Read(ITargetBlock<DS> TargetBlock);

        Func<DS, DS> target_transformation { get; set; }

    }

    
}
