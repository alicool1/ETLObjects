using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public interface IDataFlowDestination<DS>
    {
        void Insert(List<DS> resultList);
        void Init();

        int MaxBufferSize { get; }
    }
}
