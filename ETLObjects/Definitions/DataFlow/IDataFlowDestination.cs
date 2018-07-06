using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public interface IDataFlowDestination<DS> : IDisposable
    {
        void WriteBatch(DS[] resultList);

        void WriteBatch(InMemoryTable resultList);

        void Open();

        int MaxBufferSize { get; }

        int FieldCount { get; set; }

        EnumerableToDataReader<DS>.ObjectMapping ObjectMappingMethod { get; set; }
    }
}
