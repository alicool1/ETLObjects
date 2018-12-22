using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public interface IDataFlowDestination<DS> 
    {
        void WriteBatch(DS[] resultList);

        
        int MaxBufferSize { get; }

        int FieldCount { get; set; }


        EnumerableToDataReader<DS>.ObjectMapping ObjectMappingMethod { get; set; }
    }
}
