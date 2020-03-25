using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;
using System.Data;


namespace ETLObjects {

    public class ListDestination<DS> : IDataFlowDestination<DS> {

        public ListDestination()
        {
            List = new List<DS>();
        }

        public string ObjectName { get; set; }

        public List<DS> List { get; set; }

        public EnumerableToDataReader<DS>.ObjectMapping ObjectMappingMethod { get; set; }
        public int FieldCount { get; set; }

      
        public void WriteBatch(DS[] batchData)
        {
            List.AddRange(batchData);
        }

        public override string ToString()
        {
            return ObjectName;
        }

    }

}
