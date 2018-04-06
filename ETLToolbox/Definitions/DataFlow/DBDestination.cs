using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ALE.ETLToolbox {
    public class DBDestination {
        public IConnectionManager Connection { get; set; }

        public string TableName { get; set; }

        public void WriteBatch(InMemoryTable batchData) {
            new SqlTask($"Execute Bulk insert into {TableName}") { ConnectionManager = Connection }.BulkInsert(batchData, batchData.ColumnMapping, TableName);
        }
    }

}
