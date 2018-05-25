using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace ETLObjects {
    public interface IDbConnectionManager : IConnectionManager  {      
        int ExecuteNonQuery(string command);
        object ExecuteScalar(string command);
        IDataReader ExecuteReader(string command);
        void BulkInsert(IDataReader data, IColumnMappingCollection columnMapping, string tableName);
        IDbConnectionManager Clone();
    }
}
