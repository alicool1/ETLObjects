using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;

namespace ALE.ETLToolbox
{
    public class SQLDestination<DS> : IDataFlowDestination<DS>
        where DS : new()
    {
        private int _MaxBufferSize = 10000;
        public int MaxBufferSize
        {
            get
            {
                return _MaxBufferSize;
            }
            set
            {
                if (value > 0)
                {
                    _MaxBufferSize = value;
                }
            }
        }

        public SQLDestination(string ServerName, string InitialCatalog)
        {
            this.InitialCatalog = InitialCatalog;
            SqlServer = ServerName;
        }

        public int FieldCount;
        

        public string SqlServer { get; set; }
        /// <summary>
        /// SQL-Server Datenbankname
        /// </summary>
        public string InitialCatalog { get; set; }
        public int ConnectionTimeOut { get; set; }

        public string DestinationTableName { get; set; }

        private string _conectionString = "Data Source={0};Initial Catalog={1};Integrated Security=True;Connection Timeout={2}";
        private SqlConnection _con = new SqlConnection();
        private SqlBulkCopy _sqlBulkCopy = null;

        private EnumerableToDataReader<DS> _enumerableToDataReader = new EnumerableToDataReader<DS>();

        public void Init()
        {
            if (string.IsNullOrEmpty(SqlServer)) throw new InvalidOperationException("Fehler: Die SqlServer Eigengschaft wurde nicht gesetzt.");
            if (string.IsNullOrEmpty(InitialCatalog)) throw new InvalidOperationException("Fehler: Die InitialCatalog (SQL Server Datenbank) Eigengschaft wurde nicht gesetzt.");
            if (string.IsNullOrEmpty(DestinationTableName)) throw new InvalidOperationException("Fehler: Die DestinationTableName Eigengschaft wurde nicht gesetzt.");
            if (ObjectMappingMethod == null) throw new InvalidOperationException("Fehler: Die ObjectMappingMethod Eigengschaft wurde nicht gesetzt.");

            _con = new SqlConnection();
            _con.ConnectionString = string.Format(_conectionString, SqlServer, InitialCatalog, ConnectionTimeOut.ToString());
            _con.Open();

            _sqlBulkCopy = new SqlBulkCopy(_con);

            _sqlBulkCopy.DestinationTableName = DestinationTableName;
            _sqlBulkCopy.BatchSize = 1000;

            _enumerableToDataReader.ObjectMappingMethod = ObjectMappingMethod;
        }

        public EnumerableToDataReader<DS>.ObjectMapping ObjectMappingMethod { get; set; }



        private void BulkCopy(List<DS> dataList)
        {
            
            _enumerableToDataReader.EnumarableList = dataList;
            _enumerableToDataReader.FieldCount = FieldCount;

            _sqlBulkCopy.WriteToServer(_enumerableToDataReader);
        }

        /// <summary>
        /// Fügt den Datensatz in <see cref="DestinationTableName"/> ein.
        /// </summary>
        /// <param name="resultList"></param>
        public void Insert(List<DS> resultList)
        {
            BulkCopy(resultList);
        }
    }
}
