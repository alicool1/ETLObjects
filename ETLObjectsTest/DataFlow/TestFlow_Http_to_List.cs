using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Data;
using MySql.Data.MySqlClient;
using System.Collections.Generic;

namespace ETLObjectsTest
{
    public partial class ETLObjectsTest
    {

        public class Datensatz_Http
        {
            public string token;
            public override string ToString()
            {
                return token;
            }
        }

        [TestMethod]
        public void TestDataflow_Http_to_List()
        {
            Graph g_html = new Graph();
            HttpSource<Datensatz_Http> httpSource = new HttpSource<Datensatz_Http>("https://www.dreckstool.de/hitlist");
            httpSource.DataMappingMethod = (token) =>
            {
                var Datensatz_Http = new Datensatz_Http();
                Datensatz_Http.token = token;
                return Datensatz_Http;
            };
            ListDestination<Datensatz_Http> Listdestination_html = new ListDestination<Datensatz_Http>();
            g_html.GetVertex(0, httpSource);
            g_html.GetVertex(1, Listdestination_html);
            g_html.AddEdge(0, 1);
            DataFlowTask<Datensatz_Http>.Execute("http df task", 10000, 1, g_html);
        }

        

    }
}
