using ETLObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjectsSandbox
{
    class Program
    {
        public class Datensatz_Http
        {
            public string token;
            public override string ToString()
            {
                return token;
            }
        }

        // HttpClient is intended to be instantiated once per application, rather than per-use. See Remarks.
        static readonly HttpClient client = new HttpClient();

        static async Task t1()
        {
            // Call asynchronous network methods in a try/catch block to handle exceptions.
            try
            {
                HttpResponseMessage response = await client.GetAsync("https://www.boerse-frankfurt.de/index/dax/kurshistorie/tickdaten");
                response.EnsureSuccessStatusCode();
                string responseBody = await response.Content.ReadAsStringAsync();
                // Above three lines can be replaced with new helper method below
                // string responseBody = await client.GetStringAsync(uri);

                Console.WriteLine(responseBody);
            }
            catch (HttpRequestException e)
            {
                Console.WriteLine("\nException Caught!");
                Console.WriteLine("Message :{0} ", e.Message);
            }
        }


        static void Main(string[] args)
        {

             var x = t1();

            
               
            


            Graph g_html = new Graph();
            HttpSource<Datensatz_Http> httpSource = new HttpSource<Datensatz_Http>("https://www.boerse-frankfurt.de/index/dax/kurshistorie/tickdaten");
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
