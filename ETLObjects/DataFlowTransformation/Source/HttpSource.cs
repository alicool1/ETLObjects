using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLObjects
{

    public class HttpSource<DS> : IDataFlowSource<DS>
    {
        /// <summary>
        /// Delegate-Typ für Methoden, die einen string (eines Datareaders) in ein IEnumerable Klassenobjekt vom Typen DS umwandeln kann.
        /// </summary>
        /// <param name="record"></param>
        /// <returns></returns>
        public delegate DS DataMapping(string record);

        /// <summary>
        /// Enthält die Datenadapter-Methode, die das string-Objekt in eine Objekt vom Typen DS umwandelt
        /// </summary>
        public DataMapping DataMappingMethod { get; set; }

        public HttpSource(string Uri)
        {
            this.Uri = Uri;
        }

        private string Uri;

        public IEnumerable<DS> EnumerableDataSource
        {
            get { return _dataReaderToEnumerable; }
        }

        private IEnumerable<DS> _dataReaderToEnumerable;


        public char[] TokenOpen = new char[] { '<' };
        public char[] TokenClose = new char[] { '>' };

        public async Task Read(ITargetBlock<DS> target)
        {
            var readingStream = new HttpClient().GetStreamAsync(Uri);
            using (readingStream)
            {
                byte[] temp = new byte[1024];
                int len = 0;
                string token = string.Empty;
                while ((len = readingStream.Result.Read(temp, 0, temp.Length)) > 0)
                {
                    string s = Encoding.UTF8.GetString(temp, 0, len);
                    foreach (char c in s)
                    {
                        if (TokenOpen.Contains(c))
                        {
                            await target.SendAsync(DataMappingMethod(token));
                            token = string.Empty;
                        }
                        token += c;
                        if (TokenClose.Contains(c))
                        {
                            await target.SendAsync(DataMappingMethod(token));
                            token = string.Empty;
                        }
                    }
                }
            }
        }
    }
}
