using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public interface IConnectionManager
    {
        string InitialCatalog { get; set; }

        string ServerName { get; set; }

        void Open();

        void Close();

    }
}
