using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public class Node
    {
        public int item;
        public Node left;
        public Node right;
        public override string ToString()
        {
            return item.ToString();
        }
    }
}
