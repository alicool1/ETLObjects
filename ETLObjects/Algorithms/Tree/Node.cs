using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    class Node
    {
        public int item;
        public Node left;
        public Node right;
        public void display()
        {
            Console.Write("[");
            Console.Write(item);
            Console.Write("]");
        }
    }
}
