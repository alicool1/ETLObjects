using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public class Node<T>
    {
        private T item;
        public Node<T> left;
        public Node<T> right;


        public void SetItem(ref T it)
        {
            item = it;
        }


        public T GetItem()
        {
            return item;
        }



        public override string ToString()
        {
            return item.ToString();
        }
    }
}
