using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public class Tree<T> where T : System.IComparable<T>
    {
        public Node<T> root;
        public Tree()
        {
            root = null;
        }
        public Node<T> ReturnRoot()
        {
            return root;
        }

        public bool LessThan<T>(T a, T b) where T : System.IComparable<T>
        {
            if (a.GetType() == typeof(string))
            {
                return String.Compare(a.ToString(), b.ToString()) < 0 ? true : false;
            }
            else if (a.GetType() == typeof(int))
            {
                return (int)(object)a < (int)(object)b;
            }
            else
            {
                throw new Exception("not implemented type.");
            }
        }

        public void Insert(T id)
        {
            Node<T> newNode = new Node<T>();
            newNode.SetItem(ref id);
            if (root == null)
                root = newNode;
            else
            {
                Node<T> current = root;
                Node<T> parent;
                while (true)
                {
                    parent = current;
                    if (LessThan(id, current.GetItem()))
                    {
                        current = current.left;
                        if (current == null)
                        {
                            parent.left = newNode;
                            return;
                        }
                    }
                    else
                    {
                        current = current.right;
                        if (current == null)
                        {
                            parent.right = newNode;
                            return;
                        }
                    }
                }
            }
        }
        public void Preorder(Node<T> Root)
        {
            if (Root != null)
            {
                Console.Write(Root.GetItem() + ",");
                Preorder(Root.left);
                Preorder(Root.right);
            }
        }
        public void Inorder(Node<T> Root)
        {
            if (Root != null)
            {
                Inorder(Root.left);
                Console.Write(Root.GetItem() + ",");
                Inorder(Root.right);
            }
        }
        public void Postorder(Node<T> Root)
        {
            if (Root != null)
            {
                Postorder(Root.left);
                Postorder(Root.right);
                Console.Write(Root.GetItem() + ",");
            }
        }
    }
}
