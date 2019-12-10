using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ETLObjects;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.Common;

namespace ETLObjectsTest
{
    [TestClass]
    public class TestTree
    {
        public TestContext TestContext { get; set; }
        static SqlConnectionManager TestDb = null;

        [ClassInitialize]
        public static void TestInit(TestContext testContext)
        {

            string ServerName = testContext.Properties["ServerName"].ToString();
            string InitialCatalog = testContext.Properties["InitialCatalog"].ToString();
            TestDb = new SqlConnectionManager(ServerName, InitialCatalog);
            new CreateSchemaTask(TestDb.SqlConnection).Create("test");
        }


        [TestMethod]
        public void BST()
        {

            Tree BST = new Tree();

            BST.Insert(30);
            BST.Insert(35);
            BST.Insert(57);
            BST.Insert(15);
            BST.Insert(63);
            BST.Insert(49);
            BST.Insert(89);
            BST.Insert(77);
            BST.Insert(67);
            BST.Insert(98);
            BST.Insert(91);

            //TestHelper.VisualizeTree(BST);

            Console.WriteLine("Inorder Traversal : ");
            BST.Inorder(BST.ReturnRoot());
            Console.WriteLine(" ");
            Console.WriteLine();
            Console.WriteLine("Preorder Traversal : ");
            BST.Preorder(BST.ReturnRoot());
            Console.WriteLine(" ");
            Console.WriteLine();
            Console.WriteLine("Postorder Traversal : ");
            BST.Postorder(BST.ReturnRoot());
            Console.WriteLine(" ");


        }

        

    }
}
