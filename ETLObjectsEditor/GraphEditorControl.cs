using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace ETLObjectsEditor
{
    class GraphEditorControl : Panel
    {
        public GraphEditorControl()
        {
            this.DragOver += GraphEditorControl_DragOver;
            this.DragDrop += new DragEventHandler(OnDragDrop);
            this.DragEnter += new DragEventHandler(OnDragEnter);

            this.AllowDrop = true;
        }

      

        private void GraphEditorControl_DragOver(object sender, DragEventArgs e)
        {
            // Check if the picked item is the one we added to the toolbox.
            if (e.Data.GetDataPresent(typeof(ToolboxItemData)))
            {

            }

            else if (e.Data.GetDataPresent(typeof(ToolboxItemData2)))
            {
               
            }


            else if (e.Data.GetData(DataFormats.StringFormat).ToString().StartsWith("UserControl_Node"))
            {
                string s = e.Data.GetData(DataFormats.StringFormat).ToString();
                string[] tokens = s.Split('|');
                string GuidString = tokens[1];

                foreach (Control c in this.Controls)
                {
                    if (c.GetType() == typeof(UserControl_Node))
                    {
                        UserControl_Node uc = (UserControl_Node)c;
                        if (uc.Guid.ToString() == GuidString)
                        {
                            Point newPlace = new Point(
                                  Cursor.Position.X - uc.PointMouseDown.X
                                , Cursor.Position.Y - uc.PointMouseDown.Y);
                            uc.Location = this.PointToClient(newPlace);
                        }
                    }
                }
                // Specify DragDrop result
                e.Effect = DragDropEffects.Move;
            }
        }
    


        /// <summary>
        /// Handles the DragEnter event of contained RichTextBox object. 
        /// Process drag effect for the toolbox item.
        /// </summary>
        /// <param name="sender">The reference to contained RichTextBox object.</param>
        /// <param name="e">The event arguments.</param>
        void OnDragEnter(object sender, DragEventArgs e)
        {
            // Check if the source of the drag is the toolbox item
            // created by this sample.
            if (e.Data.GetDataPresent(typeof(ToolboxItemData)))
            {
                // Only in this case we will enable the drop
                e.Effect = DragDropEffects.Copy;
            }
            else if (e.Data.GetDataPresent(typeof(ToolboxItemData2)))
            {
                // Only in this case we will enable the drop
                e.Effect = DragDropEffects.Copy;
            }
            else if (e.Data.GetData(DataFormats.StringFormat).ToString().StartsWith("UserControl_Node"))
            {
                e.Effect = DragDropEffects.Move;
            }
        }

        /// <summary>
        /// Handles the DragDrop event of contained RichTextBox object. 
        /// Process text changes on drop event.
        /// </summary>
        /// <param name="sender">The reference to contained RichTextBox object.</param>
        /// <param name="e">The event arguments.</param>
        void OnDragDrop(object sender, DragEventArgs e)
        {
            // Check if the picked item is the one we added to the toolbox.
            if (e.Data.GetDataPresent(typeof(ToolboxItemData)))
            {

                GraphEditorPackage.PutTextToCS("Console.WriteLine(\"T1\");");

                ToolboxItemData myData = (ToolboxItemData)e.Data.GetData(typeof(ToolboxItemData));

                
                UserControl_Node n = new UserControl_Node("T1");
                n.Location = this.PointToClient(Cursor.Position);

                this.Controls.Add(n);

                // Specify DragDrop result
                e.Effect = DragDropEffects.Copy;
            }

            else if (e.Data.GetDataPresent(typeof(ToolboxItemData2)))
            {
                GraphEditorPackage.PutTextToCS("Console.WriteLine(\"T2\");");

                UserControl_Node n = new UserControl_Node("T2");
                n.Location = this.PointToClient(Cursor.Position);

                this.Controls.Add(n);

                // Specify DragDrop result
                e.Effect = DragDropEffects.Copy;
            }


            else if (e.Data.GetData(DataFormats.StringFormat).ToString().StartsWith("UserControl_Node"))
            {
                string s = e.Data.GetData(DataFormats.StringFormat).ToString();
                string[] tokens = s.Split('|');
                string GuidString = tokens[1];

                foreach (Control c in this.Controls)
                {
                    if (c.GetType() == typeof(UserControl_Node))
                    {
                        UserControl_Node uc = (UserControl_Node)c;
                        if (uc.Guid.ToString() == GuidString)
                        {
                            Point newPlace = new Point(
                                  Cursor.Position.X - uc.PointMouseDown.X
                                , Cursor.Position.Y - uc.PointMouseDown.Y);
                            uc.Location = this.PointToClient(newPlace);
                        }
                    }
                }
                // Specify DragDrop result
                e.Effect = DragDropEffects.Move;
            }

        }

    }
}
