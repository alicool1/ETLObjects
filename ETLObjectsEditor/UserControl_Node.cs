using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace ETLObjectsEditor
{
    public partial class UserControl_Node : UserControl
    {
        public Guid Guid;

        public UserControl_Node(string s)
        {
            InitializeComponent();
            label1.Text = s;
            Guid = Guid.NewGuid();
            this.MouseDown += UserControl_Node_MouseDown;
           
        }


        private void UserControl_Node_MouseDown(object sender, MouseEventArgs e)
        {
            DataObject data = new DataObject();
            data.SetData(DataFormats.StringFormat, "UserControl_Node|" + Guid.ToString());
            this.DoDragDrop(data, DragDropEffects.Copy | DragDropEffects.Move);
        }

       
        private void button1_Click(object sender, EventArgs e)
        {
            MessageBox.Show(label1.Text);
        }
    }
}
