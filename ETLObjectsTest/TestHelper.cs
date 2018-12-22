using ETLObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjectsTest
{
    public static class TestHelper
    {

        internal static void VisualizeGraph(Graph gr)
        {

            System.Windows.Forms.Form form = new System.Windows.Forms.Form();
            Microsoft.Msagl.GraphViewerGdi.GViewer viewer = new Microsoft.Msagl.GraphViewerGdi.GViewer();
            Microsoft.Msagl.Drawing.Graph graph = new Microsoft.Msagl.Drawing.Graph("graph");

            foreach (Vertex v in gr.Vertices())
            {
                foreach (Edge e in v.edges)
                {
                    Microsoft.Msagl.Drawing.Edge msagle_edge = graph.AddEdge(
                        string.Format("{0} - {1} ", v.key, v.UserDefinedObjects[0])
                        , string.Format("{0} - {1} ", e.dest.key, e.dest.UserDefinedObjects[0])
                        
                        );
                    if (e.cost > 0) msagle_edge.LabelText = e.cost.ToString();


                }
            }

            viewer.Graph = graph;
            form.SuspendLayout();
            viewer.Dock = System.Windows.Forms.DockStyle.Fill;
            form.Controls.Add(viewer);
            form.ResumeLayout();
            form.ShowDialog();
        }

     

    }
}
