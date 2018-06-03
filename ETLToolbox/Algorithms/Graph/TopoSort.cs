using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public class TopoSort
    {
        public static Dictionary<long, int?> sortGraph(Graph g)
        {
            Dictionary<long, int?> stufe = new Dictionary<long, int?>();

            Dictionary<Vertex, Boolean> dictAlleKnotenImGraph = new Dictionary<Vertex, Boolean>();
            int stufeKnoten = 0;

            foreach (Vertex v in g.vertices())
            { // fuer jeden Knoten
                v.indegree = 0; // setze seinen Eingangsgrad auf 0
                v.nr = -1; // vergib ungueltige Nummer
                if (!dictAlleKnotenImGraph.ContainsKey(v)) dictAlleKnotenImGraph.Add(v, true);

            }
            foreach (Vertex v in g.vertices())// fuer jeden Knoten
            {
                foreach (Edge e in v.edges) // fuer jeden Nachbarknoten
                {
                    e.dest.indegree++; // erhoehe seinen Eingangsgrad um 1
                }
            }
            List<Vertex> l = new List<Vertex>(); // Liste von Knoten, die inzwischen den Eingangsgrad 0 haben



            bool MindEinEingangsGradNull = false;
            foreach (Vertex v in g.vertices()) // jeden Knoten mit Eingangsgrad 0
            {
                if (v.indegree == 0)
                {
                    l.Add(v); // fuege hinten in die Liste ein
                    if (!stufe.ContainsKey(v.key)) stufe.Add(v.key, stufeKnoten);
                    MindEinEingangsGradNull = true;
                    dictAlleKnotenImGraph.Remove(v);
                }
            }
            if (!MindEinEingangsGradNull) foreach (Vertex w in dictAlleKnotenImGraph.Keys) w.zirkelBestandteil = true;

            int id = 0; // initialisiere Laufnummer
            Boolean stufeErhoeht = false;
            stufeKnoten++;
            while (!(l.Count == 0))
            {
                if (stufeErhoeht) stufeKnoten++;
                stufeErhoeht = false;
                // solange Liste nicht leer
                Vertex v = l[0]; // besorge und entferne Kopf aus Liste
                l.RemoveAt(0);
                v.nr = id++; // vergib naechste Nummer
                dictAlleKnotenImGraph.Remove(v);

                MindEinEingangsGradNull = false;
                foreach (Edge e in v.edges)// fuer jede ausgehende Kante
                {

                    Vertex w = e.dest; // betrachte den zugehoerigen Knoten
                    w.indegree--; // erniedrige seinen Eingangsgrad
                    if (w.indegree == 0)
                    {// falls der Eingangsgrad auf 0 sinkt
                        MindEinEingangsGradNull = true;
                        stufeErhoeht = true;
                        l.Add(w); // fuege Knoten hinten in Liste ein
                        if (!stufe.ContainsKey(w.key)) stufe.Add(w.key, stufeKnoten);
                    }
                }
            }

            if (dictAlleKnotenImGraph.Count > 0 && !MindEinEingangsGradNull)
            {
                foreach (Vertex w in dictAlleKnotenImGraph.Keys) w.zirkelBestandteil = true;

            }

            return stufe;
        }
    }
}
