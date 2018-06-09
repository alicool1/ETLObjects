using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public class TopoSort
    {
        public static Dictionary<long, int?> SortGraph(Graph g)
        {
            Dictionary<long, int?> result = new Dictionary<long, int?>();

            Dictionary<Vertex, Boolean> allVerticesInGraph = new Dictionary<Vertex, Boolean>();
            int stufeKnoten = 0;

            InitalizeAllNodes(g, allVerticesInGraph);
            DetermineGrades(g);

            bool MindEinEingangsGradNull = DoTopologicalSorting(g, result, allVerticesInGraph, ref stufeKnoten);

            if (allVerticesInGraph.Count > 0 && !MindEinEingangsGradNull)
               foreach (Vertex w in allVerticesInGraph.Keys) w.zirkelBestandteil = true;
            
            return result;
        }

        private static void InitalizeAllNodes(Graph g, Dictionary<Vertex, bool> dictAlleKnotenImGraph)
        {
            foreach (Vertex v in g.Vertices())
            { // fuer jeden Knoten
                v.indegree = 0; // setze seinen Eingangsgrad auf 0
                v.nr = -1; // vergib ungueltige Nummer
                if (!dictAlleKnotenImGraph.ContainsKey(v)) dictAlleKnotenImGraph.Add(v, true);

            }
        }

        private static void DetermineGrades(Graph g)
        {
            foreach (Vertex v in g.Vertices())// fuer jeden Knoten
                foreach (Edge e in v.edges) // fuer jeden Nachbarknoten
                    e.dest.indegree++; // erhoehe seinen Eingangsgrad um 1
        }
        private static bool DoTopologicalSorting(Graph g, Dictionary<long, int?> result, Dictionary<Vertex, bool> allVerticesInGraph, ref int stufeKnoten)
        {
            List<Vertex> l = new List<Vertex>(); // Liste von Knoten, die inzwischen den Eingangsgrad 0 haben

            bool MindEinEingangsGradNull = false;
            foreach (Vertex v in g.Vertices()) // jeden Knoten mit Eingangsgrad 0
            {
                if (v.indegree == 0)
                {
                    l.Add(v); // fuege hinten in die Liste ein
                    if (!result.ContainsKey(v.key)) result.Add(v.key, stufeKnoten);
                    MindEinEingangsGradNull = true;
                    allVerticesInGraph.Remove(v);
                }
            }
            if (!MindEinEingangsGradNull) foreach (Vertex w in allVerticesInGraph.Keys) w.zirkelBestandteil = true;

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
                allVerticesInGraph.Remove(v);

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
                        if (!result.ContainsKey(w.key)) result.Add(w.key, stufeKnoten);
                    }
                }
            }

            return MindEinEingangsGradNull;
        }


    }
}
