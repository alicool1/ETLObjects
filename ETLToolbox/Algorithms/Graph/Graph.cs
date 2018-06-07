using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    /// <summary>
    /// Klasse zur Implementation eines Graphen basierend auf Vertex und Edge.
    /// Der Graph wird implementiert als HashMap <String, Vertex>, d.h. als eine Hashtabelle mit Keys vom Typ String und Values vom Typ Knoten
    /// </summary>
    public class Graph
    {
        public Dictionary<long, Vertex> graph; // Datenstruktur fuer Graph

        /// <summary>
        /// leerer Graph wird angeleg
        /// </summary>
        public Graph()
        {
            graph = new Dictionary<long, Vertex>(); // als HashMap von String,Vertex
        }

        /// <summary>
        /// liefert true, falls Graph leer
        /// </summary>
        /// <returns></returns>
        public Boolean isEmpty()
        {
            return (graph.Count == 0); // mit isEmpty() von HashMap
        }

        /// <summary>
        /// liefert die Anzahl der Knoten
        /// </summary>
        /// <returns></returns>
        public int size()
        {
            return graph.Count; // mit size() von HashMap
        }

        /// <summary>
        /// liefert Knoten als Liste von Vertex
        /// </summary>
        /// <returns></returns>
        public List<Vertex> vertices()
        {
            List<Vertex> vertList = new List<Vertex>();
            foreach (long key in graph.Keys)
            {
                vertList.Add(graph[key]);
            }
            return vertList; // mit values() von HashMap
        }

        /// <summary>
        /// liefere Knoten und fuege Knoten ein, wenn noch nicht vorhanden 
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public Vertex getVertex(long o, List<object> BenutzerObjekte)
        {
            Vertex v = null; // besorge Knoten zu Knotennamen
            if (!graph.ContainsKey(o))
            { // falls nicht gefunden
                v = new Vertex(o); // lege neuen Knoten an
                if (BenutzerObjekte != null) v.BenutzerObjekte = BenutzerObjekte;
                graph[o] = v; ; // fuege Namen und Knoten in HashMap ein
            }
            else
            {
                v = graph[o];
            }
            return v; // liefere gefundenen oder neuen Knoten
        }
        /// <summary>
        /// fuege Kante ein von Knotennamen source zu Knotennamen dest mit Kosten cost
        /// </summary>
        /// <param name="source"></param>
        /// <param name="dest"></param>
        /// <param name="cost"></param>
        public void addEdge(long source, long dest, double cost)
        {
            Vertex v = getVertex(source, null); // finde Knoten v zum Startnamen
            Vertex w = getVertex(dest, null); // finde Knoten w zum Zielnamen
            v.edges.Add(new Edge(w, cost)); // fuege Kante (v,w) mit Kosten cost ein
        }
    }
}
