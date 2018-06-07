using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    /// <summary>
    /// Klasse zur Repraesentation eines Knoten
    /// </summary>
    public class Vertex : IComparable<Vertex>// wegen Priority-Queue
    {
        public long key; // Key des Knoten (fix)
        public List<Edge> edges; // Nachbarn als Kantenliste (fix)
        public int nr; // Knotennummer (errechnet)
        public int indegree; // Eingangsgrad (errechnet)
        public double dist; // Kosten fuer diesen Knoten (errechnet)
        public Boolean seen; // Besuchs-Status (errechnet)
        public Vertex prev; // Vorgaenger fuer diesen Knoten (errechnet)
        public Boolean zirkelBestandteil; //gibt an, ob Knoten Teil des Zirkels ist oder aufrgrund von Zirkel im Nachgang ausgesteuert wurde
        public Boolean istZirkelAnfang; //gibt an, ob Knoten Anfang des Zirkels ist   

        /// <summary>
        /// beliebige benutzerdefinierte Objekte
        /// </summary>
        public List<object> BenutzerObjekte;

        /// <summary>
        /// Konstruktor fuer Knoten
        /// </summary>
        /// <param name="s"></param>
        public Vertex(long o)
        {
            key = o; // initialisiere Name des Knoten
            edges = new List<Edge>(); // initialisiere Nachbarschaftsliste
        }

        /// <summary>
        /// testet, ob Kante zu w besteht
        /// </summary>
        /// <param name="w"></param>
        /// <returns></returns>
        public Boolean hasEdge(Vertex w)
        {
            foreach (Edge e in edges) // fuer jede ausgehende Nachbarkante pruefe
                if (e.dest == w) // falls Zielknoten mit w uebereinstimmt
                    return true; // melde Erfolg
            return false; // ansonsten: melde Misserfolg
        }

        /// <summary>
        /// vergl. Kosten mit anderem Vertex
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public int CompareTo(Vertex other)
        { // vergl. Kosten mit anderem Vertex
            if (other.dist > dist) return -1;
            if (other.dist < dist) return 1;
            return 0;
        }
    }
}
