using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    /// <summary>
    /// Klasse zur Repraesentation einer Kante
    /// </summary>
    public class Edge
    {
        public Vertex dest; // Zielknoten, zu dem die Kante fuehrt
        public double cost; // Kosten dieser Kante

        /// <summary>
        ///Konstruktor fuer Kante
        /// </summary>
        /// <param name="d"></param>
        /// <param name="c"></param>
        public Edge(Vertex d, double c)
        {
            dest = d; // initialisiere Zielknoten
            cost = c; // initialisiere Kantenkosten
        }
    }
}
