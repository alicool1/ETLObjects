using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public class IntervalPointSearch
    {
        /// <summary>
        /// Laufzeit: O(log*n)
        /// sucht in einer nach RangeVon aufsteigend sortierten Liste 
        /// von metrischen Intervallpunkten
        /// den Intervallpunkt bei dem die übergebene Ausprägung GrößerGleich dessen RangeVon ist 
        /// und die Ausprägung kleiner dessen folgenden Intervallpunktes ist - und gibt den
        /// gefundenen Intervallpunkt zurück 
        /// </summary>
        /// <param name="scaleMetric"></param>
        /// <param name="spec"></param>
        /// <returns></returns>
        public static IntervalPointMetric IntervalPointBinarySearch(List<IntervalPointMetric> scaleMetric, decimal spec)
        {
            IntervalPointMetric result = null;
            IntervalPointMetric nr = new IntervalPointMetric(spec);
            int i = scaleMetric.BinarySearch(nr);
            if (i < 0) i = (int)(~i - 1);
            if (i >= 0) result = scaleMetric[i];
            return result;
        }


        /// <summary>
        /// Laufzeit: O(log*n)
        /// sucht in einer Liste von nominalen Intervallpunkten
        /// den Intervallpunkt der als RangeVon die übergebene Ausprägung hat - und gibt den
        /// gefundenen Intervallpunkt zurück
        /// </summary>
        /// <param name="scaleNominal"></param>
        /// <param name="spec"></param>
        /// <returns></returns>
        public static IntervalPointNominal IntervalPointBinarySearch(List<IntervalPointNominal> scaleNominal, string spec)
        {
            IntervalPointNominal result = null;
            IntervalPointNominal nr = new IntervalPointNominal(spec);
            int i = scaleNominal.BinarySearch(nr);
            if (i >= 0) result = scaleNominal[i];
            return result;
        }

        /// <summary>
        /// Laufzeit: O(n)
        /// sucht in einer Liste von nominalen Intervallpunkten
        /// den Intervallpunkt der als RangeVon die übergebene Ausprägung hat - und gibt den
        /// gefundenen Intervallpunkt zurück
        /// </summary>
        /// <param name="scaleNominal"></param>
        /// <param name="spec"></param>
        /// <returns></returns>
        public static IntervalPointNominal IntervalPointLinearSearch(List<IntervalPointNominal> scaleNominal, string spec)
        {
            IntervalPointNominal r = null;

            foreach (IntervalPointNominal ra in scaleNominal)
                if (spec == ra.RangeStart) r = ra;

            return r;

        }

        /// <summary>
        /// Laufzeit: O(n)
        /// Methode IntervallpunktSucheLinear() sucht in einer nach RangeVon aufsteigend sortierten Liste 
        /// von metrischen Intervallpunkten
        /// den Intervallpunkt bei dem die übergebene Ausprägung GrößerGleich dessen RangeVon ist 
        /// und die Ausprägung kleiner dessen folgenden Intervallpunktes ist - und gibt den
        /// gefundenen Intervallpunkt zurück
        /// </summary>
        /// <param name="scaleNominal"></param>
        /// <param name="spec"></param>
        /// <returns></returns>
        public static IntervalPointMetric IntervalPointLinearSearch(List<IntervalPointMetric> SkalaMetrisch, decimal Auspraegung)
        {
            IntervalPointMetric predecessor = null;
            foreach (IntervalPointMetric r in SkalaMetrisch)
            {
                if (predecessor != null && r.RangeStart > Auspraegung)
                {
                    if (Auspraegung >= predecessor.RangeStart) return predecessor; else return null;
                }
                predecessor = r;
            }
            if (Auspraegung >= predecessor.RangeStart) return predecessor;
            return null;
        }
    }

}

