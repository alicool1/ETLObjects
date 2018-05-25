using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public class IntervallPunktSuche
    {
        /// <summary>
        /// Laufzeit: O(log*n)
        /// Methode IntervallpunktSucheBinaer() sucht in einer nach RangeVon aufsteigend sortierten Liste 
        /// von metrischen Intervallpunkten
        /// den Intervallpunkt bei dem die übergebene Ausprägung GrößerGleich dessen RangeVon ist 
        /// und die Ausprägung kleiner dessen folgenden Intervallpunktes ist - und gibt den
        /// gefundenen Intervallpunkt zurück 
        /// </summary>
        /// <param name="SkalaMetrisch"></param>
        /// <param name="Auspraegung"></param>
        /// <returns></returns>
        public static IntervallPunktMetrisch IntervallpunktSucheBinaer(List<IntervallPunktMetrisch> SkalaMetrisch, decimal Auspraegung)
        {
            IntervallPunktMetrisch erg = null;
            IntervallPunktMetrisch nr = new IntervallPunktMetrisch(Auspraegung);
            int i = SkalaMetrisch.BinarySearch(nr);
            if (i < 0) i = (int)(~i - 1);
            if (i >= 0) erg = SkalaMetrisch[i];
            return erg;
        }


        /// <summary>
        /// Laufzeit: O(log*n)
        /// Methode IntervallpunktSucheBinaer() sucht in einer Liste von nominalen Intervallpunkten
        /// den Intervallpunkt der als RangeVon die übergebene Ausprägung hat - und gibt den
        /// gefundenen Intervallpunkt zurück
        /// </summary>
        /// <param name="SkalaNominal"></param>
        /// <param name="Auspraegung"></param>
        /// <returns></returns>
        public static IntervallPunktNominal IntervallpunktSucheBinaer(List<IntervallPunktNominal> SkalaNominal, string Auspraegung)
        {
            IntervallPunktNominal erg = null;
            IntervallPunktNominal nr = new IntervallPunktNominal(Auspraegung);
            int i = SkalaNominal.BinarySearch(nr);
            if (i >= 0) erg = SkalaNominal[i];
            return erg;
        }

        /// <summary>
        /// Laufzeit: O(n)
        /// Methode IntervallpunktSucheLinear() sucht in einer Liste von nominalen Intervallpunkten
        /// den Intervallpunkt der als RangeVon die übergebene Ausprägung hat - und gibt den
        /// gefundenen Intervallpunkt zurück
        /// </summary>
        /// <param name="SkalaNominal"></param>
        /// <param name="Auspraegung"></param>
        /// <returns></returns>
        public static IntervallPunktNominal IntervallpunktSucheLinear(List<IntervallPunktNominal> SkalaNominal, string Auspraegung)
        {
            IntervallPunktNominal r = null;

            foreach (IntervallPunktNominal ra in SkalaNominal)
                if (Auspraegung == ra.RangeVon) r = ra;

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
        /// <param name="SkalaMetrisch"></param>
        /// <param name="Auspraegung"></param>
        /// <returns></returns>
        public static IntervallPunktMetrisch IntervallpunktSucheLinear(List<IntervallPunktMetrisch> SkalaMetrisch, decimal Auspraegung)
        {
            IntervallPunktMetrisch r_vorgaenger = null;
            foreach (IntervallPunktMetrisch r in SkalaMetrisch)
            {
                if (r_vorgaenger != null && r.RangeVon > Auspraegung)
                {
                    if (Auspraegung >= r_vorgaenger.RangeVon) return r_vorgaenger; else return null;
                }
                r_vorgaenger = r;
            }
            if (Auspraegung >= r_vorgaenger.RangeVon) return r_vorgaenger;
            return null;
        }
    }

}

