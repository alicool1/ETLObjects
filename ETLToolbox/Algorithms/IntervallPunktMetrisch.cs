using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    /// <summary>
    /// stellt einen Intervallpunkt dar und wird angewendet auf einer metrischen Skala
    /// </summary>
    public class IntervallPunktMetrisch : IComparable<IntervallPunktMetrisch>
    {
        public decimal RangeVon { get; private set; }
        public int IntervallScore { get; private set; }

        public string Beschreibung { get; private set; }

        public IntervallPunktMetrisch()
        {

        }

        public IntervallPunktMetrisch(decimal pRangeVon, int pIntervallScore)
        {
            RangeVon = pRangeVon;
            IntervallScore = pIntervallScore;

        }

        public IntervallPunktMetrisch(decimal pRangeVon, int pIntervallScore, string pBeschreibung)
        {
            RangeVon = pRangeVon;
            IntervallScore = pIntervallScore;
            Beschreibung = pBeschreibung;
        }

        public IntervallPunktMetrisch(decimal pRangeVon)
        {
            RangeVon = pRangeVon;
        }


        public int CompareTo(IntervallPunktMetrisch other)
        {
            return RangeVon.CompareTo(other.RangeVon);
        }


    }
}
