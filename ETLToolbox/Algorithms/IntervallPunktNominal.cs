using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    /// <summary>
    /// stellt einen Intervallpunkt dar und wird angewendet auf einer nominalen Skala
    /// </summary>
    public class IntervallPunktNominal : IComparable<IntervallPunktNominal>
    {
        public string RangeVon { get; private set; }
        public int IntervallScore { get; private set; }

        public string Beschreibung { get; private set; }

        public IntervallPunktNominal()
        {

        }

        public override string ToString()
        {
            return String.Format("{0} ({1})", RangeVon, Beschreibung);
        }

        public IntervallPunktNominal(string pRangeVon, int pIntervallScore, string pBeschreibung)
        {
            RangeVon = pRangeVon;
            IntervallScore = pIntervallScore;
            Beschreibung = pBeschreibung;
        }

        public IntervallPunktNominal(string pRangeVon)
        {
            RangeVon = pRangeVon;
        }


        public int CompareTo(IntervallPunktNominal other)
        {
            return RangeVon.CompareTo(other.RangeVon);
        }


    }
}
