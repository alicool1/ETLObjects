using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects
{
    public class IntervalPoint<TRange, TIntervalScore> : IComparable<IntervalPoint<TRange, TIntervalScore>> where TRange: IComparable
    {
        public TRange RangeStart { get; private set; }
        public TIntervalScore IntervalScore { get; private set; }

        public string Description { get; private set; }

        public IntervalPoint() { }

        public override string ToString()
        {
            return String.Format("{0} ({1})", RangeStart, Description ?? "");
        }

        public IntervalPoint(TRange rangeStart) : this()
        {
            RangeStart = rangeStart;
        }

        public IntervalPoint(TRange rangeStart, TIntervalScore intervalScore) : this(rangeStart)
        {
            IntervalScore = intervalScore;
        }

        public IntervalPoint(TRange rangeStart, TIntervalScore intervalScore, string description) : this(rangeStart, intervalScore)
        {
            Description = description;
        }

        public int CompareTo(IntervalPoint<TRange, TIntervalScore> other)
        {
            return RangeStart.CompareTo(other.RangeStart);
        }
    }

    public class IntervalPointMetric : IntervalPoint<decimal,int>
    {
        public IntervalPointMetric() : base() { }
        public IntervalPointMetric(decimal rangeStart) : base(rangeStart) { }  
        public IntervalPointMetric(decimal rangeStart, int intervalScore) : base(rangeStart, intervalScore) { }
        public IntervalPointMetric(decimal rangeStart, int intervalScore, string description) : base(rangeStart, intervalScore, description) { }
    }

    public class IntervalPointNominal : IntervalPoint<string, int>
    {
        public IntervalPointNominal() : base() { }
        public IntervalPointNominal(string rangeStart) : base(rangeStart) { }
        public IntervalPointNominal(string rangeStart, int intervalScore) : base(rangeStart, intervalScore) { }
        public IntervalPointNominal(string rangeStart, int intervalScore, string description) : base(rangeStart, intervalScore, description) { }

    }
}
