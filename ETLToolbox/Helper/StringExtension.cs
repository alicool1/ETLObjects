using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ALE.ETLToolbox {
    public static class StringExtension {
        public static string NullOrParenthesisString(this string s) => s == null ? "null" : $"'{s.Replace("'","''")}'";
    }
}
