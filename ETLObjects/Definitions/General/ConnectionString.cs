using System;
using System.Text.RegularExpressions;

namespace ETLObjects {
    //TODO replace this with build-in .net class SqlConnectionStringBuilder (or wrap it)
    public class ConnectionString {
        static string PATTERNBEGIN = $@"(.*)(";
        static string PATTERNEND = $@"\s*=\s*)(.*?)(;|$)(.*)";
        static string DATASOURCE = $@"{PATTERNBEGIN}Data Source{PATTERNEND}";
        static string INITIALCATALOG = $@"{PATTERNBEGIN}Initial Catalog{PATTERNEND}";
        static string PROVIDER = $@"{PATTERNBEGIN}Provider{PATTERNEND}";
        static string CURRENTLANGUAGE = $@"{PATTERNBEGIN}Current Language{PATTERNEND}";
        static string AUTOTRANSLATE = $@"{PATTERNBEGIN}Auto Translate{PATTERNEND}";

        static string VALIDCONNECTIONSTRING = @"[\w\s]+=([\w\s-_.+*&%$#&!§]+|"".*? "")(;|$)"; //Attention: double quotes in Regex are quoted with double quotes
               

        string _ConnectionString;
        public string Value {
            get {
                return _ConnectionString;
            }
            set {
                _ConnectionString = RemovePatternIfExists(value, PROVIDER, CURRENTLANGUAGE, AUTOTRANSLATE);
                CatalogName = ReplaceIfMatch(value, INITIALCATALOG, "${3}");
                ServerName = ReplaceIfMatch(value, DATASOURCE, "${3}");
            }
        }

        public bool IsValid {
            get {
                return Regex.IsMatch(_ConnectionString, VALIDCONNECTIONSTRING);
            }
        }

        public string ServerName { get; set; }

        public string CatalogName { get; set; }

        public ConnectionString() {

        }
        public ConnectionString(string connectionString) {
            this.Value = connectionString;
        }

        public ConnectionString GetMasterConnection() {
            return new ConnectionString(ReplaceIfMatch(Value, INITIALCATALOG, "${1}${2}master${4}${5}"));
        }

        public ConnectionString GetConnectionWithoutCatalog() {
            return new ConnectionString(ReplaceIfMatch(Value, INITIALCATALOG, "${1}${5}"));
        }

        public ConnectionString ChangeCatalog(string dbName) {
             return new ConnectionString(GetConnectionWithoutCatalog().Value 
                 + (Value.EndsWith(";") ? "" : ";") 
                 + $"Initial Catalog={dbName}");
        }

        public static implicit operator ConnectionString(string v) {
            return new ConnectionString(v);
        }

        public override string ToString() {
            return Value;
        }

        string RemovePatternIfExists(string v, params string[] patterns) {
            string result = v;
            foreach (string pattern in patterns) {
                if (Regex.IsMatch(result, pattern))
                    result = Regex.Replace(result, pattern, "${1}${5}", RegexOptions.IgnoreCase);
            }
            return result;
        }

        string ReplaceIfMatch(string input, string pattern, string replacement) {
            if (Regex.IsMatch(input, pattern, RegexOptions.IgnoreCase))
                return Regex.Replace(input, pattern, replacement, RegexOptions.IgnoreCase);
            else
                return input;
        }
    }
}
