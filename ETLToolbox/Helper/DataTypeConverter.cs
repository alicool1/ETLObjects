using System;
using System.Text.RegularExpressions;

namespace ALE.ETLToolbox {
    public class DataTypeConverter {
        public const int DefaultTinyIntegerLength = 5;
        public const int DefaultSmallIntegerLength = 7;
        public const int DefaultIntegerLength = 11;
        public const int DefaultBigIntegerLength = 21;
        public const int DefaultDateTime2Length = 41;
        public const int DefaultDateTimeLength = 27;
        public const int DefaultDecimalLength = 41;

        public const string _REGEX = @"(.*?)char\((\d*)\)(.*?)";

        public static int GetTypeLength(string dataTypeString) {
            switch (dataTypeString) {
                case "tinyint": return DefaultTinyIntegerLength;
                case "smallint": return DefaultSmallIntegerLength;
                case "int": return DefaultIntegerLength;
                case "bigint": return DefaultBigIntegerLength;
                case "decimal": return DefaultDecimalLength;
                case "datetime": return DefaultDateTimeLength;
                case "datetime2": return DefaultDateTime2Length;
                default:
                    if (IsCharTypeDefinition(dataTypeString))
                        return GetStringLengthFromCharString(dataTypeString);
                    else
                        throw new Exception("Unknown data type");
            }
        }

        public static bool IsCharTypeDefinition(string value) {
            return new Regex(_REGEX).IsMatch(value);
        }

        public static int GetStringLengthFromCharString(string value) {
            string result = Regex.Replace(value, _REGEX, "${2}");
            return int.Parse(result);
        }

        public static string GetObjectTypeString(string dataTypeString) {
            switch (dataTypeString) {

                case "tinyint": return "System.UInt16";
                case "smallint": return "System.Int16";
                case "int": return "System.Int32";
                case "bigint": return "System.Int64";
                case "decimal": return "System.Decimal";
                case "datetime": return "System.DateTime";
                case "datetime2": return "System.DateTime";
                default: return "System.String";
            }
        }

        public static Type GetTypeObject(string dataTypeString) {
            return Type.GetType(GetObjectTypeString(dataTypeString));
        }
    }
}
