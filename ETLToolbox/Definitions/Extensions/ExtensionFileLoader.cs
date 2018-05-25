using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjects {
    public class ExtensionFileLoader {
        public const string STAGEXT = "STAGEXT";
        public const string FILESUFFIX = ".sql";

        public static string ExtensionScriptsFolder = "";

        public static bool ExistsFolder => !String.IsNullOrEmpty(ExtensionScriptsFolder);
        public static List<ExtensionFile> GetSTAGEXTFiles() => GetExtensionFiles(STAGEXT);
        public static List<ExtensionFile> GetExtensionFiles(string name) {
            List<ExtensionFile> result = new List<ExtensionFile>();
            if (ExistsFolder) {
                foreach (string fileName in Directory.GetFiles(Path.GetFullPath(ExtensionScriptsFolder), $"{name}_*{FILESUFFIX}", SearchOption.TopDirectoryOnly)) {
                    ExtensionFile file = new ExtensionFile(fileName);
                    if (fileName.ToLower().EndsWith("_example.sql")) continue;
                    if (file.IsValidExtension)
                        result.Add(file);
                }
            }

            return result;
        }
    }
}
