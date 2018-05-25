using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ETLObjects {
    public class ExtensionFile {
        public const string VERSIONMATCH = @"--\W*@version\W*:\W*[><=]?(\d+.\d+)\W*([VFvf][Pp]\d+)?";
        public const string SKIPNEXT = @"--\W*@skipnext\W*:\W*[Tt]rue";
        public const string FILENAMEMATCH = @"^(\w*?)_([0-9A-Za-z!%&()=+#~äöüÄÖÜß -]*).sql";

        //TODO read the version from somewhere else!
        public string InstalledVersion => "2.2 VP1";

        public string Type { get; private set; }
        public string Name { get; private set; }

        public string Content { get; private set; }

        public string FileName { get; set; }

        public bool IsStagExtension => Type.ToLower() == ExtensionFileLoader.STAGEXT.ToLower();

        public bool IsValidExtension { get; private set; } = true;
        public bool HasSkipNextStatement { get; private set; }

        public ExtensionFile(string filename) {
            this.FileName = filename;            

            FillNameAndType();
            ReadContent();

            CheckIfHasValidVersion();
            CheckIfHasSkipNext();
        }



        private void FillNameAndType() {
            string fileName = FileName.Substring(FileName.LastIndexOf(@"\") + 1);
            Match m = Regex.Match(fileName, FILENAMEMATCH);
            if (m.Success) {
                Type = m.Groups[1].Value;
                Name = m.Groups[2].Value;
            } else {
                IsValidExtension = false;
            }
        }

        public void ReadContent() {
            Content = File.ReadAllText(FileName);
        }

        public void CheckIfHasValidVersion() {
            if (!Regex.IsMatch(Content, VERSIONMATCH)) IsValidExtension = false;
            else {
                var match = Regex.Match(Content, VERSIONMATCH);
                if (!match.Captures[0].Value.ToLower().Contains(InstalledVersion.ToLower()))
                    IsValidExtension = false;
            }

        }
        public void CheckIfHasSkipNext() {
            HasSkipNextStatement = Regex.IsMatch(Content, SKIPNEXT) ? true : false;
        }

    }
}
