using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETLObjectsEditor
{


    public static class Constants
    {
        public const string GuidClientPackage = "68a4ede6-8f63-44f2-803e-65f770e709e1";
        public const string GuidClientCmdSet = "2513aa39-e57d-47d5-b6d1-a09061e103d7";
        public const string GuidEditorFactory = "93fa4dc3-61ec-47af-b0ba-50cad3caf049";

        public const string fileExtension = ".GraphML";
    }

    internal static class ConstCollection
    {
        public static readonly Guid guidEditorCmdSet = new Guid(Constants.GuidClientCmdSet);
        public static readonly Guid guidEditorFactory = new Guid(Constants.GuidEditorFactory);
    };


}
