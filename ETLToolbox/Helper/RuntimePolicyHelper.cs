using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
//using Microsoft.AnalysisServices.Tabular;
//using Microsoft.AnalysisServices.Xmla;

namespace ETLObjects {
    public static class RuntimePolicyHelper {
        public static bool LegacyV2RuntimeEnabledSuccessfully { get; private set; }

        //public static ManagedBatchParser.Parser Parser_OnlyToIncludeBatchParserDLL { get; set; }
        //public static Microsoft.AnalysisServices.Xmla.XmlaClient XMLAClient_OnlyToIncludeDLL { get; set; }
        //public static Microsoft.AnalysisServices.Tabular.Culture Culture_OnlyToIncludeDLL { get; set; }

        public static void SetNET20Compatibilty() {
            //IncludeLibrariesInBuildButNotUsedInCode();
            if (!RuntimePolicyHelper.LegacyV2RuntimeEnabledSuccessfully)
                throw new Exception("Could not load SMO (compatibility level could not be set to .net 2.0");
        }

        //private static void IncludeLibrariesInBuildButNotUsedInCode()
        //{
        //    Parser_OnlyToIncludeBatchParserDLL = new ManagedBatchParser.Parser();
        //    Parser_OnlyToIncludeBatchParserDLL = null;
        //    XMLAClient_OnlyToIncludeDLL = new XmlaClient();
        //    XMLAClient_OnlyToIncludeDLL = null;
        //    Culture_OnlyToIncludeDLL = new Culture();
        //    Culture_OnlyToIncludeDLL = null;
        //}

        static RuntimePolicyHelper() {
            ICLRRuntimeInfo clrRuntimeInfo =
                (ICLRRuntimeInfo)RuntimeEnvironment.GetRuntimeInterfaceAsObject(
                    Guid.Empty,
                    typeof(ICLRRuntimeInfo).GUID);
            try {
                clrRuntimeInfo.BindAsLegacyV2Runtime();
                LegacyV2RuntimeEnabledSuccessfully = true;
            }
            catch (COMException) {
                // This occurs with an HRESULT meaning 
                // "A different runtime was already bound to the legacy CLR version 2 activation policy."
                LegacyV2RuntimeEnabledSuccessfully = false;
            }
        }

        [ComImport]
        [InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
        [Guid("BD39D1D2-BA2F-486A-89B0-B4B0CB466891")]
        private interface ICLRRuntimeInfo {
            void xGetVersionString();
            void xGetRuntimeDirectory();
            void xIsLoaded();
            void xIsLoadable();
            void xLoadErrorString();
            void xLoadLibrary();
            void xGetProcAddress();
            void xGetInterface();
            void xSetDefaultStartupFlags();
            void xGetDefaultStartupFlags();

            [MethodImpl(MethodImplOptions.InternalCall, MethodCodeType = MethodCodeType.Runtime)]
            void BindAsLegacyV2Runtime();
        }
    }
}
