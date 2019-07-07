using System;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using Microsoft.VisualStudio.Shell;
using EnvDTE80;
using EnvDTE;
using Microsoft.VisualStudio.Text;

namespace ETLObjectsEditor
{

    [PackageRegistration(UseManagedResourcesOnly = true)]
    [InstalledProductRegistration("#100", "#102", "1.0", IconResourceID = 400)]
    // We register our AddNewItem Templates the Miscellaneous Files Project:
    [ProvideEditorExtension(typeof(GraphEditorFactory), Constants.fileExtension, 32,
              ProjectGuid = "{A2FE74E1-B743-11d0-AE1A-00A0C90FFFC3}",
              TemplateDir = "Templates",
              NameResourceID = 106)]
    // We register that our editor supports LOGVIEWID_Designer logical view
    [ProvideEditorLogicalView(typeof(GraphEditorFactory), "{7651a703-06e5-11d1-8ebd-00a0c90f26ea}")]
    [Guid(Constants.GuidClientPackage)]
    public class GraphEditorPackage : Package, IDisposable
    {
        private GraphEditorFactory graphEditorFactory;

        private static DTE DTE { get; set; }

        public static void PutTextToCS(string text)
        {
            ThreadHelper.ThrowIfNotOnUIThread();
            EnvDTE.Solution solution = DTE.Solution;
            EnvDTE.Projects _projects = solution.Projects;

            string s = string.Empty;

            var projects = _projects.GetEnumerator();
            while (projects.MoveNext())
            {
                var items = ((Project)projects.Current).ProjectItems.GetEnumerator();
                while (items.MoveNext())
                {
                    ProjectItem item = (ProjectItem)items.Current;
                    //Recursion to get all ProjectItems
                    s += item.Name + Environment.NewLine;

                    //ExamineItem(item);

                    if (item.Name.EndsWith("cs"))
                    {
                        Window x = item.Open();
                        x.Visible = true;
                        var document = x.Document;
                        document.ReadOnly = false;
                        TextDocument editDoc = document.Object("TextDocument") as TextDocument;

                        string x1 = editDoc.ToString();
                        
                        EditPoint objEditPt = editDoc.CreateEditPoint();
                        editDoc.Selection.Insert(text + Environment.NewLine);
                        System.Windows.Forms.SendKeys.SendWait("^k");
                        System.Windows.Forms.SendKeys.SendWait("^d");
                        //objEditPt.StartOfDocument();                      
                        //while (!objEditPt.AtEndOfDocument)
                        //{
                        //    //objEditPt.Delete(objEditPt.LineLength);
                        //    objEditPt.LineDown(1);
                        //}
                        //objEditPt.Insert(text);
                        //objEditPt.DeleteWhitespace(vsWhitespaceOptions.vsWhitespaceOptionsHorizontal);
                        //objEditPt.DeleteWhitespace(vsWhitespaceOptions.vsWhitespaceOptionsVertical);

                        Console.WriteLine("saving file {0}", document.FullName);
                        //document.Save(document.FullName);



                    }
                }
            }

        }

        public static string GetProjectName(string fileName)
        {
            ThreadHelper.ThrowIfNotOnUIThread();
            EnvDTE.Solution solution = DTE.Solution;
            EnvDTE.Projects _projects = solution.Projects;


            string result = null;
            if (DTE != null && DTE.Solution != null)
            {
                ProjectItem prj = DTE.Solution.FindProjectItem(fileName);
                var x = prj.ProjectItems;
                if (prj != null)
                    result = prj.ContainingProject.Name;
            }

            return result;
        }

        protected override void Initialize()
        {
            ThreadHelper.ThrowIfNotOnUIThread();
            //Create Editor Factory
            base.Initialize();

            DTE = GetService(typeof(DTE)) as DTE;

            graphEditorFactory = new GraphEditorFactory();
            RegisterEditorFactory(graphEditorFactory);


        }
        #region IDisposable Pattern
        /// <summary>
        /// Releases the resources used by the Package object.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Releases the resources used by the Package object.
        /// </summary>
        /// <param name="disposing">This parameter determines whether the method has been called directly or indirectly by a user's code.</param>
        protected override void Dispose(bool disposing)
        {
            try
            {
                Debug.WriteLine(string.Format(CultureInfo.CurrentCulture, "Entering Dispose() of: {0}", ToString()));
                if (disposing)
                {
                    if (graphEditorFactory != null)
                    {
                        graphEditorFactory.Dispose();
                        graphEditorFactory = null;
                    }
                    GC.SuppressFinalize(this);
                }
            }
            finally
            {
                base.Dispose(disposing);
            }
        }
        #endregion
    }
}
