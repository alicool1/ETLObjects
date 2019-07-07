using System;
using System.Runtime.Serialization;

namespace ETLObjectsEditor
{
    [Serializable()]
    public class Toolbox_TestItem2 : ISerializable
    {
        #region Fields
        private string content;
        #endregion Fields

        #region Constructors
        /// <summary>
        /// Overloaded constructor.
        /// </summary>
        /// <param name="sentence">Sentence value.</param>
        public Toolbox_TestItem2(string sentence)
        {
            content = sentence;
        }
        #endregion Constructors

        #region Properties
        /// <summary>
        /// Gets the ToolboxItemData Content.
        /// </summary>
        public string Content
        {
            get { return content; }
        }
        #endregion Properties

        internal Toolbox_TestItem2(SerializationInfo info, StreamingContext context)
        {
            content = info.GetValue("Content", typeof(string)) as string;
        }

        void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info != null)
            {
                info.AddValue("Content", Content);
            }
        }
    }
}
