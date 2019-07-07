﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace ETLObjectsEditor
{
    public partial class UserControl_Node : UserControl
    {
        public Guid Guid { get; set; }
        public Point PointMouseDown { get; set; }

        int BorderWidth { get; set; } = 3;
        Color BorderColor { get; set; } = Color.Gray;
        bool Resizing_Modus { get; set; } = false;
        MousePositionOverBorder Resizing_MousePositionOverBorder { get; set; } = MousePositionOverBorder.no;

        const int MIN_Height = 50;
        const int MIN_Width = 100;

        public UserControl_Node(string s)
        {
            this.BorderStyle = BorderStyle.FixedSingle;

            Guid = Guid.NewGuid();

            InitializeComponent();
            label1.Text = s;
            
            this.Paint += UserControl_Node_Paint;
            this.MouseDown += UserControl_Node_MouseDown;
            this.MouseMove += UserControl_Node_MouseMove;
            this.MouseLeave += UserControl_Node_MouseLeave;
            this.MouseUp += UserControl_Node_MouseUp;
        }

        private void UserControl_Node_MouseUp(object sender, MouseEventArgs e)
        {
            Point PointMouseUp = new Point(e.X, e.Y);
            if (Resizing_Modus)
            {
                ResizeMe(PointMouseUp);
                Resizing_Modus = false;
            }
        }
        
        private void ResizeMe(Point p)
        {
            int HeightDiff = 0;
            int newHeight = 0;


            HeightDiff = PointMouseDown.Y - p.Y;
            

            switch (Resizing_MousePositionOverBorder)
            {
                case MousePositionOverBorder.top:
                    if (HeightDiff != 0)
                    {
                        newHeight = this.Height + HeightDiff;
                        if (newHeight >= MIN_Height)
                        {
                            this.Height = newHeight;
                            this.Location = new Point(this.Location.X, this.Location.Y - HeightDiff);
                        }
                    }
                    break;
                case MousePositionOverBorder.bottom:
                    PointMouseDown = p;
                    if (HeightDiff != 0)
                    {
                        newHeight = this.Height - HeightDiff;
                        if (newHeight >= MIN_Height && newHeight != this.Height)
                        {
                            this.Height = newHeight;
                        }
                    }
                    break;
            }

            this.Refresh();
        }
        private void UserControl_Node_MouseLeave(object sender, EventArgs e)
        {
            SetBorderColor(Color.Gray);
        }

        private void SetBorderColor(Color c)
        {
            if (BorderColor != c)
            {
                BorderColor = c;
                this.Refresh();
            }
        }

        private void DrawBorder(PaintEventArgs e)
        {
            if (this.BorderStyle == BorderStyle.FixedSingle)
            {
                ControlPaint.DrawBorder(e.Graphics
                    , this.ClientRectangle
                    , BorderColor, BorderWidth, ButtonBorderStyle.Solid // left
                    , BorderColor, BorderWidth, ButtonBorderStyle.Solid // top
                    , BorderColor, BorderWidth, ButtonBorderStyle.Solid // right 
                    , BorderColor, BorderWidth, ButtonBorderStyle.Solid); // bottom
            }
        }
        private void UserControl_Node_Paint(object sender, PaintEventArgs e)
        {
            DrawBorder(e);
        }

        private void UserControl_Node_MouseMove(object sender, MouseEventArgs e)
        {

            Point PointMouseMove = new Point(e.X, e.Y);
            
            if (Resizing_Modus) ResizeMe(PointMouseMove);

            switch (MousePointIsOverBorder(PointMouseMove))
            {
                case MousePositionOverBorder.corner_left_top:
                    System.Windows.Input.Mouse.SetCursor(System.Windows.Input.Cursors.SizeNWSE);
                    SetBorderColor(Color.LightGreen);
                    break;
                case MousePositionOverBorder.corner_right_top:
                    System.Windows.Input.Mouse.SetCursor(System.Windows.Input.Cursors.SizeNESW);
                    SetBorderColor(Color.LightGreen);
                    break;
                case MousePositionOverBorder.corner_left_bottom:
                    System.Windows.Input.Mouse.SetCursor(System.Windows.Input.Cursors.SizeNESW);
                    SetBorderColor(Color.LightGreen);
                    break;
                case MousePositionOverBorder.corner_right_bottom:
                    System.Windows.Input.Mouse.SetCursor(System.Windows.Input.Cursors.SizeNWSE);
                    SetBorderColor(Color.LightGreen);
                    break;
                case MousePositionOverBorder.top:
                    System.Windows.Input.Mouse.SetCursor(System.Windows.Input.Cursors.SizeNS);
                    SetBorderColor(Color.LightGreen);
                    break;
                case MousePositionOverBorder.bottom:
                    System.Windows.Input.Mouse.SetCursor(System.Windows.Input.Cursors.SizeNS);
                    SetBorderColor(Color.LightGreen);
                    break;
                case MousePositionOverBorder.left:
                    System.Windows.Input.Mouse.SetCursor(System.Windows.Input.Cursors.SizeWE);
                    SetBorderColor(Color.LightGreen);
                    break;
                case MousePositionOverBorder.right:
                    System.Windows.Input.Mouse.SetCursor(System.Windows.Input.Cursors.SizeWE);
                    SetBorderColor(Color.LightGreen);
                    break;
                default:
                    System.Windows.Input.Mouse.SetCursor(System.Windows.Input.Cursors.SizeAll);
                    SetBorderColor(Color.Gray);
                    break;

            }
            
        }

        enum MousePositionOverBorder : byte {
            no = 0,
            top = 1,
            bottom = 2,
            left = 3,
            right = 4,
            corner_left_top = 5,
            corner_right_top = 6,
            corner_left_bottom = 7,
            corner_right_bottom = 8
        };

        private MousePositionOverBorder MousePointIsOverBorder(Point MousePoint)
        {
            int OffSet = 3;

            bool IsInLeftBorder = MousePoint.X >= 0 && MousePoint.X <= BorderWidth;
            bool IsInTopBorder = MousePoint.Y >= 0 && MousePoint.Y <= BorderWidth;
            bool IsInRightBorder = MousePoint.X <= this.Size.Width - OffSet && MousePoint.X >= this.Size.Width - OffSet - BorderWidth;
            bool IsInBottomBorder = MousePoint.Y <= this.Size.Height - OffSet && MousePoint.Y >= this.Size.Height - OffSet - BorderWidth;

            if (IsInLeftBorder && IsInTopBorder) return MousePositionOverBorder.corner_left_top;
            else if (IsInRightBorder && IsInTopBorder) return MousePositionOverBorder.corner_right_top;
            else if(IsInRightBorder && IsInBottomBorder) return MousePositionOverBorder.corner_right_bottom;
            else if(IsInLeftBorder && IsInBottomBorder) return MousePositionOverBorder.corner_left_bottom;
            else if(IsInLeftBorder) return MousePositionOverBorder.left;
            else if(IsInTopBorder) return MousePositionOverBorder.top;
            else if(IsInRightBorder) return MousePositionOverBorder.right;
            else if (IsInBottomBorder) return MousePositionOverBorder.bottom;
            return MousePositionOverBorder.no;
        }

        private void UserControl_Node_MouseDown(object sender, MouseEventArgs e)
        {
            
            PointMouseDown = new Point(e.X, e.Y);
            MousePositionOverBorder PointMouseDownOverBorder = MousePointIsOverBorder(PointMouseDown);

            if (PointMouseDownOverBorder == MousePositionOverBorder.no)
            {
                // initiate DragDrop
                DataObject data = new DataObject();
                data.SetData(DataFormats.StringFormat, "UserControl_Node|" + Guid.ToString());
                this.DoDragDrop(data, DragDropEffects.Copy | DragDropEffects.Move);
            }
            else
            {
                // initiate Resizing
                Resizing_Modus = true;
                Resizing_MousePositionOverBorder = PointMouseDownOverBorder;
            }
        }

       
        private void button1_Click(object sender, EventArgs e)
        {
            MessageBox.Show(Guid.ToString());
        }
    }
}
