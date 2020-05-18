using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FareCalcLib
{
    public class CommonHelper
    {
        // 現在日設定
        public static DateTime CurrentDate = DateTime.Now;

        public static DateTime GetDate()
        {
            return CurrentDate;
        }
    }

}
