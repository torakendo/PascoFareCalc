using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FareCalcLib
{
    public class Constants
    {
        //public static Dictionary<string, short> CalcStatus = new Dictionary<string, short>()
        //{
        //    {"UnCalc", -1 },
        //    {"Doing", 0 },
        //    {"Done", 1 },
        //};

        // TODO: urgent akema 計算区分に変更
        public struct CnCalcStatus
        {
            // <summary> 空白 </summary>
            public const string Blank = "00";
            // <summary> 計算済 </summary>
            public const string Done = "01";
            // <summary> 未計算 </summary>
            public const string UnCalc = "02";
            // <summary> 計算中 </summary>
            public const string Doing = "03";
        }

        // TODO: urgent akema 計算終了ステータス区分に変更
        public enum CnEndStatus : short
        {
            Good = 1,
            Warning = 2,
            Error = 3
        }

        //public struct CnEndStatus
        //{
        //    public const string Good = "01";
        //    public const string Warning = "02";
        //    public const string Error = "03";
        //}

        // TODO: urgent akema 契約種別に変更
        public struct CnContractType
        {
            // <summary> 車建 </summary>
            public const string ByVehicle = "01";
            // <summary> 個建 </summary>
            public const string ByItem = "02";
        }


        // TODO: done akema タリフ軸区分に変更
        public struct CnTariffAxisKbn
        {
            // <summary> 縦 </summary>
            public const string Vertical = "01";
            // <summary> 横 </summary>
            public const string Horizontal = "02";
        }

        // TODO: urgent akema 輸送区分に変更
        public struct CnYusoKbn
        {
            // <summary> 移送 </summary>
            public const string Move = "01";
            // <summary> 配送 </summary>
            public const string Delivery = "02";
            // <summary> 支給 </summary>
            public const string Provision = "03";
        }

        // TODO: normal endo 正しいデータ分類にする
        public struct DataClass
        {
            public const string Move = "01";
            public const string Delivery = "02";
            public const string BringBack = "03";
        }

        // TODO: urgent akema 確認区分に変更
        public struct CnVerifyStatus
        {
            // <summary> 空白 </summary>
            public const string Blank = "00";
            // <summary> 確認済 </summary>
            public const string Verified = "01";
            // <summary> 未確認 </summary>
            public const string NotVerified  = "02";
        }

        public struct CalculateTypeKbn
        {
            public const string Triff = "01";
            public const string Adding = "02";
            public const string AddingRatio = "03";
        }

        public struct extraCostKbn
        {
            /// <summary> 中継料 </summary>
            public const string StoppingCharge = "01";
            /// <summary> 航送料 </summary>
            public const string CargoCharge = "02";
            /// <summary> 時間割増料 </summary>
            public const string TimeCharge = "03";
            /// <summary> 距離割増料 </summary>
            public const string DistanceCharge = "04";
            /// <summary> 助手料 </summary>
            public const string HelperCharge = "05";
            /// <summary> 燃油料 </summary>
            public const string FuelCharge = "06";
            /// <summary> 洗浄料 </summary>
            public const string WashCharge = "07";
            /// <summary> 台貫料 </summary>
            public const string StandCharge = "08";
            /// <summary> 有料道路代 </summary>
            public const string TollRoadCharge = "09";
            /// <summary> 期間割増 </summary>
            public const string SeasonalCharge = "10";
            /// <summary> 地区割増 </summary>
            public const string AreaCharge = "11";
            /// <summary> 休日割増 </summary>
            public const string HolidayCharge = "12";
            /// <summary> 特殊車両割増 </summary>
            public const string SpecialVehicleCharge = "13";
            /// <summary> その他 </summary>
            public const string OtherCharge = "99";
        }

        // 縦横軸区分
        public struct AxisKbn
        {
            /// <summary> 重量（kg） </summary>
            public const string WeightKg = "01";
            /// <summary> 距離（km） </summary>
            public const string DistanceKm = "02";
            /// <summary> 時間(分） </summary>
            public const string TimeMins = "03";
            /// <summary> 輸送手段 </summary>
            public const string YusoMeans = "04";
        }

        //public struct KeisanValueColumnName
        //{
        //    public const string WeightKg = "weight_sum_kg";
        //    public const string DistanceKm = "distance_km";
        //    public const string TimeMins = "time_mins";
        //    public const string YusoMeans = "yuso_means_kbn";
        //}

        public struct InclKbn
        {
            public const string UnDone = "02";
            public const string Done = "01";
        }

        // 縦横軸繰返し区分
        public struct AxisRepeatkbn
        {
            /// <summary> あり </summary>
            public const string Yes = "01";
            /// <summary> なし </summary>
            public const string No = "02";
        }

        // タリフ軸目盛限度区分
        public struct AxisStepToKbn
        {
            /// <summary> まで </summary>
            public const string Until = "01";
            /// <summary> 超過 </summary>
            public const string Excess = "02";
        }

        // タリフ軸目盛末端区分
        public struct AxisValueKbn
        {
            /// <summary> 金額 </summary>
            public const string Money = "01";
            /// <summary> 増分単価 </summary>
            public const string UnitPrice = "02";
            /// <summary> 繰返し </summary>
            public const string Repetition = "03";
        }
    }
}
