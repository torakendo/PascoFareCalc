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

        public enum CnCalcStatus : short
        {
            UnCalc = 0,
            Done = 1,
            Doing = -1,
            Error = -2
        }

        public enum CnEndStatus : short
        {
            Good = 0,
            Warning = -1,
            Error = -2
        }

        public enum CnContractType : short
        {
            // TODO: 文字列コードに変更
            ByVehicle = 1,
            ByItem = 2
        }

        public enum CnTariffAxisKbn : short
        {
            // TODO: 文字列コードに変更
            Vertical = 0,
            Horizontal = 1
        }

        public enum CnYusoKbn : short
        {
            Delivery = 1,
            Move = 2
        }

        public enum CnVerifyStatus : short 
        {
            NotVerified = 0,
            Verified = 1
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

        public struct AxisKbn 
        {
            public const string WeightKg = "01";
            public const string DistanceKm = "02";
            public const string TimeMins = "03";
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
    }
}
