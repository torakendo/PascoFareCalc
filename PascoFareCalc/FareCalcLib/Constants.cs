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

        public enum CnContractType : short
        {
            ByVehicle = 1,
            ByItem = 2
        }

        public enum CnTariffAxisKbn : short
        {
            Vertial = 0,
            Horizontal = 1
        }

        public enum CnYusoKbn : short
        {
            Delivery = 1,
            Move = 2
        }

        public struct CalculateTypeKbn
        {
            public const string Triff = "01";
            public const string Adding = "02";
            public const string AddingRatio = "03";
        }

        public struct extraCostKbn
        {
            public const string StoppingCharge = "01";
            public const string CargoCharge = "02";
            public const string TimeCharge = "03";
            public const string DistanceCharge = "04";
            public const string HelperCharge = "05";
            public const string FuelCharge = "06";
            public const string Other = "11";
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
    }
}
