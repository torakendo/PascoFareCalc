using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FareCalcLib
{
    class Constants
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
    }
}
