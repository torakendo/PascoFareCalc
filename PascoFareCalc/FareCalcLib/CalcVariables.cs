using FareCalcLib.Datasets;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Text;

namespace FareCalcLib
{
    public struct CalcVariables
    {
        public decimal DistanceKm { get; set; }
        public decimal WeightKg { get; set; }
        public decimal TimeMinutes { get; set; }
        public string YusoMeansKbn { get; set; }

        public CalcVariables(decimal distanceKm, decimal weightKg, decimal timeMinutes, string yusoMeansKbn)
        {
            this.DistanceKm = distanceKm;
            this.WeightKg = weightKg;
            this.TimeMinutes = timeMinutes;
            this.YusoMeansKbn = yusoMeansKbn;
        }

        public CalcVariables(CalcWk.t_extra_cost_wkRow exCostRow)
        {
            DistanceKm = exCostRow.distance_km ;
            WeightKg = exCostRow.Isweight_sum_kgNull() ? 0 :exCostRow.weight_sum_kg;
            TimeMinutes = exCostRow.Istime_minsNull() ? 0 : exCostRow.time_mins;
            YusoMeansKbn = exCostRow.Isyuso_means_kbnNull() ? "" : exCostRow.yuso_means_kbn;
        }

        public CalcVariables(CalcWk.t_keisan_wkRow keisanWkRow)
        {
            DistanceKm = keisanWkRow.distance_km;
            WeightKg = keisanWkRow.Isweight_sum_kgNull() ? 0 : keisanWkRow.weight_sum_kg;
            TimeMinutes = keisanWkRow.Istime_minsNull() ? 0 : keisanWkRow.time_mins;
            YusoMeansKbn = keisanWkRow.Isyuso_means_kbnNull() ? "" : keisanWkRow.yuso_means_kbn;
        }
    }
}
