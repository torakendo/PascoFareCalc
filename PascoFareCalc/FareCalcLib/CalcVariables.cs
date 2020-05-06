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
        public string YusoModeKbn { get; set; }

        public CalcVariables(decimal distanceKm, decimal weightKg, decimal timeMinutes, string YusoModeKbn)
        {
            this.DistanceKm = distanceKm;
            this.WeightKg = weightKg;
            this.TimeMinutes = timeMinutes;
            this.YusoModeKbn = YusoModeKbn;
        }

        public CalcVariables(CalcWk.t_extra_cost_wkRow exCostRow)
        {
            DistanceKm = exCostRow.distance_km;
            WeightKg = exCostRow.weight_sum_kg;
            TimeMinutes = exCostRow.time_mins;
            YusoModeKbn = exCostRow.yuso_means_kbn;
        }

        public CalcVariables(CalcWk.t_keisan_wkRow keisanWkRow)
        {
            DistanceKm = keisanWkRow.distance_km;
            WeightKg = keisanWkRow.weight_sum_kg;
            TimeMinutes = keisanWkRow.time_mins;
            YusoModeKbn = keisanWkRow.yuso_means_kbn;
        }
    }
}
