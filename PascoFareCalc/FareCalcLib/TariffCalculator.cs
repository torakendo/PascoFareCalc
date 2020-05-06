using FareCalcLib.Datasets;
using FareCalcLib.Datasets.TariffTableAdapters;
using static FareCalcLib.Constants;

using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Text;
using System.Linq;

namespace FareCalcLib
{
    public class TariffCalculator
    {

        public Datasets.Tariff GetTariffDataset(int tariffId)
        {
            var tariffDs = new Tariff();
            var tariffInfoAdp = new m_tariff_infoTableAdapter();
            tariffInfoAdp.ClearBeforeFill = true;
            var tariffDetailAdp = new m_tariff_detailTableAdapter();
            tariffDetailAdp.ClearBeforeFill = true;

            var cntInfo = tariffInfoAdp.FillByTariffInfoId(tariffDs.m_tariff_info, tariffId);
            var cntDetail = tariffDetailAdp.FillByTriffInfoId(tariffDs.m_tariff_detail, tariffId);

            if(!(cntInfo > 0) || !(cntDetail > 0))
            {
                // TODO: master not exist error
                throw new Exception("the master data does not exist");
            }
            return tariffDs;
        }

        internal Decimal GetKeisanValue(Tariff tariffDs, CalcVariables calcVariables, CnTariffAxisKbn tariffAxisKbn)
        {
            // TODO: get info from m_tariff_info
            var tariffInfo = tariffDs.m_tariff_info.First();
            var axisKbnColName = tariffAxisKbn.ToString() + "_axis_kbn";
            if (!tariffInfo.IsNull(axisKbnColName)) 
            {
                string axisKvn = tariffInfo[axisKbnColName].ToString();
                switch (axisKvn)
                {
                    case AxisKbn.WeightKg:
                        return calcVariables.WeightKg;
                    case AxisKbn.DistanceKm:
                        return calcVariables.DistanceKm;
                    case AxisKbn.TimeMins:
                        return calcVariables.TimeMinutes;
                    case AxisKbn.YusoMeans:
                        return Decimal.Parse(calcVariables.YusoModeKbn);
                    default:
                        // TODO: data incorrect error
                        return 0;
                }
            }
            else
            {
                // TODO: data incorrect error
                return 0;
            }
        }

        public Decimal GetTriffPrice(Tariff tariffDs, CalcVariables calcVariables)
        {
            // get column value
            Decimal vertialValue = GetKeisanValue(tariffDs, calcVariables, CnTariffAxisKbn.Vertial);
            Decimal horizontalValue = GetKeisanValue(tariffDs, calcVariables, CnTariffAxisKbn.Horizontal);

            var tariffDetailQuery = tariffDs.m_tariff_detail
                .Where(r =>
                    ((r.vertical_step_from < vertialValue && r.vertical_step_to >= vertialValue) ||
                    (r.vertical_step_from == vertialValue && r.vertical_step_to == vertialValue))
                    &&
                    ((r.horizontal_step_from < horizontalValue && r.horizontal_step_to >= horizontalValue) ||
                    (r.horizontal_step_from == horizontalValue && r.horizontal_step_to == horizontalValue))
                );

            if (tariffDetailQuery.Count() == 1)
            {
                // set the tariff price before adding
                var base_charge_before_adding = tariffDetailQuery.Select(tdr => tdr.tariff_price).ToArray()[0];

                // TODO: 加算ありの範囲の時、両端加算ありの時、繰返し範囲の時、
                var adding_charge = (decimal)0;

                return base_charge_before_adding + adding_charge;
            }
            else
            {
                // TODO: タリフが見つからないError処理
                return 0;
            }

        }

        public Decimal GetTriffPrice(int tariffId, CalcVariables calcVariables)
        {
            if (!DBNull.Value.Equals(tariffId))
            {
                var tariffDs = GetTariffDataset(tariffId);
                return GetTriffPrice(tariffDs, calcVariables);
            }
            return 0;

        }
    }


}
