using FareCalcLib.Datasets;
using FareCalcLib.Datasets.TariffTableAdapters;
using static FareCalcLib.Constants;

using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Text;
using System.Linq;
using System.Data;
using System.Data.SqlClient;

namespace FareCalcLib
{
    public class TariffCalculator
    {
        private SqlConnection connection;

        public TariffCalculator(SqlConnection connection)
        {
            this.connection = connection;
        }

        public Datasets.Tariff GetTariffDataset(int tariffId)
        {
            var tariffDs = new Tariff();
            var tariffInfoAdp = new m_tariff_infoTableAdapter();
            tariffInfoAdp.Connection = connection;
            var tariffDetailAdp = new m_tariff_detailTableAdapter();
            tariffDetailAdp.Connection = connection;

            var cntInfo = tariffInfoAdp.FillByTariffInfoId(tariffDs.m_tariff_info, tariffId);
            var cntDetail = tariffDetailAdp.FillByTriffInfoId(tariffDs.m_tariff_detail, tariffId);

            if(!(cntInfo > 0) || !(cntDetail > 0))
            {
                // TODO: master not exist error
                throw new Exception("the master data does not exist");
            }
            return tariffDs;
        }

        internal Decimal GetKeisanValue(Tariff tariffDs, CalcVariables calcVariables, string tariffAxisKbn)
        {
            // TODO: get info from m_tariff_info
            var tariffInfo = tariffDs.m_tariff_info.First();

            //var axisKbnColName = tariffAxisKbn.ToLower() + "_axis_kbn";

            // TODO: urgent akema vertical_axis_kbn、vertical_axis_kbn、horizontal_axis_kbnの振り分け方法を確認
            var axisKbnColName = "";
            switch (tariffAxisKbn) {
                case CnTariffAxisKbn.Vertical:
                    axisKbnColName = "vertical_axis_kbn";
                    break;
                case CnTariffAxisKbn.Horizontal:
                    axisKbnColName = "horizontal_axis_kbn";
                    break;
            }

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
                        return Decimal.Parse(calcVariables.YusoMeansKbn);
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

        public Decimal GetPrice(Tariff tariffDs, CalcVariables calcVariables)
        {
            // TODO: normal-low akema タリフの契約種別とデータの契約種別が一致しない場合は計算エラー、エラーオブジェクト追加
            // get column value
            Decimal vertialValue = GetKeisanValue(tariffDs, calcVariables, CnTariffAxisKbn.Vertical);
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

                // TODO: high endo 加算ありの範囲の時、両端加算ありの時、繰返し範囲の時、
                var adding_charge = (decimal)0;

                return base_charge_before_adding + adding_charge;
            }
            else
            {
                // TODO: normal akema タリフが見つからないError処理
                // エラー終了しない。計算エラーを行にマークして、さらに、ログ出力「タリフデータが見つからない、または、複数あります。」
                return 0;
            }

        }

        public Decimal GetPrice(int tariffId, CalcVariables calcVariables)
        {
            if (!DBNull.Value.Equals(tariffId))
            {
                var tariffDs = GetTariffDataset(tariffId);
                return GetPrice(tariffDs, calcVariables);
            }
            return 0;

        }
    }


}
