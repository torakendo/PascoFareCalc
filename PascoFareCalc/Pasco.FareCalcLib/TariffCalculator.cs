using Pasco.FareCalcLib.Datasets;
using Pasco.FareCalcLib.Datasets.TariffTableAdapters;
using static Pasco.FareCalcLib.Constants;

using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Text;
using System.Linq;
using System.Data;
using System.Data.SqlClient;

namespace Pasco.FareCalcLib
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
            var tariffAxisAdp = new m_tariff_axisTableAdapter();
            tariffAxisAdp.Connection = connection;

            var cntInfo = tariffInfoAdp.FillByTariffInfoId(tariffDs.m_tariff_info, tariffId);
            var cntDetail = tariffDetailAdp.FillByTriffInfoId(tariffDs.m_tariff_detail, tariffId);
            var cntAxis = tariffAxisAdp.FillByTriffInfoId(tariffDs.m_tariff_axis, tariffId);

            return tariffDs;
        }

        internal Decimal GetKeisanValue(Tariff tariffDs, CalcVariables calcVariables, string tariffAxisKbn)
        {
            // TODO: get info from m_tariff_info
            var tariffInfo = tariffDs.m_tariff_info.First();

            //var axisKbnColName = tariffAxisKbn.ToLower() + "_axis_kbn";

            // TODO: done akema vertical_axis_kbn、vertical_axis_kbn、horizontal_axis_kbnの振り分け方法を確認
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


        public void GetRepetitionPrice(Tariff tariffDs, decimal Value, string tariffAxisKbn 
        , out decimal repetitionPrice
        , out decimal step_to
        )
        {
            repetitionPrice = 0;
            step_to = 0;

            // タリフ軸マスタデータ取得
            var tariffAxisQuery = tariffDs.m_tariff_axis.Where(r => (r.tariff_axis_kbn == CnTariffAxisKbn.Vertical && r.value_kbn == AxisValueKbn.Repetition));

            if (tariffAxisQuery.Count() == 1)
            {
                decimal price = 0;

                // 繰り返し回数 ＆ 可算額取得
                var tav = tariffAxisQuery.Select(tdr => tdr).ToArray()[0];
                step_to = tav.step_to;
                decimal repetitionCount = Math.Floor(Value / tav.step_to);
                decimal num = Value % tav.step_to;
                var numlist = new List<decimal>();
                if (num != 0) numlist.Add(num);

                // repetitionCount)
                {
                    numlist.Add(tav.step_to);
                    repetitionCount--;
                }

                // 繰り返し範囲のデータ取得
                numlist.ForEach(loopNum =>
                {
                    switch (tariffAxisKbn)
                    {
                        case CnTariffAxisKbn.Vertical:
                            price += tariffDs.m_tariff_detail
                            .Where(r => (r.vertical_step_from < loopNum && r.vertical_step_to >= loopNum))
                            .OrderByDescending(x => x.tariff_price)
                            .Select(tdr => tdr).ToArray()[0].tariff_price;
                            break;
                        case CnTariffAxisKbn.Horizontal:
                            price += tariffDs.m_tariff_detail
                            .Where(r => (r.horizontal_step_from < loopNum && r.horizontal_step_to >= loopNum))
                            .OrderByDescending(x => x.tariff_price)
                            .Select(tdr => tdr).ToArray()[0].tariff_price;
                            break;
                    }
                });
                repetitionPrice = price;
            }
        }

        public void GetUnitPrice(EnumerableRowCollection<Tariff.m_tariff_detailRow> tariffDetailQuery, decimal vertialValue, decimal horizontalValue
            , out decimal base_charge_before_adding    // 可算前
            , out decimal vertical_adding_price        // 縦軸の加算額
            , out decimal horizontal_adding_price      // 横軸の加算額
            , out decimal vertical_adding_count        // タリフ縦増分数
            , out decimal horizontalValue_adding_count // タリフ横増分数
            , out decimal vh_adding_price              // 両軸の加算額
            , out decimal adding_charge                // 縦軸・横軸・両軸の加算額合計
            , out decimal total_adding_charge          // 可算前 + 加算額合計
            )
        {
            base_charge_before_adding = 0;    // 可算前
            vertical_adding_price = 0;        // 縦軸の加算額
            horizontal_adding_price = 0;      // 横軸の加算額
            vertical_adding_count = 0;        // タリフ縦増分数
            horizontalValue_adding_count = 0; // タリフ横増分数
            vh_adding_price = 0;              // 両軸の加算額
            adding_charge = 0;                // 縦軸・横軸・両軸の加算額合計
            total_adding_charge = 0;          // 可算前 + 加算額合計

            if (tariffDetailQuery.Count() == 1)
            {
                var td = tariffDetailQuery.Select(tdr => tdr).ToArray()[0];

                // 追加する前に関税価格を設定する set the tariff price before adding
                base_charge_before_adding = td.tariff_price; // 可算前

                if (td.vertical_adding_flg == 1)
                {
                    // 縦軸データ取得
                    var vd = tariffDetailQuery
                    .Where(r => ((r.vertical_step_from == td.vertical_step_from && r.horizontal_step_to >= td.horizontal_step_from)))
                    .Select(tdr => tdr).ToArray()[0];
                    // タリフ縦増分数 = （縦軸値-タリフ縦軸目盛値FR）/ （タリフ縦増分値）
                    vertical_adding_count = ((vertialValue - vd.vertical_step_from) / vd.vertical_adding_step_value);
                    vertical_adding_price = vertical_adding_count * vd.vertical_adding_unit_price;
                }

                // 横軸の加算額 = タリフ横増分数 * 横軸加算額
                if (td.horizontal_adding_flg == 1)
                {
                    // 横軸データ取得
                    var hd = tariffDetailQuery
                    .Where(r => ((r.vertical_step_to == td.vertical_step_from && r.horizontal_step_from >= td.horizontal_step_from)))
                    .Select(tdr => tdr).ToArray()[0];
                    // タリフ横増分数 = （横軸値-タリフ横軸目盛値FR）/（タリフ横増分値）
                    horizontalValue_adding_count = ((horizontalValue - hd.horizontal_step_from) / hd.horizontal_adding_step_value);
                    horizontal_adding_price = horizontalValue_adding_count * hd.horizontal_adding_unit_price;
                }

                // 両軸の加算額
                if (td.vertical_adding_flg == 1 && td.horizontal_adding_flg == 1)
                {
                    // 増分値取得（verticalとhorizontal同値の想定だが、異なった場合は大きい方を取得）
                    decimal adding_unit_price = td.vertical_adding_unit_price > td.horizontal_adding_unit_price ? td.vertical_adding_unit_price : td.horizontal_adding_unit_price;

                    // 両軸の加算額 = 両軸増分単価 * (タリフ縦増分数 * タリフ横増分数)
                    vh_adding_price = adding_unit_price * (vertical_adding_count * horizontalValue_adding_count);
                }

                // 縦軸・横軸・両軸の加算額合計
                adding_charge = vertical_adding_price + horizontal_adding_price + vh_adding_price;

                // 可算前 + 加算額合計
                total_adding_charge = base_charge_before_adding + adding_charge;
            }
        }

        public decimal GetPrice(Tariff tariffDs, CalcVariables calcVariables)
        {

            // TODO: normal-low akema タリフの契約種別とデータの契約種別が一致しない場合は計算エラー、エラーオブジェクト追加
            // 列の値を取得する get column value
            Decimal vertialValue = GetKeisanValue(tariffDs, calcVariables, CnTariffAxisKbn.Vertical);
            Decimal horizontalValue = GetKeisanValue(tariffDs, calcVariables, CnTariffAxisKbn.Horizontal);

            decimal price = 0;

            int tarifInfoId = tariffDs.m_tariff_info.Select(tdr => tdr).ToArray()[0].tariff_info_id;

            //  繰返し・増分フラグ
            bool Repetition = 0 < tariffDs.m_tariff_axis.Where(r => (r.value_kbn == AxisValueKbn.Repetition)).Select(tdr => tdr).ToList().Count();
            bool UnitPrice = 0 < tariffDs.m_tariff_axis.Where(r => (r.value_kbn == AxisValueKbn.UnitPrice)).Select(tdr => tdr).ToList().Count();

             GetRepetitionPrice(tariffDs, vertialValue, CnTariffAxisKbn.Vertical
            , out decimal verticalRepetitionPrice
            , out decimal verticalStepTo
            );
             GetRepetitionPrice(tariffDs, horizontalValue, CnTariffAxisKbn.Horizontal
            , out decimal horizontalRepetitionPrice
            , out decimal horizontalStepto
             );

            // 繰返し範囲縦件数
            var verticalLoopCount = tariffDs.m_tariff_axis.Where(r => (r.tariff_axis_kbn == CnTariffAxisKbn.Vertical && r.value_kbn == AxisValueKbn.Repetition)).Select(tdr => tdr).ToList().Count();
            var horizontalLoopCount = tariffDs.m_tariff_axis.Where(r => (r.tariff_axis_kbn == CnTariffAxisKbn.Horizontal && r.value_kbn == AxisValueKbn.Repetition)).Select(tdr => tdr).ToList().Count();

            if (Repetition && !UnitPrice)
            {
                // TODO: done akema 繰返し範囲の時
                if (0 < verticalLoopCount) price += verticalRepetitionPrice;
                if (0 < horizontalLoopCount) price += horizontalRepetitionPrice;

                return price;
            }
            else if (!Repetition && UnitPrice)
            {
                // TODO: done akema 加算ありの範囲の時、両端加算ありの時
                var tariffDetailQuery = tariffDs.m_tariff_detail
                .Where(r =>
                ((r.vertical_step_from < vertialValue && r.vertical_step_to >= vertialValue) ||
                (r.vertical_step_from == vertialValue && r.vertical_step_to == vertialValue))
                &&
                ((r.horizontal_step_from < horizontalValue && r.horizontal_step_to >= horizontalValue) ||
                (r.horizontal_step_from == horizontalValue && r.horizontal_step_to == horizontalValue))
                );

                GetUnitPrice(tariffDetailQuery, vertialValue, horizontalValue
                , out decimal base_charge_before_adding    // 可算前
                , out decimal vertical_adding_price        // 縦軸の加算額
                , out decimal horizontal_adding_price      // 横軸の加算額
                , out decimal vertical_adding_count        // タリフ縦増分数
                , out decimal horizontalValue_adding_count // タリフ横増分数
                , out decimal vh_adding_price              // 両軸の加算額
                , out decimal adding_charge                // 縦軸・横軸・両軸の加算額合計
                , out decimal total_adding_charge          // 可算前 + 加算額合計
                    );

                return total_adding_charge;
            }
            else if (Repetition && UnitPrice)
            {
                // TODO: urgent akema 繰返し範囲・加算ありの範囲の時、両端加算あり両方あるとき
                return price;
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
