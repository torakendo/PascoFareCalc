using FareCalcLib.Datasets;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Text;
using StartCalcTableAdapters = FareCalcLib.Datasets.StartCalcTableAdapters;
using CalcTrnTableAdapters = FareCalcLib.Datasets.CalcTrnTableAdapters;
using CalcWkTableAdapters = FareCalcLib.Datasets.CalcWkTableAdapters;
using FareCalcLib.Datasets.CalcNoTableAdapters;
using FareCalcLib.Datasets.TariffTableAdapters;

using static FareCalcLib.Constants;
using System.Data;
using System.Linq;
using System.Security.Cryptography;
using System.Collections;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices.ComTypes;
using FareCalcLib.Datasets.ExtraCostPatternTableAdapters;
using System.Transactions;

namespace FareCalcLib


{
    public class CalculateManager
    {
        // TODO: urgent akema 最新のテーブル定義に合わせる
        // TODO: urgent 

        #region "fields"
        private int CalcNo = 0;
        private CalcTrn CalcTrnDs;
        private CalcWk CalcWkDs;
        // TODO: バッチサイズConfigへ移動
        private static int UpdateBatchSize = 100;

        List<String> yusoWkColNames = new List<String>()
                    {
                        "base_charge_amount",
                        "special_charge_amount",
                        "stopping_charge_amount",
                        "cargo_charge_amount",
                        "other_charge_amount",
                        "actual_km_surcharge_amount",
                        "actual_time_surcharge_amount",
                        "actual_assist_surcharge_amount",
                        "actual_load_surcharge_amount",
                        "actual_stand_surcharge_amount",
                        "actual_wash_surcharge_amount",
                        "total_charge_amount"
                    };

        #endregion

        #region "properties"
        public SqlConnection Connection { get; set; }
        #endregion

        public CalculateManager(int calcNo)
        {
            CalcNo = calcNo;
            CalcWkDs = new CalcWk();
            CalcTrnDs = new CalcTrn();
        }
        /// <summary>
        /// execute calculation 
        /// </summary>
        public void Calcurate()
        {
            try
            {
                this.PrepareCalculate();
                // TODO: debug code for p1
                Console.WriteLine("Calculating Now....");

                this.CalculateBaseFare();
                this.CalculateExtraCharge();
                this.SetResultToKeisanWk();
                this.SetResultToYusoWk();
                this.DivideResultAmount();
                this.RefrectToResultData();
            }
            catch (Exception)
            {

                throw;
            }
        }

        private void SetResultToKeisanWk()
        {
            // TODO: normal akema 時間割増料、期間割増料、地区割増、
            // TODO: spec endo 時間指定割増料、特別作業割増、特殊車両割増
            // 基本運賃と付帯費用項目のトータル金額を算出して、セット
            CalcWkDs.t_keisan_wk.ToList().ForEach(keisanWkRow => 
            {
                decimal totalExtraCost = 0;
                // set extra_cost value to keisan_wk
                CalcWkDs.t_extra_cost_wk.Where(exCostWkRow =>
                                exCostWkRow.calc_no == this.CalcNo &&
                                exCostWkRow.keisan_key == keisanWkRow.keisan_key)
                            .ToList()
                            .ForEach(exCostWkRow =>
                            {
                                // TODO: 加算先は設定値から取得する
                                switch (exCostWkRow.extra_cost_kind_kbn)
                                {
                                    case extraCostKbn.StoppingCharge:
                                        // 中継料 
                                        keisanWkRow.stopping_charge_amount += exCostWkRow.extra_charge_amount;
                                        break;
                                    case extraCostKbn.CargoCharge:
                                        // 航送料
                                        keisanWkRow.cargo_charge_amount += exCostWkRow.extra_charge_amount;
                                        break;
                                    case extraCostKbn.DistanceCharge:
                                        // 距離割増
                                        keisanWkRow.actual_km_surcharge_amount += exCostWkRow.extra_charge_amount;
                                        break;
                                    case extraCostKbn.HelperCharge:
                                        // 助手料
                                        keisanWkRow.actual_assist_surcharge_amount += exCostWkRow.extra_charge_amount;
                                        break;
                                    case extraCostKbn.WashCharge:
                                        // 洗浄料
                                        keisanWkRow.actual_wash_surcharge_amount = exCostWkRow.extra_charge_amount;
                                        break;
                                    case extraCostKbn.StandCharge:
                                        // 台貫料
                                        keisanWkRow.actual_stand_surcharge_amount = exCostWkRow.extra_charge_amount;
                                        break;
                                    case extraCostKbn.TollRoadCharge:
                                        // 有料道路代
                                        keisanWkRow.actual_load_surcharge_amount = exCostWkRow.extra_charge_amount;
                                        break;
                                    case extraCostKbn.OtherCharge:
                                    case extraCostKbn.FuelCharge:
                                        // 燃油料、その他
                                        keisanWkRow.other_charge_amount += exCostWkRow.extra_charge_amount;
                                        break;
                                    default:
                                        keisanWkRow.other_charge_amount += exCostWkRow.extra_charge_amount;
                                        break;
                                }
                                totalExtraCost += exCostWkRow.extra_charge_amount;
                            });

                keisanWkRow.total_charge_amount =
                                keisanWkRow.base_charge_amount + totalExtraCost;
            }
            );            
        }

        private void SetResultToYusoWk()
        {
            // TODO: SQLに変更することを検討
            // TODO: spec endo 実績項目を上書きするのはまずいか
            foreach (var yusoWkRow in this.CalcWkDs.t_yuso_wk)
            {
                if (yusoWkRow.contract_type == CnContractType.ByItem) 
                {
                    // set keisan wk values to same key datarow of uso_wk when ByItem(ko-date)
                    // get keisan_wk row at the same key
                    var keisanWkQuery =
                        this.CalcWkDs.t_keisan_wk
                            .Where(r => r.calc_no == CalcNo && r.yuso_key == yusoWkRow.yuso_key);
                    if (keisanWkQuery.Count() == 1)
                    {
                        // set value to yusoWkRow
                        var keisanWkRow = keisanWkQuery.First();
                        yusoWkRow.weight_sum_kg = keisanWkRow.weight_sum_kg;
                        yusoWkColNames.ForEach(colname => yusoWkRow[colname] = keisanWkRow[colname]);                       
                    } else
                    {
                        // TODO: 想定外データ不整合err
                    }

                } else if (yusoWkRow.contract_type == CnContractType.ByVehicle)
                {
                    // get max total_charge_amaunt row of keisan_wk by yuso key of ByVehicle(sha-date)
                    var keisanWkRowQuery = this.CalcWkDs.t_keisan_wk
                            .Where(r =>
                                r.calc_no == this.CalcNo &&
                                r.yuso_key == yusoWkRow.yuso_key)
                            .OrderByDescending(r => r.total_charge_amount);
                            

                    if (keisanWkRowQuery.Count() > 0 )
                    {
                        var keisanWkRow = keisanWkRowQuery.First();
                        // set value to yusoWkRow
                        yusoWkRow.weight_sum_kg = keisanWkRow.weight_sum_kg;
                        yusoWkColNames.ForEach(colname => yusoWkRow[colname] = keisanWkRow[colname]);

                        // set "on" to max_flg in keisanWkRow
                        keisanWkRow.max_flg = 1;

                    } else
                    {
                        // TODO: 想定外データ不整合err
                    }
                }
            }
        }

        private void PrepareCalculate()
        {
            try
            {
                /* --------------------------------------
                * fill trn datatables by calcNo
                * -------------------------------------- */
                
                var yusoAdp = new CalcTrnTableAdapters.t_yusoTableAdapter();
                yusoAdp.Connection = Connection;
                var yusoRowCnt = yusoAdp.FillOriginalDataByCalcNo(this.CalcTrnDs.t_yuso, this.CalcNo, (short)int.Parse(CnCalcStatus.Doing));
                // TODO: debug code for p1
                Console.WriteLine("retrieve from t_yuso COUNT = {0}", yusoRowCnt);

                var keisanAdp = new CalcTrnTableAdapters.t_keisanTableAdapter();
                keisanAdp.Connection = Connection;
                keisanAdp.FillOriginalDataByCalcNo(this.CalcTrnDs.t_keisan, this.CalcNo, (short)int.Parse(CnCalcStatus.Doing));

                var detailAdp = new CalcTrnTableAdapters.t_detailTableAdapter();
                detailAdp.Connection = Connection;
                detailAdp.FillOrigialDataByCalcNo(this.CalcTrnDs.t_detail, this.CalcNo, (short)int.Parse(CnCalcStatus.Doing));

                /* --------------------------------------
                * fill yuso_wk
                * -------------------------------------- */
                var yusoWkAdp = new CalcWkTableAdapters.t_yuso_wkTableAdapter();
                yusoWkAdp.Connection = Connection;
                yusoWkAdp.FillByCalcNo(this.CalcWkDs.t_yuso_wk, this.CalcNo);

                /* --------------------------------------
                * insert data to keisan_wk and detail_wk
                * -------------------------------------- */
                //  copy keisan_trn datatable to keisan_wk
                // TODO:クエリーにするか検討
                var colnameOfKeisanWk = Enumerable.Range(0, CalcWkDs.t_keisan_wk.Columns.Count)
                            .Select(i => CalcWkDs.t_keisan_wk.Columns[i].ColumnName).ToList();
                CalcTrnDs.t_keisan.ToList().ForEach(r =>
                {
                    var newRow = CalcWkDs.t_keisan_wk.NewRow();
                    colnameOfKeisanWk.ForEach(colname =>
                    {
                        if (CalcTrnDs.t_keisan.Columns.Contains(colname)) newRow[colname] = r[colname];
                    });
                    newRow["calc_no"] = CalcNo;
                    newRow["max_flg"] = 0;
                    CalcWkDs.t_keisan_wk.Rows.Add(newRow);
                });

                //  insert into keisan_wk
                var keisanWkAdp = new CalcWkTableAdapters.t_keisan_wkTableAdapter();
                keisanWkAdp.Connection = Connection;
                keisanWkAdp.SetUpdateBatchSize(UpdateBatchSize);
                keisanWkAdp.Update(this.CalcWkDs.t_keisan_wk);

                //   copy detail_trn datatable to detail_wk
                // TODO:クエリーにするか検討
                var colnameOfDetailWk = Enumerable.Range(0, CalcWkDs.t_detail_wk.Columns.Count)
                            .Select(i => CalcWkDs.t_detail_wk.Columns[i].ColumnName).ToList();
                CalcTrnDs.t_detail.ToList().ForEach(r =>
                {
                    var newRow = CalcWkDs.t_detail_wk.NewRow();
                    colnameOfDetailWk.ForEach(colname =>
                    {
                        if (CalcTrnDs.t_detail.Columns.Contains(colname)) newRow[colname] = r[colname];
                    });
                    newRow["calc_no"] = CalcNo;
                    CalcWkDs.t_detail_wk.Rows.Add(newRow);
                });

                // 　insert into detail_wk
                var detailWkAdp = new CalcWkTableAdapters.t_detail_wkTableAdapter();
                detailWkAdp.Connection = Connection;
                detailWkAdp.SetUpdateBatchSize(UpdateBatchSize);
                detailWkAdp.Update(this.CalcWkDs.t_detail_wk);

                /* --------------------------------------
                * set info to keisan_wk by server query
                * -------------------------------------- */
                // TODO: 業者コードブランクの発着別に対応する。場合によっては1行ずつ適用   
                // TODO: urgent akema keisanWk の行ごとに、発着別のデータを取得し、業者コードが一致するデータあれば、それを使う。なければブランクのデータを使う
                // TODO: urgent akema 適用開始終了　出庫日で判定
                // set calcinfo to keisan_wk by server update query

                // TODO: urgent akema 以下が実行できなくなっているので確認する→クエリをFORMAT(orig_date, 'MMdd')→orig_dateに修正し解消
                var updCalcinfoCnt = keisanWkAdp.UpdateCalcInfo(DateTime.Now, "", this.CalcNo);

                // fill keisan_wk after update
                keisanWkAdp.FillByCalcNo(CalcWkDs.t_keisan_wk, this.CalcNo);

                /* --------------------------------------
                * set Sum of Weight to keisan_wk on server 
                * -------------------------------------- */
                SumWeightByKeisanUnit();

                /* --------------------------------------
                * create extra charge data on server 
                * -------------------------------------- */
                CreateExtraChargeData();
            }
            catch (Exception ex)
            {

                throw new Exception("PrepareCalculate Error", ex);
            }

        }

        private void CreateExtraChargeData()
        {
            try
            {
                var hasExtraCostRows = CalcWkDs.t_keisan_wk.Where(r => !r.Isextra_cost_pattern_idNull());

                hasExtraCostRows
                    .GroupBy(g => new { g.extra_cost_pattern_id })
                    .ToList()
                    .ForEach(group =>
                    {
                        //m_extra_pattern_cost_detail fill by extra_cost_pattern_id
                        var exCostPtnDAdp = new m_extra_cost_pattern_detailTableAdapter();
                        exCostPtnDAdp.Connection = Connection;
                        var exCostPtnDetailTbl = exCostPtnDAdp.GetDataByExtraCostPatternId(group.Key.extra_cost_pattern_id);

                        group.ToList().ForEach(keisanWkRow =>
                        {

                            //create t_extra_cost data for each extra_cost_detail row
                            exCostPtnDetailTbl.ToList().ForEach(r =>
                            {
                                var newExCostWkRow = CalcWkDs.t_extra_cost_wk.Newt_extra_cost_wkRow();

                                // set value from ex_cost_pattern row
                                Enumerable.Range(0, exCostPtnDetailTbl.Columns.Count)
                                    .Select(i => exCostPtnDetailTbl.Columns[i].ColumnName)
                                    .ToList().ForEach(colname =>
                                    {
                                        newExCostWkRow[colname] = r[colname];
                                    });

                                // set value from keisan_wk
                                newExCostWkRow["calc_no"] = keisanWkRow["calc_no"];
                                newExCostWkRow["calc_ym"] = keisanWkRow["calc_ym"];
                                newExCostWkRow["contract_type"] = keisanWkRow["contract_type"];
                                newExCostWkRow["yuso_kbn"] = keisanWkRow["yuso_kbn"];
                                newExCostWkRow["orig_warehouse_block_cd"] = keisanWkRow["orig_warehouse_block_cd"];
                                newExCostWkRow["orig_warehouse_cd"] = keisanWkRow["orig_warehouse_cd"];
                                newExCostWkRow["terminal_id"] = keisanWkRow["terminal_id"];
                                newExCostWkRow["vehicle_id"] = keisanWkRow["vehicle_id"];
                                newExCostWkRow["dest_jis"] = keisanWkRow["dest_jis"];
                                newExCostWkRow["dest_warehouse_cd"] = keisanWkRow["dest_warehouse_cd"];
                                newExCostWkRow["yuso_mode_kbn"] = keisanWkRow["yuso_mode_kbn"];
                                newExCostWkRow["carrier_company_cd"] = keisanWkRow["carrier_company_cd"];
                                newExCostWkRow["orig_date"] = keisanWkRow["orig_date"];
                                newExCostWkRow["arriving_date"] = keisanWkRow["arriving_date"];
                                newExCostWkRow["dest_cd"] = keisanWkRow["dest_cd"];
                                newExCostWkRow["yuso_means_kbn"] = keisanWkRow["yuso_means_kbn"];
                                newExCostWkRow.yuso_key = keisanWkRow.yuso_key;
                                newExCostWkRow.keisan_key = keisanWkRow.keisan_key;

                                newExCostWkRow["distance_km"] = keisanWkRow["distance_km"];
                                newExCostWkRow["time_mins"] = keisanWkRow["time_mins"];
                                newExCostWkRow["fuel_cost_amount"] = keisanWkRow["fuel_cost_amount"];
                                newExCostWkRow["stopping_count"] = keisanWkRow["stopping_count"];
                                newExCostWkRow["weight_sum_kg"] = keisanWkRow["weight_sum_kg"];
                                newExCostWkRow["base_charge_amount"] = keisanWkRow["base_charge_amount"];
                                newExCostWkRow["extra_charge_amount"] = 0;

                                // add row to ex_cost_wk
                                CalcWkDs.t_extra_cost_wk.Addt_extra_cost_wkRow(newExCostWkRow);

                            });
                        });

                    });

            }
            catch (Exception)
            {

                throw;
            }
        }

        private void CalculateBaseFare()
        {
            try
            {
                var query = this.CalcWkDs.t_keisan_wk.AsEnumerable()
                    .OrderBy(x => x.apply_tariff_id)
                    .GroupBy(g => g.apply_tariff_id);

                var tariffCalculator = new TariffCalculator(Connection);

                // TODO: urgent akema GetTariffDataset()が取得できないので確認
                //foreach (var group in query)
                //{
                //    var tariffDs = tariffCalculator.GetTariffDataset(group.Key);

                //    foreach (var item in group)
                //    {
                //        // set price to keisan_wk row
                //        var calcVar = new CalcVariables(item);
                //        item.apply_vertical_value = tariffCalculator.GetKeisanValue(tariffDs, calcVar, CnTariffAxisKbn.Vertical);
                //        item.apply_horizonatl_value = tariffCalculator.GetKeisanValue(tariffDs, calcVar, CnTariffAxisKbn.Horizontal);
                //        item.original_base_charge_amount = tariffCalculator.GetPrice(tariffDs, calcVar);

                //        // TODO: high akema 業者別調整率
                //        item.base_charge_amount = item.original_base_charge_amount;

                //        // TODO: high akema 持ち戻り率適用
                //    }
                //}
            }
            catch (Exception ex)
            {

                throw new Exception("CalculateBaseFare Error", ex);
            }
        }

        private void CalculateExtraCharge()
        {
            try
            {
                var tariffCalculator = new TariffCalculator(Connection);
                foreach (var exCostRow in CalcWkDs.t_extra_cost_wk)
                {
                    // if not applicable, break
                    // TODO: 適用期間チェック
                    // TODO: normal akema 時間割増料、期間割増料、地区割増、
                    // TODO: spec endo 時間指定割増料、特別作業割増、特殊車両割増
                    var yusoWkquery = CalcWkDs.t_yuso_wk.Where(r => r.yuso_key == exCostRow.yuso_key);
                    switch (exCostRow.calculate_type_kbn)
                    {
                        case extraCostKbn.StoppingCharge:
                            // 中継料　タリフ金額＊中継回数
                            // TODO: 中継回数をNull check
                            var tariffPrice = tariffCalculator.GetPrice(exCostRow.tariff_id, new CalcVariables(exCostRow));
                            exCostRow.extra_charge_amount = tariffPrice * exCostRow.stopping_count;
                            break;
                        case extraCostKbn.CargoCharge:
                            // 航送料　タリフ金額
                            exCostRow.extra_charge_amount = tariffCalculator.GetPrice(exCostRow.tariff_id, new CalcVariables(exCostRow));
                            break;
                        case extraCostKbn.DistanceCharge:
                            // 距離割増　超過距離　タリフ金額
                            if (yusoWkquery.Count() == 1 && !(yusoWkquery.First().Isactual_distance_kmNull()))
                            {
                                var actualDistanceKm = yusoWkquery.First().actual_distance_km;
                                var calcVariables = new CalcVariables(exCostRow);
                                calcVariables.DistanceKm = (actualDistanceKm - exCostRow.distance_km) > 0 ? (actualDistanceKm - exCostRow.distance_km) : 0;
                                exCostRow.extra_charge_amount = tariffCalculator.GetPrice(exCostRow.tariff_id, calcVariables);
                            }
                            break;
                        case extraCostKbn.HelperCharge:
                            // 助手料　助手料(=付帯費用パターン明細金額)＊人数
                            if (yusoWkquery.Count() == 1 && !(yusoWkquery.First().Isactual_assistant_countNull()))
                            {
                                exCostRow.extra_charge_amount = exCostRow.adding_price * yusoWkquery.First().actual_assistant_count;
                            }
                            break;
                        case extraCostKbn.FuelCharge:
                            // 燃油料　発着別運賃計算情報より取得
                            exCostRow.extra_charge_amount = exCostRow.Isfuel_cost_amountNull() ? 0 : exCostRow.fuel_cost_amount;
                            break;
                        case extraCostKbn.WashCharge:
                            // 洗浄料　実績値より取得
                            if (yusoWkquery.Count() == 1)
                            {
                                exCostRow.extra_charge_amount = yusoWkquery.First().actual_wash_surcharge_amount;
                            }
                            break;
                        case extraCostKbn.StandCharge:
                            // 台貫料　実績値より取得
                            if (yusoWkquery.Count() == 1)
                            {
                                exCostRow.extra_charge_amount = yusoWkquery.First().actual_stand_surcharge_amount;
                            }
                            break;
                        case extraCostKbn.TollRoadCharge:
                            // 有料道路代　実績値より取得
                            if (yusoWkquery.Count() == 1)
                            {
                                exCostRow.extra_charge_amount = yusoWkquery.First().actual_load_surcharge_amount;
                            }
                            break;
                        case extraCostKbn.OtherCharge:
                            // その他
                            switch (exCostRow.calculate_type_kbn)
                            {
                                case CalculateTypeKbn.Triff:
                                    exCostRow.extra_charge_amount = tariffCalculator.GetPrice(exCostRow.tariff_id, new CalcVariables(exCostRow));
                                    break;
                                case CalculateTypeKbn.Adding:
                                    exCostRow.extra_charge_amount = exCostRow.adding_price;
                                    break;
                                case CalculateTypeKbn.AddingRatio:
                                    // TODO: normal spec 業者別調整率適用前の金額を適用するでよいか。
                                    exCostRow.extra_charge_amount = Decimal.Floor(exCostRow.base_charge_amount * exCostRow.adding_ratio / 100);
                                    break;
                                default:
                                    break;
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("CalculateExtraCharge Error", ex);
            }
        }

        private string GetValueColName(Tariff tariffDs, string tariffAxisKbn)
        {
            // TODO: get info from m_tariff_info
            return tariffAxisKbn == CnTariffAxisKbn.Vertical ? "yuso_means_kbn" : "distance_km"; 
        }

        private void SumWeightByKeisanUnit()
        {
            // summmary weight of detail
            var query = this.CalcWkDs.t_detail_wk
                .GroupBy(x =>
                    new
                    {
                        x.calc_ym,
                        x.keisan_key
                    })
                .Select(x => new { Keys = x.Key, SumWeight = x.Sum(y => y.item_weight_kg) });

            foreach (var group in query)
            {
                var keisanWkQuery = this.CalcWkDs.t_keisan_wk
                    .Where(r => 
                        r.calc_ym == group.Keys.calc_ym &&
                        r.keisan_key == group.Keys.keisan_key);
                if (keisanWkQuery.Count() == 1)
                {
                    var row = keisanWkQuery.First();
                    row.weight_sum_kg = group.SumWeight;
                }
                else
                {
                    // TODO エラー処理
                }
            }
        }

        /// <summary>
        /// instruct calculation range by yuso_key and get calculate_no
        /// </summary>
        /// <param name="sqlConn"></param>
        /// <param name="yusoKeyList">list of yuso_key</param>
        /// <returns>calculate_no</returns>
        public static int StartCalc(SqlConnection sqlConn, List<string> yusoKeyList)
        {
            // get new calc_no
            var calcNoAdp = new calc_noTableAdapter();
            calcNoAdp.Connection = sqlConn;
            int newCalcNo = Convert.ToInt32(calcNoAdp.InsertNewNo(DateTime.Now));

            // TODO: high akema yusoKeyListのt_yusoの計算ステータスを計算中に更新. 条件に計算ステータス≠計算中を入れる

            // TODO: high akema t_yuso_wkデータ作成

            return newCalcNo;
        }

        /// <summary>
        /// instruct calculation range by MonthlyVerifyKey and get calculate_no
        /// </summary>
        /// <param name="sqlConn"></param>
        /// <param name="monthlyVerifyKeyList">list of MonthlyVerifyKey</param>
        /// <returns>calculate_no</returns>
        public static int StartCalc(SqlConnection sqlConn, List<MonthlyVerifyKey> monthlyVerifyKeyList)
        {
            // get new calc_no
            var calcNoAdp = new calc_noTableAdapter();
            calcNoAdp.Connection = sqlConn;
            int newCalcNo = Convert.ToInt32(calcNoAdp.InsertNewNo(DateTime.Now));

            // TODO: high akema monthlyVerifyKeyListのt_yusoの計算ステータスを計算中に更新. 条件に計算ステータス≠計算中を入れる

            // TODO: high akema t_yuso_wkデータ作成

            return newCalcNo;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlConn"></param>
        /// <returns></returns>
        public static int StartCalcBatch(SqlConnection sqlConn)
        {
            try
            {
                // get new calc_no
                var calcNoAdp = new calc_noTableAdapter();
                calcNoAdp.Connection = sqlConn;
                int newCalcNo = Convert.ToInt32(calcNoAdp.InsertNewNo(DateTime.Now));

                // update calc_no and calc_status to "doing"
                // TODO: create calcstatus index on t_yuso
                var tYusoAdp = new StartCalcTableAdapters.t_yusoTableAdapter();
                tYusoAdp.Connection = sqlConn;
                var rtn = tYusoAdp.UpdateCalcStatus(CnCalcStatus.Doing, newCalcNo, DateTime.Now, "", CnCalcStatus.UnCalc);

                var dsStartCalc = new StartCalc();
                tYusoAdp.Connection = sqlConn;
                var cnt = tYusoAdp.FillByCalcNo(dsStartCalc.t_yuso, newCalcNo, CnCalcStatus.Doing);

                // TODO: クエリに変更するかどうか検討
                // copy tran to wk
                var colNamesOfYusoWk = Enumerable.Range(0, dsStartCalc.t_yuso_wk.Columns.Count)
                                            .Select(i => dsStartCalc.t_yuso_wk.Columns[i].ColumnName).ToList();
                dsStartCalc.t_yuso.ToList().ForEach(r =>
                {
                    var newRow = dsStartCalc.t_yuso_wk.NewRow();
                    colNamesOfYusoWk.ForEach(colname =>
                    {
                        if (dsStartCalc.t_yuso.Columns.Contains(colname)) newRow[colname] = r[colname];
                    });
                    dsStartCalc.t_yuso_wk.Rows.Add(newRow);
                });

                // wk table update(batch)
                var adpYusoWk = new StartCalcTableAdapters.t_yuso_wkTableAdapter();
                adpYusoWk.Connection = sqlConn;
                adpYusoWk.SetUpdateBatchSize(100);
                var updYusoWkCnt = adpYusoWk.Update(dsStartCalc);

                // TODO: バッチ仕様に反映　出荷実績の連携時には、輸送単位が"doing"になっているときは取込みをスキップする
                // TODO: 画面仕様に反映　"doing"が含まれる時、再計算指示できない。"doing"の時、実績値は登録できない

                return newCalcNo;

            }
            catch (Exception)
            {

                throw;
            }
        }

        private void RefrectToResultData()
        {
            try
            {
                // TODO: 値をトランにセット
                // TODD: TranをUpdate
                // update wk tables
                var yusoWkAdp = new CalcWkTableAdapters.t_yuso_wkTableAdapter();
                yusoWkAdp.Connection = Connection;
                yusoWkAdp.SetUpdateBatchSize(UpdateBatchSize);
                var cntYusoWk = yusoWkAdp.Update(this.CalcWkDs);
                // TODO: debug code for p1
                Console.WriteLine("t_yuso_wk COUNT = {0}", cntYusoWk);

                var keisanWkAdp = new CalcWkTableAdapters.t_keisan_wkTableAdapter();
                keisanWkAdp.Connection = Connection;
                keisanWkAdp.SetUpdateBatchSize(UpdateBatchSize);
                var cntKeisanWk = keisanWkAdp.Update(this.CalcWkDs);
                // TODO: debug code for p1
                Console.WriteLine("t_keisan_wk COUNT = {0}", cntKeisanWk);

                var detailWkAdp = new CalcWkTableAdapters.t_detail_wkTableAdapter();
                detailWkAdp.Connection = Connection;
                detailWkAdp.SetUpdateBatchSize(UpdateBatchSize);
                var cntDetailWk = detailWkAdp.Update(CalcWkDs);
                // TODO: debug code for p1
                Console.WriteLine("t_detail_wk COUNT = {0}", cntDetailWk);

                var extraCostWkAdp = new CalcWkTableAdapters.t_extra_cost_wkTableAdapter();
                extraCostWkAdp.Connection = Connection;
                extraCostWkAdp.SetUpdateBatchSize(UpdateBatchSize);
                var cntExCostWk = extraCostWkAdp.Update(CalcWkDs);
                // TODO: debug code for p1
                Console.WriteLine("t_extra_cost_wk COUNT = {0}", cntExCostWk);
            }
            catch (Exception ex)
            {
                throw new Exception("RefrectToResultData Error", ex);
            }
        }

        public void EndCalc()
        {
            try
            {
                // TODO: エラー件数をカウント

                // update calc_no end status
                var calcNoAdp = new calc_noTableAdapter();
                calcNoAdp.Connection = Connection;
                int newCalcNo = calcNoAdp.UpdateEndStatus(DateTime.Now, (short)CnEndStatus.Good, CalcNo);

                // update calc_status to "done"
                var tYusoAdp = new StartCalcTableAdapters.t_yusoTableAdapter();
                tYusoAdp.Connection = Connection;
                var rtn = tYusoAdp.UpdateCalcStatusDone(CnCalcStatus.Done, DateTime.Now, "", CnCalcStatus.Doing, CalcNo);
            }
            catch (Exception ex)
            {
                throw new Exception("EndCalc Error", ex);
            }
        }

        private void DivideResultAmount()
        {
            try
            {
                var query = this.CalcWkDs.t_detail_wk
                    .GroupBy(x => new {
                        x.calc_ym,
                        x.yuso_key
                    });

                foreach (var yusoKeyGroup in query)
                {
                    // get yusoWkRow by group key
                    var yusoWkRowQuery = this.CalcWkDs.t_yuso_wk.Where(x =>
                        x.calc_ym == yusoKeyGroup.Key.calc_ym &&
                        x.yuso_key == yusoKeyGroup.Key.yuso_key);

                    if (yusoWkRowQuery.Count() != 1)
                    {
                        // TODO: エラー処理
                        break;
                    }
                    var yusoWkRow = yusoWkRowQuery.First();
                    var sumAmounts = new Dictionary<String, Decimal>();
                    var maxAmountInfo = new Dictionary<String, Dictionary<String, Decimal>>();
                    //initialize dictionary
                    var colNamesForDevide = yusoWkColNames.Where(name => name != "total_charge_amount").ToList(); // totalは除く
                    colNamesForDevide.ForEach(name => sumAmounts.Add("distributed_" + name, 0));
                    colNamesForDevide.ForEach(name => maxAmountInfo.Add("distributed_" + name, null));

                    foreach (var detailRow in yusoKeyGroup)
                    {
                        colNamesForDevide.ForEach(usoWkColname =>
                        {
                            // TODO: urgent akema yusoWkRow.weight_sum_kg の値がNULLで処理できないデータ確認 エラーメッセ：DivideResultAmount（）でタイプ「System.DBNull」のオブジェクトをタイプ「System.Decimal」にキャストできません。
                            //if (!DBNull.Value.Equals(yusoWkRow.weight_sum_kg) && !yusoWkRow.weight_sum_kg.Equals(0))
                            //{
                            //    var detailColName = "distributed_" + usoWkColname;
                            //    var devidedlAmount = Decimal.Floor(
                            //                            (decimal)yusoWkRow[usoWkColname] * detailRow.item_weight_kg / yusoWkRow.weight_sum_kg);
                            //    detailRow[detailColName] = devidedlAmount;

                            //    // add to summary
                            //    sumAmounts[detailColName] += devidedlAmount;

                            //    // set id and value if amount is greater than before
                            //    if (maxAmountInfo[detailColName] == null || maxAmountInfo[detailColName]["Amount"] < devidedlAmount)
                            //    {
                            //        maxAmountInfo[detailColName] = new Dictionary<String, Decimal>() { { "DetailId", detailRow.detail_id }, { "Amount", devidedlAmount } };
                            //    }
                            //}
                        });
                    }

                    // TODO: urgent spec endo 基本設計書　重量按分を確認
                    // compare yuso amount to max detail sum amount. if it's defferent, add the difference to row that has max amount
                    colNamesForDevide.ForEach(usoWkColname =>
                    {
                        if (!DBNull.Value.Equals(yusoWkRow[usoWkColname]))
                        {
                            var detailColName = "distributed_" + usoWkColname;
                            var defference = Decimal.Parse(yusoWkRow[usoWkColname].ToString()) - sumAmounts[detailColName];
                            if (defference != 0)
                            {
                                var maxAmountRow = yusoKeyGroup.Where(x => x.detail_id == maxAmountInfo[detailColName]["DetailId"]).FirstOrDefault();
                                maxAmountRow[detailColName] = Decimal.Parse(maxAmountRow[detailColName].ToString()) + defference;
                            }
                        }
                    });

                    // set total amount
                    foreach (var detailRow in yusoKeyGroup)
                    {
                        detailRow.distributed_total_charge_amount = 0;  // TODO: Not Null制約入れる
                        colNamesForDevide.ForEach(name => detailRow.distributed_total_charge_amount += (decimal)detailRow["distributed_" + name]);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("DivideResultAmount", ex);
            }
        }

    }
}
