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

        public void Calcurate()
        {
            try
            {
                this.PrepareCalculate();
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
            // TODO: 基本運賃と付帯費用項目のトータル金額を算出して、セット
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
                                    case extraCostKbn.FuelCharge:
                                        // 燃油料
                                        keisanWkRow.fuel_cost_amount = exCostWkRow.extra_charge_amount;
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
            foreach (var yusoWkRow in this.CalcWkDs.t_yuso_wk)
            {
                if (yusoWkRow.contract_type == ((int)CnContractType.ByItem).ToString()) 
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
                        // TODO: errセット
                    }

                } else if (yusoWkRow.contract_type == ((int)CnContractType.ByVehicle).ToString())
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
                        // TODO: エラー処理
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
                yusoAdp.FillOriginalDataByCalcNo(this.CalcTrnDs.t_yuso, this.CalcNo, (short)CnCalcStatus.Doing);

                var keisanAdp = new CalcTrnTableAdapters.t_keisanTableAdapter();
                keisanAdp.Connection = Connection;
                keisanAdp.FillOriginalDataByCalcNo(this.CalcTrnDs.t_keisan, this.CalcNo, (short)CnCalcStatus.Doing);

                var detailAdp = new CalcTrnTableAdapters.t_detailTableAdapter();
                detailAdp.Connection = Connection;
                detailAdp.FillOrigialDataByCalcNo(this.CalcTrnDs.t_detail, this.CalcNo, (short)CnCalcStatus.Doing);

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
                var colnameOfKeisanWk = Enumerable.Range(0, CalcWkDs.t_keisan_wk.Columns.Count - 1)
                            .Select(i => CalcWkDs.t_keisan_wk.Columns[i].ColumnName).ToList();
                CalcTrnDs.t_keisan.ToList().ForEach(r =>
                {
                    var newRow = CalcWkDs.t_keisan_wk.NewRow();
                    colnameOfKeisanWk.ForEach(colname =>
                    {
                        if (CalcTrnDs.t_keisan.Columns.Contains(colname)) newRow[colname] = r[colname];
                    });
                    newRow["calc_no"] = CalcNo;
                    CalcWkDs.t_keisan_wk.Rows.Add(newRow);
                });

                //  insert into keisan_wk
                var keisanWkAdp = new CalcWkTableAdapters.t_keisan_wkTableAdapter();
                keisanWkAdp.Connection = Connection;
                keisanWkAdp.SetUpdateBatchSize(UpdateBatchSize);
                keisanWkAdp.Update(this.CalcWkDs.t_keisan_wk);

                //   copy detail_trn datatable to detail_wk
                // TODO:クエリーにするか検討
                var colnameOfDetailWk = Enumerable.Range(0, CalcWkDs.t_detail_wk.Columns.Count - 1)
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
                // set calcinfo to keisan_wk by server update query
                keisanWkAdp.UpdateCalcInfo(DateTime.Now, "", this.CalcNo);

                // set vehicle info to keisan_wk by server update query
                keisanWkAdp.UpdateVehicleInfo(this.CalcNo, ((int)CnContractType.ByVehicle).ToString());

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
            catch (Exception)
            {

                throw;
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
                        var exCostPtnDTbl = exCostPtnDAdp.GetDataByExtraCostPatternId(group.Key.extra_cost_pattern_id);

                        group.ToList().ForEach(keisanWkRow =>
                        {
                            var newExCostWkRow = CalcWkDs.t_extra_cost_wk.Newt_extra_cost_wkRow();

                            //create t_extra_cost data
                            exCostPtnDTbl.ToList().ForEach(r =>
                            {
                                // set value from ex_cost_pattern row
                                Enumerable.Range(0, exCostPtnDTbl.Columns.Count - 1)
                                    .Select(i => exCostPtnDTbl.Columns[i].ColumnName)
                                    .ToList().ForEach(colname =>
                                    {
                                        newExCostWkRow[colname] = r[colname];
                                    });
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
                            newExCostWkRow["stopping_count"] = keisanWkRow["stopping_count"];
                            newExCostWkRow["weight_sum_kg"] = keisanWkRow["weight_sum_kg"];
                            newExCostWkRow["base_charge_amount"] = keisanWkRow["base_charge_amount"];
                            newExCostWkRow["extra_charge_amount"] = 0;

                            // add row to ex_cost_wk
                            CalcWkDs.t_extra_cost_wk.Addt_extra_cost_wkRow(newExCostWkRow);
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
            var query = this.CalcWkDs.t_keisan_wk.AsEnumerable()
                .OrderBy(x => x.apply_tariff_id)
                .GroupBy(g => g.apply_tariff_id);

            var tariffCalculator = new TariffCalculator(Connection);

            foreach (var group in query)
            {
                var tariffDs = tariffCalculator.GetTariffDataset(group.Key);

                foreach (var item in group)
                {
                    // set price to keisan_wk row
                    var calcVar = new CalcVariables(item);
                    item.apply_vertical_value = tariffCalculator.GetKeisanValue(tariffDs, calcVar, CnTariffAxisKbn.Vertical);
                    item.apply_horizonatl_value = tariffCalculator.GetKeisanValue(tariffDs, calcVar, CnTariffAxisKbn.Horizontal);
                    item.base_charge_amount = tariffCalculator.GetPrice(tariffDs, calcVar);                    
                }
            }
        }

        private void CalculateExtraCharge()
        {
            var tariffCalculator = new TariffCalculator(Connection);
            foreach (var exCostRow in CalcWkDs.t_extra_cost_wk)
            {
                // if not applicable, break
                // TODO: 適用期間チェック
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
                        // 燃油料　実績値より取得
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

        private string GetValueColName(Tariff tariffDs, CnTariffAxisKbn tariffAxisKbn)
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
                var rtn = tYusoAdp.UpdateCalcStatus((short)CnCalcStatus.Doing, newCalcNo, DateTime.Now, "", (short)CnCalcStatus.UnCalc);

                var dsStartCalc = new StartCalc();
                tYusoAdp.Connection = sqlConn;
                var cnt = tYusoAdp.FillByCalcNo(dsStartCalc.t_yuso, newCalcNo, (short)CnCalcStatus.Doing);

                // TODO: クエリに変更するかどうか検討
                // copy tran to wk
                var colNamesOfYusoWk = Enumerable.Range(0, dsStartCalc.t_yuso_wk.Columns.Count - 1)
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
            // TODO: 値をトランにセット
            // TODD: TranをUpdate
            // update wk tables
            var yusoWkAdp = new CalcWkTableAdapters.t_yuso_wkTableAdapter();
            yusoWkAdp.Connection = Connection;
            yusoWkAdp.SetUpdateBatchSize(UpdateBatchSize);
            yusoWkAdp.Update(this.CalcWkDs);

            var keisanWkAdp = new CalcWkTableAdapters.t_keisan_wkTableAdapter();
            keisanWkAdp.Connection = Connection;
            keisanWkAdp.SetUpdateBatchSize(UpdateBatchSize);
            keisanWkAdp.Update(this.CalcWkDs);

            var detailWkAdp = new CalcWkTableAdapters.t_detail_wkTableAdapter();
            detailWkAdp.Connection = Connection;
            detailWkAdp.SetUpdateBatchSize(UpdateBatchSize);
            detailWkAdp.Update(CalcWkDs);

            var extraCostWkAdp = new CalcWkTableAdapters.t_extra_cost_wkTableAdapter();
            extraCostWkAdp.Connection = Connection;
            extraCostWkAdp.SetUpdateBatchSize(UpdateBatchSize);
            extraCostWkAdp.Update(CalcWkDs);

        }

        public void EndCalc()
        {
            // TODO: エラー件数をカウント

            // update calc_no end status
            var calcNoAdp = new calc_noTableAdapter();
            calcNoAdp.Connection = Connection;
            int newCalcNo = calcNoAdp.UpdateEndStatus(DateTime.Now, (short)CnEndStatus.Good, CalcNo);

            // update calc_status to "done"
            var tYusoAdp = new StartCalcTableAdapters.t_yusoTableAdapter();
            tYusoAdp.Connection = Connection;
            var rtn = tYusoAdp.UpdateCalcStatusDone((short)CnCalcStatus.Done, DateTime.Now, "", (short)CnCalcStatus.Doing, CalcNo);

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
                    yusoWkColNames.ForEach(name => sumAmounts.Add("distributed_" + name, 0));
                    yusoWkColNames.ForEach(name => maxAmountInfo.Add("distributed_" + name, null));

                    foreach (var detailRow in yusoKeyGroup)
                    {
                        yusoWkColNames.ForEach(usoWkColname =>
                        {
                            if (!DBNull.Value.Equals(yusoWkRow.weight_sum_kg) && !yusoWkRow.weight_sum_kg.Equals(0))
                            {
                                var detailColName = "distributed_" + usoWkColname;
                                var devidedlAmount = Decimal.Floor(
                                                        (decimal)yusoWkRow[usoWkColname] * detailRow.item_weight_kg / yusoWkRow.weight_sum_kg);
                                detailRow[detailColName] = devidedlAmount;

                                // add to summary
                                sumAmounts[detailColName] += devidedlAmount;

                                // set id and value if amount is greater than before
                                if (maxAmountInfo[detailColName] == null || maxAmountInfo[detailColName]["Amount"] < devidedlAmount)
                                {
                                    maxAmountInfo[detailColName] = new Dictionary<String, Decimal>() { { "DetailId", detailRow.detail_id }, { "Amount", devidedlAmount } };
                                }
                            }
                        });
                    }

                    // compare yuso amount to max detail sum amount. if it's defferent, add the difference to row that has max amount
                    yusoWkColNames.ForEach(usoWkColname =>
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
                        yusoWkColNames.ForEach(name => detailRow.distributed_total_charge_amount += (decimal)detailRow["distributed_" + name]);
                    }
                }
            }
            catch (Exception)
            {

                throw;
            }
        }

    }
}
