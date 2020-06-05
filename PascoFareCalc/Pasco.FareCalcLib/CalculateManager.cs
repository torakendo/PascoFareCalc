using Pasco.FareCalcLib.Datasets;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Text;
using StartCalcTableAdapters = Pasco.FareCalcLib.Datasets.StartCalcTableAdapters;
using CalcTrnTableAdapters = Pasco.FareCalcLib.Datasets.CalcTrnTableAdapters;
using CalcWkTableAdapters = Pasco.FareCalcLib.Datasets.CalcWkTableAdapters;
using GyosyaAdjustmentAdapters = Pasco.FareCalcLib.Datasets.GyosyaAdjustmentTableAdapters;
using Pasco.FareCalcLib.Datasets.CalcNoTableAdapters;
using Pasco.FareCalcLib.Datasets.TariffTableAdapters;

using static Pasco.FareCalcLib.Constants;
using System.Data;
using System.Linq;
using System.Security.Cryptography;
using System.Collections;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices.ComTypes;
using Pasco.FareCalcLib.Datasets.ExtraCostPatternTableAdapters;

using Pasco.FareCalcLib.Datasets.CalcWkTableAdapters;

namespace Pasco.FareCalcLib


{
    public class CalculateManager
    {
        // TODO: done akema 最新のテーブル定義に合わせる
        // TODO: done akema 最新5/22のテーブル定義の変更分に対応

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
                        "actual_distance_surcharge_amount",
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
        /// 計算を実行する execute calculation 
        /// このメソッドを呼び出す前に、CalcNoとConnectionが設定されていることを確認してください before calling this method make sure CalcNo and Connection to be set
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

        /// <summary>
        /// 再計算を実行する execute re-calculation 
        /// このメソッドを呼び出す前に、CalcNoとConnectionが設定されていることを確認してください before calling this method make sure CalcNo and Connection to be set
        /// </summary>
        public void ReCalcurate()
        {
            // TODO: high akema 実装　VN側と同じ環境で動くことを確認してからMergeする
        }

        private void SetResultToKeisanWk()
        {
            // TODO: normal akema 時間割増料、期間割増料、地区割増、
            // TODO: spec endo 時間指定割増料、特別作業割増、特殊車両割増
            // 基本運賃と付帯費用項目のトータル金額を算出して、セット
            CalcWkDs.t_keisan_wk.ToList().ForEach(keisanWkRow =>
            {
                decimal totalExtraCost = 0;
                // extra_cost値をkeisan_wkに設定します　set extra_cost value to keisan_wk
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
                                        // TODO: function akema 中継料
                                        keisanWkRow.stopping_charge_amount += exCostWkRow.extra_charge_amount;
                                        break;
                                    case extraCostKbn.CargoCharge:
                                        // TODO: function akema 航送料
                                        keisanWkRow.cargo_charge_amount += exCostWkRow.extra_charge_amount;
                                        break;
                                    case extraCostKbn.DistanceCharge:
                                        // TODO: function akema 距離割増料
                                        keisanWkRow.actual_km_surcharge_amount += exCostWkRow.extra_charge_amount;
                                        break;
                                    case extraCostKbn.HelperCharge:
                                        // TODO: function akema 助手料
                                        keisanWkRow.actual_assist_surcharge_amount += exCostWkRow.extra_charge_amount;
                                        break;
                                    case extraCostKbn.WashCharge:
                                        // TODO: function akema 洗浄料
                                        keisanWkRow.actual_wash_surcharge_amount = exCostWkRow.extra_charge_amount;
                                        break;
                                    case extraCostKbn.StandCharge:
                                        // TODO: function akema 台貫料
                                        keisanWkRow.actual_stand_surcharge_amount = exCostWkRow.extra_charge_amount;
                                        break;
                                    case extraCostKbn.TollRoadCharge:
                                        // TODO: function akema 有料道路代
                                        keisanWkRow.actual_load_surcharge_amount = exCostWkRow.extra_charge_amount;
                                        break;
                                    case extraCostKbn.FuelCharge:
                                    // TODO: function akema 燃油料
                                    case extraCostKbn.OtherTariff:
                                    // TODO: function akema その他料金
                                    // その他（タリフ）
                                    case extraCostKbn.OtherRateMultiplication:
                                    // TODO: function akema その他料金
                                    // その他（率乗算）
                                    case extraCostKbn.OtherSimpleAddition:
                                    // TODO: function akema その他料金
                                    // その他（単純加算）
                                    case extraCostKbn.TimeCharge:
                                    // TODO: function akema 時間割増料
                                    case extraCostKbn.SeasonalCharge:
                                    // TODO: function akema 期間割増料
                                    case extraCostKbn.AreaCharge:
                                    // TODO: function akema 地区割増料
                                    case extraCostKbn.TimeDesignationCharge:
                                    // TODO: function akema 時間指定割増料
                                    case extraCostKbn.SpecialWorkCharge:
                                    // TODO: function akema 特別作業割増料
                                    case extraCostKbn.SpecialVehicleCharge:
                                        // TODO: function akema 特殊車両割増料
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
                    // keisan wkの値を、ByItem（ko-date）のときにuso_wkの同じデータ行に設定する set keisan wk values to same key datarow of uso_wk when ByItem(ko-date)
                    // 同じキーでkeisan_wk行を取得する get keisan_wk row at the same key
                    var keisanWkQuery =
                        this.CalcWkDs.t_keisan_wk
                            .Where(r => r.calc_no == CalcNo && r.yuso_key == yusoWkRow.yuso_key);
                    if (keisanWkQuery.Count() == 1)
                    {
                        // 値をyusoWkRowに設定します set value to yusoWkRow
                        var keisanWkRow = keisanWkQuery.First();
                        yusoWkRow.weight_sum_kg = keisanWkRow.weight_sum_kg;
                        yusoWkColNames.ForEach(colname => yusoWkRow[colname] = keisanWkRow[colname]);
                    }
                    else
                    {
                        // TODO: 想定外データ不整合err
                    }

                }
                else if (yusoWkRow.contract_type == CnContractType.ByVehicle)
                {
                    // ByVehicle（sha-date）のyusoキーでkeisan_wkの最大total_charge_amaunt行を取得 get max total_charge_amaunt row of keisan_wk by yuso key of ByVehicle(sha-date)
                    var keisanWkRowQuery = this.CalcWkDs.t_keisan_wk
                            .Where(r =>
                                r.calc_no == this.CalcNo &&
                                r.yuso_key == yusoWkRow.yuso_key)
                            .OrderByDescending(r => r.total_charge_amount);


                    if (keisanWkRowQuery.Count() > 0)
                    {
                        var keisanWkRow = keisanWkRowQuery.First();
                        // 値をyusoWkRowに設定します　set value to yusoWkRow
                        yusoWkRow.weight_sum_kg = keisanWkRow.weight_sum_kg;
                        yusoWkColNames.ForEach(colname => yusoWkRow[colname] = keisanWkRow[colname]);

                        // keisanWkRowで「on」をmax_flgに設定　set "on" to max_flg in keisanWkRow
                        keisanWkRow.max_flg = 1;

                    }
                    else
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
                // calcNoでtrnデータテーブルを埋める fill trn datatables by calcNo
                var yusoAdp = new CalcTrnTableAdapters.t_yusoTableAdapter();
                yusoAdp.Connection = Connection;
                var yusoRowCnt = yusoAdp.FillOriginalDataByCalcNo(this.CalcTrnDs.t_yuso, this.CalcNo, CnCalcStatus.Doing);
                // TODO: debug code for p1
                Console.WriteLine("retrieve from t_yuso COUNT = {0}", yusoRowCnt);

                var keisanAdp = new CalcTrnTableAdapters.t_keisanTableAdapter();
                keisanAdp.Connection = Connection;
                keisanAdp.FillOriginalDataByCalcNo(this.CalcTrnDs.t_keisan, this.CalcNo, CnCalcStatus.Doing);

                var detailAdp = new CalcTrnTableAdapters.t_detailTableAdapter();
                detailAdp.Connection = Connection;
                detailAdp.FillOrigialDataByCalcNo(this.CalcTrnDs.t_detail, this.CalcNo, CnCalcStatus.Doing);

                var origDestAdp = new CalcWkTableAdapters.m_orig_dest_calcinfoTableAdapter();
                origDestAdp.Connection = Connection;

                // yuso_wkを埋める fill yuso_wk
                var yusoWkAdp = new CalcWkTableAdapters.t_yuso_wkTableAdapter();
                yusoWkAdp.Connection = Connection;
                yusoWkAdp.FillByCalcNo(this.CalcWkDs.t_yuso_wk, this.CalcNo);

                // keisan_wkとdetail_wkにデータを挿入する insert data to keisan_wk and detail_wk
                // keisan_trnデータテーブルをkeisan_wkにコピー copy keisan_trn datatable to keisan_wk
                // TODO:クエリーにするか検討
                var colnameOfKeisanWk = Enumerable.Range(0, CalcWkDs.t_keisan_wk.Columns.Count)
                            .Select(i => CalcWkDs.t_keisan_wk.Columns[i].ColumnName).ToList();
                foreach (var r in CalcTrnDs.t_keisan)
                {
                    var newRow = CalcWkDs.t_keisan_wk.Newt_keisan_wkRow();
                    colnameOfKeisanWk.ForEach(colname =>
                    {
                        if (CalcTrnDs.t_keisan.Columns.Contains(colname))
                        {
                            newRow[colname] = r[colname];
                        }

                    });



                    // m_orig_dest_calcinfoからcalcinfoを取得し、keisanWkRowを設定します get calcinfo from m_orig_dest_calcinfo and set keisanWkRow
                    var calcinfoDt = new CalcWk.m_orig_dest_calcinfoDataTable();
                    var yusoModeKbn = r.contract_type == CnContractType.ByItem ? "" : r.yuso_mode_kbn;
                    if (r.yuso_kbn == CnYusoKbn.Delivery)
                    {
                        calcinfoDt = origDestAdp.GetDataByDeliveryKey(
                            r.orig_warehouse_block_cd,
                            r.dest_jis,
                            r.contract_type,
                            r.yuso_kbn,
                            r.carrier_company_cd,
                            yusoModeKbn);
                    }
                    else
                    {
                        calcinfoDt = origDestAdp.GetDataByMoveKey(
                            r.orig_warehouse_cd,
                            r.dest_warehouse_cd,
                            r.contract_type,
                            r.yuso_kbn,
                            r.carrier_company_cd,
                            yusoModeKbn);
                    }

                    if (calcinfoDt.Count == 0)
                    {
                        // set err if no match data in calcinfo
                        newRow.calc_err_flg = 1;
                    }
                    else if (calcinfoDt.Count >= 1)
                    {
                        var q = calcinfoDt.Where(calcinfo => calcinfo.carrier_company_cd == r.carrier_company_cd);
                        if (q.Count() == 0)
                        {
                            q = calcinfoDt.Where(calcinfo => calcinfo.carrier_company_cd == "");
                        }
                        var calcinfoRow = q.First();
                        newRow["fare_tariff_id"] = calcinfoRow["fare_tariff_id"];
                        newRow["special_tariff_id"] = calcinfoRow["special_tariff_id"];
                        newRow["apply_tariff_id"] =
                            !calcinfoRow.Isspecial_tariff_idNull()
                            && Int16.Parse(calcinfoRow.special_tariff_start_md) <= Int16.Parse(r.orig_date.Substring(2, 4))
                            && Int16.Parse(calcinfoRow.special_tariff_end_md) >= Int16.Parse(r.orig_date.Substring(2, 4))
                            ? calcinfoRow.special_tariff_id
                            : calcinfoRow.fare_tariff_id;
                        newRow["extra_cost_pattern_id"] = calcinfoRow["extra_cost_pattern_id"];
                        newRow["distance_km"] = calcinfoRow["distance_km"];
                        newRow["time_mins"] = calcinfoRow["time_mins"];
                        newRow["fuel_cost_amount"] = calcinfoRow["fuel_cost_amount"];
                        newRow["stopping_count"] = calcinfoRow["stopping_count"];
                        newRow["special_tariff_start_md"] = calcinfoRow["special_tariff_start_md"];
                        newRow["special_tariff_end_md"] = calcinfoRow["special_tariff_end_md"];
                        newRow["calcinfo_carrier_company_cd"] = calcinfoRow["carrier_company_cd"];
                    }

                    newRow["calc_no"] = CalcNo;
                    newRow["max_flg"] = 0;
                    newRow["original_base_charge_amount"] = 0;
                    newRow["stopping_charge_amount"] = 0;
                    newRow["cargo_charge_amount"] = 0;
                    newRow["other_charge_amount"] = 0;
                    newRow["actual_km_surcharge_amount"] = 0;
                    newRow["actual_time_surcharge_amount"] = 0;
                    newRow["actual_assist_surcharge_amount"] = 0;
                    newRow["actual_load_surcharge_amount"] = 0;
                    newRow["actual_stand_surcharge_amount"] = 0;
                    newRow["actual_wash_surcharge_amount"] = 0;
                    newRow["total_charge_amount"] = 0;
                    newRow["back_flg"] = 0;
                    newRow["calc_err_flg"] = 0;
                    CalcWkDs.t_keisan_wk.Rows.Add(newRow);
                }

                // keisan_wkに挿入 insert into keisan_wk
                var keisanWkAdp = new CalcWkTableAdapters.t_keisan_wkTableAdapter();
                keisanWkAdp.Connection = Connection;
                keisanWkAdp.SetUpdateBatchSize(UpdateBatchSize);
                keisanWkAdp.Update(this.CalcWkDs.t_keisan_wk);

                // detail_trnデータテーブルをdetail_wkにコピーします  copy detail_trn datatable to detail_wk
                // TODO:クエリーにするか検討
                var colnameOfDetailWk = Enumerable.Range(0, CalcWkDs.t_detail_wk.Columns.Count)
                            .Select(i => CalcWkDs.t_detail_wk.Columns[i].ColumnName).ToList();
                CalcTrnDs.t_detail.ToList().ForEach(r =>
                {
                    var newRow = CalcWkDs.t_detail_wk.NewRow();
                    colnameOfDetailWk.ForEach(colname =>
                    {
                        if (CalcTrnDs.t_detail.Columns.Contains(colname))
                        {
                            // TODO: done akema 値が入るデータを用意
                            if (colname == "distributed_base_charge_amoun"
                            || colname == "distributed_special_charge_amount"
                            || colname == "distributed_special_charge_amount"
                            || colname == "distributed_stopping_charge_amount"
                            || colname == "distributed_cargo_charge_amount"
                            || colname == "distributed_other_charge_amount"
                            || colname == "distributed_actual_km_surcharge_amount"
                            || colname == "distributed_actual_time_surcharge_amount"
                            || colname == "distributed_actual_assist_surcharge_amount"
                            || colname == "distributed_actual_load_surcharge_amount"
                            || colname == "distributed_actual_stand_surcharge_amount"
                            || colname == "distributed_actual_wash_surcharge_amount"
                            || colname == "distributed_total_charge_amount"
                            || colname == "distributed_actual_adjust_surcharge_amount"
                            || colname == "distributed_total_amount"
                            )
                            {
                                newRow[colname] = 0;
                            }
                            else
                            {
                                newRow[colname] = r[colname];
                            }
                        }
                    });
                    newRow["calc_no"] = CalcNo;
                    CalcWkDs.t_detail_wk.Rows.Add(newRow);
                });

                // detail_wkに挿入insert into detail_wk
                var detailWkAdp = new CalcWkTableAdapters.t_detail_wkTableAdapter();
                detailWkAdp.Connection = Connection;
                detailWkAdp.SetUpdateBatchSize(UpdateBatchSize);
                detailWkAdp.Update(this.CalcWkDs.t_detail_wk);


                // サーバークエリによって情報をkeisan_wkに設定 set info to keisan_wk by server query
                // TODO: cancel 業者コードブランクの発着別に対応する。場合によっては1行ずつ適用   
                // TODO: done endo keisanWk の行ごとに、発着別のデータを取得し、業者コードが一致するデータあれば、それを使う。なければブランクのデータを使う
                // TODO: done akema 適用開始終了　出庫日で判定             

                // サーバー更新クエリによってcalcinfoをkeisan_wkに設定　set calcinfo to keisan_wk by server update query

                // TODO: done akema 以下が実行できなくなっているので確認する→クエリをFORMAT(orig_date, 'MMdd')→orig_dateに修正し解消
                //var updCalcinfoCnt = keisanWkAdp.UpdateCalcInfo(DateTime.Now, "", this.CalcNo);

                // 更新後にkeisan_wkを埋める fill keisan_wk after update
                keisanWkAdp.FillByCalcNo(CalcWkDs.t_keisan_wk, this.CalcNo);


                // サーバーで重みの合計をkeisan_wkに設定します set Sum of Weight to keisan_wk on server 
                SumWeightByKeisanUnit();

                // サーバーに追加料金データを作成する create extra charge data on server 
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

                            // extra_cost_detail行ごとにt_extra_costデータを作成する create t_extra_cost data for each extra_cost_detail row
                            foreach (var r in exCostPtnDetailTbl)
                            {
                                var newExCostWkRow = CalcWkDs.t_extra_cost_wk.Newt_extra_cost_wkRow();

                                // ex_cost_pattern行から値を設定します set value from ex_cost_pattern row
                                string[] exCostCols = {
                                    "extra_cost_pattern_id","extra_cost_detail_id","extra_cost_kind_kbn",
                                    "calculate_type_kbn","tariff_id","adding_price","adding_ratio",
                                    "applicable_start_md","applicable_end_md","extra_cost_pdfcol_kbn"};
                                exCostCols.ToList().ForEach(colname =>
                                {
                                    newExCostWkRow[colname] = r[colname];
                                });

                                // keisan_wkから値を設定 set value from keisan_wk
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

                                newExCostWkRow["keisan_id"] = keisanWkRow["keisan_id"];
                                newExCostWkRow["extra_seq"] = 0;
                                newExCostWkRow["extra_cost_amount"] = 0;
                                newExCostWkRow["extra_cost_other_kbn"] = "";
                                newExCostWkRow["yuso_means_kbn"] = "";
                                newExCostWkRow["extra_cost_detail_id"] = 0;

                                // add row to ex_cost_wk add row to ex_cost_wk
                                CalcWkDs.t_extra_cost_wk.Addt_extra_cost_wkRow(newExCostWkRow);
                            }
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
                    .Where(x => x.calc_err_flg == 0)
                    .OrderBy(x => x.apply_tariff_id)
                    .GroupBy(g => g.apply_tariff_id);

                var tariffCalculator = new TariffCalculator(Connection);

                var gyosyaAdp = new GyosyaAdjustmentAdapters.m_gyosya_adjustmentTableAdapter();
                gyosyaAdp.Connection = Connection;

                // TODO: done akema GetTariffDataset()が取得できないので確認 → CnTariffAxisKbnの区分変更によるエラー修正
                // TODO: function akema 特別タリフ
                foreach (var group in query)
                {
                    var tariffDs = tariffCalculator.GetTariffDataset(group.Key);
                    if (tariffDs.m_tariff_info.Count == 0 || tariffDs.m_tariff_detail.Count == 0)
                    {
                        foreach (var item in group)
                        {
                            item.calc_err_flg = 1;
                        }
                    }
                    else
                    {
                        foreach (var item in group)
                        {
                            // 価格をkeisan_wk行に設定 set price to keisan_wk row
                            var calcVar = new CalcVariables(item);
                            item.apply_vertical_value = tariffCalculator.GetKeisanValue(tariffDs, calcVar, CnTariffAxisKbn.Vertical);
                            item.apply_horizonatl_value = tariffCalculator.GetKeisanValue(tariffDs, calcVar, CnTariffAxisKbn.Horizontal);
                            // TODO: function akema タリフの末端繰返し・増分単価
                            item.original_base_charge_amount = tariffCalculator.GetPrice(tariffDs, calcVar);

                            // TODO: function akema 業者別調整率
                            // TODO: high akema 業者別調整率　適用した発着別の業者コードに指定がない時に適用　仕様4-2-2
                            // 計算情報．業者コードに業者の指定がない場合、業者別運賃調整率マスタより調整率取得
                            var gyoshaDt = new GyosyaAdjustment.m_gyosya_adjustmentDataTable();
                            gyoshaDt = gyosyaAdp.GetDataByPk(item.carrier_company_cd, item.yuso_kbn, item.orig_warehouse_block_cd);
                            if (String.IsNullOrEmpty(item.carrier_company_cd))
                            {
                                item.base_charge_amount = Decimal.Floor(item.base_charge_amount * gyoshaDt.First().adjustment_rate / 100);
                            }
                            else if (item.yuso_kbn == CnYusoKbn.BringBack)
                            {
                                // TODO: high akema 持ち戻り率適用　仕様4-2-3　base_charge_amount　にセット
                                // TODO: function akema 個建(配送・持ち戻り)
                                // 計算単位．モード区分＝路線 の場合「持戻率（路線）」、それ以外の場合「持戻率（基本）」
                                if (item.yuso_mode_kbn == YusoModeKbn.RegularFlights)
                                {
                                    item.base_charge_amount = Decimal.Floor(item.base_charge_amount * gyoshaDt.First().bring_back_rate_route / 100);
                                }
                                else
                                    item.base_charge_amount = Decimal.Floor(item.base_charge_amount * gyoshaDt.First().bring_back_rate_base / 100);
                                {
                                }
                            }
                            else
                            {
                                item.base_charge_amount = item.original_base_charge_amount;
                            }


                        }
                    }
                }
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
                    // 該当しない場合は、break　 if not applicable, break
                    // TODO: high akema 適用期間チェック
                    // TODO: normal akema 時間割増料、期間割増料、地区割増、
                    // TODO: spec endo 時間指定割増料、特別作業割増、特殊車両割増
                    // TODO: high base_charge 参照部分を　original_base_charge_amountに変更
                    // TODO: function akema 発着別運賃計算情報の適用期間
                    // TODO: function akema 付帯費用の適用期間
                    // TODO: function akema 実績調整額
                    var yusoWkquery = CalcWkDs.t_yuso_wk.Where(r => r.yuso_key == exCostRow.yuso_key);
                    var gyosyaAdp = new GyosyaAdjustmentAdapters.m_gyosya_adjustmentTableAdapter();
                    gyosyaAdp.Connection = Connection;
                    var gyoshaDt = new GyosyaAdjustment.m_gyosya_adjustmentDataTable();
                    gyoshaDt = gyosyaAdp.GetDataByPk(yusoWkquery.First().carrier_company_cd, yusoWkquery.First().yuso_kbn, yusoWkquery.First().orig_warehouse_block_cd);

                    switch (exCostRow.calculate_type_kbn)
                    {
                        case extraCostKbn.StoppingCharge:
                            // 中継料　タリフ．単価 × 発着別運賃計算情報．中継回数
                            // TODO: 中継回数をNull check
                            var tariffPrice = tariffCalculator.GetPrice(exCostRow.tariff_id, new CalcVariables(exCostRow));
                            exCostRow.extra_charge_amount = tariffPrice * exCostRow.stopping_count;
                            break;
                        case extraCostKbn.CargoCharge:
                            // 航送料　タリフ．金額
                            exCostRow.extra_charge_amount = tariffCalculator.GetPrice(exCostRow.tariff_id, new CalcVariables(exCostRow));
                            break;
                        case extraCostKbn.DistanceCharge:
                            // 距離割増　タリフ．金額 × 超過距離
                            if (yusoWkquery.Count() == 1 && !(yusoWkquery.First().actual_distance_km == 0))
                            {
                                var actualDistanceKm = yusoWkquery.First().actual_distance_km;
                                var calcVariables = new CalcVariables(exCostRow);
                                calcVariables.DistanceKm = (actualDistanceKm - exCostRow.distance_km) > 0 ? (actualDistanceKm - exCostRow.distance_km) : 0;
                                exCostRow.extra_charge_amount = tariffCalculator.GetPrice(exCostRow.tariff_id, calcVariables);
                            }
                            break;
                        case extraCostKbn.HelperCharge:
                            // 助手料　助手料 × 助手人数 助手料(=付帯費用パターン明細金額)＊人数
                            if (yusoWkquery.Count() == 1 && !(yusoWkquery.First().actual_assistant_count == 0))
                            {
                                exCostRow.extra_charge_amount = exCostRow.adding_price * yusoWkquery.First().actual_assistant_count;
                            }
                            break;
                        case extraCostKbn.FuelCharge:
                            // 燃油料　発着別運賃計算情報．燃油料
                            exCostRow.extra_charge_amount = exCostRow.Isfuel_cost_amountNull() ? 0 : exCostRow.fuel_cost_amount;
                            break;
                        case extraCostKbn.WashCharge:
                            // 洗浄料　輸送単位ワーク.洗浄料
                            if (yusoWkquery.Count() == 1)
                            {
                                exCostRow.extra_charge_amount = yusoWkquery.First().actual_wash_surcharge_amount;
                            }
                            break;
                        case extraCostKbn.StandCharge:
                            // 台貫料　輸送単位ワーク.台貫料
                            if (yusoWkquery.Count() == 1)
                            {
                                exCostRow.extra_charge_amount = yusoWkquery.First().actual_stand_surcharge_amount;
                            }
                            break;
                        case extraCostKbn.TollRoadCharge:
                            // 有料道路代　輸送単位ワーク.有料道路代 
                            if (yusoWkquery.Count() == 1)
                            {
                                exCostRow.extra_charge_amount = yusoWkquery.First().actual_load_surcharge_amount;
                            }
                            break;
                        case extraCostKbn.OtherTariff:
                        case extraCostKbn.OtherRateMultiplication:
                        case extraCostKbn.OtherSimpleAddition:
                            switch (exCostRow.calculate_type_kbn)
                            {
                                case CalculateTypeKbn.Triff:
                                    // その他（タリフ）タリフ．金額
                                    exCostRow.extra_charge_amount = tariffCalculator.GetPrice(exCostRow.tariff_id, new CalcVariables(exCostRow));
                                    break;
                                case CalculateTypeKbn.Adding:
                                    // その他（率乗算）付帯費用設定．加算額
                                    exCostRow.extra_charge_amount = exCostRow.adding_price;
                                    break;
                                case CalculateTypeKbn.AddingRatio:
                                    // その他（単純加算）基本運賃 × 付帯費用設定．掛率　÷ 100
                                    // TODO: normal spec 業者別調整率適用前の金額を適用するでよいか。
                                    exCostRow.extra_charge_amount = Decimal.Floor(exCostRow.base_charge_amount * gyoshaDt.First().adjustment_rate / 100);
                                    break;
                                default:
                                    break;
                            }
                            break;
                        case extraCostKbn.TimeCharge:
                            // 時間割増料 タリフ．金額 × 超過時間
                            if (yusoWkquery.Count() == 1 && !(yusoWkquery.First().actual_time_mins == 0))
                            {
                                var actualTimeMins = yusoWkquery.First().actual_time_mins;
                                var calcVariables = new CalcVariables(exCostRow);
                                calcVariables.DistanceKm = (actualTimeMins - exCostRow.time_mins) > 0 ? (actualTimeMins - exCostRow.time_mins) : 0;
                                exCostRow.extra_charge_amount = tariffCalculator.GetPrice(exCostRow.tariff_id, calcVariables);
                            }
                            break;
                        case extraCostKbn.SeasonalCharge:
                            // 期間割増料 タリフ．金額
                            if (Int16.Parse(exCostRow.applicable_start_md) <= Int16.Parse(exCostRow.orig_date.Substring(2, 4))
                             && Int16.Parse(exCostRow.applicable_end_md) >= Int16.Parse(exCostRow.orig_date.Substring(2, 4)))
                            {
                                exCostRow.extra_charge_amount = tariffCalculator.GetPrice(exCostRow.tariff_id, new CalcVariables(exCostRow));
                            }
                            break;
                        case extraCostKbn.AreaCharge:
                            // 地区割増料 タリフ．金額	
                            if (yusoWkquery.Count() == 1)
                            {
                                exCostRow.extra_charge_amount = tariffCalculator.GetPrice(exCostRow.tariff_id, new CalcVariables(exCostRow));
                            }
                            break;
                        case extraCostKbn.TimeDesignationCharge:
                            // 時間指定割増料 基本運賃 × 業者別調整率．時間指定割増率 ÷ 100
                            if (yusoWkquery.Count() == 1)
                            {
                                exCostRow.extra_charge_amount = Decimal.Floor(exCostRow.base_charge_amount * gyoshaDt.First().time_adjustment_rate / 100);
                            }
                            break;
                        case extraCostKbn.SpecialWorkCharge:
                            // 特別作業割増料 基本運賃 × 業者別調整率．特別作業割増率 ÷ 100
                            if (yusoWkquery.Count() == 1)
                            {
                                exCostRow.extra_charge_amount = Decimal.Floor(exCostRow.base_charge_amount * gyoshaDt.First().additional_work_rate / 100);
                            }
                            break;
                        case extraCostKbn.SpecialVehicleCharge:
                            // 特殊車両割増料 付帯費用設定．加算額
                            if (yusoWkquery.Count() == 1)
                            {
                                exCostRow.extra_charge_amount = exCostRow.adding_price;
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
            // TODO: low endo 参照されていないので削除
            // TODO: get info from m_tariff_info
            return tariffAxisKbn == CnTariffAxisKbn.Vertical ? "yuso_means_kbn" : "distance_km";
        }

        private void SumWeightByKeisanUnit()
        {
            // 合計重量をt_detailにセット summmary weight of t_detail
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
        /// yuso_keyで計算範囲を指示し、calculate_noを取得 instruct calculation range by yuso_key and get calculate_no
        /// </summary>
        /// <param name="sqlConn"></param>
        /// <param name="yusoKeyList">list of yuso_key</param>
        /// <returns>calculate_no</returns>
        public static int StartCalc(SqlConnection sqlConn, List<string> yusoKeyList)
        {
            try
            {
                // 新しいcalc_noを取得 get new calc_no
                var calcNoAdp = new calc_noTableAdapter();
                calcNoAdp.Connection = sqlConn;
                //int newCalcNo = Convert.ToInt32(calcNoAdp.InsertNewNo(DateTime.Now));
                int calcNo = 0;
                int updateCount = 0;

                // TODO: high akema yusoKeyListのt_yusoの計算ステータスを計算中に更新. 条件に計算ステータス≠計算中を入れる
                var tYusoAdp = new StartCalcTableAdapters.t_yusoTableAdapter();
                var dsStartCalc = new StartCalc();
                tYusoAdp.Connection = sqlConn;
                tYusoAdp.Fill(dsStartCalc.t_yuso);

                var adpYusoWk = new StartCalcTableAdapters.t_yuso_wkTableAdapter();
                adpYusoWk.Connection = sqlConn;
                adpYusoWk.SetUpdateBatchSize(100);

                foreach (StartCalc.t_yusoRow yusoRow in dsStartCalc.t_yuso)
                {
                    foreach (string yuso_key in yusoKeyList)
                    {
                        if (yusoRow.yuso_key == yuso_key)
                        {
                            calcNo = yusoRow.calc_no;

                            updateCount = tYusoAdp.UpdateCalcStatusById(CnCalcStatus.Doing, yusoRow.calc_no, DateTime.Now, "", yusoRow.yuso_id);

                            adpYusoWk.FillByPK(dsStartCalc.t_yuso_wk, yusoRow.calc_no, yusoRow.yuso_id);
                            // TODO: high akema t_yuso_wkデータ作成
                            var colNamesOfYusoWk = dsStartCalc.t_yuso.Where(r => (r.yuso_id == yusoRow.yuso_id) && (r.calc_no == calcNo))
                                                        .Select(tdr => tdr).ToList();
                            dsStartCalc.t_yuso_wk.ToList().ForEach(r =>
                            {
                                r.calc_ym = yusoRow.calc_ym;
                                r.contract_type = yusoRow.contract_type;
                                r.yuso_kbn = yusoRow.yuso_kbn;
                                r.orig_warehouse_block_cd = yusoRow.orig_warehouse_block_cd;
                                r.orig_warehouse_cd = yusoRow.orig_warehouse_cd;
                                r.terminal_id = yusoRow.terminal_id;
                                r.vehicle_id = yusoRow.vehicle_id;
                                r.dest_jis = yusoRow.dest_jis;
                                r.dest_warehouse_cd = yusoRow.dest_warehouse_cd;
                                r.yuso_mode_kbn = yusoRow.yuso_mode_kbn;
                                r.carrier_company_cd = yusoRow.carrier_company_cd;
                                r.orig_date = yusoRow.orig_date;
                                r.arriving_date = yusoRow.arriving_date;
                                r.dest_cd = yusoRow.dest_cd;
                                r.base_charge_amount = yusoRow.base_charge_amount;
                                r.special_charge_amount = yusoRow.special_charge_amount;
                                r.stopping_charge_amount = yusoRow.stopping_charge_amount;
                                r.cargo_charge_amount = yusoRow.cargo_charge_amount;
                                r.other_charge_amount = yusoRow.other_charge_amount;
                                r.actual_distance_km = yusoRow.actual_distance_km;
                                r.actual_distance_surcharge_amount = yusoRow.actual_distance_surcharge_amount;
                                r.actual_time_mins = yusoRow.actual_time_mins;
                                r.actual_time_surcharge_amount = yusoRow.actual_time_surcharge_amount;
                                r.actual_assistant_count = yusoRow.actual_assistant_count;
                                r.actual_assist_surcharge_amount = yusoRow.actual_assist_surcharge_amount;
                                r.actual_load_surcharge_amount = yusoRow.actual_load_surcharge_amount;
                                r.actual_stand_surcharge_amount = yusoRow.actual_stand_surcharge_amount;
                                r.actual_wash_surcharge_amount = yusoRow.actual_wash_surcharge_amount;
                                r.total_charge_amount = yusoRow.total_charge_amount;
                                r.calc_status = yusoRow.calc_status;
                                r.verify_status = yusoRow.verify_status;
                                r.yuso_key = yusoRow.yuso_key;
                                r.UpdateDay = yusoRow.UpdateDay;
                                r.UpdateUserCode = yusoRow.UpdateUserCode;
                                r.BatchUpdateDay = yusoRow.BatchUpdateDay;
                                r.last_calc_at = yusoRow.last_calc_at;
                                r.release_ymd = yusoRow.release_ymd;
                                r.verify_ymd = yusoRow.verify_ymd;
                            });
                            adpYusoWk.Update(dsStartCalc);
                        }
                    }
                }

                Console.WriteLine("{1}update (calcNo={0})", calcNo, updateCount);

                return calcNo;
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// MonthlyVerifyKeyで計算範囲を指示し、calculate_noを取得 instruct calculation range by MonthlyVerifyKey and get calculate_no
        /// </summary>
        /// <param name="sqlConn"></param>
        /// <param name="monthlyVerifyKeyList">list of MonthlyVerifyKey</param>
        /// <returns>calculate_no</returns>
        public static int StartCalc(SqlConnection sqlConn, List<MonthlyVerifyKey> monthlyVerifyKeyList)
        {
            // 新しいcalc_noを取得 get new calc_no
            var calcNoAdp = new calc_noTableAdapter();
            calcNoAdp.Connection = sqlConn;
            //int newCalcNo = Convert.ToInt32(calcNoAdp.InsertNewNo(DateTime.Now));
            int calcNo = 0;
            int updateCount = 0;

            // TODO: high akema monthlyVerifyKeyListのt_yusoの計算ステータスを計算中に更新. 条件に計算ステータス≠計算中を入れる
            // TODO: high akema t_yuso_wkデータ作成
            var tYusoAdp = new StartCalcTableAdapters.t_yusoTableAdapter();
            var dsStartCalc = new StartCalc();
            tYusoAdp.Connection = sqlConn;
            tYusoAdp.Fill(dsStartCalc.t_yuso);

            var adpYusoWk = new StartCalcTableAdapters.t_yuso_wkTableAdapter();
            adpYusoWk.Connection = sqlConn;
            adpYusoWk.SetUpdateBatchSize(100);
            foreach (StartCalc.t_yusoRow yusoRow in dsStartCalc.t_yuso)
            {
                foreach (var yuso_key in monthlyVerifyKeyList.Select((v, i) => new { Value = v, Index = i }))
                {
                    if (yusoRow.calc_ym == monthlyVerifyKeyList[yuso_key.Index].CalcYm
                     && yusoRow.calc_status == monthlyVerifyKeyList[yuso_key.Index].CalcStatus
                     && yusoRow.yuso_kbn == monthlyVerifyKeyList[yuso_key.Index].YusoKbn
                     && yusoRow.orig_warehouse_cd == monthlyVerifyKeyList[yuso_key.Index].OrigWarehouseCd
                     && yusoRow.carrier_company_cd == monthlyVerifyKeyList[yuso_key.Index].CarrierCompanyCode
                     && yusoRow.contract_type == monthlyVerifyKeyList[yuso_key.Index].ContractType
                        )
                    {
                        calcNo = yusoRow.calc_no;
                        updateCount = tYusoAdp.UpdateCalcStatusById(CnCalcStatus.Doing, yusoRow.calc_no, DateTime.Now, "", yusoRow.yuso_id);

                        adpYusoWk.FillByPK(dsStartCalc.t_yuso_wk, yusoRow.calc_no, yusoRow.yuso_id);
                        var colNamesOfYusoWk = dsStartCalc.t_yuso.Where(r => (r.yuso_id == yusoRow.yuso_id) && (r.calc_no == calcNo))
                            .Select(tdr => tdr).ToList();
                        dsStartCalc.t_yuso_wk.ToList().ForEach(r =>
                        {
                            r.calc_ym = yusoRow.calc_ym;
                            r.contract_type = yusoRow.contract_type;
                            r.yuso_kbn = yusoRow.yuso_kbn;
                            r.orig_warehouse_block_cd = yusoRow.orig_warehouse_block_cd;
                            r.orig_warehouse_cd = yusoRow.orig_warehouse_cd;
                            r.terminal_id = yusoRow.terminal_id;
                            r.vehicle_id = yusoRow.vehicle_id;
                            r.dest_jis = yusoRow.dest_jis;
                            r.dest_warehouse_cd = yusoRow.dest_warehouse_cd;
                            r.yuso_mode_kbn = yusoRow.yuso_mode_kbn;
                            r.carrier_company_cd = yusoRow.carrier_company_cd;
                            r.orig_date = yusoRow.orig_date;
                            r.arriving_date = yusoRow.arriving_date;
                            r.dest_cd = yusoRow.dest_cd;
                            r.base_charge_amount = yusoRow.base_charge_amount;
                            r.special_charge_amount = yusoRow.special_charge_amount;
                            r.stopping_charge_amount = yusoRow.stopping_charge_amount;
                            r.cargo_charge_amount = yusoRow.cargo_charge_amount;
                            r.other_charge_amount = yusoRow.other_charge_amount;
                            r.actual_distance_km = yusoRow.actual_distance_km;
                            r.actual_distance_surcharge_amount = yusoRow.actual_distance_surcharge_amount;
                            r.actual_time_mins = yusoRow.actual_time_mins;
                            r.actual_time_surcharge_amount = yusoRow.actual_time_surcharge_amount;
                            r.actual_assistant_count = yusoRow.actual_assistant_count;
                            r.actual_assist_surcharge_amount = yusoRow.actual_assist_surcharge_amount;
                            r.actual_load_surcharge_amount = yusoRow.actual_load_surcharge_amount;
                            r.actual_stand_surcharge_amount = yusoRow.actual_stand_surcharge_amount;
                            r.actual_wash_surcharge_amount = yusoRow.actual_wash_surcharge_amount;
                            r.total_charge_amount = yusoRow.total_charge_amount;
                            r.calc_status = yusoRow.calc_status;
                            r.verify_status = yusoRow.verify_status;
                            r.yuso_key = yusoRow.yuso_key;
                            r.UpdateDay = yusoRow.UpdateDay;
                            r.UpdateUserCode = yusoRow.UpdateUserCode;
                            r.BatchUpdateDay = yusoRow.BatchUpdateDay;
                            r.last_calc_at = yusoRow.last_calc_at;
                            r.release_ymd = yusoRow.release_ymd;
                            r.verify_ymd = yusoRow.verify_ymd;
                        });
                        adpYusoWk.Update(dsStartCalc);
                    }
                }
            }

            Console.WriteLine("{1}update (calcNo={0})", calcNo, updateCount);

            return calcNo;
        }

        public static int StartCalcBatch(SqlConnection sqlConn)
        {
            try
            {
                // 新しいcalc_noを取得 get new calc_no
                var calcNoAdp = new calc_noTableAdapter();
                calcNoAdp.Connection = sqlConn;
                int newCalcNo = Convert.ToInt32(calcNoAdp.InsertNewNo(DateTime.Now));

                // calc_noとcalc_statusを "doing"に更新します　update calc_no and calc_status to "doing"
                // TODO: create calcstatus index on t_yuso　t_yusoにcalcstatusインデックスを作成します
                var tYusoAdp = new StartCalcTableAdapters.t_yusoTableAdapter();
                tYusoAdp.Connection = sqlConn;
                var rtn = tYusoAdp.UpdateCalcStatus(CnCalcStatus.Doing, newCalcNo, DateTime.Now, "", CnCalcStatus.UnCalc);

                var dsStartCalc = new StartCalc();
                tYusoAdp.Connection = sqlConn;
                var cnt = tYusoAdp.FillByCalcNo(dsStartCalc.t_yuso, newCalcNo, CnCalcStatus.Doing);

                // TODO: クエリに変更するかどうか検討
                // トランをワークにコピー
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

                // wkテーブルの更新 wk table update(batch)
                var adpYusoWk = new StartCalcTableAdapters.t_yuso_wkTableAdapter();
                adpYusoWk.Connection = sqlConn;
                adpYusoWk.SetUpdateBatchSize(100);
                var updYusoWkCnt = adpYusoWk.Update(dsStartCalc);

                // TODO: done バッチ仕様に反映　出荷実績の連携時には、輸送単位が"doing"になっているときは取込みをスキップする
                // TODO: high endo 画面仕様に反映　"doing"が含まれる時、再計算指示できない。"doing"の時、実績値は登録できない

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

                // wkテーブルを更新する update wk tables
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

                // Trnテーブルを更新する update Trn tables 
                var yusoTrnAdp = new CalcTrnTableAdapters.t_yusoTableAdapter();
                var keisanTrnAdp = new CalcTrnTableAdapters.t_keisanTableAdapter();
                var detailTrnAdp = new CalcTrnTableAdapters.t_detailTableAdapter();

                yusoTrnAdp.Connection = Connection;
                keisanTrnAdp.Connection = Connection;
                detailTrnAdp.Connection = Connection;

                yusoTrnAdp.SetUpdateBatchSize(UpdateBatchSize);
                keisanTrnAdp.SetUpdateBatchSize(UpdateBatchSize);
                detailTrnAdp.SetUpdateBatchSize(UpdateBatchSize);


                foreach (CalcWk.t_yuso_wkRow yusoWkRow in CalcWkDs.t_yuso_wk)
                {
                    // t_yuso 更新
                    foreach (CalcTrn.t_yusoRow yusoRow in CalcTrnDs.t_yuso)
                    {
                        if (yusoRow.yuso_id == yusoWkRow.yuso_id)
                        {
                            yusoRow.yuso_id = DataRow<int>(yusoWkRow, "yuso_id");
                            yusoRow.calc_ym = DataRow<string>(yusoWkRow, "calc_ym");
                            yusoRow.contract_type = DataRow<string>(yusoWkRow, "contract_type");
                            yusoRow.yuso_kbn = DataRow<string>(yusoWkRow, "yuso_kbn");
                            yusoRow.orig_warehouse_block_cd = DataRow<string>(yusoWkRow, "orig_warehouse_block_cd");
                            yusoRow.orig_warehouse_cd = DataRow<string>(yusoWkRow, "orig_warehouse_cd");
                            yusoRow.terminal_id = DataRow<string>(yusoWkRow, "terminal_id");
                            yusoRow.vehicle_id = DataRow<string>(yusoWkRow, "vehicle_id");
                            yusoRow.dest_jis = DataRow<string>(yusoWkRow, "dest_jis");
                            yusoRow.dest_warehouse_cd = DataRow<string>(yusoWkRow, "dest_warehouse_cd");
                            yusoRow.yuso_mode_kbn = DataRow<string>(yusoWkRow, "yuso_mode_kbn");
                            yusoRow.carrier_company_cd = DataRow<string>(yusoWkRow, "carrier_company_cd");
                            yusoRow.orig_date = DataRow<string>(yusoWkRow, "orig_date");
                            yusoRow.arriving_date = DataRow<string>(yusoWkRow, "arriving_date");
                            yusoRow.dest_cd = DataRow<string>(yusoWkRow, "dest_cd");
                            yusoRow.distance_km = DataRow<int>(yusoWkRow, "distance_km");
                            yusoRow.time_mins = DataRow<int>(yusoWkRow, "time_mins");
                            yusoRow.fuel_cost_amount = DataRow<decimal>(yusoWkRow, "fuel_cost_amount");
                            yusoRow.stopping_count = DataRow<short>(yusoWkRow, "stopping_count");
                            yusoRow.weight_sum_kg = DataRow<decimal>(yusoWkRow, "weight_sum_kg");
                            yusoRow.item_quantity_sum = DataRow<decimal>(yusoWkRow, "item_quantity_sum");
                            yusoRow.base_charge_amount = DataRow<decimal>(yusoWkRow, "base_charge_amount");
                            yusoRow.special_charge_amount = DataRow<decimal>(yusoWkRow, "special_charge_amount");
                            yusoRow.stopping_charge_amount = DataRow<decimal>(yusoWkRow, "stopping_charge_amount");
                            yusoRow.cargo_charge_amount = DataRow<decimal>(yusoWkRow, "cargo_charge_amount");
                            yusoRow.other_charge_amount = DataRow<decimal>(yusoWkRow, "other_charge_amount");
                            yusoRow.actual_distance_km = DataRow<decimal>(yusoWkRow, "actual_distance_km");
                            yusoRow.actual_distance_surcharge_amount = DataRow<decimal>(yusoWkRow, "actual_distance_surcharge_amount");
                            yusoRow.actual_time_mins = DataRow<decimal>(yusoWkRow, "actual_time_mins");
                            yusoRow.actual_time_surcharge_amount = DataRow<decimal>(yusoWkRow, "actual_time_surcharge_amount");
                            yusoRow.actual_assistant_count = DataRow<int>(yusoWkRow, "actual_assistant_count");
                            yusoRow.actual_assist_surcharge_amount = DataRow<decimal>(yusoWkRow, "actual_assist_surcharge_amount");
                            yusoRow.actual_load_surcharge_amount = DataRow<decimal>(yusoWkRow, "actual_load_surcharge_amount");
                            yusoRow.actual_stand_surcharge_amount = DataRow<decimal>(yusoWkRow, "actual_stand_surcharge_amount");
                            yusoRow.actual_wash_surcharge_amount = DataRow<decimal>(yusoWkRow, "actual_wash_surcharge_amount");
                            yusoRow.total_charge_amount = DataRow<decimal>(yusoWkRow, "total_charge_amount");
                            yusoRow.actual_chosei_sum_amount = DataRow<decimal>(yusoWkRow, "actual_chosei_sum_amount");
                            yusoRow.chosei_total_charge_amount = DataRow<decimal>(yusoWkRow, "chosei_total_charge_amount");
                            yusoRow.verify_status = DataRow<string>(yusoWkRow, "verify_status");
                            yusoRow.verify_ymd = DataRow<string>(yusoWkRow, "verify_ymd");
                            yusoRow.release_ymd = DataRow<string>(yusoWkRow, "release_ymd");
                            yusoRow.yuso_means_kbn = DataRow<string>(yusoWkRow, "yuso_means_kbn");
                            yusoRow.dest_nm = DataRow<string>(yusoWkRow, "dest_nm");
                            yusoRow.calc_status = DataRow<string>(yusoWkRow, "calc_status");
                            yusoRow.calc_no = DataRow<int>(yusoWkRow, "calc_no");
                            yusoRow.last_calc_at = DataRowDateTime(yusoWkRow, "last_calc_at");
                            yusoRow.send_flg = DataRow<short>(yusoWkRow, "send_flg");
                            yusoRow.send_at = DataRowDateTime(yusoWkRow, "send_at");
                            yusoRow.back_flg = DataRow<short>(yusoWkRow, "back_flg");
                            yusoRow.calc_err_flg = DataRow<short>(yusoWkRow, "calc_err_flg");
                            yusoRow.yuso_key = DataRow<string>(yusoWkRow, "yuso_key");
                            yusoRow.BatchUpdateDay = DataRowDateTime(yusoWkRow, "BatchUpdateDay");
                            //yusoRow.CreateDay = DataRowDateTime(yusoWkRow, "CreateDay");
                            //yusoRow.CreateUserCode = DataRow<string>(yusoWkRow, "CreateUserCode");
                            yusoRow.UpdateDay = DataRowDateTime(yusoWkRow, "UpdateDay");
                            yusoRow.UpdateUserCode = DataRow<string>(yusoWkRow, "UpdateUserCode");
                        }
                    }
                }
                foreach (CalcWk.t_keisan_wkRow keisanWkRow in CalcWkDs.t_keisan_wk)
                {
                    // t_keisan 更新
                    foreach (CalcTrn.t_keisanRow keisanRow in CalcTrnDs.t_keisan)
                    {
                        if (keisanRow.keisan_id == keisanWkRow.keisan_id)
                        {
                            keisanRow.keisan_id = DataRow<int>(keisanWkRow, "keisan_id");
                            keisanRow.calc_ym = DataRow<string>(keisanWkRow, "calc_ym");
                            keisanRow.contract_type = DataRow<string>(keisanWkRow, "contract_type");
                            keisanRow.yuso_kbn = DataRow<string>(keisanWkRow, "yuso_kbn");
                            keisanRow.orig_warehouse_block_cd = DataRow<string>(keisanWkRow, "orig_warehouse_block_cd");
                            keisanRow.orig_warehouse_cd = DataRow<string>(keisanWkRow, "orig_warehouse_cd");
                            keisanRow.terminal_id = DataRow<string>(keisanWkRow, "terminal_id");
                            keisanRow.vehicle_id = DataRow<string>(keisanWkRow, "vehicle_id");
                            keisanRow.dest_jis = DataRow<string>(keisanWkRow, "dest_jis");
                            keisanRow.dest_warehouse_cd = DataRow<string>(keisanWkRow, "dest_warehouse_cd");
                            keisanRow.yuso_mode_kbn = DataRow<string>(keisanWkRow, "yuso_mode_kbn");
                            keisanRow.carrier_company_cd = DataRow<string>(keisanWkRow, "carrier_company_cd");
                            keisanRow.orig_date = DataRow<string>(keisanWkRow, "orig_date");
                            keisanRow.arriving_date = DataRow<string>(keisanWkRow, "arriving_date");
                            keisanRow.dest_cd = DataRow<string>(keisanWkRow, "dest_cd");
                            keisanRow.fare_tariff_id = DataRow<int>(keisanWkRow, "fare_tariff_id");
                            keisanRow.special_tariff_id = DataRow<int>(keisanWkRow, "special_tariff_id");
                            keisanRow.extra_cost_pattern_id = DataRow<int>(keisanWkRow, "extra_cost_pattern_id");
                            keisanRow.distance_km = DataRow<int>(keisanWkRow, "distance_km");
                            keisanRow.time_mins = DataRow<int>(keisanWkRow, "time_mins");
                            keisanRow.fuel_cost_amount = DataRow<int>(keisanWkRow, "fuel_cost_amount");
                            keisanRow.stopping_count = DataRow<short>(keisanWkRow, "stopping_count");
                            keisanRow.special_tariff_start_md = DataRow<string>(keisanWkRow, "special_tariff_start_md");
                            keisanRow.special_tariff_end_md = DataRow<string>(keisanWkRow, "special_tariff_end_md");
                            keisanRow.weight_sum_kg = DataRow<decimal>(keisanWkRow, "weight_sum_kg");
                            keisanRow.item_quantity_sum = DataRow<decimal>(keisanWkRow, "item_quantity_sum");
                            keisanRow.base_charge_amount = DataRow<decimal>(keisanWkRow, "base_charge_amount");
                            keisanRow.special_charge_amount = DataRow<decimal>(keisanWkRow, "special_charge_amount");
                            keisanRow.yuso_means_kbn = DataRow<string>(keisanWkRow, "yuso_means_kbn");
                            keisanRow.dest_nm = DataRow<string>(keisanWkRow, "dest_nm");
                            keisanRow.max_flg = DataRow<short>(keisanWkRow, "max_flg");
                            keisanRow.back_flg = DataRow<short>(keisanWkRow, "back_flg");
                            keisanRow.keisan_key = DataRow<string>(keisanWkRow, "keisan_key");
                            keisanRow.yuso_key = DataRow<string>(keisanWkRow, "yuso_key");
                            //keisanRow.CreateDay = DataRowDateTime(keisanWkRow, "CreateDay");
                            //keisanRow.UpdateDay = DataRowDateTime(keisanWkRow, "UpdateDay");
                            keisanRow.CreateUserCode = DataRow<string>(keisanWkRow, "CreateUserCode");
                            keisanRow.UpdateUserCode = DataRow<string>(keisanWkRow, "UpdateUserCode");
                        }

                    }
                }
                foreach (CalcWk.t_detail_wkRow detailWkRow in CalcWkDs.t_detail_wk)
                {
                    // t_detail 更新
                    foreach (CalcTrn.t_detailRow detailRow in CalcTrnDs.t_detail)
                    {
                        if (detailRow.detail_Id == detailWkRow.detail_Id)
                        {
                            detailRow.detail_Id = DataRow<int>(detailWkRow, "detail_Id");
                            detailRow.calc_ym = DataRow<string>(detailWkRow, "calc_ym");
                            detailRow.contract_type = DataRow<string>(detailWkRow, "contract_type");
                            detailRow.yuso_kbn = DataRow<string>(detailWkRow, "yuso_kbn");
                            detailRow.orig_warehouse_block_cd = DataRow<string>(detailWkRow, "orig_warehouse_block_cd");
                            detailRow.orig_warehouse_cd = DataRow<string>(detailWkRow, "orig_warehouse_cd");
                            detailRow.terminal_id = DataRow<string>(detailWkRow, "terminal_id");
                            detailRow.vehicle_id = DataRow<string>(detailWkRow, "vehicle_id");
                            detailRow.dest_jis = DataRow<string>(detailWkRow, "dest_jis");
                            detailRow.dest_warehouse_cd = DataRow<string>(detailWkRow, "dest_warehouse_cd");
                            detailRow.yuso_mode_kbn = DataRow<string>(detailWkRow, "yuso_mode_kbn");
                            detailRow.carrier_company_cd = DataRow<string>(detailWkRow, "carrier_company_cd");
                            detailRow.orig_date = DataRow<string>(detailWkRow, "orig_date");
                            detailRow.arriving_date = DataRow<string>(detailWkRow, "arriving_date");
                            detailRow.dest_cd = DataRow<string>(detailWkRow, "dest_cd");
                            detailRow.slip_no = DataRow<string>(detailWkRow, "slip_no");
                            detailRow.slip_suffix_no = DataRow<string>(detailWkRow, "slip_suffix_no");
                            detailRow.slip_detail_no = DataRow<string>(detailWkRow, "slip_detail_no");
                            detailRow.item_cd = DataRow<string>(detailWkRow, "item_cd");
                            detailRow.item_kigo = DataRow<string>(detailWkRow, "item_kigo");
                            detailRow.item_name = DataRow<string>(detailWkRow, "item_name");
                            detailRow.item_quantity = DataRow<int>(detailWkRow, "item_quantity");
                            detailRow.itme_unit = DataRow<string>(detailWkRow, "itme_unit");
                            detailRow.item_weight_kg = DataRow<decimal>(detailWkRow, "item_weight_kg");
                            detailRow.yuso_means_kbn = DataRow<string>(detailWkRow, "yuso_means_kbn");
                            detailRow.special_vehicle_kbn = DataRow<string>(detailWkRow, "special_vehicle_kbn");
                            detailRow.transport_lead_time_hours = DataRow<string>(detailWkRow, "transport_lead_time_hours");
                            detailRow.distributed_base_charge_amoun = DataRow<decimal>(detailWkRow, "distributed_base_charge_amoun");
                            detailRow.distributed_special_charge_amount = DataRow<decimal>(detailWkRow, "distributed_special_charge_amount");
                            detailRow.distributed_stopping_charge_amount = DataRow<decimal>(detailWkRow, "distributed_stopping_charge_amount");
                            detailRow.distributed_cargo_charge_amount = DataRow<decimal>(detailWkRow, "distributed_cargo_charge_amount");
                            detailRow.distributed_other_charge_amount = DataRow<decimal>(detailWkRow, "distributed_other_charge_amount");
                            detailRow.distributed_actual_km_surcharge_amount = DataRow<decimal>(detailWkRow, "distributed_actual_km_surcharge_amount");
                            detailRow.distributed_actual_time_surcharge_amount = DataRow<decimal>(detailWkRow, "distributed_actual_time_surcharge_amount");
                            detailRow.distributed_actual_assist_surcharge_amount = DataRow<decimal>(detailWkRow, "distributed_actual_assist_surcharge_amount");
                            detailRow.distributed_actual_load_surcharge_amount = DataRow<decimal>(detailWkRow, "distributed_actual_load_surcharge_amount");
                            detailRow.distributed_actual_stand_surcharge_amount = DataRow<decimal>(detailWkRow, "distributed_actual_stand_surcharge_amount");
                            detailRow.distributed_actual_wash_surcharge_amount = DataRow<decimal>(detailWkRow, "distributed_actual_wash_surcharge_amount");
                            detailRow.distributed_actual_adjust_surcharge_amount = DataRow<decimal>(detailWkRow, "distributed_actual_adjust_surcharge_amount");
                            detailRow.distributed_total_amount = DataRow<decimal>(detailWkRow, "distributed_total_amount");
                            detailRow.keisan_key = DataRow<string>(detailWkRow, "keisan_key");
                            detailRow.yuso_key = DataRow<string>(detailWkRow, "yuso_key");
                            //detailRow.CreateDay = DataRowDateTime(detailWkRow, "CreateDay");
                            //detailRow.CreateUserCode = DataRow<string>(detailWkRow, "CreateUserCode");
                            detailRow.UpdateDay = DataRowDateTime(detailWkRow, "UpdateDay");
                            detailRow.UpdateUserCode = DataRow<string>(detailWkRow, "UpdateUserCode");
                        }
                    }
                }

                var cntYusoTrn = yusoTrnAdp.Update(this.CalcTrnDs);
                var cntKeisanTrn = keisanTrnAdp.Update(this.CalcTrnDs);
                var cntDetailTrn = detailTrnAdp.Update(CalcTrnDs);

                // TODO: debug code for p1
                Console.WriteLine("t_yuso UPDATE COUNT = {0}", cntYusoTrn);
                Console.WriteLine("t_keisan UPDATE COUNT = {0}", cntKeisanTrn);
                Console.WriteLine("t_detail UPDATE COUNT = {0}", cntDetailTrn);

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

                // calc_no終了ステータスを更新 update calc_no end status
                var calcNoAdp = new calc_noTableAdapter();
                calcNoAdp.Connection = Connection;
                int newCalcNo = calcNoAdp.UpdateEndStatus(DateTime.Now, (short)CnEndStatus.Good, CalcNo);

                // calc_statusを "done"に更新します update calc_status to "done"
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
                    .GroupBy(x => new
                    {
                        x.calc_ym,
                        x.yuso_key
                    });

                foreach (var yusoKeyGroup in query)
                {
                    // グループキーでyusoWkRowを取得 get yusoWkRow by group key
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
                    // 初期化 initialize dictionary
                    var colNamesForDevide = yusoWkColNames.Where(name => name != "total_charge_amount").ToList(); // totalは除く
                    colNamesForDevide.ForEach(name => sumAmounts.Add("distributed_" + name, 0));
                    colNamesForDevide.ForEach(name => maxAmountInfo.Add("distributed_" + name, null));

                    foreach (var detailRow in yusoKeyGroup)
                    {
                        colNamesForDevide.ForEach(usoWkColname =>
                        {
                            if (!DBNull.Value.Equals(yusoWkRow.weight_sum_kg) && !yusoWkRow.weight_sum_kg.Equals(0))
                            {
                                var detailColName = "distributed_" + usoWkColname;
                                var devidedlAmount = Decimal.Floor(
                                                        (decimal)yusoWkRow[usoWkColname] * detailRow.item_weight_kg / yusoWkRow.weight_sum_kg);
                                detailRow[detailColName] = devidedlAmount;

                                // 合計に追加 add to summary
                                sumAmounts[detailColName] += devidedlAmount;

                                // 金額が以前よりも大きい場合は、IDと値を設定します set id and value if amount is greater than before
                                if (maxAmountInfo[detailColName] == null || maxAmountInfo[detailColName]["Amount"] < devidedlAmount)
                                {
                                    maxAmountInfo[detailColName] = new Dictionary<String, Decimal>() { { "DetailId", detailRow.detail_Id }, { "Amount", devidedlAmount } };
                                }
                            }
                        });
                    }

                    // TODO: urgent spec endo 基本設計書　重量按分を確認
                    // yusoの金額と最大詳細合計金額を比較します。 異なる場合は、最大量を持つ行に差を追加します compare yuso amount to max detail sum amount. if it's defferent, add the difference to row that has max amount
                    colNamesForDevide.ForEach(usoWkColname =>
                    {
                        if (!DBNull.Value.Equals(yusoWkRow[usoWkColname]))
                        {
                            var detailColName = "distributed_" + usoWkColname;
                            var defference = Decimal.Parse(yusoWkRow[usoWkColname].ToString()) - sumAmounts[detailColName];
                            if (defference != 0)
                            {
                                var maxAmountRow = yusoKeyGroup.Where(x => x.detail_Id == maxAmountInfo[detailColName]["DetailId"]).FirstOrDefault();
                                maxAmountRow[detailColName] = Decimal.Parse(maxAmountRow[detailColName].ToString()) + defference;
                            }
                        }
                    });

                    // 合計金額を設定 set total amount
                    foreach (var detailRow in yusoKeyGroup)
                    {// TODO: high akema Not Null制約入れる
                        detailRow.distributed_total_charge_amount = 0;
                        colNamesForDevide.ForEach(name => detailRow.distributed_total_charge_amount += (decimal)detailRow["distributed_" + name]);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("DivideResultAmount", ex);
            }
        }

        // 値がNULLの場合に初期値をセットする
        public static Type DataRow<Type>(DataRow dr, String columnName)
        {
            Type ret = (dr.IsNull(columnName)) ? default(Type) : dr.Field<Type>(columnName);

            return ret;
        }

        public static DateTime DataRowDateTime(DataRow dr, String columnName)
        {
            DateTime ret = (dr.IsNull(columnName)) ? DateTime.Now : dr.Field<DateTime>(columnName);

            return ret;
        }
    }

}
