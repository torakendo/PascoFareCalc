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

namespace FareCalcLib


{
    public class Calculator
    {
        #region "fields"
        private int CalcNo = 0;
        private CalcTrn CalcTrnDs;
        private CalcWk CalcWkDs;
        // TODO: バッチサイズConfigへ移動
        // TODO: バッチ更新がエラーを解決するUpdatedRowSource property value of UpdateRowSource.FirstReturnedRecord or UpdateRowSource.Both is invalid
        private static int UpdateBatchSize = 1;

        #endregion

        #region "properties"
        public SqlConnection Connection { get; private set; }
        public SqlTransaction Transaction { get; private set; }
        #endregion

        public Calculator(int calcNo)
        {
            CalcNo = calcNo;
            CalcTrnDs = new CalcTrn();
            CalcWkDs = new CalcWk();

        }

        public void Calcurate(SqlConnection conn)
        {
            try
            {
                //Transaction = sqlTrn;
                Connection = conn;
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
                // 付帯費用を計算WKにセット
                CalcWkDs.t_extra_cost_wk.Where(exCostWkRow =>
                                exCostWkRow.calc_no == this.CalcNo &&
                                exCostWkRow.contract_type == keisanWkRow.contract_type &&
                                exCostWkRow.yuso_kbn == keisanWkRow.yuso_kbn &&
                                exCostWkRow.orig_warehouse_block_cd == keisanWkRow.orig_warehouse_block_cd &&
                                exCostWkRow.orig_warehouse_cd == keisanWkRow.orig_warehouse_cd &&
                                exCostWkRow.terminal_id == keisanWkRow.terminal_id &&
                                exCostWkRow.vehicle_id == keisanWkRow.vehicle_id &&
                                exCostWkRow.yuso_mode_kbn == keisanWkRow.yuso_mode_kbn &&
                                exCostWkRow.carrier_company_cd == keisanWkRow.carrier_company_cd &&
                                exCostWkRow.orig_date == keisanWkRow.orig_date &&
                                exCostWkRow.dest_jis == keisanWkRow.dest_cd &&
                                exCostWkRow.dest_warehouse_cd == keisanWkRow.dest_warehouse_cd &&
                                exCostWkRow.arriving_date == keisanWkRow.arriving_date &&
                                exCostWkRow.dest_cd == keisanWkRow.dest_cd)
                            .ToList()
                            .ForEach(exCostWkRow =>
                            {
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
                                        break;
                                    case extraCostKbn.HelperCharge:
                                        // 助手料
                                        // TODO: 助手料のカラム作成
                                        keisanWkRow.other_charge_amount += exCostWkRow.extra_charge_amount;
                                        break;
                                    case extraCostKbn.FuelCharge:
                                        // 燃油料
                                        keisanWkRow.fuel_cost_amount = exCostWkRow.extra_charge_amount;
                                        break;
                                    case extraCostKbn.Other:
                                        keisanWkRow.other_charge_amount += exCostWkRow.extra_charge_amount;
                                        break;
                                    default:
                                        break;
                                }
                            });

                // TODO: 付帯費用項目の足りないカラムを追加
                keisanWkRow.total_charge_amount =
                                keisanWkRow.base_charge_amount +
                                keisanWkRow.stopping_charge_amount +
                                keisanWkRow.cargo_charge_amount +
                                ( keisanWkRow.Isfuel_cost_amountNull() ? 0 : keisanWkRow.fuel_cost_amount) +
                                keisanWkRow.other_charge_amount;
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
                    // TODO: 自動生成メソッド名長すぎるので
                    // get keisan_wk row at the same key
                    var keisanWkRow =
                        this.CalcWkDs.t_keisan_wk.FindBycalc_nocalc_ymcontract_typeyuso_kbnorig_warehouse_block_cdorig_warehouse_cdterminal_idvehicle_iddest_jisdest_warehouse_cdyuso_mode_kbncarrier_company_cdorig_datearriving_datedest_cd(
                            this.CalcNo,
                            yusoWkRow.calc_ym,
                            yusoWkRow.contract_type,
                            yusoWkRow.yuso_kbn,
                            yusoWkRow.orig_warehouse_block_cd,
                            yusoWkRow.orig_warehouse_cd,
                            yusoWkRow.terminal_id,
                            yusoWkRow.vehicle_id,
                            yusoWkRow.dest_jis,
                            yusoWkRow.dest_warehouse_cd,
                            yusoWkRow.yuso_mode_kbn,
                            yusoWkRow.carrier_company_cd,
                            yusoWkRow.orig_date,
                            yusoWkRow.arriving_date,
                            yusoWkRow.dest_cd
                        );
                    if (keisanWkRow != null)
                    {
                        // set value to yusoWkRow
                        yusoWkRow.weight_sum_kg = keisanWkRow.weight_sum_kg;
                        yusoWkRow.base_charge_amount = keisanWkRow.base_charge_amount;
                        yusoWkRow.special_charge_amount = keisanWkRow.special_charge_amount;
                        yusoWkRow.stopping_charge_amount = keisanWkRow.stopping_charge_amount;
                        yusoWkRow.cargo_charge_amount = keisanWkRow.cargo_charge_amount;
                        yusoWkRow.other_charge_amount = keisanWkRow.other_charge_amount;
                       
                    } else
                    {
                        // TODO: errセット
                    }

                } else if (yusoWkRow.contract_type == ((int)CnContractType.ByVehicle).ToString())
                {
                    // get max total_charge_amaunt row of keisan_wk by yuso key of ByVehicle(sha-date)
                    var keisanWkRow = this.CalcWkDs.t_keisan_wk.AsEnumerable()
                            .Where(r =>
                                r.calc_no == this.CalcNo &&
                                r.contract_type == yusoWkRow.contract_type &&
                                r.yuso_kbn == yusoWkRow.yuso_kbn &&
                                r.orig_warehouse_block_cd == yusoWkRow.orig_warehouse_block_cd &&
                                r.orig_warehouse_cd == yusoWkRow.orig_warehouse_cd &&
                                r.terminal_id == yusoWkRow.terminal_id &&
                                r.vehicle_id == yusoWkRow.vehicle_id &&
                                r.yuso_mode_kbn == yusoWkRow.yuso_mode_kbn &&
                                r.carrier_company_cd == yusoWkRow.carrier_company_cd &&
                                r.orig_date == yusoWkRow.orig_date
                                )
                            .OrderByDescending(r => r.total_charge_amount)
                            .First();

                    if (keisanWkRow != null )
                    {
                        // set value to yusoWkRow
                        yusoWkRow.weight_sum_kg = keisanWkRow.weight_sum_kg;
                        yusoWkRow.base_charge_amount = keisanWkRow.base_charge_amount;
                        yusoWkRow.special_charge_amount = keisanWkRow.special_charge_amount;
                        yusoWkRow.stopping_charge_amount = keisanWkRow.stopping_charge_amount;
                        yusoWkRow.cargo_charge_amount = keisanWkRow.cargo_charge_amount;
                        yusoWkRow.other_charge_amount = keisanWkRow.other_charge_amount;

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

                // TODO: 個建に対応する
                var keisanAdp = new CalcTrnTableAdapters.t_keisanTableAdapter();
                keisanAdp.Connection = Connection;
                keisanAdp.FillOriginalDataByCalcNoByVehicle(this.CalcTrnDs.t_keisan, this.CalcNo, (short)CnCalcStatus.Doing);

                var detailAdp = new CalcTrnTableAdapters.t_detailTableAdapter();
                keisanAdp.Connection = Connection;
                detailAdp.FillOrigialDataByCalcNoByVehicle(this.CalcTrnDs.t_detail, this.CalcNo, (short)CnCalcStatus.Doing);

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
                keisanWkAdp.InnerAdapter.UpdateBatchSize = UpdateBatchSize;
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
                detailWkAdp.InnerAdapter.UpdateBatchSize = UpdateBatchSize;
                detailWkAdp.Update(this.CalcWkDs.t_detail_wk);

                /* --------------------------------------
                    * set info to keisan_wk on server 
                    * -------------------------------------- */
                // set calcinfo to keisan_wk by server update query
                keisanWkAdp.UpdateCalcInfo(DateTime.Now, "", this.CalcNo);

                // set vehicle info to keisan_wk on server
                keisanWkAdp.UpdateVehicleInfo(this.CalcNo, ((int)CnContractType.ByVehicle).ToString());

                // fill keisan_wk after update
                keisanWkAdp.Fill(CalcWkDs.t_keisan_wk);

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

            // init tariff dataset and adapter
            var tariffDs = new Tariff();
            var tariffInfoAdp = new m_tariff_infoTableAdapter();
            tariffInfoAdp.ClearBeforeFill = true;
            var tariffDetailAdp = new m_tariff_detailTableAdapter();
            tariffDetailAdp.ClearBeforeFill = true;

            foreach (var group in query)
            {
                // TODO: get info from m_tariff
                tariffInfoAdp.FillByTariffInfoId(tariffDs.m_tariff_info, group.Key);
                tariffDetailAdp.FillByTriffInfoId(tariffDs.m_tariff_detail, group.Key);

                // get column name
                var vartialValueColName = GetValueColName(tariffDs, CnTariffAxisKbn.Vertial);
                var horizontalValueColName = GetValueColName(tariffDs, CnTariffAxisKbn.Horizontal);

                foreach (var item in group)
                {
                    Decimal vertialValue = Decimal.Parse(item[vartialValueColName].ToString());
                    Decimal horizontalValue = Decimal.Parse(item[horizontalValueColName].ToString());
                    item.apply_vertical_value = vertialValue;
                    item.apply_horizonatl_value = horizontalValue;

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

                        item.base_charge_amount = base_charge_before_adding + adding_charge;

                    }
                    else
                    {
                        // TODO: タリフが見つからないError処理
                    }
                    
                }

            }
        }

        private void CalculateExtraCharge()
        {
            // TODO タリフ計算共通化
            Func<int, Datasets.Tariff> funcGetTriffDataset = (tariffId) =>
            {
                var tariffDs = new Tariff();
                var tariffInfoAdp = new m_tariff_infoTableAdapter();
                tariffInfoAdp.ClearBeforeFill = true;
                var tariffDetailAdp = new m_tariff_detailTableAdapter();
                tariffDetailAdp.ClearBeforeFill = true;

                tariffInfoAdp.FillByTariffInfoId(tariffDs.m_tariff_info, tariffId);
                tariffDetailAdp.FillByTriffInfoId(tariffDs.m_tariff_detail, tariffId);
                return tariffDs;
            };

            Func<DataRow, Decimal> funcGetTriffPrice = (exCostRow) =>
             {
                 if (!DBNull.Value.Equals(exCostRow["tariff_id"]))
                 {
                     var tariffDs = funcGetTriffDataset((int)exCostRow["tariff_id"]);


                     // get column name
                     var vartialValueColName = GetValueColName(tariffDs, CnTariffAxisKbn.Vertial);
                     var horizontalValueColName = GetValueColName(tariffDs, CnTariffAxisKbn.Horizontal);

                     Decimal vertialValue = Decimal.Parse(exCostRow[vartialValueColName].ToString());
                     Decimal horizontalValue = Decimal.Parse(exCostRow[horizontalValueColName].ToString());

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

                         return  base_charge_before_adding + adding_charge;
                     }
                     else
                     {
                         // TODO: タリフが見つからないError処理
                         return 0;
                     }
                 }
                 return 0;
             };

            foreach (var exCostRow in CalcWkDs.t_extra_cost_wk)
            {
                // if not applicable, break
                // TODO: 適用期間チェック

                switch (exCostRow.calculate_type_kbn)
                {
                    case extraCostKbn.StoppingCharge:
                        // 中継料　タリフ金額＊中継回数
                        // TODO: 中継回数をNull check
                        exCostRow.extra_charge_amount = funcGetTriffPrice(exCostRow) * exCostRow.stopping_count;
                        break;
                    case extraCostKbn.CargoCharge:
                        // 航送料　タリフ金額
                        exCostRow.extra_charge_amount = funcGetTriffPrice(exCostRow);
                        break;
                    case extraCostKbn.DistanceCharge:
                        // TODO: 実績距離を取得する
                        break;
                    case extraCostKbn.HelperCharge:
                        // 助手料　助手料＊人数
                        // TODO: 実績距離を取得する
                        exCostRow.extra_charge_amount = exCostRow.adding_price;
                        break;
                    case extraCostKbn.FuelCharge:
                        // 燃油料
                        exCostRow.extra_charge_amount = exCostRow.Isfuel_cost_amountNull() ? 0 : exCostRow.fuel_cost_amount;
                        break;
                    case extraCostKbn.Other:
                        // その他
                        switch (exCostRow.calculate_type_kbn)
                        {
                            case CalculateTypeKbn.Triff:
                                exCostRow.extra_charge_amount = funcGetTriffPrice(exCostRow);
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
            return tariffAxisKbn == CnTariffAxisKbn.Vertial ? "yuso_means_kbn" : "distance_km"; 
        }

        private void SumWeightByKeisanUnit()
        {
            // summmary weight of detail
            var query = this.CalcWkDs.t_detail_wk
//                .Where(r => r.contract_type == ((int)CnContractType.ByItem).ToString())
                .GroupBy(x =>
                    new
                    {
                        x.calc_ym,
                        x.contract_type,
                        x.yuso_kbn,
                        x.orig_warehouse_block_cd,
                        x.orig_warehouse_cd,
                        x.terminal_id,
                        x.vehicle_id,
                        x.dest_jis,
                        x.dest_warehouse_cd,
                        x.yuso_mode_kbn,
                        x.carrier_company_cd,
                        x.orig_date,
                        x.arriving_date,
                        x.dest_cd
                    })
                .Select(x => new { Keys = x.Key, SumWeight = x.Sum(y => y.item_weight_kg) });

            foreach (var group in query)
            {
                // TODO:自動生成メソッド名長すぎるので、Extentionをつくる
                var row = 
                this.CalcWkDs.t_keisan_wk.FindBycalc_nocalc_ymcontract_typeyuso_kbnorig_warehouse_block_cdorig_warehouse_cdterminal_idvehicle_iddest_jisdest_warehouse_cdyuso_mode_kbncarrier_company_cdorig_datearriving_datedest_cd(
                        this.CalcNo,
                        group.Keys.calc_ym,
                        group.Keys.contract_type,
                        group.Keys.yuso_kbn,
                        group.Keys.orig_warehouse_block_cd,
                        group.Keys.orig_warehouse_cd,
                        group.Keys.terminal_id,
                        group.Keys.vehicle_id,
                        group.Keys.dest_jis,
                        group.Keys.dest_warehouse_cd,
                        group.Keys.yuso_mode_kbn,
                        group.Keys.carrier_company_cd,
                        group.Keys.orig_date,
                        group.Keys.arriving_date,
                        group.Keys.dest_cd
                    );
                row.weight_sum_kg = group.SumWeight;
            }
        }

        public static int StartCalcBatch(SqlConnection conn)
        {
            try
            {
                // get new calc_no
                var calcNoAdp = new calc_noTableAdapter();
                calcNoAdp.Connection = conn;                
                int newCalcNo = Convert.ToInt32(calcNoAdp.InsertNewNo(DateTime.Now));

                // update calc_no and calc_status to "doing"
                // TODO: create calcstatus index on t_yuso
                var tYusoAdp = new StartCalcTableAdapters.t_yusoTableAdapter();
                tYusoAdp.Connection = conn;
                var rtn = tYusoAdp.UpdateCalcStatus((short)CnCalcStatus.Doing, newCalcNo, DateTime.Now, "", (short)CnCalcStatus.UnCalc);

                var dsStartCalc = new StartCalc();
                var cnt = tYusoAdp.FillByCalcNo(dsStartCalc.t_yuso, newCalcNo, (short)CnCalcStatus.Doing);

                // TODO: クエリに変更するかどうか検討
                // copy tran to wk
                var colNamesOfYusoWk = Enumerable.Range(0, dsStartCalc.t_yuso_wk.Columns.Count - 1)
                                            .Select(i => dsStartCalc.t_yuso_wk.Columns[i].ColumnName).ToList();
                dsStartCalc.t_yuso.ToList().ForEach(r => {
                    var newRow = dsStartCalc.t_yuso_wk.NewRow();
                    colNamesOfYusoWk.ForEach(colname =>
                    {
                        if (dsStartCalc.t_yuso.Columns.Contains(colname)) newRow[colname] = r[colname];
                    });
                    dsStartCalc.t_yuso_wk.Rows.Add(newRow);
                });

                // wk table update(batch)
                var adpYusoWk = new StartCalcTableAdapters.t_yuso_wkTableAdapter();
                adpYusoWk.Connection = conn;
                //adpYusoWk.InnerAdapter.UpdateBatchSize = UpdateBatchSize;
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
        private int GetNewCalcNo()
        {
            return 0;
        }
        
        private void CreateYusoWork()
        {

        }

        public static void FillCalcInfo(int calcNo)
        {
            throw new NotImplementedException();
        }

        public static void UpdateSumWeight(int calcNo)
        {
            throw new NotImplementedException();
        }

        public static void UpdateWeightSummay(int calcNo)
        {
            throw new NotImplementedException();
        }

        public static void UpdateBasicFare(int calcNo)
        {
            throw new NotImplementedException();
        }

        private void RefrectToResultData()
        {
            // TODO: 値をトランにセット
            // TODD: TranをUpdate
            // update wk tables
            var yusoWkAdp = new CalcWkTableAdapters.t_yuso_wkTableAdapter();
            yusoWkAdp.InnerAdapter.UpdateBatchSize = UpdateBatchSize;
            yusoWkAdp.Update(this.CalcWkDs);

            var keisanWkAdp = new CalcWkTableAdapters.t_keisan_wkTableAdapter();
            keisanWkAdp.InnerAdapter.UpdateBatchSize = UpdateBatchSize;
            keisanWkAdp.Update(this.CalcWkDs);

            var detailWkAdp = new CalcWkTableAdapters.t_detail_wkTableAdapter();
            detailWkAdp.InnerAdapter.UpdateBatchSize = UpdateBatchSize;
            detailWkAdp.Update(CalcWkDs);

        }

        private void EndCalc(int calcNo)
        {
            throw new NotImplementedException();

        }

        private void DivideResultAmount()
        {
            try
            {
                // TODO: 個建ロジックは後で対応する
                var query = this.CalcWkDs.t_detail_wk.GroupBy(x => new {
                    x.calc_ym,
                    x.contract_type,
                    x.yuso_kbn,
                    x.orig_warehouse_block_cd,
                    x.orig_warehouse_cd,
                    x.terminal_id,
                    x.vehicle_id,
                    x.yuso_mode_kbn,
                    x.carrier_company_cd,
                    x.orig_date});
            
                foreach (var group in query)
                {
                    // get yusoWkRow by group key
                    var yusoWkRow = this.CalcWkDs.t_yuso_wk.Where(x =>
                        x.calc_ym == group.Key.calc_ym &&
                        x.contract_type == group.Key.contract_type &&
                        x.yuso_kbn == group.Key.yuso_kbn &&
                        x.orig_warehouse_block_cd == group.Key.orig_warehouse_block_cd &&
                        x.orig_warehouse_cd == group.Key.orig_warehouse_cd &&
                        x.terminal_id == group.Key.terminal_id &&
                        x.vehicle_id == group.Key.vehicle_id &&
                        //x.dest_jis == group.Key.dest_jis &&
                        //x.dest_warehouse_cd == group.Key.dest_warehouse_cd &&
                        x.yuso_mode_kbn == group.Key.yuso_mode_kbn &&
                        x.carrier_company_cd == group.Key.carrier_company_cd &&
                        x.orig_date == group.Key.orig_date
                        //x.arriving_date == group.Key.arriving_date &&
                        //x.dest_cd == group.Key.dest_cd
                        ).FirstOrDefault();

                    if (yusoWkRow == null )
                    {
                        // TODO: エラー処理
                        break;
                    }

                    var usoWkColNames = new List<String>()
                    {
                        "base_charge_amount",
                        "special_charge_amount",
                        "stopping_charge_amount",
                        "cargo_charge_amount",
                        "other_charge_amount",
                        "actual_time_surcharge_amount"
                    };
                    var sumAmounts = new Dictionary<String, Decimal>();
                    var maxAmountInfo = new Dictionary<String, Dictionary<String, Decimal>>();
                    //initialize dictionary
                    usoWkColNames.ForEach(name => sumAmounts.Add("distributed_" + name, 0));
                    usoWkColNames.ForEach(name => maxAmountInfo.Add("distributed_" + name, null));

                    foreach (var detailRow in group)
                    {
                        usoWkColNames.ForEach(usoWkColname => 
                        {
                            if (!DBNull.Value.Equals(yusoWkRow.weight_sum_kg) && !yusoWkRow.weight_sum_kg.Equals(0))
                            {
                                var detailColName = "distributed_" + usoWkColname;
                                var devidedlAmount = Decimal.Floor(
                                                        (decimal)yusoWkRow[usoWkColname] * detailRow.item_weight_kg / yusoWkRow.weight_sum_kg );
                                detailRow[detailColName] = devidedlAmount;

                                // add to summary
                                sumAmounts[detailColName] += devidedlAmount;

                                // set id and value if amount is greater than before
                                if (maxAmountInfo[detailColName] == null || maxAmountInfo[detailColName]["Amount"] < devidedlAmount)
                                {
                                    maxAmountInfo[detailColName] = new Dictionary<String, Decimal>(){ {"DetailId", detailRow.detail_id }, { "Amount", devidedlAmount } };
                                }
                            }
                        });
                    }

                    // compare yuso amount to max detail sum amount. if it's defferent, add the difference to row that has max amount
                    usoWkColNames.ForEach(usoWkColname =>
                    {
                        if (!DBNull.Value.Equals(yusoWkRow[usoWkColname]))
                        {
                            var detailColName = "distributed_" + usoWkColname;
                            var defference = Decimal.Parse(yusoWkRow[usoWkColname].ToString()) - sumAmounts[detailColName];
                            if (defference != 0)
                            {
                                var maxAmountRow = group.Where(x => x.detail_id == maxAmountInfo[detailColName]["DetailId"]).FirstOrDefault();
                                maxAmountRow[detailColName] = Decimal.Parse(maxAmountRow[detailColName].ToString()) + defference;
                            }
                        }
                    });

                    // set total amount
                    foreach (var detailRow in group)
                    {
                        detailRow.distributed_total_charge_amount = 0;  // TODO: Not Null制約入れる
                        usoWkColNames.ForEach(name => detailRow.distributed_total_charge_amount += (decimal)detailRow["distributed_" + name]);
                    }
                }

            }
            catch (Exception)
            {

                throw;
            }
        }

        private void UpdateExtraCharge(int calcNo)
        {
            throw new NotImplementedException();
        }

    }
}
