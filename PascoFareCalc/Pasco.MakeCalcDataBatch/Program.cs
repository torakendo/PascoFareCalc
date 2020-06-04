using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using static Pasco.FareCalcLib.Constants;
using Pasco.FareCalcLib;
using Pasco.FareCalcLib.Datasets;
using System.Text;
using System.Net.Http.Headers;
using System.Transactions;
using Pasco.MakeCalcDataBatch.Datasets;
using Pasco.MakeCalcDataBatch.Datasets.MakeCalcDsTableAdapters;

namespace Pasco.MakeCalcDataBatch
{
    class Program
    {
        private static Dictionary<string,string> ColumnNameMap = new Dictionary<string, string>(){
            // TODO: done akema 出庫実績トラン対応カラム名確認
            {"calc_ym", "scheduleactualdate" },
            {"contract_type","contracttype" },
            {"yuso_kbn","skbn" },
            {"orig_warehouse_block_cd","outblockcode"},
            {"orig_warehouse_cd","fromloccode"},
            {"terminal_id","terminal_id"},
            {"vehicle_id","vehicleid"},
            {"dest_warehouse_cd","toloccode"},
            {"dest_jis","municipalitycode"},
            {"yuso_mode_kbn","transmodekbn"},
            {"carrier_company_cd","transcompanycode"},
            {"orig_date","scheduleactualdate"},            
            {"arriving_date","rltunloaddate"},
            {"dest_cd","shipcustcode"},
            {"slip_no","senderordcode"},
            {"slip_suffix_no","sendercodesubno"},
            {"slip_detail_no","senderorddtlcode"},
            {"item_cd","fromproductcode"},
            {"item_kigo","fromlotno"},
            {"item_name","aggproductname"},
            {"item_quantity","fromquantity"},
            {"itme_unit","fromunit"},
            {"item_weight_kg","rltloadweight"},
            {"yuso_means_kbn","yusocode"},
            {"special_vehicle_kbn","specialvehiclekbn"}
        };

        private static Dictionary<string, string> BringBackColumnNameMap = new Dictionary<string, string>() {
            // TODO: high endo 持ち戻りのカラムマッピング
            {"yuso_kbn","skbn" },// データ分類
            {"contract_type","contracttype" },// 契約種別
            {"orig_date","scheduleactualdate"},  // 出庫日
            {"yuso_mode_kbn","transmodekbn"},// （発地）
            {"orig_warehouse_block_cd","outblockcode"},// （着地）
            {"dest_jis","municipalitycode"},// モード区分
            {"carrier_company_cd","transcompanycode"},// 業者コード
            {"dest_cd","shipcustcode"},// 届け先コード
            {"arriving_date","rltunloaddate"},// 届け日
        };

        private static string[] commonColNames = { "CreateDay", "UpdateDay", "CreateUserCode", "" };

        // TODO: バッチサイズConfigへ移動
        private static int UpdateBatchSize = 100;

        static void Main(string[] args)
        {

            // TODO: log write
            Console.WriteLine("----- START MakeCalcDataBatch -----");

            try
            {
                using (TransactionScope scope = new TransactionScope())
                {
                    using (SqlConnection conn = new SqlConnection(
                        ConfigurationManager.ConnectionStrings["PcsCalcdbConnectionString"].ConnectionString))
                    {
                        conn.Open();

                        var makeCalcDs = new MakeCalcDs();

                        //setting tableadapters
                        var yusoAdp = new t_yusoTableAdapter();
                        yusoAdp.Connection = conn;
                        yusoAdp.ClearBeforeFill = false;
                        yusoAdp.SetUpdateBatchSize(UpdateBatchSize);

                        var keisanAdp = new t_keisanTableAdapter();
                        keisanAdp.Connection = conn;
                        keisanAdp.ClearBeforeFill = false;
                        keisanAdp.SetUpdateBatchSize(UpdateBatchSize);

                        var detailAdp = new t_detailTableAdapter();
                        detailAdp.Connection = conn;
                        detailAdp.ClearBeforeFill = false;
                        detailAdp.SetUpdateBatchSize(UpdateBatchSize);

                        var shkJskAdp = new t_shk_jskTableAdapter();
                        shkJskAdp.Connection = conn;

                        // 出荷データを取得する（未処理） get shipment data (not processed) 
                        var shkJskCnt = shkJskAdp.FillByInclKbn(makeCalcDs.t_shk_jsk, InclKbn.UnDone);
                        // TODO: debug code for p1
                        Console.WriteLine("retrieve from t_shk_jsk COUNT = {0}", shkJskCnt);

                        var calcKeyGen = new CalcKeyGenerator();
                        // TODO: normal-low akema システム日付取得共通化
                        var batchExecDate = DateTime.Now;

                        foreach (var shkJskRow in makeCalcDs.t_shk_jsk)
                        {
                            // 新しいdetailRowを作成し、shkJskRowから値を設定します create new detailRow and set value from shkJskRow
                            var detailRow = SetDetailDataFromShkJskData(makeCalcDs.t_detail.Newt_detailRow(), shkJskRow);

                            // keisanKeyとyusoKeyを生成する generate keisanKey and yusoKey
                            var paramTable = calcKeyGen.GetParamTable();
                            // TODO: done 出荷実績のカラム名に変更する
                            // TODO: normal akema 輸送区分：持ち戻り、返品、引き取りに対応する

                            paramTable.Keys.ToList().ForEach(paramName => paramTable[paramName] = detailRow[paramName]);
                            // TODO: endo high　calcKeyGen 輸送キーの変更、持ち戻り輸送キー対応（？）
                            var calcKeys = calcKeyGen.getCalcKeys(paramTable);

                            // t_yuso挿入または更新 t_yuso insert or update
                            // t_yusoを挿入または更新します。 更新時に、calc_statusを確認します insert or update t_yuso. when update, check calc_status

                            var yusoFillCnt = yusoAdp.FillByYusoKey(makeCalcDs.t_yuso, calcKeys.YusoKey, detailRow.calc_ym);
                            var yusoQ = makeCalcDs.t_yuso.Where(r => r.yuso_key == calcKeys.YusoKey);
                            if (yusoFillCnt == 0 && yusoQ.Count() == 0)
                            {
                                // データをyusoRowに設定して挿入 set data to yusoRow and insert
                                var newYusoRow = SetYusoDataFromShkJskData(makeCalcDs.t_yuso.Newt_yusoRow(), shkJskRow);

                                // 値がNULLの場合に初期値をセットする
                                newYusoRow.yuso_id = DataRow<int>(newYusoRow, "yuso_id");
                                newYusoRow.calc_ym = DataRow<string>(newYusoRow, "calc_ym");
                                newYusoRow.contract_type = DataRow<string>(newYusoRow, "contract_type");
                                newYusoRow.yuso_kbn = DataRow<string>(newYusoRow, "yuso_kbn");
                                newYusoRow.orig_warehouse_block_cd = DataRow<string>(newYusoRow, "orig_warehouse_block_cd");
                                newYusoRow.orig_warehouse_cd = DataRow<string>(newYusoRow, "orig_warehouse_cd");
                                newYusoRow.terminal_id = DataRow<string>(newYusoRow, "terminal_id");
                                newYusoRow.vehicle_id = DataRow<string>(newYusoRow, "vehicle_id");
                                newYusoRow.dest_jis = DataRow<string>(newYusoRow, "dest_jis");
                                newYusoRow.dest_warehouse_cd = DataRow<string>(newYusoRow, "dest_warehouse_cd");
                                newYusoRow.yuso_mode_kbn = DataRow<string>(newYusoRow, "yuso_mode_kbn");
                                newYusoRow.carrier_company_cd = DataRow<string>(newYusoRow, "carrier_company_cd");
                                newYusoRow.orig_date = DataRow<string>(newYusoRow, "orig_date");
                                newYusoRow.arriving_date = DataRow<string>(newYusoRow, "arriving_date");
                                newYusoRow.dest_cd = DataRow<string>(newYusoRow, "dest_cd");
                                newYusoRow.distance_km = DataRow<int>(newYusoRow, "distance_km");
                                newYusoRow.time_mins = DataRow<int>(newYusoRow, "time_mins");
                                newYusoRow.fuel_cost_amount = DataRow<decimal>(newYusoRow, "fuel_cost_amount");
                                newYusoRow.stopping_count = DataRow<short>(newYusoRow, "stopping_count");
                                newYusoRow.weight_sum_kg = DataRow<decimal>(newYusoRow, "weight_sum_kg");
                                newYusoRow.item_quantity_sum = DataRow<decimal>(newYusoRow, "item_quantity_sum");
                                newYusoRow.base_charge_amount = DataRow<decimal>(newYusoRow, "base_charge_amount");
                                newYusoRow.special_charge_amount = DataRow<decimal>(newYusoRow, "special_charge_amount");
                                newYusoRow.stopping_charge_amount = DataRow<decimal>(newYusoRow, "stopping_charge_amount");
                                newYusoRow.cargo_charge_amount = DataRow<decimal>(newYusoRow, "cargo_charge_amount");
                                newYusoRow.other_charge_amount = DataRow<decimal>(newYusoRow, "other_charge_amount");
                                newYusoRow.actual_distance_km = DataRow<decimal>(newYusoRow, "actual_distance_km");
                                newYusoRow.actual_distance_surcharge_amount = DataRow<decimal>(newYusoRow, "actual_distance_surcharge_amount");
                                newYusoRow.actual_time_mins = DataRow<decimal>(newYusoRow, "actual_time_mins");
                                newYusoRow.actual_time_surcharge_amount = DataRow<decimal>(newYusoRow, "actual_time_surcharge_amount");
                                newYusoRow.actual_assistant_count = DataRow<int>(newYusoRow, "actual_assistant_count");
                                newYusoRow.actual_assist_surcharge_amount = DataRow<decimal>(newYusoRow, "actual_assist_surcharge_amount");
                                newYusoRow.actual_load_surcharge_amount = DataRow<decimal>(newYusoRow, "actual_load_surcharge_amount");
                                newYusoRow.actual_stand_surcharge_amount = DataRow<decimal>(newYusoRow, "actual_stand_surcharge_amount");
                                newYusoRow.actual_wash_surcharge_amount = DataRow<decimal>(newYusoRow, "actual_wash_surcharge_amount");
                                newYusoRow.total_charge_amount = DataRow<decimal>(newYusoRow, "total_charge_amount");
                                newYusoRow.actual_chosei_sum_amount = DataRow<decimal>(newYusoRow, "actual_chosei_sum_amount");
                                newYusoRow.chosei_total_charge_amount = DataRow<decimal>(newYusoRow, "chosei_total_charge_amount");
                                newYusoRow.verify_status = DataRow<string>(newYusoRow, "verify_status");
                                newYusoRow.verify_ymd = DataRow<string>(newYusoRow, "verify_ymd");
                                newYusoRow.release_ymd = DataRow<string>(newYusoRow, "release_ymd");
                                newYusoRow.yuso_means_kbn = DataRow<string>(newYusoRow, "yuso_means_kbn");
                                newYusoRow.dest_nm = DataRow<string>(newYusoRow, "dest_nm");
                                newYusoRow.calc_status = DataRow<string>(newYusoRow, "calc_status");
                                newYusoRow.calc_no = DataRow<int>(newYusoRow, "calc_no");
                                newYusoRow.last_calc_at = DataRowDateTime(newYusoRow, "last_calc_at");
                                newYusoRow.send_flg = DataRow<short>(newYusoRow, "send_flg");
                                newYusoRow.send_at = DataRowDateTime(newYusoRow, "send_at");
                                newYusoRow.back_flg = DataRow<short>(newYusoRow, "back_flg");
                                newYusoRow.calc_err_flg = DataRow<short>(newYusoRow, "calc_err_flg");
                                newYusoRow.yuso_key = DataRow<string>(newYusoRow, "yuso_key");
                                newYusoRow.BatchUpdateDay = DataRowDateTime(newYusoRow, "BatchUpdateDay");
                                newYusoRow.CreateDay = DataRowDateTime(newYusoRow, "CreateDay");
                                newYusoRow.UpdateDay = DataRowDateTime(newYusoRow, "UpdateDay");
                                newYusoRow.CreateUserCode = DataRow<string>(newYusoRow, "CreateUserCode");
                                newYusoRow.UpdateUserCode = DataRow<string>(newYusoRow, "UpdateUserCode");

                                // 値セット
                                newYusoRow.yuso_key = calcKeys.YusoKey;
                                newYusoRow.calc_status = CnCalcStatus.UnCalc;
                                newYusoRow.verify_status = CnVerifyStatus.NotVerified;
                                // TODO: "last_calc_at", "BatchUpdateDay" notNullになっているので仮にセットしているが、Nullable変更したい
                                newYusoRow.last_calc_at = batchExecDate;
                                newYusoRow.BatchUpdateDay = batchExecDate;
                                newYusoRow.UpdateDay = batchExecDate;

                                makeCalcDs.t_yuso.Addt_yusoRow(newYusoRow);
                            }
                            else
                            {
                                // TODO: yuso_key + calcYm またはyuso_Keyで一意指定のIndexになっていること確認
                                var yusoRow = yusoQ.First();
                                if (yusoRow.calc_status != CnCalcStatus.Doing)
                                {
                                    // update
                                    yusoRow.calc_status = CnCalcStatus.UnCalc;
                                    yusoRow.verify_status = CnVerifyStatus.NotVerified;
                                    // TODO: normal　endo 計算結果項目をクリアするかどうか、検討
                                    // TODO: normal akema システム日付取得ヘルパーから取得 datetime.now commonHeler.getdate()
                                    // TODO: low akema 登録更新情報設定の共通化 update_at, update_user, create_at
                                    // TODO: done endo バッチ更新情報用の項目を持たなくてよいか
                                    yusoRow.last_calc_at = batchExecDate;
                                    yusoRow.BatchUpdateDay = batchExecDate;
                                    yusoRow.UpdateDay = batchExecDate;
                                }
                                else
                                {
                                    // when calc "doing" record warning message and continue
                                    // TODO: メッセージ記録
                                    continue;
                                }
                            }

                            // t_keisan挿入または更新 t_keisan insert or update
                            var keisanFillCnt = keisanAdp.FillByKeisanKey(makeCalcDs.t_keisan, calcKeys.KeisanKey, detailRow.calc_ym);
                            var keisanQ = makeCalcDs.t_keisan.Where(r => r.keisan_key == calcKeys.KeisanKey);
                            if (keisanFillCnt == 0 && keisanQ.Count() == 0)
                            {
                                // データをkeisanRowに設定して挿入 set data to keisanRow and insert
                                var newKeisanRow = SetKeisanDataFromShkJskData(makeCalcDs.t_keisan.Newt_keisanRow(), shkJskRow);

                                // 値がNULLの場合に初期値をセットする
                                newKeisanRow.keisan_id = DataRow<int>(newKeisanRow, "keisan_id");
                                newKeisanRow.calc_ym = DataRow<string>(newKeisanRow, "calc_ym");
                                newKeisanRow.contract_type = DataRow<string>(newKeisanRow, "contract_type");
                                newKeisanRow.yuso_kbn = DataRow<string>(newKeisanRow, "yuso_kbn");
                                newKeisanRow.orig_warehouse_block_cd = DataRow<string>(newKeisanRow, "orig_warehouse_block_cd");
                                newKeisanRow.orig_warehouse_cd = DataRow<string>(newKeisanRow, "orig_warehouse_cd");
                                newKeisanRow.terminal_id = DataRow<string>(newKeisanRow, "terminal_id");
                                newKeisanRow.vehicle_id = DataRow<string>(newKeisanRow, "vehicle_id");
                                newKeisanRow.dest_jis = DataRow<string>(newKeisanRow, "dest_jis");
                                newKeisanRow.dest_warehouse_cd = DataRow<string>(newKeisanRow, "dest_warehouse_cd");
                                newKeisanRow.yuso_mode_kbn = DataRow<string>(newKeisanRow, "yuso_mode_kbn");
                                newKeisanRow.carrier_company_cd = DataRow<string>(newKeisanRow, "carrier_company_cd");
                                newKeisanRow.orig_date = DataRow<string>(newKeisanRow, "orig_date");
                                newKeisanRow.arriving_date = DataRow<string>(newKeisanRow, "arriving_date");
                                newKeisanRow.dest_cd = DataRow<string>(newKeisanRow, "dest_cd");
                                newKeisanRow.fare_tariff_id = DataRow<int>(newKeisanRow, "fare_tariff_id");
                                newKeisanRow.special_tariff_id = DataRow<int>(newKeisanRow, "special_tariff_id");
                                newKeisanRow.extra_cost_pattern_id = DataRow<int>(newKeisanRow, "extra_cost_pattern_id");
                                newKeisanRow.distance_km = DataRow<int>(newKeisanRow, "distance_km");
                                newKeisanRow.time_mins = DataRow<int>(newKeisanRow, "time_mins");
                                newKeisanRow.fuel_cost_amount = DataRow<int>(newKeisanRow, "fuel_cost_amount");
                                newKeisanRow.stopping_count = DataRow<short>(newKeisanRow, "stopping_count");
                                newKeisanRow.special_tariff_start_md = DataRow<string>(newKeisanRow, "special_tariff_start_md");
                                newKeisanRow.special_tariff_end_md = DataRow<string>(newKeisanRow, "special_tariff_end_md");
                                newKeisanRow.weight_sum_kg = DataRow<decimal>(newKeisanRow, "weight_sum_kg");
                                newKeisanRow.item_quantity_sum = DataRow<decimal>(newKeisanRow, "item_quantity_sum");
                                newKeisanRow.base_charge_amount = DataRow<decimal>(newKeisanRow, "base_charge_amount");
                                newKeisanRow.special_charge_amount = DataRow<decimal>(newKeisanRow, "special_charge_amount");
                                newKeisanRow.yuso_means_kbn = DataRow<string>(newKeisanRow, "yuso_means_kbn");
                                newKeisanRow.dest_nm = DataRow<string>(newKeisanRow, "dest_nm");
                                newKeisanRow.max_flg = DataRow<short>(newKeisanRow, "max_flg");
                                newKeisanRow.back_flg = DataRow<short>(newKeisanRow, "back_flg");
                                newKeisanRow.keisan_key = DataRow<string>(newKeisanRow, "keisan_key");
                                newKeisanRow.yuso_key = DataRow<string>(newKeisanRow, "yuso_key");
                                newKeisanRow.CreateDay = DataRowDateTime(newKeisanRow, "CreateDay");
                                newKeisanRow.UpdateDay = DataRowDateTime(newKeisanRow, "UpdateDay");
                                newKeisanRow.CreateUserCode = DataRow<string>(newKeisanRow, "CreateUserCode");
                                newKeisanRow.UpdateUserCode = DataRow<string>(newKeisanRow, "UpdateUserCode");

                                // 値セット
                                newKeisanRow.keisan_key = calcKeys.KeisanKey;
                                newKeisanRow.yuso_key = calcKeys.YusoKey;
                                newKeisanRow.UpdateDay = batchExecDate;
                                makeCalcDs.t_keisan.Addt_keisanRow(newKeisanRow);
                            }
                            else
                            {
                                var keisanRow = keisanQ.First();
                                keisanRow.yuso_means_kbn = shkJskRow.yusocode;
                                keisanRow.max_flg = 0;
                                // TODO: 登録更新情報設定の共通化
                                keisanRow.UpdateDay = batchExecDate;
                            }

                            // t_denpyo挿入または更新 t_denpyo insert or update
                            // TODO: high akema 伝票単位のデータを作成する

                            // t_detail挿入 t_detail insert
                            // 挿入する前に同じデータがないことを確認してください make sure there's no same data before insert
                            var detailFillCnt = detailAdp.FillBySlipKey(makeCalcDs.t_detail,
                                shkJskRow.senderordcode.ToString(), shkJskRow.sendercodesubno, shkJskRow.senderorddtlcode);
                            var detailQ = makeCalcDs.t_detail.Where(
                                r => r.slip_no == shkJskRow.senderordcode.ToString() &&
                                    r.slip_suffix_no == shkJskRow.sendercodesubno &&
                                    r.slip_detail_no == shkJskRow.senderorddtlcode);
                            if (detailFillCnt == 0 && detailQ.Count() == 0)
                            {
                                detailRow.keisan_key = calcKeys.KeisanKey;
                                detailRow.yuso_key = calcKeys.YusoKey;
                                detailRow.UpdateDay = batchExecDate;
                                makeCalcDs.t_detail.Addt_detailRow(detailRow);
                            }
                            else
                            {
                                // TODO: normal akema エラーメッセージ出力 log出力
                                throw new Exception("出荷実績データ重複エラー ");
                            }


                            // inclKbn "完了"を設定 set inclKbn "Done"
                            shkJskRow.incl_kbn = InclKbn.Done;
                            // TODO: normal akema udpate_day更新
                        }

                        // update database
                        var yusoCnt = yusoAdp.Update(makeCalcDs);
                        var keisanCnt = keisanAdp.Update(makeCalcDs);
                        var detailCnt = detailAdp.Update(makeCalcDs);

                        // TODO: debug code for p1
                        // TODO: normal akema  log出力
                        Console.WriteLine("t_yuso COUNT = {0}", yusoCnt);
                        Console.WriteLine("t_keisan COUNT = {0}", keisanCnt);
                        Console.WriteLine("t_detail COUNT = {0}", detailCnt);

                        // update shkJsk
                        shkJskAdp.Update(makeCalcDs.t_shk_jsk);
                    }

                    scope.Complete();
                }
                // TODO: debug code for p1
                Console.WriteLine(" MakeCalcDataBatch End");
            }
            catch (Exception ex)
            {
                // TODO: normal akema エラーメッセージ出力 log出力
                Console.WriteLine("Error occur ");
                var stringBuilder = new StringBuilder();
                var innerEx = ex;
                while (innerEx != null)
                {
                    stringBuilder.AppendLine(innerEx.Message);
                    stringBuilder.AppendLine(innerEx.StackTrace);
                    innerEx = innerEx.InnerException;
                }
                Console.WriteLine(stringBuilder.ToString());
            }
        }

        private static MakeCalcDs.t_detailRow SetDetailDataFromShkJskData(MakeCalcDs.t_detailRow t_detailRow, MakeCalcDs.t_shk_jskRow shkJskRow)
        {
            ColumnNameMap.ToList().ForEach(colname => 
                {
                    if (t_detailRow.Table.Columns.Contains(colname.Key))
                        t_detailRow[colname.Key] = getEditedColValue(colname.Key, colname.Value, shkJskRow);
                });
            return t_detailRow;            
        }

        private static MakeCalcDs.t_keisanRow SetKeisanDataFromShkJskData(MakeCalcDs.t_keisanRow t_keisanRow, MakeCalcDs.t_shk_jskRow shkJskRow)
        {
            ColumnNameMap.ToList().ForEach(colname =>
            {
                if (t_keisanRow.Table.Columns.Contains(colname.Key))
                    t_keisanRow[colname.Key] = getEditedColValue(colname.Key, colname.Value, shkJskRow);
            });
            return t_keisanRow;
        }

        private static MakeCalcDs.t_yusoRow SetYusoDataFromShkJskData(MakeCalcDs.t_yusoRow t_yusoRow, MakeCalcDs.t_shk_jskRow shkJskRow)
        {
            ColumnNameMap.ToList().ForEach(colname =>
            {
                if (t_yusoRow.Table.Columns.Contains(colname.Key))
                    t_yusoRow[colname.Key] = getEditedColValue(colname.Key, colname.Value, shkJskRow);
            });
            return t_yusoRow;
        }

        private static object getEditedColValue(string key, string value, MakeCalcDs.t_shk_jskRow shkJskRow)
        {
            // TODO: high endo 持ち戻り時の項目マッピング、その他のデータ種類など見直す
            switch (key)
            {
                case "calc_ym":
                    // TODO: orig_date 妥当性チェック必要？
                    var test = shkJskRow[value];
                    return shkJskRow[value].ToString().Length >= 6 ? shkJskRow[value].ToString().Substring(0,6) : shkJskRow[value].ToString();
                case "yuso_kbn":
                    // TODO: endo high 持ち戻り、支給などに対応。dataclass と他の項目で判定
                    return shkJskRow.dataclass;
                case "vehicle_id":
                    // vehicle_id  vehicleid + tripno
                    return String.Concat(shkJskRow[value], shkJskRow["tripno"].ToString());
                case "terminal_id":
                    // TODO: low DB項目を削除し、PGMからも削除
                    return "";
                case "orig_warehouse_block_cd":
                case "orig_warehouse_cd":
                case "dest_jis":
                case "arriving_date":
                case "dest_cd":
                    // return empty if null
                    return shkJskRow[value].ToString();
                default:
                    return shkJskRow[value];
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
