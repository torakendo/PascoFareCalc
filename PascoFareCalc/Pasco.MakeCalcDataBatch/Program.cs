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

        // TODO: high endo 持ち戻りのカラムマッピング
        private static Dictionary<string, string> BringBackColumnNameMap = new Dictionary<string, string>()
        { };

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
    }
}
