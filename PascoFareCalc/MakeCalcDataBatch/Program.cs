using FareCalcLib;
using MakeCalcDataBatch.Datasets;
using MakeCalcDataBatch.Datasets.MakeCalcDsTableAdapters;
using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Transactions;
using static FareCalcLib.Constants;
using FareCalcLib.Datasets;
using System.Text;

namespace MakeCalcDataBatch
{
    class Program
    {
        private static Dictionary<string,string> keyColumnNames = new Dictionary<string, string>(){
            {"contract_type","contracttype" },
            {"yuso_kbn","skbn" },
            {"orig_warehouse_block_cd","outblockcode"},
            //{"orig_warehouse_cd","orig_warehouse_cd"},
            //{"terminal_id","terminal_id"},
            {"vehicle_id","vehicleid"},
            {"dest_warehouse_cd","inblockcode"},
            {"dest_jis","shipcustcode"},
            {"yuso_mode_kbn","transmodekbn"},
            {"carrier_company_cd","transcompanycode"},
            {"orig_date","rltloaddate"},
            {"arriving_date","unloaddateorder"},
            {"dest_cd","ordcustcode"},
            {"slip_no","senderordcode"},
            {"slip_suffix_no","sendercodesubno"},
            {"slip_detail_no","senderorddtlcode"},
            //{"item_cd","item_cd"},
            //{"item_kigo","item_kigo"},
            //{"item_name","item_name"},
            //{"item_quantity","item_quantity"},
            //{"itme_unit","itme_unit"},
            //{"item_weight_kg","item_weight_kg"},
            //{"yuso_means_kbn","yuso_means_kbn"},
            //{"special_vehicle_kbn","special_vehicle_kbn"}
        };

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

                        // get shipment data (not processed) 
                        var shkJskCnt = shkJskAdp.FillByInclKbn(makeCalcDs.t_shk_jsk, InclKbn.UnDone);
                        // TODO: debug code for p1
                        Console.WriteLine("retrieve from t_shk_jsk COUNT = {0}", shkJskCnt);

                        var calcKeyGen = new CalcKeyGenerator();

                        foreach (var shkJskRow in makeCalcDs.t_shk_jsk)
                        {
                            /* --  generate keisanKey and yusoKey -- */
                            var paramTable = calcKeyGen.GetParamTable();
                            // TODO: 出荷実績のカラム名に変更する
                            // TODO: normal akema 輸送区分：持ち戻り、返品、引き取りに対応する
                            paramTable.Keys.ToList().ForEach(paramName => paramTable[paramName] = shkJskRow[paramName]);
                            var calcKeys = calcKeyGen.getCalcKeys(paramTable);

                            // calcYm from origDate
                            // TODO: orig_date　日付→文字列対応
                            var calcYm = shkJskRow.rltloaddate;

                            /* -- t_yuso insert or update -- */
                            /* --    insert or update t_yuso. when update, check calc_status -- */
                            
                            var yusoFillCnt = yusoAdp.FillByYusoKey(makeCalcDs.t_yuso, calcKeys.YusoKey, calcYm);
                            var yusoQ = makeCalcDs.t_yuso.Where(r => r.yuso_key == calcKeys.YusoKey);
                            if (yusoFillCnt == 0 && yusoQ.Count() == 0)
                            {
                                // set data to yusoRow and insert
                                var newYusoRow = SetYusoDataFromShkJskData(makeCalcDs.t_yuso.Newt_yusoRow(), shkJskRow);
                                newYusoRow.yuso_key = calcKeys.YusoKey;
                                newYusoRow.calc_ym = calcYm;
                                newYusoRow.calc_status = CnCalcStatus.UnCalc;
                                newYusoRow.verify_status = CnVerifyStatus.NotVerified;
                                newYusoRow.UpdateDay = DateTime.Now;

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
                                    // TODO: high endo バッチ更新情報用の項目を持たなくてよいか
                                    yusoRow.UpdateDay = DateTime.Now;
                                }
                                else
                                {
                                    // when calc "doing" record warning message and continue
                                    // TODO: メッセージ記録
                                    continue;
                                }
                            }

                            /* -- t_keisan insert or update -- */
                            var keisanFillCnt = keisanAdp.FillByKeisanKey(makeCalcDs.t_keisan, calcKeys.KeisanKey, calcYm);
                            var keisanQ = makeCalcDs.t_keisan.Where(r => r.keisan_key == calcKeys.KeisanKey);
                            if (keisanFillCnt == 0 && keisanQ.Count() == 0)
                            {
                                // set data to keisanRow and insert
                                var newKeisanRow = SetKeisanDataFromShkJskData(makeCalcDs.t_keisan.Newt_keisanRow(), shkJskRow);
                                newKeisanRow.keisan_key = calcKeys.KeisanKey;
                                newKeisanRow.yuso_key = calcKeys.YusoKey;
                                newKeisanRow.calc_ym = calcYm;
                                newKeisanRow.UpdateDay = DateTime.Now;
                                makeCalcDs.t_keisan.Addt_keisanRow(newKeisanRow);
                            }
                            else
                            {
                                var keisanRow = keisanQ.First();
                                keisanRow.yuso_means_kbn = shkJskRow.yusocode;
                                keisanRow.max_flg = 0;
                                // TODO: 登録更新情報設定の共通化
                                keisanRow.UpdateDay = DateTime.Now;
                            }

                            /* -- t_detail insert -- */
                            // make sure there's no same data before insert
                            var detailFillCnt = detailAdp.FillBySlipKey(makeCalcDs.t_detail,
                                shkJskRow.senderordcode.ToString(), shkJskRow.sendercodesubno, shkJskRow.senderorddtlcode);
                            var detailQ = makeCalcDs.t_detail.Where(
                                r => r.slip_no == shkJskRow.senderordcode.ToString() &&
                                    r.slip_suffix_no == shkJskRow.sendercodesubno &&
                                    r.slip_detail_no == shkJskRow.senderorddtlcode);
                            if (detailFillCnt == 0 && detailQ.Count() == 0)
                            {
                                var detailRow = SetDetailDataFromShkJskData(makeCalcDs.t_detail.Newt_detailRow(), shkJskRow);
                                detailRow.keisan_key = calcKeys.KeisanKey;
                                detailRow.yuso_key = calcKeys.YusoKey;
                                detailRow.calc_ym = calcYm;
                                detailRow.UpdateDay = DateTime.Now;
                                makeCalcDs.t_detail.Addt_detailRow(detailRow);
                            }
                            else
                            {
                                // TODO: normal akema エラーメッセージ出力 log出力
                                throw new Exception("出荷実績データ重複エラー ");
                            }


                            // set inclKbn "Done"
                            shkJskRow.incl_kbn = InclKbn.Done;
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
            keyColumnNames.ToList().ForEach(colname => 
                {
                    if (t_detailRow.Table.Columns.Contains(colname.Key))
                        t_detailRow[colname.Key] = shkJskRow[colname.Value];
                });
            return t_detailRow;            
        }

        private static MakeCalcDs.t_keisanRow SetKeisanDataFromShkJskData(MakeCalcDs.t_keisanRow t_keisanRow, MakeCalcDs.t_shk_jskRow shkJskRow)
        {
            keyColumnNames.ToList().ForEach(colname =>
            {
                if (t_keisanRow.Table.Columns.Contains(colname.Key))
                    t_keisanRow[colname.Key] = shkJskRow[colname.Value];
            });
            return t_keisanRow;
        }

        private static MakeCalcDs.t_yusoRow SetYusoDataFromShkJskData(MakeCalcDs.t_yusoRow t_yusoRow, MakeCalcDs.t_shk_jskRow shkJskRow)
        {
            keyColumnNames.ToList().ForEach(colname =>
            {
                if (t_yusoRow.Table.Columns.Contains(colname.Key))
                    t_yusoRow[colname.Key] = shkJskRow[colname.Value];
            });
            return t_yusoRow;
        }
    }
}
