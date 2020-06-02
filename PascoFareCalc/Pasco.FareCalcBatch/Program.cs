﻿using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Text;
using System.Transactions;
using Pasco.FareCalcLib;

namespace Pasco.FareCalcBatch
{
    class Program
    {
        static void Main(string[] args)
        {
            FareCalcBatch();

            // ReCalc Test
            //TestStartCalcYusoKeyList();
            //TestCalcurate(TestStartCalcMonthlyVerifyKeyList();
        }

        public static void FareCalcBatch() 
        {
            // 計算を準備する prepare calcuration
            //  新しい計算番号を取得 get new calc number
            // yuso-tableから未完了リストを取得し、ステータスを実行に変更 get not-done list from yuso-table, and change status to doing

            // TODO: normal akema log write
            Console.WriteLine("----- START FareCalcBatch -----");

            int calcNo = 0;
            try
            {
                // TODO: normal-low akema 計算中で残ってしまっているデータを未計算に戻す処理を追加

                using (TransactionScope scope1 = new TransactionScope())
                {
                    using (SqlConnection conn = new SqlConnection(
                        ConfigurationManager.ConnectionStrings["PcsCalcdbConnectionString"].ConnectionString))
                    {
                        conn.Open();
                        calcNo = CalculateManager.StartCalcBatch(conn);
                    }
                    scope1.Complete();
                }

                // TODO: debug code for p1
                Console.WriteLine("CalcNo = {0}", calcNo);

                using (TransactionScope scope2 = new TransactionScope())
                {
                    using (SqlConnection conn = new SqlConnection(
                        ConfigurationManager.ConnectionStrings["PcsCalcdbConnectionString"].ConnectionString))
                    {
                        conn.Open();
                        var calcManager = new CalculateManager(calcNo);
                        calcManager.Connection = conn;
                        calcManager.Calcurate();

                        //計算終了処理
                        // TODO: high akema Calculate側に移す
                        calcManager.EndCalc();
                    }
                    scope2.Complete();
                }
                // TODO: debug code for p1
                Console.WriteLine(" FareCalcBatch End");
            }
            catch (Exception ex)
            {
                // TODO: function akema ログ出力
                // TODO: normal akema log
                Console.WriteLine("Error occur (calcNo={0})", calcNo);
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

        // 計算指示（輸送キー）　車両別一覧と配送先別一覧の再計算時 テスト用
        public static int TestStartCalcYusoKeyList() 
        {
            var calcNo = 0;
            using (TransactionScope scope1 = new TransactionScope())
            {
                using (SqlConnection conn = new SqlConnection(
                    ConfigurationManager.ConnectionStrings["PcsCalcdbConnectionString"].ConnectionString))
                {
                    conn.Open();
                    List<string> yusoKeyList = new List<string>();
                    yusoKeyList.Add("A40BCC7D862311C6A68396894B8A813D9C08ACC6");
                    yusoKeyList.Add("A362AF4500365229ACFB54DA9A9C909BF890FA65");
                    calcNo = CalculateManager.StartCalc(conn, yusoKeyList);
                }
                scope1.Complete();
            }
            return calcNo;
        }

        // 計算指示（月次確認キー）　月次確認登録画面の再計算時 テスト用
        public static int TestStartCalcMonthlyVerifyKeyList() 
        {
            var calcNo = 0;
            using (TransactionScope scope1 = new TransactionScope())
            {
                using (SqlConnection conn = new SqlConnection(
                    ConfigurationManager.ConnectionStrings["PcsCalcdbConnectionString"].ConnectionString))
                {
                    conn.Open();
                    List<MonthlyVerifyKey> monthlyVerifyKeyList = new List<MonthlyVerifyKey>();
                    monthlyVerifyKeyList.Add(new MonthlyVerifyKey() { CalcYm = "201909", CalcStatus = "01", YusoKbn = "02", OrigWarehouseCd = "", CarrierCompanyCode = "4904", ContractType = "02" });
                    monthlyVerifyKeyList.Add(new MonthlyVerifyKey() { CalcYm = "201909", CalcStatus = "01", YusoKbn = "01", OrigWarehouseCd = "21", CarrierCompanyCode = "5310", ContractType = "01" });
                    calcNo = CalculateManager.StartCalc(conn, monthlyVerifyKeyList);
                    Console.WriteLine("CalcNo = {0}", calcNo);
                }
                scope1.Complete();
            }
            return calcNo;
        }
    }
}
