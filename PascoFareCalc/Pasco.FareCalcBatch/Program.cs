using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Text;
using System.Transactions;
using Pasco.FareCalcLib;
using Pasco.FareCalcLib.Datasets;

namespace Pasco.FareCalcBatch
{
    class Program
    {
        static void Main(string[] args)
        {
            FareCalcBatch();
        }

        public static void FareCalcBatch() 
        {
            /*
             * prepare calcuration
             *  get new calc number
             *  get not-done list from yuso-table, and change status to doing
             *  
             *  
             */

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
    }
}
