using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Transactions;
using FareCalcLib;

namespace FareCalcBatch
{
    class Program
    {
        static void Main(string[] args)
        {
            /*
             * prepare calcuration
             *  get new calc number
             *  get not-done list from yuso-table, and change status to doing
             *  
             *  
             */

            try
            {
                // TODO: 計算中で残ってしまっているデータを未計算に戻す処理を追加

                int calcNo;
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
                        //calcManager.EndCalcBatch();
                    }
                    scope2.Complete();
                }
            }
            catch (Exception)
            {

                throw;
            }
        }
    }
}
