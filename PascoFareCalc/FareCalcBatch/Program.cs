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

                    }
                    scope2.Complete();
                }

                //using (SqlConnection conn = new SqlConnection(
                //    ConfigurationManager.ConnectionStrings["PcsCalcdbConnectionString"].ConnectionString))
                //{
                //int calcNo = 0;
                //conn.Open();


                //using (SqlTransaction sqlTrn = conn.BeginTransaction())
                //{
                // TODO: 計算中で残ってしまっているデータがある場合は、未計算にする
                // start batch and get calcNo
                //calcNo = Calculator.StartCalcBatch();
                //sqlTrn.Commit();
                //}

                //using (SqlTransaction sqlTrn = conn.BeginTransaction())
                //{
                // calculate
                //var calculator = new Calculator(calcNo);
                //calculator.Calcurate(conn);

                //    //計算終了処理
                //    Calc.EndCalc(calcNo);

                //    sqlTrn.Commit();
                //}
                //}
            }
            catch (Exception)
            {

                throw;
            }
        }
    }
}
