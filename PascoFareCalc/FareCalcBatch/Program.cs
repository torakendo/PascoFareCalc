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
                //using (TransactionScope scope = new TransactionScope())
                //{
                //    scope.Complete();
                //}

                using (SqlConnection conn = new SqlConnection(
                    ConfigurationManager.ConnectionStrings["PcsCalcdbConnectionString"].ConnectionString))
                {
                    int calcNo = 0;
                    conn.Open();

                    // TODO: TransactionScopeが.net coreで使えない。SqlトランザクションをTableAdapterのコマンドにセットしなければならない

                    //using (SqlTransaction sqlTrn = conn.BeginTransaction())
                    //{
                        // TODO: 計算中で残ってしまっているデータがある場合は、未計算にする
                        // start batch and get calcNo
                        calcNo = Calculator.StartCalcBatch(conn);
                        //sqlTrn.Commit();
                    //}

                    //using (SqlTransaction sqlTrn = conn.BeginTransaction())
                    //{
                        // calculate
                        var calculator = new Calculator(calcNo);
                        calculator.Calcurate(conn);

                        //    //計算終了処理
                        //    Calc.EndCalc(calcNo);

                    //    sqlTrn.Commit();
                    //}
                }
            }
            catch (Exception)
            {

                throw;
            }
        }
    }
}
