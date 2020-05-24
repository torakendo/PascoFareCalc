using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using static FareCalcLib.Constants;

namespace FareCalcLib
{
    public class CalcKeyGenerator
    {

        private const string delim = "|";
        private static List<string> paramNames = new List<string>(){
            "yuso_kbn",
            "contract_type",
            "orig_date",
            "vehicle_id",
            "orig_warehouse_block_cd",
            "orig_warehouse_cd",
            "yuso_mode_kbn",
            "carrier_company_cd",
            "dest_jis",
            "dest_warehouse_cd",
            "dest_cd",
            "arriving_date"
            };

        private SHA1Managed shHash;

        public CalcKeyGenerator()
        {
            this.shHash = new SHA1Managed();
        }

        public struct CalcKeys
        {
            public string YusoKey { get; set; }
            public string KeisanKey { get; set; }
        }

        public Dictionary<string, object> GetParamTable()
        {
            var paramTable = new Dictionary<string,object>();
            paramNames.ForEach(paramname => paramTable.Add(paramname, null));
            return paramTable;            
        }

        public CalcKeys getCalcKeys(Dictionary<string, object> paramTable)
        {
            ArrayList paramValues = new ArrayList();

            // check params
            foreach (var name in paramNames)
            {
                if (!paramTable.ContainsKey(name) || paramTable[name] == null)
                {
                    // TODO: エラー処理
                    throw new Exception(String.Format("{0}の値がNullです", name));
                }
            }

            var keisanKeyStr = String.Join(delim, paramTable.Values);
            string yusoKeyStr = "";
            if (paramTable["contract_type"].Equals(CnContractType.ByVehicle))
            {
                // if ByVehicle set yusoKey
                yusoKeyStr =
                    paramTable["yuso_kbn"].ToString() + delim +
                    paramTable["contract_type"].ToString() + delim +
                    paramTable["orig_date"].ToString() + delim +
                    paramTable["vehicle_id"].ToString()
                    ;
            }
            else
            {
                // if ByItem set keisanKey to yusoKey
                yusoKeyStr = keisanKeyStr;
            }

            /* -- create hash -- */
            //Create a new instance of the SHA1Managed class to create the hash value

            string GetSHA1HashedString(string value)
               => string.Join("", shHash.ComputeHash(Encoding.UTF8.GetBytes(value)).Select(x => $"{x:X2}"));

            var calcKeys = new CalcKeys();
            calcKeys.KeisanKey = GetSHA1HashedString(keisanKeyStr);
            calcKeys.YusoKey = GetSHA1HashedString(yusoKeyStr);

            return calcKeys;
        }

    }
}
