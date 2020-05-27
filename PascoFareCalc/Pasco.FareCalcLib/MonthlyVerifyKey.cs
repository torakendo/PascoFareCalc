using System;
using System.Collections.Generic;
using System.Text;

namespace Pasco.FareCalcLib
{
    public struct MonthlyVerifyKey
    {
        public string CalcYm { get; set; }
        public string CalcStatus { get; set; }
        public string YusoKbn { get; set; }
        public string OrigWarehouseCd { get; set; }
        public string CarrierCompanyCode { get; set; }
        public string ContractType { get; set; }
    }
}
