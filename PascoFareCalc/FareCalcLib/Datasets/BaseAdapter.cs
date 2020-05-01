using System.ComponentModel;
using System.Data.SqlClient;
using System.Reflection;

namespace FareCalcLib.Datasets
{
    public class BaseAdapter : Component
    {
        public SqlDataAdapter InnerAdapter
        {
            get
            {
                return (SqlDataAdapter)GetType().GetProperty("Adapter",
                  BindingFlags.NonPublic | BindingFlags.Instance).GetValue(this, null);
            }
        }

        public SqlCommand[] InnerCommandCollection
        {
            get
            {
                return (SqlCommand[])GetType().GetProperty("CommandCollection",
                  BindingFlags.NonPublic | BindingFlags.Instance).GetValue(this, null);
            }
        }

        public void SetTransaction(SqlTransaction sqlTrn)
        {
            foreach (var cmd in InnerCommandCollection)
            {
                cmd.Transaction = sqlTrn;
            }
        }
    }
}