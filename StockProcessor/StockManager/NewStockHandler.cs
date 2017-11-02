using System;
using System.Collections.Generic;
using System.Text;

namespace StockProcessor.StockManager
{
    public class NewStockHandler
    {
        public void Accept(string message)
        {
            //Insert business logic here
            Console.WriteLine(String.Format("***New stock message received: {0}", message));
        }
    }
}
