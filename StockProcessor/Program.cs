
using StockProcessor.Messaging;
using StockProcessor.Messaging.Kafka;
using StockProcessor.StockManager;
using System;
using System.Collections.Generic;
using System.Text;

namespace StockProcessor
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Starting stock processor");
            var newStockHandler = new NewStockHandler();
            MessageReader messageReader = new KafkaMessageReader(newStockHandler);
            messageReader.Start();
            Console.WriteLine("Startup is complete. Hit Ctrl-C to exit.");
            Console.Read();
            messageReader.Stop();
           
        }
    }
}
