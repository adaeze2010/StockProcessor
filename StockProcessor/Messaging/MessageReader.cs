using System;
using System.Collections.Generic;
using System.Text;

namespace StockProcessor.Messaging
{
    public interface MessageReader
    {
        void Start();
        void Stop();
    }
}
