using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using StockProcessor.StockManager;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace StockProcessor.Messaging.Kafka
{
    public class KafkaMessageReader : MessageReader
    {
        // The Kafka endpoint address
        //todo: Move to configuration file
        private const string kafkaEndpoint = "127.0.0.1:9092";

        // The Kafka topic we'll be using
        //todo: Move to configuration file
        private const string kafkaTopic = "stock-import";

        //todo: Move to configuration file
        private const string kafkaConsumerName = "StockProcessor";

        //todo: Move to configuration file
        private const int pollerTimeoutInMilliseconds = 5000;

        private readonly Consumer<Null, string> _consumer;

        private readonly NewStockHandler newStockHandler;

        private volatile bool _pollerCancelled;

        public KafkaMessageReader(NewStockHandler newStockHandler)
        {
            // Create the consumer configuration
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", kafkaConsumerName },
                { "bootstrap.servers", kafkaEndpoint },
            };

            // Create the consumer
            _consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8));

            this.newStockHandler = newStockHandler;
        }

        public void Start()
        {
            Console.WriteLine(String.Format("Starting kafka message reader"));

            //Reset status
            _pollerCancelled = false;

            // Subscribe to the OnMessage event
            _consumer.OnMessage += OnMessage;

            // Subscribe to the Kafka topic
            _consumer.Subscribe(new List<string>() { kafkaTopic });

            // Create new thread to constantly poll for messages
            var poller = new Thread(() => {

                while (!_pollerCancelled)
                {
                    //Console.WriteLine(String.Format("Polling for new message with timeout {0}ms", pollerTimeoutInMilliseconds));

                    // Poll for messages
                    _consumer.Poll(pollerTimeoutInMilliseconds);
                }

                Console.WriteLine(String.Format("Poller has stopped"));
            });

            poller.Start();

            Console.WriteLine(String.Format("Started kafka message reader"));
        }

        public void Stop()
        {
            Console.WriteLine(String.Format("Stopping kafka message reader"));

            //Cancel poller
            _pollerCancelled = true;

            //Waiting for poller to cancel
            Thread.Sleep(pollerTimeoutInMilliseconds + 1000);

            //Unsubscribe from topic
            _consumer.Unsubscribe();

            //Unsubscribe from event
            _consumer.OnMessage -= OnMessage;

            Console.WriteLine(String.Format("Stopped kafka message reader"));     
        }

        private void OnMessage(object sender, Message<Null, string> e)
        {
            Console.WriteLine(String.Format("New event received on {0}.{1} at {2}", e.TopicPartition, e.Offset, e.Timestamp.UtcDateTime.ToLongTimeString()));

            newStockHandler.Accept(e.Value);
        }
    }
}
