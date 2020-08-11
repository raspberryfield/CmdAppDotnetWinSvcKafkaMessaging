using Confluent.Kafka;
using Raspberryfield.Protobuf.Person;
using Serilog.Core;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaMessagingService
{
    class KafkaConsumer
    {
        public static IConsumer<Ignore, Person> Consumer { get; set; }
        private static Logger _logger;
        private static ProtoDeserializer<Person> _protoDeserializer;

        static  KafkaConsumer()
        {
            _logger = LogHandler.Logger;
            _protoDeserializer = new ProtoDeserializer<Person>();           
            Consumer = CreateKafkaConsumer();
        }

        private static IConsumer<Ignore, Person> CreateKafkaConsumer()
        {
            var consumerConfig = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "192.168.39.121:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Ignore, Person>(consumerConfig)
                .SetValueDeserializer(_protoDeserializer)
                .SetErrorHandler((_, e) => { 
                    Console.WriteLine($"Error: {e.Reason}");
                    _logger.Error(e.Reason);
                })
                .Build();
            return consumer;
        }
    }
}
