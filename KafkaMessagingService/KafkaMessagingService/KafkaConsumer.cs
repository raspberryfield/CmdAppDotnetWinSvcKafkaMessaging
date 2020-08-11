using Confluent.Kafka;
using Raspberryfield.Protobuf.Person;
using Serilog;
using Serilog.Core;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaMessagingService
{
    class KafkaConsumer
    {        
        public IConsumer<Ignore, Person> Consumer { get; set; }

        private static ILogger _logger;
        private static ProtoDeserializer<Person> _protoDeserializer;
        

        public  KafkaConsumer(ILogger logger, ProtoDeserializer<Person> protoDeserializer)
        {
            _logger = logger;
            _protoDeserializer = protoDeserializer;           
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
                    Console.WriteLine($" >> Error Consumer: {e.Reason}");
                    _logger.Error($" >> Error Consumer: {e.Reason}");
                })
                .Build();
            return consumer;
        }
    }
}
