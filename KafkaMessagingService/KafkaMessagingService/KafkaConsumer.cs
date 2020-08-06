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

        static KafkaConsumer()//Todo, add logger and ProtoDeserializer as dependency injections.
        {
            Consumer = CreateKafkaConsumer();
        }

        private static IConsumer<Ignore, Person> CreateKafkaConsumer()
        {
            var consumerConfig = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "172.18.82.14:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var consumer = new ConsumerBuilder<Ignore, Person>(consumerConfig)
                .SetValueDeserializer(new ProtoDeserializer<Person>())
                .SetErrorHandler((_, e) => { 
                    Console.WriteLine($"Error: {e.Reason}");
                    LogHandler.Logger.Error(e.Reason);
                })
                .Build();
            return consumer;
        }
    }
}
