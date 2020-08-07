using Raspberryfield.Protobuf.Person;
using System;
using Topshelf;

namespace KafkaMessagingService
{
    class Program
    {
        static void Main(string[] args)
        {
            //init() Logger instance - will be used over whole application.
            var logger = LogHandler.Logger;
            logger.Information(">> Service started.");
            Console.WriteLine(">> Service started.");

            try
            {
                //var config = GetJsonConfig();
                //Console.WriteLine("json-config: " + config["key1"]);//Example how to access configurations.
                               
                //create a KafkaConsumer
                var consumer = KafkaConsumer.Consumer;

                var exitCode = HostFactory.Run(x =>
                {
                    x.Service<KafkaMessageService>(s =>
                    {
                        s.ConstructUsing(kafkaMessageService => new KafkaMessageService(logger, consumer));
                        s.WhenStarted(kafkaMessageService => kafkaMessageService.Start());
                        s.WhenStopped(kafkaMessageService => kafkaMessageService.Stop());
                    });

                    x.RunAsLocalSystem();

                    x.SetServiceName("MyKafkaMessageService");//TODO: change these settings.
                    x.SetDisplayName("My Kafka Message Service");
                    x.SetDescription("This is my service that handles messages from a local Kafka-cluster.");

                });

                int exitCodeValue = (int)Convert.ChangeType(exitCode, exitCode.GetTypeCode());
                Environment.ExitCode = exitCodeValue;

            }
            catch (Exception e)
            {
                logger.Error(e.ToString());
                Console.WriteLine(e.ToString());
            }

        }
    }
}
