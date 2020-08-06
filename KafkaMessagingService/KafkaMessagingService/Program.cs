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
            logger.Information(">> Application started.");
            Console.WriteLine(">> Application started.");

            try
            {
                //var config = GetJsonConfig();
                //Console.WriteLine("json-config: " + config["key1"]);//Example how to access configurations.

                //create a KafkaConsumer
                var consumer = KafkaConsumer.Consumer;//TODO add logger and deserializer as dependencies.

                var exitCode = HostFactory.Run(x =>
                {
                    x.Service<MessageService>(s =>
                    {
                        s.ConstructUsing(messageService => new MessageService(logger));
                        s.WhenStarted(messageService => messageService.Start());
                        s.WhenStopped(messageService => messageService.Stop());
                    });

                    x.RunAsLocalSystem();

                    x.SetServiceName("MyMessageService");
                    x.SetDisplayName("My Message Service");
                    x.SetDescription("This is my test.");

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
