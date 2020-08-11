using Confluent.Kafka;
using Raspberryfield.Protobuf.Person;
using Serilog;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Topshelf;
using Topshelf.Builders;

namespace KafkaMessagingService
{
    class KafkaMessageService
    {
        private static ILogger _logger;
        private static IConsumer<Ignore, Person> _kafkaConsumer;
        private static SQLhandler _sqlHandler;
        private static Thread _mainThread;

        private static bool _running;
        private static CancellationTokenSource _cancellationTokenSrc;
        

        public KafkaMessageService(ILogger logger, IConsumer<Ignore, Person> kafkaConsumer, SQLhandler sqlHandler)
        {
            _logger = logger;
            _kafkaConsumer = kafkaConsumer;
            _sqlHandler = sqlHandler;
            _mainThread = new Thread(MainMessageService);
            _mainThread.IsBackground = true;
            _cancellationTokenSrc = new CancellationTokenSource();
        }

        public bool Start()
        {
            _logger.Information(">> Start called.");
            Console.WriteLine(">> Start called.");

            _logger.Information(">> Starting Service Thread.");
            Console.WriteLine(">> Starting Service Thread.");
            _mainThread.Start();
            
            return true;

        }

        public void Stop()
        {
            _logger.Information(">> Stop called.");
            Console.WriteLine(">> Stop called.");

            //...
            _cancellationTokenSrc.Cancel();
            _running = false;
            Thread.Sleep(1000); // Give time to thread to finish it work.

        }

        public static void MainMessageService()
        {
            _running = true;
                      
            _logger.Information(">> Start Kafka Messaging Service (MainMessageService).");
            Console.WriteLine(">> Start Kafka Messaging Service (MainMessageService).");

            using (_kafkaConsumer)
            {
                _kafkaConsumer.Subscribe("my_first_topic");//TODO: config file.

                try
                {
                    while (_running)
                    {
                        try
                        {
                            var consumerResult = _kafkaConsumer.Consume(_cancellationTokenSrc.Token);
                            _logger.Information($">> Consumed message '{consumerResult.Message.Value}' at: '{consumerResult.TopicPartitionOffset}'.");
                            Console.WriteLine($">> Consumed message '{consumerResult.Message.Value}' at: '{consumerResult.TopicPartitionOffset}'.");
                            _sqlHandler.InsertPerson(consumerResult.Message.Value);
                        }
                        catch (Exception e) //Consume exception (see. confluent examples.) can be thrown here, if you don't want to stop service on error. TODO: catch different errors.
                        {
                            _logger.Error($">> Trying to stop service. Consume Error occured: {e}");
                            Console.WriteLine($">> Trying to stop service. Consume Error occured: {e}");
                            throw new OperationCanceledException();
                        }
                    }                    
                    _logger.Information($">> Exited Consume Loop.");
                    Console.WriteLine($">> Exited Consume Loop.");                    
                }
                catch (OperationCanceledException e)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    _kafkaConsumer.Close();
                    _logger.Information($">> Kafka consumer closed: '{e}'.");
                    Console.WriteLine($">> Kafka consumer closed: '{e}'.");
                }

                _logger.Information($">> End using Kafka consumer.");
                Console.WriteLine($">> End using Kafka consumer.");

            }
            //Getting out of the loop = kill the service.
            //_logger.Error($">> Trying to kill service.");
            //Console.WriteLine($">> Trying to kill service.");
            //Environment.Exit(-1);
            _logger.Information($">> End MainMessageService()");
            Console.WriteLine($">> End MainMessageService()");

        }
    }
}
