using Confluent.Kafka;
using Raspberryfield.Protobuf.Person;
using Serilog;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace KafkaMessagingService
{
    class KafkaMessageService
    {
        private static ILogger _logger;
        private static IConsumer<Ignore, Person> _kafkaConsumer;
        private static Thread _mainThread;

        private static bool _running;
        

        public KafkaMessageService(ILogger logger, IConsumer<Ignore, Person> kafkaConsumer)
        {
            _logger = logger;
            _kafkaConsumer = kafkaConsumer;
            _mainThread = new Thread(MainMessageService);
            _mainThread.IsBackground = true;
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

            _running = false;
            Thread.Sleep(1000); // Give time to thread to finish it work.

        }

        public static void MainMessageService()
        {
            _running = true;
            int iterator = 0;

            while (_running)
            {
                Thread.Sleep(1000);
                _logger.Information(">> Iteration: " + iterator);
                Console.WriteLine(">> Iteration: " + iterator);
                iterator++;
            }
            _logger.Information(">> After while loop in MainLogging().");
            Console.WriteLine(">> After while loop in MainLogging().");



        }
    }
}
