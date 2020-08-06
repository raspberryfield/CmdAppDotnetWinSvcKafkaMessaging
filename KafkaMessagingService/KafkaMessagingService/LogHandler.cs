using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Serilog;
using Serilog.Core;

namespace KafkaMessagingService
{
    class LogHandler
    {
        private static readonly string _pathLogDirectory; //e.g. @"C:\Temp\MyServices\Logger\Logs"; If this is not a valid path, error 1053 may occur in service.
        private static string _pathLogFile;
        public static Logger Logger { get; set; }

        static LogHandler()
        {
            _pathLogDirectory = SetPathToLogDirectory();
            CreateLogFile();
            Logger = CreateLogger();
        }

        private static string SetPathToLogDirectory()
        {   //This should return the path of the executing assembly of this application, even if it is called from Service Manager in Windows.
            return Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location).ToString() + "\\Logs"; //Directory.GetCurrentDirectory(), not alwyas works.
        }

        //Create a log file with a timestamp in the filename and a standard header in that file.
        private static void CreateLogFile()
        {
            System.IO.Directory.CreateDirectory(_pathLogDirectory);
            var appStartDatetime = DateTime.Now.ToString("yyyyMMdd_HHmmss");//e.g. 2020-08-05 13:49 -> 20200805_1346
            string fileName = appStartDatetime + " - Log.txt";
            _pathLogFile = System.IO.Path.Combine(_pathLogDirectory, fileName);
            var logHeader = "### LOG ### \n";
            System.IO.File.WriteAllText(_pathLogFile, logHeader);
        }

        //Create a logger from Serilog.Core.Logger library.
        private static Logger CreateLogger()
        {
            var logger = new LoggerConfiguration().WriteTo.File(_pathLogFile).CreateLogger();
            return logger;
        }
    }
}
