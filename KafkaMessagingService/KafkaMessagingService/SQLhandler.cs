using Raspberryfield.Protobuf.Person;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace KafkaMessagingService
{
    class SQLhandler
    {
        private static string _connectionString;        

        public SQLhandler()
        {
            _connectionString = "Data Source = localhost; initial catalog = Slask; Integrated Security = True;";            
        }

        public void InsertPerson(Person person)
        {
            using (SqlConnection sqlConnection = new SqlConnection(_connectionString))
            {
                sqlConnection.Open();
                SqlCommand command = new SqlCommand(null, sqlConnection);

                // Create and prepare an SQL statement.
                command.CommandText =
                                    "INSERT INTO People (Name, Age) " +
                                    "VALUES (@name, @age)";

                command.Parameters.Add("@name", SqlDbType.NVarChar, 255).Value = person.Name;
                command.Parameters.Add("@age", SqlDbType.Int).Value = person.Age;

                // Call Prepare after setting the Commandtext and Parameters.
                command.Prepare();
                command.ExecuteNonQuery();
            }
        }           

    }
}
