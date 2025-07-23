using Microsoft.Data.SqlClient;

namespace TransferDataBetweenTwoTables
{
    public class Program
    {
        static void Main(string[] args)
        {
            string firstConnection = "Database one";
            string secondConnection = "Database two";

            string logFile = @"\logFile.csv";
            string logPath = Directory.GetCurrentDirectory() + logFile;

            ReadFromFirstBaseInsertToSecondBase(firstConnection, secondConnection, logPath);
        }

        private static List<Guid> Ids(string firstConnection)
        {
            List<Guid> idsFirstDatabase = new List<Guid>();

            using (SqlConnection conn = new SqlConnection(firstConnection))
            {
                using (SqlCommand command = new SqlCommand("SELECT * from firstTable", conn))
                {
                    conn.Open();

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            idsFirstDatabase.Add((Guid)reader["Id"]);
                        }
                    }
                }

                conn.Close();
            }

            return idsFirstDatabase;
        }

        private static void ReadFromFirstBaseInsertToSecondBase(string firstConnection, string secondConnection, string logPath)
        {
            // Get Ids from first DB
            List<Guid> ids = Ids(firstConnection);
            // Table coloumns
            Guid id;
            string nameDB;
            string realName;
            byte[] content;
            // Read from log what is already added to second, in case program stop before transfer all data
            List<string> allLinesFromFromLog = ReadAllLinesInLog(logPath);
            string currentLineFromDB;
            int idFromFirstDB;

            using (SqlConnection connFirstDB = new SqlConnection(firstConnection))
            {
                connFirstDB.Open();

                // loop all founded ids
                for (int i = 0; i < ids.Count; i++)
                {
                    using (SqlCommand commandFirstDB = new SqlCommand($"SELECT * from table WHERE Id = \'{ids[i]}\'", connFirstDB))
                    {
                        using (SqlDataReader reader = commandFirstDB.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                idFromFirstDB = (int)reader["Id"];
                                id = (Guid)reader["Id"];
                                nameDB = reader["Name"].ToString();
                                realName = reader["RealName"].ToString();
                                content = (byte[])reader["PDF"];

                                // Get current line coming from first DB
                                currentLineFromDB = $"{idFromFirstDB}, {id}";

                                // Check log file
                                if (!allLinesFromFromLog.Contains(currentLineFromDB))
                                {
                                    // Write in second DB
                                    WriteInDB(firstConnection, id, realName, content, logPath);

                                    // Write in log file
                                    File.AppendAllText(logPath, currentLineFromDB + Environment.NewLine);
                                }
                            }
                        }
                    }
                }

                connFirstDB.Close();
            }
        }

        // Read log file
        private static List<string> ReadAllLinesInLog(string logPath)
        {
            List<string> allLinesFromLog = new List<string>();

            if (File.Exists(logPath))
            {
                allLinesFromLog = File.ReadAllLines(logPath).ToList();
            }

            return allLinesFromLog;
        }

        // Write in second DB
        private static void WriteInDB(string secondConnection, Guid id, string realName, byte[] content, string logPath)
        {
            using (SqlConnection connSecond = new SqlConnection(secondConnection))
            {
                connSecond.Open();

                using (SqlCommand command = new SqlCommand("INSERT INTO secondDBTable (Id, RealName, Content)" +
                                                                              "VALUES(@Id, @RealName, @Content)", connSecond))
                {
                    command.Parameters.Add(new SqlParameter("@Id", 1));
                    command.Parameters.Add(new SqlParameter("@RealName", 1));
                    command.Parameters.Add(new SqlParameter("@Content", ""));

                    command.Parameters["@Id"].Value = id;
                    command.Parameters["@RealName"].Value = realName;
                    command.Parameters["@Content"].Value = content;

                    command.ExecuteNonQuery();
                }

                connSecond.Close();
            }
        }
    }
}
