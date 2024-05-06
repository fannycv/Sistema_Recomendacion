using System;
using System.Threading;
using StackExchange.Redis;
using Npgsql;

namespace Worker
{
    public class Program
    {
        private static ConnectionMultiplexer _redisConn;
        private static IDatabase _redisDb;

        public static int Main(string[] args)
        {
            try
            {
                _redisConn = OpenRedisConnection();
                _redisDb = _redisConn.GetDatabase();

                // Temporizador de 20s
                var timer = new Timer(state => RefreshBooks(), null, TimeSpan.Zero, TimeSpan.FromSeconds(20));

                while (true)
                {
                    Thread.Sleep(100);

                    if (_redisConn == null || !_redisConn.IsConnected)
                    {
                        Console.WriteLine("Reconnecting Redis");
                        _redisConn = OpenRedisConnection();
                        _redisDb = _redisConn.GetDatabase();
                    }
                    RedisValue value = _redisDb.ListLeftPop("books");
                    if (!value.IsNull)
                    {
                        string bookName = value.ToString();
                        string bookList = _redisDb.StringGet(bookName);
                        Console.WriteLine($"Processing book '{bookName}'");
                        SaveToPostgres(bookName, bookList);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
                return 1;
            }
        }

        private static void RefreshBooks()
        {
            Console.WriteLine("Refrescando libros...");
            
            // Conexión a PostgreSQL
            var pgConnectionString = "Host=mi_postgres_container;Username=postgres;Password=password;Database=recomendaciones_libros";
            
            using (var pgConn = new NpgsqlConnection(pgConnectionString))
            {
                try
                {
                    pgConn.Open();
                    Console.WriteLine("Connected to PostgreSQL");

                    using (var cmdDelete = new NpgsqlCommand())
                    {
                        cmdDelete.Connection = pgConn;
                        cmdDelete.CommandText = "DELETE FROM books";
                        cmdDelete.ExecuteNonQuery();
                        Console.WriteLine("Deleted all existing records from the books table");
                    }
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"Error connecting to PostgreSQL: {ex.Message}");
                    return;
                }
            }

            var redisKeys = _redisConn.GetServer("my-redis-container", 6379).Keys(pattern: "*");

            foreach (var redisKey in redisKeys)
            {
                string bookName = redisKey.ToString();
                string bookList = _redisDb.StringGet(redisKey);
                Console.WriteLine($"Processing book '{bookName}'");
                SaveToPostgres(bookName, bookList);
            }
        }

        private static void SaveToPostgres(string bookName, string bookList)
        {
            var connectionString = "Host=mi_postgres_container;Username=postgres;Password=password;Database=recomendaciones_libros";
            Console.WriteLine($"Connecting to PostgreSQL at {connectionString}");

            using (var conn = new NpgsqlConnection(connectionString))
            {
                try
                {
                    conn.Open();
                    Console.WriteLine("Connected to PostgreSQL");

                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.CommandText = "INSERT INTO books (book_name, book_list) VALUES (@bookName, @bookList)";
                        cmd.Parameters.AddWithValue("@bookName", bookName);
                        cmd.Parameters.AddWithValue("@bookList", bookList);
                        cmd.ExecuteNonQuery();
                        Console.WriteLine($"Data inserted successfully for book '{bookName}'");
                    }
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"Error connecting to PostgreSQL: {ex.Message}");
                }
            }
        }

        private static ConnectionMultiplexer OpenRedisConnection()
        {
            var options = ConfigurationOptions.Parse("my-redis-container");
            options.ConnectTimeout = 5000;

            while (true)
            {
                try
                {
                    Console.WriteLine("Connecting to Redis");
                    var redisConn = ConnectionMultiplexer.Connect(options);
                    Console.WriteLine("Connected to Redis");
                    return redisConn;
                }
                catch (RedisConnectionException)
                {
                    Console.WriteLine("Waiting for Redis");
                    Thread.Sleep(1000);
                }
            }
        }
    }
}
