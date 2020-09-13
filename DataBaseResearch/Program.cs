using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DataBaseResearch
{
    class Program
    {
        static void Main(string[] args)
        {
            //GenerateReal(@"C:\Users\micjan11\Desktop\gen");
            var r = new Random();


            var ids = AllUserIds();
            var binIds = AllBinIds();
            MongoTest1(r, ids);
            var t1 = Test(() => MongoTest1(r, ids), 100);
            var t2 = Test(() => MongoTest2(r, ids), 100);
            var t3 = Test(() => MongoTest3(r, ids), 100);
            var t4 = Test(() => MongoTest4(r, ids), 100);
            var t5 = Test(() => MongoTest5(r), 100);
            var t6 = Test(() => MongoTest6(r), 100);

            var t7 = Test(() => MongoTest7(r, 1, binIds), 100);
            var t8 = Test(() => MongoTest8(r, 1, binIds), 100);
            //var t9 = Test(() => MongoTest6(r, 100), 10);

            var t9 = Test(() => MongoTest7(r, 5, binIds), 100);
            var t10 = Test(() => MongoTest8(r, 5, binIds), 100);
            var t11 = Test(() => MongoTest7(r, 10, binIds), 100);
            var t12 = Test(() => MongoTest8(r, 10, binIds), 100);
            var t13 = Test(() => MongoTest7(r, 20, binIds), 100);
            var t14 = Test(() => MongoTest8(r, 20, binIds), 100);
            Console.WriteLine($"Losowy user i id podpisów {t1}");
            Console.WriteLine($"Losowy user i podpisy z wartościami {t2}");
            Console.WriteLine($"Losowy user i template z wartościami {t3}");
            Console.WriteLine($"Losowy user i id templatów {t4}");
            Console.WriteLine($"Wszystkie centroidy {t5}");
            Console.WriteLine($"Wszystkie centroidy z wartościami {t6}");
            Console.WriteLine($"1 centroid z wartościami z id podpisów {t7}");
            Console.WriteLine($"1 centroid z podpisami z wartościami i z wartościami podpisów {t8}");
            Console.WriteLine($"5 centroidów z wartościami z id podpisów {t9}");
            Console.WriteLine($"5 centroidów z podpisami z wartościami i z wartościami podpisów {t10}");
            Console.WriteLine($"10 centroidów z wartościami z id podpisów {t11}");
            Console.WriteLine($"10 centroidów z podpisami z wartościami i z wartościami podpisów {t12}");
            Console.WriteLine($"20 centroidów z wartościami z id podpisów {t13}");
            Console.WriteLine($"20 centroidów z podpisami z wartościami i z wartościami podpisów {t14}");


            #region old
            //ReadFromMongoDB();
            //var path = @"C:\Users\micjan11\Desktop\db_big2.json";
            //Gen(path, 1000000);
            //PopulateMSSQL(Read(path));
            //PopulateMongoDB(Read(path));
            //PopulatePostgreSQL(Read(path));


            //            TestMSSQL();
            //            TestMONGO();
            //            TestPSQL();
            //;
            //            //var a = Test(ReadFromPostgreSQL, 10);
            ////var b =Test(ReadFromPostgreSQLJSON, 100);
            ////var c =Test(ReadFromPostgreSQLJSONB, 100);

            //var d = Test(ReadFromMongoDB, 10);
            //var e = Test(ReadFromMSSQL, 10);
            //Console.WriteLine($"PSQL: {a}");
            ////Console.WriteLine($"PSQL JSON: {b}");
            ////Console.WriteLine($"PSQL JSONB: {c}");
            //Console.WriteLine($"MONGODB: {d}");
            //Console.WriteLine($"MSSQL: {e}");
            ////ReadFromPostgreSQL();
            ////ReadFromPostgreSQLJSON();
            //ReadFromPostgreSQLJSONB();

            //ReadFromMongoDB();
            #endregion
            Console.ReadKey();
        }

        private static void GenerateReal(string path)
        {

            var client = new MongoClient("mongodb://localhost:27017");


            var database = client.GetDatabase("mydb");
            var users = database.GetCollection<UserInSystem>("Users");
            var bins = database.GetCollection<Bin>("Bins");
            var features = database.GetCollection<FeatureValue>("Features");
            users.DeleteMany(x => true);
            bins.DeleteMany(x => true);
            features.DeleteMany(x => true);
            for (int xd = 0;xd < 1; xd++){


                var r = new Random();
                var NumOfUsers = 100000;


                for (int i = 0; i < NumOfUsers; i++)
                {
                    if (i % 10000 == 0)
                        Console.WriteLine($"User {i} generated");
                    users.InsertOne(new UserInSystem(r));

                }

                var signatureIds = new List<ObjectId>();
                users.Find(x => true).ForEachAsync(x =>
                {
                    signatureIds.AddRange(x.Signatures.Select(y => y._id));
                }
                ).Wait();
                var NumOfBins = (int)Math.Sqrt((double)signatureIds.Count()) + 1;
                //var NumOfBins = (int)Math.Log((double)signatureIds.Count()) + 1;
                var chunkSize = signatureIds.Count / NumOfBins;
                for (int i = 0; i < NumOfBins; i++)
                    bins.InsertOne(new Bin(r));
                int j = 0;
                foreach (var binId in bins.Find(x => true).ToList().Select(x => x._id))
                {
                    Console.WriteLine($"Bin {j} generated");

                    var chunk = signatureIds.Take(chunkSize).ToList();
                    if (j < NumOfBins - 1)
                        signatureIds = signatureIds.Skip(chunkSize).ToList();
                    else
                        chunk = signatureIds;
                    bins.UpdateOne(x => x._id == binId, Builders<Bin>.Update.PushEach("Signatures", chunk));
                    j++;
                }


                PipelineDefinition<UserInSystem, BsonDocument> options = new BsonDocument[]
                {
                new BsonDocument("$project", new BsonDocument("Signatures", 1)),
                new BsonDocument("$unwind","$Signatures"),
                new BsonDocument("$group", new BsonDocument{{"_id","result"},{"sum",new BsonDocument("$sum",1)}})
                };


                var totalCount = users.Aggregate(options).First().GetElement("sum").Value.ToInt64() + (int)bins.CountDocuments(x => true);
                j = 0;

                users.Find(x => true).ForEachAsync(user =>
                {
                    var values = user.Populate(r);
                    users.ReplaceOne(new FilterDefinitionBuilder<UserInSystem>().Eq("_id", user._id), user);
                    features.InsertMany(values);

                    Console.WriteLine($"{j} items of {totalCount} populated {xd}");

                    j += user.Signatures.Count();
                }).Wait();
                bins.Find(x => true).ForEachAsync(bin => {
                    var values = bin.Populate(r);
                    bins.ReplaceOne(new FilterDefinitionBuilder<Bin>().Eq("_id", bin._id), bin);
                    features.InsertMany(values);
                    Console.WriteLine($"{j} items of {totalCount} populated {xd}");
                    j++;
                }).Wait();
               
            }
            //File.AppendAllText(path + "_users.json", JsonConvert.SerializeObject(Users));
            //File.AppendAllText(path + "_bins.json", JsonConvert.SerializeObject(Bins));
            //File.AppendAllText(path + "_features.json", JsonConvert.SerializeObject(FeatureValues));
        }

        public static void TestMSSQL()
        {
            foreach (var file in Directory.EnumerateFiles(@"C:\Users\micjan11\Desktop\db\mssql\"))
            {
                var command = File.ReadAllText(file);
                var sum = Test(() => ReadFromMSSQL(command), 1);
                Console.WriteLine($"MSSQL {file}: {sum}");
            }
        }
        public static  void TestPSQL()
        {
            foreach (var file in Directory.EnumerateFiles(@"C:\Users\micjan11\Desktop\db\psql\"))
            {
                var command = File.ReadAllText(file);
                var sum = Test(() => ReadFromPostgreSQL(command), 1);
                Console.WriteLine($"PSQL {file}: {sum}");
            }
        }


        public static void TestMONGO()
        {

            foreach (var file in Directory.EnumerateFiles(@"C:\Users\micjan11\Desktop\db\mongo\"))
            {
                var command = File.ReadAllText(file);
                var xd = new JsonFilterDefinition<Record>(command);
                var sum = Test(() => ReadFromMongoDB(xd), 1);
                Console.WriteLine($"MONGO {file}: {sum}");
            }
        }




        public static long Test(Func<long> f, int n)
        {
            long sum = 0;
            for (int i = 0; i < n; i++)
            {
                var t = new Task<long>(() =>
                {
                    return f();
                });
                t.Start();
                t.Wait();
                sum += t.Result;

            }
            return sum / n;
        }

        public static long TestAsync(Func<long> f, int n)
        {
            int failed = 0;
            long sum = 0;
            Stopwatch w = Stopwatch.StartNew();
            List<Task<long>> tasks = new List<Task<long>>();
            for (int i = 0; i < n; i++)
            {
                var t = new Task<long>(() => 
                {
                    try { return f(); }
                    catch (Exception e)
                    { failed++; return 0; }
                });
                t.Start();
                tasks.Add(t);

            }
            tasks.ForEach(x => x.Wait());

            Console.WriteLine($"Failed: {failed}");
            if (failed < n)
            {
                var result = tasks.Select(x => x.Result).Sum() / (n - failed);
                w.Stop();
                Console.WriteLine($"Average: {w.ElapsedMilliseconds / n}");

                return result;
            } else return -1;
        }



        private static void PopulatePostgreSQLJSON(List<Record> list)
        {
            var cs = "Host=localhost;Username=postgres;Password=mleko02;Database=postgres";

            using (var conn = new NpgsqlConnection(cs))
            {
                conn.Open();
                int i = 0;
                foreach (var record in list) {
                    using (var cmd = new NpgsqlCommand($"INSERT INTO record_jsonb (id,value) values ({i}, '{record.ToString()}')", conn))
                    {
                        var rows = cmd.ExecuteNonQuery();
                        Console.WriteLine(i);
                        i++;
                    }
                }
            }
        }

        static List<Record> Read(string path)
        {
            List<Record> records = JsonConvert.DeserializeObject<List<Record>>(File.ReadAllText(path));
            return records;
        }

        static void Gen(string path, int count)
        {
            File.Delete(path);
            Random r = new Random();
            Console.WriteLine("Hello World!");
            List<Record> records = new List<Record>();
            for (int i = 0; i < count; i++)
            {
                Console.WriteLine(i);
                records.Add(new Record(r));
            }
            File.AppendAllText(path, JsonConvert.SerializeObject(records));

        }

        static void PopulateMongoDB(List<Record> records)
        {
            var client = new MongoClient("mongodb://localhost:27017");

            var collectionName = "Records";
            var database = client.GetDatabase("mydb");
            database.GetCollection<Record>(collectionName).InsertMany(records);
            Console.WriteLine("Done!");
        }
        static long MongoTest1(Random r, List<ObjectId> allIds)
        {
            var randomId = allIds.OrderBy(x => r.Next()).First();
            Stopwatch w = Stopwatch.StartNew();

            var client = new MongoClient("mongodb://localhost:27017");

            var collectionName = "Users";
            var database = client.GetDatabase("mydb");
            var users = database.GetCollection<UserInSystem>(collectionName);
            var user = users.Find(x => x._id == randomId).First();
            var ids = user.Signatures.SelectMany(x=>x.Values).ToList();
            w.Stop();
            Console.WriteLine($"1 : {w.ElapsedMilliseconds}");
            return w.ElapsedMilliseconds;
        }
        static long MongoTest2(Random r, List<ObjectId> allIds)
        {
            var randomId = allIds.OrderBy(x => r.Next()).First();

            Stopwatch w = Stopwatch.StartNew();
            var client = new MongoClient("mongodb://localhost:27017");

            var collectionName = "Users";
            var database = client.GetDatabase("mydb");
            var users = database.GetCollection<UserInSystem>(collectionName);
            var features = database.GetCollection<FeatureValue>("Features");

            var user = users.Find(x => x._id == randomId).First();
            var ids = user.Signatures.SelectMany(x => x.Values).ToList();

            var featureValues = features.Find(new FilterDefinitionBuilder<FeatureValue>().In("_id", ids)).ToList();
            w.Stop();

            Console.WriteLine($"2 : {w.ElapsedMilliseconds}");
            return w.ElapsedMilliseconds;
        }
        static long MongoTest3(Random r,List<ObjectId> allIds)
        {
            var randomId = allIds.OrderBy(x => r.Next()).First();
            Stopwatch w = Stopwatch.StartNew();

            var client = new MongoClient("mongodb://localhost:27017");

            var collectionName = "Users";
            var database = client.GetDatabase("mydb");
            var users = database.GetCollection<UserInSystem>(collectionName);
            var features = database.GetCollection<FeatureValue>("Features");

            var user = users.Find(x => x._id == randomId).First();
            var ids = user.UserTemplateBin.Templates.SelectMany(x => x.Values).ToList();
            var featureValues = features.Find(new FilterDefinitionBuilder<FeatureValue>().In("_id", ids)).ToList();
            w.Stop();

            Console.WriteLine($"3 : {w.ElapsedMilliseconds}");
            return w.ElapsedMilliseconds;
        }
        static long MongoTest4(Random r, List<ObjectId> allIds)
        {
            var randomId = allIds.OrderBy(x => r.Next()).First();
            Stopwatch w = Stopwatch.StartNew();

            var client = new MongoClient("mongodb://localhost:27017");

            var collectionName = "Users";
            var database = client.GetDatabase("mydb");
            var users = database.GetCollection<UserInSystem>(collectionName);
            var features = database.GetCollection<FeatureValue>("Features");
            var user = users.Find(x => x._id == randomId).First();
            var ids = user.UserTemplateBin.Templates.SelectMany(x => x.Values).ToList();
            w.Stop();

            Console.WriteLine($"4 : {w.ElapsedMilliseconds}");
            return w.ElapsedMilliseconds;
        }
        static long MongoTest5(Random r)
        {
            Stopwatch w = Stopwatch.StartNew();

            var client = new MongoClient("mongodb://localhost:27017");
            
            var database = client.GetDatabase("mydb");
            var bins = database.GetCollection<Bin>("Bins");
            PipelineDefinition<Bin, BsonDocument> options = new BsonDocument[]
            {
                new BsonDocument("$project", new BsonDocument{{"Centroid", 1 },{ "_id", 0 } })                
            };
            var allBins = bins.Aggregate(options).ToList();
            w.Stop();

            Console.WriteLine($"wszystkie centroidy : {w.ElapsedMilliseconds}");
            return w.ElapsedMilliseconds;
        }

        static long MongoTest6(Random r)
        {
            Stopwatch w = Stopwatch.StartNew();

            var client = new MongoClient("mongodb://localhost:27017");

            var database = client.GetDatabase("mydb");
            var bins = database.GetCollection<Bin>("Bins");
            PipelineDefinition<Bin, Centroid> options = new BsonDocument[]
            {
                new BsonDocument("$project", new BsonDocument{{"Centroid", 1 },{ "_id", 0 } }),
                new BsonDocument("$replaceRoot", new BsonDocument("newRoot", "$Centroid"))
            };
            var allCentroids = bins.Aggregate(options).ToList();
            var ids = allCentroids.SelectMany(x => x.Values).ToList();
            var featureValues = database.GetCollection<FeatureValue>("Features").Find(new FilterDefinitionBuilder<FeatureValue>().In("_id", new BsonArray(ids))).ToList();
            w.Stop();

            Console.WriteLine($"wszystkie centroidy z wartośćiami : {w.ElapsedMilliseconds}");
            return w.ElapsedMilliseconds;
        }

        static long MongoTest7(Random r, int c, List<ObjectId> allBinIds)
        {
            var binIds = allBinIds.OrderBy(x => r.Next()).Take(c);

            Stopwatch w = Stopwatch.StartNew();
            var client = new MongoClient("mongodb://localhost:27017");

            var database = client.GetDatabase("mydb");
            var binsCollection = database.GetCollection<Bin>("Bins");
            var bins = binsCollection.Find(new FilterDefinitionBuilder<Bin>().In("_id", binIds)).ToList();
            var ids = bins.SelectMany(x => x.Centroid.Values).ToList();

            var featureValues = database.GetCollection<FeatureValue>("Features").Find(new FilterDefinitionBuilder<FeatureValue>().In("_id", ids)).ToList();
            w.Stop();

            Console.WriteLine($"{c} centroidów z wartościami i id podpisów : {w.ElapsedMilliseconds}");
            return w.ElapsedMilliseconds;
        }
        static long MongoTest8(Random r, int c, List<ObjectId> allBinIds)
        {
            
            var binIds = allBinIds.OrderBy(x=>r.Next()).Take(c);
            Stopwatch w = Stopwatch.StartNew();

            var client = new MongoClient("mongodb://localhost:27017");

            var database = client.GetDatabase("mydb");
            var bins = database.GetCollection<Bin>("Bins");

            var bin = bins.Find(new FilterDefinitionBuilder<Bin>().In("_id",binIds)).ToList();
            var ids = bin.SelectMany(x=>x.Centroid.Values);
            var featureValues = database.GetCollection<FeatureValue>("Features").Find(new FilterDefinitionBuilder<FeatureValue>().In("_id", ids)).ToList();
            var signatureIds = bin.SelectMany(x=>x.Signatures);

            PipelineDefinition<UserInSystem, Signature> opts = new BsonDocument[]
            {
                new BsonDocument("$match", new BsonDocument("Signatures",new BsonDocument("$elemMatch",new BsonDocument("_id",new BsonDocument("$in",new BsonArray(signatureIds)))))),



                 new BsonDocument("$project",
    new BsonDocument
        {
            { "_id", 0 },
            { "Signatures", 1 }
        }),
    new BsonDocument("$unwind",
    new BsonDocument("path", "$Signatures")),
    new BsonDocument("$replaceRoot", new BsonDocument("newRoot", "$Signatures")),
    new BsonDocument("$match", new BsonDocument("_id",new BsonDocument("$in", new BsonArray(signatureIds))))
            };


            var signatures = database.GetCollection<UserInSystem>("Users").Aggregate(opts).ToList();
            var ids2 = signatures.SelectMany(x => x.Values).ToList();
            List<FeatureValue> features = new List<FeatureValue>();
            int chunks = 1;
            var size = ids2.Count;
            while (true)
            {
                try
                {
                    var copy = new List<ObjectId>(ids2);
                    for (int i = 0; i< chunks; i++)
                    {
                        features.AddRange(database.GetCollection<FeatureValue>("Features").Find(new FilterDefinitionBuilder<FeatureValue>().In("_id", copy.Take(size/chunks))).ToList());
                        copy = copy.Skip(size / chunks).ToList();
                    }
                    features.AddRange(database.GetCollection<FeatureValue>("Features").Find(new FilterDefinitionBuilder<FeatureValue>().In("_id", copy)).ToList());
                    break;
                } catch (Exception)
                {
                    features = new List<FeatureValue>();
                    chunks *= 2;
                }
            }

            w.Stop();

            Console.WriteLine($"{c} centroidów z wartościami i i podpisy z wartościami : {w.ElapsedMilliseconds}");
            return w.ElapsedMilliseconds;
        }

        static List<ObjectId> AllBinIds()
        {
            var client = new MongoClient("mongodb://localhost:27017");

            var database = client.GetDatabase("mydb");
            var bins = database.GetCollection<Bin>("Bins");
            PipelineDefinition<Bin, BsonDocument> opts = new BsonDocument[] { new BsonDocument("$project", new BsonDocument("_id", 1)) };
            return bins.Aggregate(opts).ToList().Select(x => x.GetElement("_id").Value.AsObjectId).ToList();
        }
        static List<ObjectId> AllUserIds()
        {
            var client = new MongoClient("mongodb://localhost:27017");

            var database = client.GetDatabase("mydb");
            var users = database.GetCollection<UserInSystem>("Users");

            PipelineDefinition<UserInSystem,BsonDocument> opts = new BsonDocument[] { new BsonDocument("$project", new BsonDocument("_id", 1)) };
            return users.Aggregate(opts).ToList().Select(x => x.GetElement("_id").Value.AsObjectId).ToList();
        }
        
        static long ReadFromMongoDB()
        {
            Stopwatch w = Stopwatch.StartNew();

            var client = new MongoClient("mongodb://localhost:27017");

            var collectionName = "Records";
            var database = client.GetDatabase("mydb");


            var filter = Builders<Record>.Filter.Not(Builders<Record>.Filter.Size(new StringFieldDefinition<Record, List<int>>("F"), 0));




            PipelineDefinition<Record, BsonDocument> options = new BsonDocument[]
{
    new BsonDocument("$match",
    new BsonDocument("F",
    new BsonDocument("$ne",
    new BsonArray()))),
    new BsonDocument("$project",
    new BsonDocument
        {
            { "sum",
    new BsonDocument("$sum", "$F") },
            { "id", "$_id" }
        }),
    new BsonDocument("$sort",
    new BsonDocument("sum", 1)),
    new BsonDocument("$limit",1)
};

            var collection = database.GetCollection<Record>(collectionName)
                .Aggregate(options).FirstOrDefault();
            Console.WriteLine(collection);
            w.Stop();
            Console.WriteLine($"MONGODB : {w.ElapsedMilliseconds}");
            return w.ElapsedMilliseconds;
        }
        static long ReadFromMongoDB(FilterDefinition<Record> options)
        {
            Stopwatch w = Stopwatch.StartNew();

            var client = new MongoClient("mongodb://localhost:27017");

            var collectionName = "Records";
            var database = client.GetDatabase("mydb");


            
            var collection = database.GetCollection<Record>(collectionName)
                .Find(options).ToList();
            //Console.WriteLine(collection);
            w.Stop();
            //Console.WriteLine($"MONGODB : {w.ElapsedMilliseconds}");
            return w.ElapsedMilliseconds;
        }

        static void PopulatePostgreSQL(List<Record> records)
        {
            var cs = "Host=localhost;Username=postgres;Password=mleko02;Database=postgres";

            using (var conn = new NpgsqlConnection(cs))
            {
                conn.Open();
                int i = 0;

                int j = 0;
                foreach (var record in records)
                {
                    using (var cmd = new NpgsqlCommand($"INSERT INTO record (\"A\",\"B\",\"C\",\"D\",\"E\",\"ID\") values ({record.A},{record.B.ToString().Replace(',', '.')},{record.C.ToString().Replace(',', '.')},'{record.D}','{record.E}',{i})", conn))
                    {
                        //cmd.Parameters.AddWithValue("ID", NpgsqlDbType.Integer, i);
                        //cmd.Parameters.AddWithValue("A", NpgsqlDbType.Integer, record.A);
                        //cmd.Parameters.AddWithValue("B", NpgsqlDbType.Real,record.B);
                        //cmd.Parameters.AddWithValue("C", NpgsqlDbType.Double, record.C);
                        //cmd.Parameters.AddWithValue("D", NpgsqlDbType.Char, record.D);
                        //cmd.Parameters.AddWithValue("E", NpgsqlDbType.TimestampTz, record.E);

                        Console.WriteLine(i);
                        var rows = cmd.ExecuteNonQuery();
                    }
                    foreach (var attr in record.F)
                    {
                        using (var cmd = new NpgsqlCommand($"INSERT INTO attribute (\"ID\",value, record_id) values ({j},{attr},{i})", conn))
                        {
                            //cmd.Parameters.AddWithValue("ID", NpgsqlDbType.Integer, i);
                            //cmd.Parameters.AddWithValue("A", NpgsqlDbType.Integer, record.A);
                            //cmd.Parameters.AddWithValue("B", NpgsqlDbType.Real,record.B);
                            //cmd.Parameters.AddWithValue("C", NpgsqlDbType.Double, record.C);
                            //cmd.Parameters.AddWithValue("D", NpgsqlDbType.Char, record.D);
                            //cmd.Parameters.AddWithValue("E", NpgsqlDbType.TimestampTz, record.E);

                            var rows = cmd.ExecuteNonQuery();
                        }
                        j++;
                    }
                    i++;
                }
            }

        }


        public static long ReadFromPostgreSQL()
        {
            var cs = "Host=localhost;Username=postgres;Password=mleko02;Database=postgres";
            Stopwatch w = Stopwatch.StartNew();

            using (var conn = new NpgsqlConnection(cs))
            {
                conn.Open();
                string command = "SELECT * FROM public.record WHERE \"ID\" = (SELECT a.record_id FROM public.attribute a GROUP BY a.record_id ORDER BY Sum(CAST(a.value as BIGINT)) limit 1) limit 1";
                //string command = "SELECT \"A\", \"B\", \"C\", \"D\", \"E\", public.record.\"ID\", public.attribute.value " +
                //    "FROM public.record " +
                //    "JOIN public.attribute " +
                //    "on public.attribute.record_id = public.record.\"ID\" " +
                //    "order by public.attribute.value " +
                //    "limit 1";
                using (var cmd = new NpgsqlCommand(command, conn))
                {
                    var res = cmd.ExecuteReader();
                    while (res.Read())
                    {
                        Console.WriteLine(res.GetValue(5));
                    }
                }
                w.Stop();
                Console.WriteLine($"PSQL {w.ElapsedMilliseconds}");

                return w.ElapsedMilliseconds;
            }
        }
        public static long ReadFromPostgreSQL(string command)
        {
            var cs = "Host=localhost;Username=postgres;Password=mleko02;Database=postgres";
            Stopwatch w = Stopwatch.StartNew();

            using (var conn = new NpgsqlConnection(cs))
            {
                conn.Open();
                //string command = "SELECT \"A\", \"B\", \"C\", \"D\", \"E\", public.record.\"ID\", public.attribute.value " +
                //    "FROM public.record " +
                //    "JOIN public.attribute " +
                //    "on public.attribute.record_id = public.record.\"ID\" " +
                //    "order by public.attribute.value " +
                //    "limit 1";
                using (var cmd = new NpgsqlCommand(command, conn))
                {
                    var res = cmd.ExecuteReader();
                    while (res.Read())
                    {
                        //Console.WriteLine(res.GetValue(5));
                    }
                }
                w.Stop();
                //Console.WriteLine($"PSQL {w.ElapsedMilliseconds}");

                return w.ElapsedMilliseconds;
            }
        }
        public static long ReadFromMSSQL(string command)
        {
            var connectionString = "data source=(localdb)\\MSSQLLocalDB; persist security info = True;Integrated Security = SSPI;";
            var sqlUpdate = "UPDATE [MealHintTest].[dbo].[record] SET [A] = [A]+1  WHERE [ID]=0 ";
            Stopwatch w = Stopwatch.StartNew();
            long r = 0;
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                Console.WriteLine(conn.ClientConnectionId);
                using (var cmd = new SqlCommand(command, conn))
                {
                    var res = cmd.ExecuteReader();
                    while (res.Read())
                    {
                        //Console.WriteLine(res.GetValue(0));
                    }
                }
                w.Stop();
                //using (var conn2 = new SqlConnection(connectionString))
                //{
                //    conn2.Open();
                //    using (var cmd = new SqlCommand(sqlUpdate, conn2))
                //    {
                //        // original 272464377
                //        var rows = cmd.ExecuteNonQuery();
                //        Console.WriteLine($"{rows} rows affected");
                //    }
                //}
                //Console.WriteLine($"MSSQL {w.ElapsedMilliseconds}");


                r = w.ElapsedMilliseconds;
                conn.Close();
                conn.Dispose();
            }
            return r;
        }
        public static long ReadFromMSSQL()
        {
            var connectionString = "data source=(localdb)\\MSSQLLocalDB; persist security info = True;Integrated Security = SSPI;";

            Stopwatch w = Stopwatch.StartNew();

            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                string command = "SELECT TOP 1 * FROM [MealHintTest].[dbo].[record] WHERE ID = (SELECT TOP 1 a.record_id FROM[MealHintTest].[dbo].[attribute] a GROUP BY a.record_id ORDER BY Sum(CAST(a.v as BIGINT)))";
                using (var cmd = new SqlCommand(command, conn))
                {
                    var res = cmd.ExecuteReader();
                    while (res.Read())
                    {
                        Console.WriteLine(res.GetValue(0));
                    }
                }
                w.Stop();
                Console.WriteLine($"MSSQL {w.ElapsedMilliseconds}");

                return w.ElapsedMilliseconds;
            }
        }
        public static long ReadFromPostgreSQLJSON()
        {
            var cs = "Host=localhost;Username=postgres;Password=mleko02;Database=postgres";
            Stopwatch w = Stopwatch.StartNew();

            using (var conn = new NpgsqlConnection(cs))
            {
                conn.Open();
                string command =
"SELECT jazda.* "+
"FROM(SELECT p.*, (select Min(x::text::int) from json_array_elements(p.value->'F') as x) as x " +
"FROM public.record_json as p) as jazda " +
"ORDER BY jazda.x " +
"limit 1";
                using (var cmd = new NpgsqlCommand(command, conn))
                {
                    var res = cmd.ExecuteReader();
                    while (res.Read())
                    {
                        Console.WriteLine(res.GetValue(2));
                    }
                }
                w.Stop();
                Console.WriteLine($"PSQL JSON: {w.ElapsedMilliseconds}");
                return w.ElapsedMilliseconds;
            }
        }
        public static long ReadFromPostgreSQLJSONB()
        {
            var cs = "Host=localhost;Username=postgres;Password=mleko02;Database=postgres";
            Stopwatch w = Stopwatch.StartNew();

            using (var conn = new NpgsqlConnection(cs))
            {
                conn.Open();
                string command =
"SELECT jazda.* " +
"FROM(SELECT p.*, (select Min(x::text::int) from jsonb_array_elements(p.value->'F') as x) as x " +
"FROM public.record_jsonb as p) as jazda " +
"ORDER BY jazda.x " +
"limit 1";
                using (var cmd = new NpgsqlCommand(command, conn))
                {
                    var res = cmd.ExecuteReader();
                    while (res.Read())
                    {
                        Console.WriteLine(res.GetValue(2));
                    }
                }
                w.Stop();
                Console.WriteLine($"PSQL JSONB: {w.ElapsedMilliseconds}");
                return w.ElapsedMilliseconds;
            }
        }


        public static void PopulateMSSQL(List<Record> records)
        {
            var connectionString = "data source=(localdb)\\MSSQLLocalDB; persist security info = True;Integrated Security = SSPI;";


            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();

                var i = 0;
                var j = 0;
                foreach (var record in records)
                {
                    string time = record.E.ToString("yyyy-MM-dd");
                    var sql = $"INSERT INTO [MealHintTest].[dbo].[record] (\"A\",\"B\",\"C\",\"D\",\"E\",\"ID\") values ({record.A},{record.B.ToString().Replace(',', '.')},{record.C.ToString().Replace(',', '.')},'{record.D}','{time}',{i})";
                    using (var cmd = new SqlCommand(sql,conn))
                    {
                        var rows = cmd.ExecuteNonQuery();
                    }

                    foreach (var attr in record.F)
                    {
                        using (var cmd = new SqlCommand($"INSERT INTO [MealHintTest].[dbo].[attribute] (\"ID\",v, record_id) values ({j},{attr},{i})", conn))
                        {
                            //cmd.Parameters.AddWithValue("ID", NpgsqlDbType.Integer, i);
                            //cmd.Parameters.AddWithValue("A", NpgsqlDbType.Integer, record.A);
                            //cmd.Parameters.AddWithValue("B", NpgsqlDbType.Real,record.B);
                            //cmd.Parameters.AddWithValue("C", NpgsqlDbType.Double, record.C);
                            //cmd.Parameters.AddWithValue("D", NpgsqlDbType.Char, record.D);
                            //cmd.Parameters.AddWithValue("E", NpgsqlDbType.TimestampTz, record.E);

                            var rows = cmd.ExecuteNonQuery();
                        }
                        j++;
                    }
                    Console.WriteLine(i);

                    i++;
                }
            }
        }
    }
}
