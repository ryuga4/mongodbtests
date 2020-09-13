using MongoDB.Bson.Serialization.Attributes;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataBaseResearch
{
    [BsonIgnoreExtraElements]
    public class Record
    {
        public int A { get; set; }
        public float B { get; set; }
        public double C { get; set; }
        public string D { get; set; }
        public DateTimeOffset E { get; set; }
        public List<int> F { get; set; }

        public Record(Random r)
        {
            A = r.Next();
            B = (float)r.NextDouble()*100;
            C = r.NextDouble()*10;
            D = Guid.NewGuid().ToString("N");
            E = new DateTimeOffset().AddDays(r.NextDouble()*10000);
            var length = (r.Next() % 5) * (r.Next() % 5) * (r.Next() % 5);
            F = new List<int>();
            for (int i = 0; i < length; i++)
                F.Add(r.Next());
        }

        public Record()
        {

        }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}
