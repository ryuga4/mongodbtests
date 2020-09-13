using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataBaseResearch
{
    public class FeatureValue
    {
        [BsonId]
        public ObjectId _id;
        public List<float> Values;
        public string DataType;

        public string FeatureName;

        public FeatureValue(Random r)
        {
            _id = ObjectId.GenerateNewId();
            DataType = char.ConvertFromUtf32(100 + r.Next() % 100);
            FeatureName = DataType;
            Values = new List<float>();
            var length = r.Next() % 20;
            for (int i = 0; i < length; i++)
                Values.Add((float)r.NextDouble());
        }
    }
}
