using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataBaseResearch
{
    public class Centroid
    {
        public DateTimeOffset CreationTime;
        public List<ObjectId> Values;
        public Centroid()
        {
            CreationTime = DateTimeOffset.Now;
            Values = new List<ObjectId>();
        }

        public List<FeatureValue> Populate(Random r)
        {
            var values = new List<FeatureValue>();
            var length = 1 + r.Next() % 100;
            for (int i = 0; i < length; i++)
                values.Add(new FeatureValue(r));
            Values = values.Select(x => x._id).ToList();
            return values;
        }
    }
}
