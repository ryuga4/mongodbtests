using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DataBaseResearch
{
    public class Template
    {
        public List<ObjectId> Values;
        public DateTimeOffset CreationTime;
        public Template()
        {
            CreationTime = DateTimeOffset.Now;
            Values = new List<ObjectId>();
        }
        internal List<FeatureValue> Populate(Random r)
        {

            var values = new List<FeatureValue>();
            var length = r.Next() % 100;
            for (int i = 0; i < length; i++)
                values.Add(new FeatureValue(r));
            Values = values.Select(x => x._id).ToList();
            return values;
        }
    }
}