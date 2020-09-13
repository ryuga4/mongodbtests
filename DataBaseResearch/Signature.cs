using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataBaseResearch
{
    public class Signature
    {
        [BsonId]
        public ObjectId _id;

        public DateTimeOffset TimeAdded;
        public List<string> SignatureTypes;
        public List<string> SignatureAuthenticityTypes;
        public List<string> SignatureStoringTypes;
        public List<ObjectId> Values;
        public List<Location> SignatureLocations;

        public Signature(Random r)
        {
            _id = ObjectId.GenerateNewId();
            TimeAdded = DateTimeOffset.Now;
            SignatureTypes = new List<string>();
            var length = r.Next() % 5;
            for (int i = 0; i < length; i++)
                SignatureTypes.Add(Guid.NewGuid().ToString());
            SignatureAuthenticityTypes = new List<string>();
            length = r.Next() % 5;
            for (int i = 0; i < length; i++)
                SignatureAuthenticityTypes.Add(Guid.NewGuid().ToString());
            SignatureStoringTypes = new List<string>();
            length = r.Next() % 5;
            for (int i = 0; i < length; i++)
                SignatureStoringTypes.Add(Guid.NewGuid().ToString());
            Values = new List<ObjectId>();

            SignatureLocations = new List<Location>();
            SignatureTypes = new List<string>();
            length = r.Next() % 5;
            for (int i = 0; i < length; i++)
                SignatureLocations.Add(new Location());

        }

        public List<FeatureValue> Populate(Random r)
        {
            var values = new List<FeatureValue>();
            var length = 1+r.Next() % 99;
            for (int i = 0; i < length; i++)
                values.Add(new FeatureValue(r));
            Values = values.Select(x => x._id).ToList();
            return values;
        }
    }
}
