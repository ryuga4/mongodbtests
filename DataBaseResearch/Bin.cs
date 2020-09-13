using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataBaseResearch
{
    public class Bin
    {
        [BsonId]
        public ObjectId _id;
        public string BinName;
        public bool IsDefaultBin;
        public DateTimeOffset CreationTime;
        public Centroid Centroid;
        public List<ObjectId> Signatures;

        public Bin(Random r)
        {
            BinName = Guid.NewGuid().ToString();
            IsDefaultBin = false;
            CreationTime = DateTimeOffset.Now;
            Centroid = new Centroid();
            Signatures = new List<ObjectId>();
        }

        public List<FeatureValue> Populate(Random r)
        {
            return Centroid.Populate(r);
        }

        public void AddSignatures(List<ObjectId> signatures)
        {
            Signatures.AddRange(signatures);
        }
    }
}
