using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataBaseResearch
{
    public class UserInSystem
    {
        [BsonId]
        public ObjectId _id;
        public Guid UserGuid;

        public UserTemplateBin UserTemplateBin;
        public List<Signature> Signatures;
        public UserInSystem(Random r)
        {
            Signatures = new List<Signature>();
            var length = 6;
            for (int i = 0; i < length; i++)
                Signatures.Add(new Signature(r));
            UserGuid = Guid.NewGuid();
            UserTemplateBin = new UserTemplateBin(r);
        }

        public List<FeatureValue> Populate(Random r)
        {
            return UserTemplateBin.Populate(r).Concat(Signatures.SelectMany(x=>x.Populate(r))).ToList();
        }
    }
}
