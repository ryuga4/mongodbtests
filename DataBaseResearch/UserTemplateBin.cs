using System;
using System.Collections.Generic;
using System.Linq;

namespace DataBaseResearch
{
    public class UserTemplateBin
    {
        public DateTimeOffset CreationTime;

        public List<Template> Templates;
        public UserTemplateBin(Random r)
        {
            Templates = new List<Template>();

            var length = 1;
            for (int i = 0; i < length; i++)
                Templates.Add(new Template());
            CreationTime = DateTimeOffset.Now;
        }

        public List<FeatureValue> Populate(Random r)
        {
            return Templates.SelectMany(x => x.Populate(r)).ToList();
        }

    }
}