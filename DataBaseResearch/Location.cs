using System;

namespace DataBaseResearch
{
    public class Location
    {
        public string SignatureLocation;
        public string SignatureStorageType;
        public Location()
        {
            SignatureLocation = Guid.NewGuid().ToString();
            SignatureStorageType = Guid.NewGuid().ToString();
        }
    }
}