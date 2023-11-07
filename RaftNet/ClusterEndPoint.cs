namespace RaftNet
{

    [ProtoBuf.ProtoContract()]
    public class ClusterEndPoint
    {
       public string Host {  get; set; }

        public int Port { get; set; }

    }
}
