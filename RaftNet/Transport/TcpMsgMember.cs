/* 
  Copyright (C) 2018 tiesky.com / Alex Solovyov
  It's a free software for those, who think that it should be free.
*/
using DBreeze.Utils;
using RaftNet;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Raft.Transport
{
    internal class TcpMsgMember: Biser.IEncoder
    {
        public TcpMsgMember()
        {
        }

        public string Host { get; set; }
        
        public int Port { get; set; }

        public MemberCmdType MemberCmdType { get; set; }

        public byte[] ClusterEndPoints { get; set; }

        public Biser.Encoder BiserEncoder(Biser.Encoder existingEncoder = null)
        {
            Biser.Encoder enc = new Biser.Encoder(existingEncoder);

            enc
            .Add(Host)
            .Add(Port)
            .Add((int)MemberCmdType)
            .Add (ClusterEndPoints)
            ;
            return enc;
        }

        public static TcpMsgMember BiserDecode(byte[] enc = null, Biser.Decoder extDecoder = null) //!!!!!!!!!!!!!! change return type
        {
            Biser.Decoder decoder = null;
            if (extDecoder == null)
            {
                if (enc == null || enc.Length == 0)
                    return null;
                decoder = new Biser.Decoder(enc);
                if (decoder.CheckNull())
                    return null;
            }
            else
            {
                if (extDecoder.CheckNull())
                    return null;
                else
                    decoder = extDecoder;
            }

            TcpMsgMember m = new TcpMsgMember(); 

            m.Host = decoder.GetString();
            m.Port = decoder.GetInt();
            m.MemberCmdType=(MemberCmdType)decoder.GetInt();
           m.ClusterEndPoints=decoder.GetByteArray();

            return m;
        }
    }
}