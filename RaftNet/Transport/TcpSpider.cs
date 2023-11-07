/* 
  Copyright (C) 2018 tiesky.com / Alex Solovyov
  It's a free software for those, who think that it should be free.
*/
using RaftNet;
using System.Net.Sockets;
using static Raft.RaftNode;

namespace Raft.Transport
{
    /// <summary>
    /// Spider manages connections for all listed nodes of the net
    /// </summary>
    internal class TcpSpider : IRaftComSender, IDisposable
    {
        ReaderWriterLockSlim _sync = new ReaderWriterLockSlim();
        Dictionary<string, TcpPeer> Peers = new Dictionary<string, TcpPeer>();     //Key - NodeUID    

        internal TcpRaftNode trn = null;

        internal  int _peerCount = 0;

        public TcpSpider(TcpRaftNode trn)
        {
            this.trn = trn;
        }

        public void AddTcpClient(TcpClient peer)
        {
            var p = new TcpPeer(peer, trn);
        }

        public void RemoveAll()
        {
            var lst = Peers.ToList();

            _sync.EnterWriteLock();
            try
            {

                Peers.Clear();
            }
            catch (Exception ex)
            {
                //throw;
            }
            finally
            {
                _sync.ExitWriteLock();
            }

            foreach (var peer in lst)
            {
                try
                {
                    if (peer.Value != null)
                    {
                        peer.Value.Dispose(true);
                    }
                }
                catch
                {
                }

            }

        }



        internal void RemovePeerFromClusterEndPoints(string endpointsid)
        {
            if (String.IsNullOrEmpty(endpointsid))
                return;

            _sync.EnterWriteLock();
            try
            {
                //if (peer == null || peer.Handshake == null)
                //    return;

                //trn.log.Log(new WarningLogEntry()
                //{
                //    LogType = WarningLogEntry.eLogType.DEBUG,
                //    Description = $"{trn.port} ({trn.rn.NodeState})> removing EP: {endpointsid}; Peers: {Peers.Count}"
                //});

                Peers.Remove(endpointsid);

                //trn.log.Log(new WarningLogEntry()
                //{
                //    LogType = WarningLogEntry.eLogType.DEBUG,
                //    Description = $"{trn.port} ({trn.rn.NodeState})> removed EP: {endpointsid}; Peers: {Peers.Count}"
                //});

            }
            catch (Exception ex)
            {
                //throw;
            }
            finally
            {
                _sync.ExitWriteLock();
            }

            this.trn.PeerIsDisconnected(endpointsid);
        }

        public void AddPeerToClusterEndPoints(TcpPeer peer, bool handshake)
        {
            _sync.EnterWriteLock();
            try
            {
                if (peer.Handshake.NodeUID == trn.GetNodeByEntityName("default").NodeAddress.NodeUId)   //Self disconnect
                {

                    trn.NodeSettings.TcpClusterEndPoints.Where(r => r.EndPointSID == peer.EndPointSID)
                        .FirstOrDefault().Me = true;

                    peer.Dispose(true);
                    return;
                }

                //Choosing priority connection
                if (!Peers.ContainsKey(peer.EndPointSID))
                {

                    if (handshake && trn.GetNodeByEntityName("default").NodeAddress.NodeUId > peer.Handshake.NodeUID)
                    {
                        trn.log.Log(new WarningLogEntry()
                        {
                            LogType = WarningLogEntry.eLogType.DEBUG,
                            Description = $"{trn.port}> !!!!!dropped{peer.Handshake.NodeListeningPort} {peer.Handshake.NodeListeningPort} on handshake as weak"
                        });


                        peer.Dispose(true);
                        return;
                    }

                    Peers[peer.EndPointSID] = peer;
                    peer.FillNodeAddress();

                    //trn.log.Log(new WarningLogEntry()
                    //{
                    //    LogType = WarningLogEntry.eLogType.DEBUG,
                    //    Description = $"{trn.port}> >>>>>>connected{peer.Handshake.NodeListeningPort}  {peer.Handshake.NodeListeningPort} by {(handshake ? "handshake" : "ACK")} with diff: {(trn.rn.NodeAddress.NodeUId - peer.Handshake.NodeUID)}"
                    //});

                    if (handshake)
                    {
                        //sending back handshake ack
                        trn.log.Log(new WarningLogEntry()
                        {
                            LogType = WarningLogEntry.eLogType.DEBUG,
                            Description = $"{trn.port} sending back handshake ack"
                        });

                        peer.Write(
                            cSprot1Parser.GetSprot1Codec(
                            new byte[] { 00, 03 }, (new TcpMsgHandshake()
                            {
                                NodeListeningPort = trn.port,
                                NodeUID = trn.GetNodeByEntityName("default").NodeAddress.NodeUId,
                            }).SerializeBiser())
                        );
                    }

                }
                else
                {
                    trn.log.Log(new WarningLogEntry()
                    {
                        LogType = WarningLogEntry.eLogType.DEBUG,
                        Description = $"{trn.port}> !!!!!dropped{peer.Handshake.NodeListeningPort} {peer.Handshake.NodeListeningPort} as existing"
                    });

                    //Sending ping on existing connection (may be it is alredy old)

                    Peers[peer.EndPointSID].Write(cSprot1Parser.GetSprot1Codec(new byte[] { 00, 05 }, null)); //ping

                    //removing incoming connection                    
                    peer.Dispose(true);

                    return;
                }
            }
            catch (Exception ex)
            {
                //throw;
            }
            finally
            {
                _sync.ExitWriteLock();
            }

        }

        public async Task Handshake()
        {
            await HandshakeTo(trn.NodeSettings.TcpClusterEndPoints);
            trn.GetNodeByEntityName("default").TM.FireEventEach(3000, RetestConnections, null, false);
        }

        async Task HandshakeTo(List<TcpClusterEndPoint> clusterEndPoints)
        {
            foreach (var el in clusterEndPoints)
            {
                try
                {
                    TcpClient cl = new TcpClient();
                    await cl.ConnectAsync(el.Host, el.Port);

                    el.Peer = new TcpPeer(cl, trn);

                    trn.log.Log(new WarningLogEntry()
                    {
                        LogType = WarningLogEntry.eLogType.DEBUG,
                        Description = $"{trn.port}> try connect {el.Host}:{el.Port}"
                    });


                    el.Peer.Write(cSprot1Parser.GetSprot1Codec(new byte[] { 00, 01 }, (new TcpMsgHandshake()
                    {
                        NodeListeningPort = trn.port,
                        NodeUID = trn.GetNodeByEntityName("default").NodeAddress.NodeUId, //Generated GUID on Node start                        
                    }).SerializeBiser()));
                }
                catch (Exception ex)
                {

                }
            }
        }

        public List<TcpPeer> GetPeers(bool useLock = true)
        {
            List<TcpPeer> peers = null;

            if (useLock)
            {
                _sync.EnterReadLock();
                try
                {
                    peers = Peers.Values.ToList();
                }
                catch (Exception ex)
                { }
                finally
                {
                    _sync.ExitReadLock();
                }
            }
            else
                peers = Peers.Values.ToList();
            return peers ?? new List<TcpPeer>();
        }

        void RetestConnections(object obj)
        {
            RetestConnectionsAsync();
        }

        async Task RetestConnectionsAsync()
        {
            try
            {
                if (!trn.GetNodeByEntityName("default").IsRunning)
                    return;

                List<TcpPeer> peers = GetPeers();
             
                if (peers.Count == trn.NodeSettings.TcpClusterEndPoints.Count - 1)
                    return;

                var list2Lookup = new HashSet<string>(peers.Select(r => r?.EndPointSID));
                var ws = trn.NodeSettings.TcpClusterEndPoints.Where(r => !r.Me && (!list2Lookup.Contains(r.EndPointSID))).ToList();
                trn.log.Log(new WarningLogEntry()
                {
                    LogType = WarningLogEntry.eLogType.DEBUG,
                    Description = $"{trn.port} 定时重连"
                });
                if (ws.Count > 0)
                {
                    //trn.log.Log(new WarningLogEntry()
                    //{
                    //    LogType = WarningLogEntry.eLogType.DEBUG,
                    //    Description = $"{trn.port} ({trn..NodeState})> peers: {peers.Count}; ceps: {trn.clusterEndPoints.Count}; will send: {ws.Count}"
                    //});

                    await HandshakeTo(ws);
                }

            }
            catch (Exception ex)
            {

            }
        }

        public void SendToAll(eRaftSignalType signalType, byte[] data, NodeAddress senderNodeAddress, string entityName, bool highPriority = false)
        {
            try
            {
                List<TcpPeer> peers = null;
                _sync.EnterReadLock();
                try
                {
                    peers = Peers.Values.ToList();
                }
                catch (Exception ex)
                {
                    //throw;
                }
                finally
                {
                    _sync.ExitReadLock();
                }

                foreach (var peer in peers)
                {

                    peer.Write(cSprot1Parser.GetSprot1Codec(new byte[] { 00, 02 },
                        (
                            new TcpMsgRaft() { EntityName = entityName, RaftSignalType = signalType, Data = data }
                        ).SerializeBiser()), highPriority);
                }
            }
            catch (Exception ex)
            {

            }
        }

        public void SendTo(NodeAddress nodeAddress, eRaftSignalType signalType, byte[] data, NodeAddress senderNodeAddress, string entityName)
        {
            try
            {
                TcpPeer peer = null;
                if (Peers.TryGetValue(nodeAddress.EndPointSID, out peer))
                {
                    peer.Write(cSprot1Parser.GetSprot1Codec(new byte[] { 00, 02 },
                       (
                           new TcpMsgRaft() { EntityName = entityName, RaftSignalType = signalType, Data = data }
                       ).SerializeBiser()));
                }
            }
            catch (Exception ex)
            {


            }



        }

        public void SendToAllFreeMessage(string msgType, string dataString = "", byte[] data = null, NodeAddress senderNodeAddress = null)
        {
            try
            {
                List<TcpPeer> peers = null;
                _sync.EnterReadLock();
                try
                {
                    peers = Peers.Values.ToList();
                }
                catch (Exception ex)
                {
                    //throw;
                }
                finally
                {
                    _sync.ExitReadLock();
                }

                foreach (var peer in peers)
                {
                    peer.Write(cSprot1Parser.GetSprot1Codec(new byte[] { 00, 04 },
                        (
                            new TcpMsg() { DataString = dataString, MsgType = msgType, Data = data }
                        ).SerializeBiser()));
                }

            }
            catch (Exception ex)
            {

            }

        }

        /// <summary>
        /// 通知其它已经运行的节点
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        public  void SendToAllMemmbers(string host, int port)
        {
            try
            {
              
                List<TcpPeer> peers = null;
                _sync.EnterReadLock();
                try
                {
                    peers = Peers.Values.ToList();
                }
                catch (Exception ex)
                {
                    //throw;
                }
                finally
                {
                    _sync.ExitReadLock();
                }

                foreach (var peer in peers)
                {

                    peer.Write(cSprot1Parser.GetSprot1Codec(new byte[] { 00, 10 },
                        (
                            new TcpMsgMember() { Host = host, Port = port, MemberCmdType = MemberCmdType.Add }
                        ).SerializeBiser()));
                }

                AddMemberToClusterEndPoints(host, port);
            }
            catch (Exception ex)
            {

            }
        }




        int disposed = 0;
        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref disposed, 1, 0) != 0)
                return;

            RemoveAll();
        }

        /// <summary>
        /// 新节点加入，重新连接新节点在内的所有节点
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        internal async void AddMemberToClusterEndPoints(string host, int port)
        {
            TcpClusterEndPoint tcpClusterEndPoint = new TcpClusterEndPoint() { Host = host, Port = port };
            if(!trn.NodeSettings.TcpClusterEndPoints.Exists(p=>p.Host==host && p.Port==port))
            {
                trn.NodeSettings.TcpClusterEndPoints.Add(tcpClusterEndPoint);
                trn.log.Log(new WarningLogEntry()
                {
                    LogType = WarningLogEntry.eLogType.DEBUG,
                    Description = $"{trn.port} 加入新节点{host}:{port}"
                });
            }
         GetMembers(host, port);
            
        }

        /// <summary>
        /// 获取集群节点
        /// </summary>
        /// <param name="host">注入的IP</param>
        /// <param name="port">注入的端口</param>
        /// <returns></returns>
        internal async Task SendToMemmberAsync(string host, int port)
        {
            TcpClusterEndPoint tcpClusterEndPoint = new TcpClusterEndPoint() { Host = host, Port = port };
            trn.NodeSettings.TcpClusterEndPoints.Add(tcpClusterEndPoint);
            trn.NodeSettings.TcpClusterEndPoints.Add(new TcpClusterEndPoint() { Host="127.0.0.1", Port= trn.port});
          
            await GetFromMemmber(host, port);
        }

        /// <summary>
        /// 获取对方的数据
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <returns></returns>
        internal async Task GetFromMemmber(string host, int port)
        {
            var clus = trn.NodeSettings.TcpClusterEndPoints.FindLast(p => p.Port == port && p.Host == host);

            while (true)
            {
                if(clus.Peer==null)
                {
                    Thread.Sleep(1000);
                    continue;
                }
                trn.log.Log(new WarningLogEntry()
                {
                    LogType = WarningLogEntry.eLogType.DEBUG,
                    Description = $"{trn.port} 获取集群节点-{host}:{port}"
                });

                clus.Peer.Write(cSprot1Parser.GetSprot1Codec(new byte[] { 00, 10 },
                           (
                               new TcpMsgMember() { Host = host, Port = port, MemberCmdType = MemberCmdType.Get }
                           ).SerializeBiser()));
                    break;
            }
          
        }

        /// <summary>
        /// 移除
        /// </summary>
        /// <param name="host">移除地址</param>
        /// <param name="port"></param>
        internal void RemoveMember(string host, int port)
        {
            var  peer= trn.NodeSettings.TcpClusterEndPoints.Find(p=>p.Host == host && p.Port == port);
            if(peer != null)
            {
                peer.Peer.Dispose();
                if(Peers.TryGetValue(peer.Peer.EndPointSID,out var p))

                {
                    if(!p.Disposed)
                    {
                        p.Dispose();
                    }
                }
            }

        }


        /// <summary>
        /// 推送集群节点
        /// </summary>
        /// <param name="host">地址</param>
        /// <param name="port"></param>
        /// <param name="from">是否是连接</param>
        internal void GetMembers(string host, int port,bool rev=true)
        {
            var peer = trn.NodeSettings.TcpClusterEndPoints.Find(p => p.Host == host && p.Port == port);
            if (peer != null)
            {
                Task.Run(() =>
                {
                    List<ClusterEndPoint> lst = new List<ClusterEndPoint>();
                    while (true)
                    {
                        if (peer.Peer == null)
                        {
                            Thread.Sleep(1000);
                            continue;
                        }
                        trn.NodeSettings.TcpClusterEndPoints.ForEach(p =>
                        {
                            lst.Add(new ClusterEndPoint { Host = p.Host, Port = p.Port });
                        });
                        var data = lst.SerializeProtobuf();
                        if (rev)
                        {
                            //回复
                            if (Peers.TryGetValue(peer.Peer.EndPointSID, out var recPeer))
                            {
                                peer.Peer.Write(cSprot1Parser.GetSprot1Codec(new byte[] { 00, 10 },
                                        (
                                            new TcpMsgMember() { Host = host, Port = port, MemberCmdType = RaftNet.MemberCmdType.Response, ClusterEndPoints = data }
                                        ).SerializeBiser()));
                                break;
                            }
                            continue;
                        }
                        else
                        {
                            //主动连接
                           
                            peer.Peer.Write(cSprot1Parser.GetSprot1Codec(new byte[] { 00, 10 },
                                    (
                                        new TcpMsgMember() { Host = host, Port = port, MemberCmdType = RaftNet.MemberCmdType.Response, ClusterEndPoints = data }
                                    ).SerializeBiser()));break;
                        }
                        
                    }
                });


            }
        }

        /// <summary>
        /// 获取到集群
        /// </summary>
        /// <param name="clusterEndPoints"></param>
        internal async void AddPeers(byte[] clusterEndPoints)
        {
            Interlocked.Increment(ref _peerCount);
            List<ClusterEndPoint> lst = clusterEndPoints.DeserializeProtobuf<List<ClusterEndPoint>>();
            trn.log.Log(new WarningLogEntry()
            {
                LogType = WarningLogEntry.eLogType.DEBUG,
                Description = $"{trn.port}> 获取集群个数 {lst.Count}"
            }); 
            foreach (var pair in lst)
            {
                if(!trn.NodeSettings.TcpClusterEndPoints.Exists(p=>p.Host==pair.Host&&p.Port==pair.Port))
                {
                    trn.NodeSettings.TcpClusterEndPoints.Add(new TcpClusterEndPoint() { Host = pair.Host, Port = pair.Port });
                }
                
            }
        }
    }

}//eo namespace
