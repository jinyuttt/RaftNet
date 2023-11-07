using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DBreeze;
using Raft;
using Raft.Transport;

namespace Raft.RaftEmulator
{
    public class UdpEmulator : IRaftComSender, IWarningLog
    {
        Dictionary<long,RaftNode> nodes = new Dictionary<long, RaftNode>();
        internal DBreezeEngine dbEngine;
        public void StartEmulateNodes(int nodesQuantity)
        {
            RaftNode rn =null;

            NodeSettings rn_settings = new NodeSettings()
            {
                 
            };
            DBreezeConfiguration conf = new DBreezeConfiguration()
            {
                DBreezeDataFolderName = ""
            };

            if (rn_settings.RaftEntitiesSettings.Where(MyEnt => !MyEnt.InMemoryEntity).Count() == 0)
            {
                conf.Storage = DBreezeConfiguration.eStorage.MEMORY;
            }
            else
            {
                conf.Storage = DBreezeConfiguration.eStorage.DISK;
            }


            //conf = new DBreezeConfiguration()
            //{
            //    DBreezeDataFolderName = dbreezePath,
            //    Storage = DBreezeConfiguration.eStorage.DISK,
            //};
            conf.AlternativeTablesLocations.Add("mem_*", String.Empty);

            dbEngine = new DBreezeEngine(conf);
          // var spider = new TcpSpider();
            for (int i = 0; i < nodesQuantity; i++)
            {
                rn = new RaftNode(new RaftEntitySettings(), dbEngine, this,null,null);
                rn.Verbose = true;
                rn.SetNodesQuantityInTheCluster((uint)nodesQuantity);
                rn.NodeAddress.NodeAddressId = i + 1;
                nodes.Add(rn.NodeAddress.NodeAddressId, rn);
                System.Threading.Thread.Sleep((new Random()).Next(30, 150));
               // System.Threading.Thread.Sleep(500);
                rn.NodeStart();
            }
        }

        /// <summary>
        /// Test method
        /// </summary>
        /// <param name="nodeId"></param>
        /// <param name="data"></param>
        public void SendData(int nodeId, string data)
        {
            RaftNode node = null;
            if(!nodes.TryGetValue(nodeId, out node))             
                 return;

             node.AddLogEntry(System.Text.Encoding.UTF8.GetBytes(data));
        }

        /// <summary>
        /// Test method
        /// </summary>
        /// <param name="nodeId"></param>
        /// <param name="stateLogId"></param>
        /// <returns></returns>
        public bool ContainsStateLogIdData(int nodeId, ulong stateLogId)
        {
            RaftNode node = null;
            if (!nodes.TryGetValue(nodeId, out node))
                return false;
          
            return node.ContainsStateLogEntryId(stateLogId);
        }


        public void Start(int nodeId)
        {
            RaftNode node = null;
            if (nodes.TryGetValue(nodeId, out node))
                node.NodeStart();
        }

        public void Stop(int nodeId)
        {
            RaftNode node = null;
            if (nodes.TryGetValue(nodeId, out node))
                node.NodeStop();
        }

        #region "IRaftComSender"

        public void SendToAll(eRaftSignalType signalType, byte[] data, NodeAddress myNodeAddress)
        {
            Task.Run(() =>
                {
                    foreach (var n in nodes)
                    {
                        if (!n.Value.IsRunning)
                            continue;

                        if (n.Value.NodeAddress.NodeAddressId == myNodeAddress.NodeAddressId)
                            continue;       //Skipping sending to self

                        //May be put it all into new Threads or so
                        ((IRaftComReceiver)n.Value).IncomingSignalHandler(myNodeAddress, signalType, data);
                    }
                });
            
        }

        public void SendTo(NodeAddress nodeAddress, eRaftSignalType signalType, byte[] data, NodeAddress myNodeAddress)
        {
            Task.Run(() =>
            {
                foreach (var n in nodes)
                {
                    if (!n.Value.IsRunning)
                        continue;

                    if (n.Value.NodeAddress.NodeAddressId == myNodeAddress.NodeAddressId)
                        continue;       //Skipping sending to self

                    if (n.Value.NodeAddress.NodeAddressId == nodeAddress.NodeAddressId)
                    {
                        //May be put it all into new Threads or so
                        ((IRaftComReceiver)n.Value).IncomingSignalHandler(myNodeAddress, signalType, data);

                        break;
                    }
                }
            });
        }
        #endregion

        #region "IWarningLog"
        public void LogError(WarningLogEntry logEntry)
        {
            throw new NotImplementedException();
        }

        public void Log(WarningLogEntry logEntry)
        {
            throw new NotImplementedException();
        }

        public void SendToAll(eRaftSignalType signalType, byte[] data, NodeAddress senderNodeAddress, string entityName, bool highPriority = false)
        {
            throw new NotImplementedException();
        }

        public void SendTo(NodeAddress nodeAddress, eRaftSignalType signalType, byte[] data, NodeAddress senderNodeAddress, string entityName)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
