// See https://aka.ms/new-console-template for more information
using Raft;
using Raft.Transport;

Test();
Console.WriteLine("Hello, World!");

Console.ReadLine();
static void Test()
{
    List<TcpClusterEndPoint> eps = new List<TcpClusterEndPoint>();
    var re_settings = new RaftEntitySettings()
    {
        VerboseRaft = true,
        //VerboseRaft = false,
        VerboseTransport = false,

        DelayedPersistenceIsActive = true,

        //InMemoryEntity = true,
        //InMemoryEntityStartSyncFromLatestEntity = true
    };
    TcpRaftNode node = null;
    for (int i = 0; i <1; i++)
        eps.Add(new TcpClusterEndPoint() { Host = "127.0.0.1", Port = 4250 + i });
    for (int i = 0; i < 1; i++)
    {
        var tc = new TcpRaftNode(new Raft.NodeSettings() { TcpClusterEndPoints = eps, RaftEntitiesSettings = new List<RaftEntitySettings>() { re_settings } }, @"D:\Temp\RaftDBreeze\node" + (4250 + i), (entityName, index, data) => { Console.WriteLine($"wow committed {entityName}/{index}; DataLen: {(data == null ? -1 : data.Length)}"); return true; },
                        4250 + i, new Logger());
        tc.Handler += Tc_Handler;

        tc.Start();
        node = tc;

    }
    Thread.Sleep(20000);
   // var ret = node.AddLogEntry(new byte[] { 1, 1, 1, 1 });


   // Console.WriteLine(ret);

    var rn = new TcpRaftNode(new Raft.NodeSettings() { RaftEntitiesSettings = new List<RaftEntitySettings>() { re_settings } }, @"D:\Temp\RaftDBreeze\node" + 3333, (entityName, index, data) => { Console.WriteLine($"wow committed {entityName}/{index}; DataLen: {(data == null ? -1 : data.Length)}"); return true; },
                      5433, new Logger());
    rn.Handler += Tc_Handler;
    rn.Start();

    Thread.Sleep(10000);
   // node.AddNewMember("127.0.0.1", 5433);
  // rn.AddSelfClusterEndPoints("127.0.0.1", 4250);
}

static void Tc_Handler(string arg1, long arg2, long arg3)
{
    Console.WriteLine($"{arg1},{arg2},{arg3}");
}

public class Logger : IWarningLog
{
    public void Log(WarningLogEntry logEntry)
    {
        Console.WriteLine(logEntry.ToString());
    }
}