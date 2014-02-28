/*
 * SHS -- The Scalable Hyperlink Store 
 * 
 * Copyright (c) Microsoft Corporation
 * All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0 
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Text;
using System.Text.RegularExpressions;

namespace SHS {
  class Server {
    private readonly string ownName;
    private readonly string leaderName;
    private readonly bool isLeader;
    private bool readyForService;

    private readonly List<string> servers;                     // Protected by locking this.servers
    private readonly Dictionary<Guid, StoreMetaData> stores;   // Protected by locking this.stores
    private readonly Dictionary<SPID, Partition> partitions;   // Protected by locking this.partitions
    // Locking hierarchy: lock servers before stores before partitions

    // Object invariants:
    // (isLeader && servers != null && stores != null) || (!isLeader && servers == null && stores == null)

    private Server(string leaderName) {
      this.ownName = System.Net.Dns.GetHostName();
      this.leaderName = leaderName;
      this.isLeader = this.ownName.StartsWith(this.leaderName, true, CultureInfo.InvariantCulture);
      this.readyForService = false;

      this.partitions = new Dictionary<SPID, Partition>();
      if (this.isLeader) {
        this.servers = new List<string>();
        this.stores = new Dictionary<Guid, StoreMetaData>();
        this.ReadMetaData();
      } else {
        this.servers = null;
        this.stores = null;
      }
    }

    private const string smdFileName = "stores.shs";

    private void ReadMetaData() {
      if (File.Exists(smdFileName)) {
        using (var rd = new BinaryReader(new FileStream(smdFileName, FileMode.Open, FileAccess.Read))) {
          int numStores = rd.ReadInt32();
          for (int i = 0; i < numStores; i++) {
            var smd = new StoreMetaData(rd);
            this.stores[smd.storeID] = smd;
          }
          Contract.Assert(rd.BaseStream.Position == rd.BaseStream.Length);
        }
      }
    }

    private void WriteMetaData() {
      Contract.Requires(Monitor.IsEntered(this.stores));
      using (var wr = new BinaryWriter(new AtomicStream(smdFileName, FileMode.Create, FileAccess.Write))) {
        var smds = this.stores.Values.ToList();
        wr.Write(smds.Count);
        foreach (var smd in smds) {
          smd.Write(wr);
        }
      }
    }

    private List<string> AvailableServers() {
      var res = new List<string>();
      lock (this.servers) {
        if (!this.servers.Contains(this.ownName)) throw new Exception("Leader not available");
        res.Add(this.ownName);
        foreach (var server in this.servers) {
          if (server != this.ownName) res.Add(server);
        }
      }
      return res;
    }

    private static Int64[] ConsistentSample(List<long> links, int numSamples, Permuter perm) {
      // The two arrays samples and hashes form a heap ordered by hashes.
      // The root of the heap contains the largest element.
      Int64[] samples = new Int64[numSamples];
      UInt64[] hashes = new UInt64[numSamples];

      for (int i = 0; i < links.Count; i++) {
        UInt64 hash = perm.Permute((UInt64)links[i]);
        if (i < numSamples) {
          // The heap is below capacity, so we add (prevUid,hash) to the end
          // and then reestablish the heap invariant.
          int c = i;
          samples[c] = links[i];
          hashes[c] = hash;
          while (c > 0) {
            int p = (c - 1) >> 1;
            if (hashes[p] >= hashes[c]) break;
            Int64 t1 = samples[p]; samples[p] = samples[c]; samples[c] = t1;
            UInt64 t2 = hashes[p]; hashes[p] = hashes[c]; hashes[c] = t2;
            c = p;
          }
        } else if (hash < hashes[0]) {
          // The heap is at capacity, and hash is less than the maximum element
          // in the heap, so we replace the root of the heap (the victim) by
          // (prevUid,hash) and then reestablish the heap invariant.
          int p = 0;
          samples[p] = links[i];
          hashes[p] = hash;
          while (p < numSamples) {
            int c = 2 * p + 1;
            if (c >= numSamples) break;
            if ((c + 1 < numSamples) && (hashes[c] < hashes[c+1])) c++;
            if (hashes[p] >= hashes[c]) break;
            Int64 t1 = samples[p]; samples[p] = samples[c]; samples[c] = t1;
            UInt64 t2 = hashes[p]; hashes[p] = hashes[c]; hashes[c] = t2;
            p = c;
          }
        } else {
          // The heap is at capacity, and hash is no less than the maximum
          // element in the heap, so we do not include (prevUid,hash) into the heap.
        }
      }
      // If we care about the sampled UIDs being in sorted order, we should sort
      // them now.
      return samples;
    }

    internal static IEnumerable<byte[]> ReceiveUrls(Channel channel) {
      for (; ; ) {
        byte[] urlBytes = channel.ReadBytes();
        if (urlBytes == null) break;
        yield return urlBytes;
      }
    }

    internal static IEnumerable<Tuple<byte[], byte[][]>> ReceiveLinks(Channel channel) {
      for (; ; ) {
        var page = channel.ReadBytes();
        if (page == null) break;
        var n = channel.ReadInt32();
        var links = new byte[n][];
        for (int i = 0; i < n; i++) {
          links[i] = channel.ReadBytes();
        }
        yield return new Tuple<byte[], byte[][]>(page, links);
      }
    }

    private static void GetFileFromServer(string filename, string server) {
      var buf = new byte[8192];
      var channel = new Channel(server, Service.PortNumber);
      channel.WriteUInt32((UInt32)OpCodes.PullFile);
      channel.WriteString(filename);
      channel.Flush();
      var numBytes = channel.ReadInt64();
      if (numBytes < 0) throw new FileNotFoundException();
      using (var fs = new FileStream(filename, FileMode.Create, FileAccess.Write)) {
        while (numBytes > 0) {
          var max = numBytes < buf.Length ? (int)numBytes : buf.Length;
          channel.ReadNumBytes(buf, max);
          fs.Write(buf, 0, max);
          numBytes -= max;
        }
      }
      channel.Close();
    }

    private string[] GetReplicaServers(SPID spid) {
      while (true) {
        try {
          var channel = new Channel(this.leaderName, Service.PortNumber);
          channel.WriteUInt32((uint)OpCodes.ReplicaServers);
          channel.WriteBytes(spid.storeID.ToByteArray());
          channel.WriteInt32(spid.partID);
          channel.Flush();
          var n = channel.ReadInt32();
          var res = new string[n];
          for (int i = 0; i < n; i++) {
            res[i] = channel.ReadString();
          }
          channel.Close();
          return res;
        } catch (SocketException) {
          Thread.Sleep(500);
        }
      }
    }

    private Cell LoadCell(string name, string[] servers) {
      try {
        return new Cell(null, name, false);
      } catch {
        // If the cell does not exist or is malformed, fetch it from a peer
        while (true) {
          foreach (var server in servers) {
            if (server != this.ownName) {
              try {
                GetFileFromServer(name, server);
                return new Cell(null, name, false);
              } catch {
                // continue in the foreach-server loop
              }
            }
          }
          Console.WriteLine("### LoadCell did not manage to reach any peer ... sleeping for 5 seconds");
          Thread.Sleep(5000);
        }
      }
    }

    internal void ReportToLeader(object obj) {
      var replacedServer = (string)obj;
      while (true) {
        try {
          var channel = new Channel(this.leaderName, Service.PortNumber);
          // Advertise yourself
          channel.WriteUInt32((uint)OpCodes.AdvertiseService);
          channel.WriteString(this.ownName);
          channel.WriteString(replacedServer);
          channel.Flush();
          // Receive a list of the partitions and cells the leader expects this Server to have
          var neededCells = new List<string>();
          int resp = channel.ReadInt32();
          if (resp == -2) {
            throw new Exception(string.Format("{0} is not a leader", this.leaderName));
          } else if (resp == -3) {
            throw new Exception(string.Format("{0} is not offline", replacedServer));
          }
          for (int i = 0; i < resp; i++) {
            var storeID = new Guid(channel.ReadBytes());
            var partID = channel.ReadInt32();
            var numParts = channel.ReadInt32();
            var numReplicas = channel.ReadInt32();
            var numPartitionBits = channel.ReadInt32();
            var numRelativeBits = channel.ReadInt32();
            var replicas = new string[numReplicas];
            for (int j = 0; j < numReplicas; j++) {
              replicas[j] = channel.ReadString();
            }
            var spid = new SPID(storeID, partID);
            var ping = new Partitioning(numParts, numReplicas, numPartitionBits, numRelativeBits);
            var numEpochs = channel.ReadInt32();
            var cells = new List<Cell>();
            for (int j = 0; j < numEpochs; j++) {
              var name = Cell.Name(spid, channel.ReadInt32());
              neededCells.Add(name);
              cells.Add(this.LoadCell(name, replicas));
            }

            // Load the partition. 
            var part = new Partition(spid, ping, cells);
            lock (this.partitions) {
              this.partitions[spid] = part;
            }
          }
          // Delete all superfluous cell files on this Server
          var cellFileRegex = new Regex(Cell.NamePattern, RegexOptions.IgnoreCase);
          foreach (var x in Directory.GetFiles(Directory.GetCurrentDirectory())
                                      .Select(x => Path.GetFileName(x))
                                      .Where(x => cellFileRegex.IsMatch(x))
                                      .Except(neededCells)) {
            File.Delete(x);
          }

          // Inform leader that everything is ready.
          this.readyForService = true;
          channel.WriteInt32(9999);
          channel.Flush();

          var noResp = channel.ReadInt32();
          throw new Exception("Received unexpected message from leader");
        } catch (SocketException) {
          this.readyForService = false;
          this.partitions.Clear();
          Thread.Sleep(1000);
        }
      }
    }

    private void ProvideService(object obj) {
      var channel = (Channel)obj;

      // Permuter really does not need to be thread-local
      var perm = new Permuter();
      var random = new Random();

      var sessionID = Guid.Empty;

      // Partition and associated ThreadLocalState of the partition opened by the OpCodes.OpenPartition handler
      Partition openPart = null;
      Partition.ThreadLocalState openPTLS = null;

      // Client requested per-UID state
      var serials = new Dictionary<int,Serializer>();
      var states = new Dictionary<int,BigArray>();
      var delHandles = new HashSet<int>();

      for (;;) {
        try {
          uint opCode = channel.ReadUInt32();

          while (opCode != (uint)OpCodes.AdvertiseService && !this.readyForService) Thread.Sleep(50);

          switch (opCode) {
            case (uint)OpCodes.AdvertiseService:
              #region
              {
                // Receive the name of the server that advertised itself
                var advertisedServer = channel.ReadString();
                var replacedServer = channel.ReadString();
                if (!this.isLeader) {
                  channel.WriteInt32(-2);
                  channel.Flush();
                } else {

                  // Make a list of all the stores that exist right now.  Another thread may add 
                  // a new store after the lock on "this.stores" has been dropped, but that is no
                  // problem, since "advertisedServer" cannot possibly hold any partitions of a
                  // new store.
                  var smds = new List<StoreMetaData>();
                  lock (this.servers) {
                    if (replacedServer != null && this.servers.Contains(replacedServer)) {
                      channel.WriteInt32(-3);
                      channel.Flush();
                    }
                    lock (this.stores) {
                      foreach (var smd in this.stores.Values) {
                        if (replacedServer != null) {
                          for (int i = 0; i < smd.servers.Length; i++ ) {
                            if (smd.servers[i].StartsWith(replacedServer, true, CultureInfo.InvariantCulture)) {
                              smd.servers[i] = advertisedServer;
                            }
                          }
                        }
                        smds.Add(smd);
                      }
                    }
                  }

                  // Obtain a read share on the rwlock of these stores.  Must do so outside the lock!
                  foreach (var smd in smds) {
                    smd.rwlock.EnterReadLock();
                  }
                  try {
                    // Send a list of partitions and cells that the server is expected to have,
                    // and the list of servers that hold a replica if the server does 
                    // not have a cell. Server acks once it is up-to-date.
                    // Tell server how many partitions it is expected to have.
                    channel.WriteInt32(smds.Select(smd => smd.PartitionsOnServer(advertisedServer).Count()).Sum());
                    foreach (var smd in smds) {
                      foreach (var partID in smd.PartitionsOnServer(advertisedServer)) {
                        var spid = new SPID(smd.storeID, partID);
                        channel.WriteBytes(spid.storeID.ToByteArray());
                        channel.WriteInt32(spid.partID);
                        channel.WriteInt32(smd.ping.numPartitions);
                        channel.WriteInt32(smd.ping.numReplicas);
                        channel.WriteInt32(smd.ping.numPartitionBits);
                        channel.WriteInt32(smd.ping.numRelativeBits);
                        foreach (var server in smd.ServersOfPartition(spid.partID)) {
                          channel.WriteString(server);
                        }
                        channel.WriteInt32(smd.epochs.Count);
                        foreach (var epoch in smd.epochs) {
                          channel.WriteInt32(epoch);
                        }
                        // It is possible that the top cell of an epoch is already of the next epoch, 
                        // but that smd.epochs has not been updated yet.
                      }
                    }
                    channel.Flush();
                    var resp = channel.ReadInt32();
                    Contract.Assert(resp == 9999);
                    lock (this.servers) {
                      this.servers.Add(advertisedServer);
                    }
                  } finally {
                    foreach (var smd in smds) {
                      smd.rwlock.ExitReadLock();
                    }
                  }
                }
                Console.WriteLine("[{0} servers] Server {1} now available", this.servers.Count, advertisedServer);
                try {
                  var noResp = channel.ReadInt32();
                  throw new Exception(string.Format("Leader unexpectedly received response {0} from {1}", noResp, advertisedServer));
                } catch (SocketException) {
                  lock (this.servers) {
                    this.servers.Remove(advertisedServer);
                  }
                  throw;
                }
              }
              #endregion
            case (uint)OpCodes.ReplicaServers:
            #region
              {
                var storeID = new Guid(channel.ReadBytes());
                var partID = channel.ReadInt32();
                StoreMetaData smd;
                lock (this.stores) {
                  smd = this.stores[storeID];
                }
                var servers = smd.ServersOfPartition(partID);
                channel.WriteInt32(servers.Length);
                foreach (var server in servers) {
                  channel.WriteString(server);
                }
                channel.Flush();
                break;
              }
            #endregion
            case (uint)OpCodes.PullFile:
              #region
              {
                var filename = channel.ReadString();
                if (File.Exists(filename)) {
                  using (var fs = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read)) {
                    var numBytes = fs.Length;
                    channel.WriteInt64(numBytes);
                    var buf = new byte[8192];
                    while (numBytes > 0) {
                      var max = numBytes < buf.Length ? (int)numBytes : buf.Length;
                      int n = fs.Read(buf, 0, max);
                      channel.WriteNumBytes(buf, n);
                      numBytes -= n;
                    }
                  }
                } else {
                  channel.WriteInt64(-1);
                }
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.AddRowOnSecondary:
              #region
              {
                var storeID = new Guid(channel.ReadBytes());
                var num = channel.ReadInt32();
                var partIDs = new int[num];
                var primaries = new string[num];
                for (int i = 0; i < num; i++) {
                  partIDs[i] = channel.ReadInt32();
                  primaries[i] = channel.ReadString();
                }
                for (int i = 0; i < num; i++) {
                  var spid = new SPID(storeID, partIDs[i]);
                  Partition part;
                  lock (this.partitions) {
                    part = this.partitions[spid];
                  }
                  while (true) {
                    try {
                      GetFileFromServer(part.NextCellName(), primaries[i]);
                      break;  
                    } catch (SocketException) {
                      // try again
                    }
                    Console.WriteLine("### AddRowOnSecondary did not manage to reach primary ... sleeping for 5 seconds");
                    Thread.Sleep(5000);
                  }
                }

                // signal back that cells have been replicated
                channel.WriteInt32(0);
                channel.Flush();

                int resp = channel.ReadInt32();
                Contract.Assert(resp == 0);
                for (int i = 0; i < num; i++) {
                  var spid = new SPID(storeID, partIDs[i]);
                  Partition part;
                  lock (this.partitions) {
                    part = this.partitions[spid];
                  }
                  part.AdvanceEpoch();
                }
                break;
              }
              #endregion
            case (uint)OpCodes.AddChkptOnSecondary:
              #region
                {
                var sessID = new Guid(channel.ReadBytes());
                var num = channel.ReadInt32();
                var partIDs = new int[num];
                var primaries = new string[num];
                for (int i = 0; i < num; i++) {
                  partIDs[i] = channel.ReadInt32();
                  primaries[i] = channel.ReadString();
                }
                var chkptNum = channel.ReadInt32();
                for (int i = 0; i < num; i++) {
                  var name = SessionStateFileName(sessID, partIDs[i], chkptNum);
                  while (true) {
                    try {
                      GetFileFromServer(name, primaries[i]);
                      break;
                    } catch (SocketException) {
                      // try again
                    }
                    Console.WriteLine("### AddChkptOnSecondary did not manage to reach primary ... sleeping for 5 seconds");
                    Thread.Sleep(5000);
                  }
                }

                // signal back that states have been replicated
                channel.WriteInt32(0);
                channel.Flush();

                int resp = channel.ReadInt32();
                Contract.Assert(resp == 0);
                for (int i = 0; i < num; i++) {
                  if (chkptNum > 0) {
                    var name = SessionStateFileName(sessID, partIDs[i], chkptNum - 1);
                    File.Delete(name);
                  }
                }
                break;
              }
              #endregion
            case (uint)OpCodes.NumAvailableServers:
              #region
              {
                if (!this.isLeader) {
                  channel.WriteInt32(-2); // This process is not the leader -- return -2
                } else {
                  lock (this.servers) {
                    channel.WriteInt32(this.servers.Count);
                  }
                }
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.ListStores:
              #region
              {
                if (!this.isLeader) {
                  channel.WriteInt32(-2); // This process is not the leader -- return -2
                } else {
                  lock (this.stores) {
                    channel.WriteInt32(this.stores.Keys.Count);
                    foreach (var storeID in this.stores.Keys) {
                      var smd = this.stores[storeID];
                      channel.WriteBytes(smd.storeID.ToByteArray());
                      channel.WriteString(smd.friendlyName);
                      channel.WriteInt32(smd.ping.numPartitions);
                      channel.WriteInt32(smd.ping.numReplicas);
                      channel.WriteInt32(smd.ping.numPartitionBits);
                      channel.WriteInt32(smd.ping.numRelativeBits);
                      channel.WriteBoolean(smd.isSealed);  
                      channel.WriteBoolean(smd.CoveringServers(this.AvailableServers()) != null);
                    }
                  }
                }
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.CreateStore:
              #region
              {
                var storeID = new Guid(channel.ReadBytes());
                var friendlyName = Encoding.UTF8.GetString(channel.ReadBytes());
                var numPartitionBits = channel.ReadInt32();
                var numRelativeBits = channel.ReadInt32();
                var numPartitions = channel.ReadInt32();
                var numReplicas = channel.ReadInt32();

                Contract.Assert(numPartitions > 0);
                lock (this.servers) {
                  var serversNeeded = numPartitions + numReplicas - 1;
                  if (!this.isLeader) {
                    channel.WriteInt32(-2); // This process is not the leader -- return -2
                  } else {
                    var storeServers = new List<string>();
                    while (storeServers.Count < serversNeeded) {
                      var availServers = this.AvailableServers();
                      Contract.Assert(availServers[0] == this.ownName);  // the leader must come first
                      foreach (var server in availServers) {
                        var i = storeServers.Count;
                        if (!storeServers.Contains(server)) {
                          try {
                            var ch = new Channel(server, Service.PortNumber);
                            for (int j = 0; j < numReplicas; j++) {
                              var partID = i - j;
                              if (partID >= 0 && partID < numPartitions) {
                                ch.WriteUInt32((uint)OpCodes.CreatePartition);
                                ch.WriteBytes(storeID.ToByteArray());
                                ch.WriteInt32(partID);
                                ch.WriteInt32(numPartitions);
                                ch.WriteInt32(numReplicas);
                                ch.WriteInt32(numPartitionBits);
                                ch.WriteInt32(numRelativeBits);
                                ch.Flush();
                                var resp2 = ch.ReadInt32();
                                Contract.Assert(resp2 == 0);
                              }
                            }
                            ch.Close();
                            storeServers.Add(server);
                          } catch (SocketException) {
                            // Could not talk to server s
                          }
                        }
                        if (storeServers.Count == serversNeeded) break;
                      }
                      if (storeServers.Count < serversNeeded) Thread.Sleep(5000);
                    }
                    Contract.Assert(storeServers[0] == this.ownName && storeServers.Count == serversNeeded);
                    lock (this.stores) {
                      var ping = new Partitioning(numPartitions, numReplicas, numPartitionBits, numRelativeBits);
                      this.stores[storeID] = new StoreMetaData(storeID, friendlyName, ping, storeServers.ToArray());
                      this.WriteMetaData();
                      channel.WriteInt32(0);
                    }
                  }
                }
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.StatStore:
              #region
              {
                var storeID = new Guid(channel.ReadBytes());
                if (!this.isLeader) {
                  channel.WriteInt32(-2);
                } else {
                  lock (this.stores) {
                    if (!this.stores.ContainsKey(storeID)) {
                      channel.WriteInt32(-3);
                    } else {
                      channel.WriteInt32(0);
                      var smd = this.stores[storeID];
                      channel.WriteString(smd.friendlyName);
                      channel.WriteInt32(smd.ping.numPartitions);
                      channel.WriteInt32(smd.ping.numReplicas);
                      channel.WriteInt32(smd.ping.numPartitionBits);
                      channel.WriteInt32(smd.ping.numRelativeBits);
                      channel.WriteBoolean(smd.isSealed);
                      channel.WriteBoolean(smd.CoveringServers(this.AvailableServers()) != null);
                    }
                  }
                }
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.OpenStore:
              #region
              {
                sessionID = new Guid(channel.ReadBytes());
                var storeID = new Guid(channel.ReadBytes());
                if (!this.isLeader) {
                  channel.WriteInt32(-2);
                } else {
                  lock (this.stores) {
                    if (!this.stores.ContainsKey(storeID)) {
                      channel.WriteInt32(-3);
                    } else {
                      var smd = this.stores[storeID];
                      var servers = smd.CoveringServers(this.AvailableServers());
                      if (servers != null) {
                        channel.WriteInt32(servers.Length);
                        for (int i = 0; i < servers.Length; i++) {
                          channel.WriteString(servers[i]);
                        }
                      } else {
                        channel.WriteInt32(-1);
                      }
                    }
                  }
                }
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.CreatePartition:
              #region
              {
                var storeID = new Guid(channel.ReadBytes());
                var partID = channel.ReadInt32();
                var spid = new SPID(storeID, partID);
                var numParts = channel.ReadInt32();
                var numReplicas = channel.ReadInt32();
                var numPartitionBits = channel.ReadInt32();
                var numRelativeBits = channel.ReadInt32();
                var ping = new Partitioning(numParts, numReplicas, numPartitionBits, numRelativeBits);
                var part = new Partition(spid, ping, new List<Cell>());
                lock (this.partitions) {
                  this.partitions[spid] = part;
                }
                channel.WriteInt32(0);
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.OpenPartition:
              #region
              {
                sessionID = new Guid(channel.ReadBytes());
                var storeID = new Guid(channel.ReadBytes());
                var partID = channel.ReadInt32();

                var chkptNum = channel.ReadInt32();
                var typeNames = new string[channel.ReadInt32()];
                for (int i = 0; i < typeNames.Length; i++) {
                  typeNames[i] = channel.ReadString();
                }

                var spid = new SPID(storeID, partID);
                lock (this.partitions) {
                  if (this.partitions.ContainsKey(spid)) {
                    openPart = this.partitions[spid];
                    openPTLS = new Partition.ThreadLocalState(openPart);
                    
                    // Load session-specific checkpointed state if there is any
                    if (chkptNum < 0) {
                      Contract.Assert(typeNames.Length == 0);
                      channel.WriteInt32(0);
                    } else {
                      int epoch;
                      var numUids = openPart.NumUids(out epoch);
                      var name = SessionStateFileName(sessionID, partID, chkptNum);
                      while (!File.Exists(name)) {
                        foreach (var server in this.GetReplicaServers(openPart.spid)) {
                          if (server != this.ownName) {
                            try {
                              GetFileFromServer(name, server);
                              break;
                            } catch {
                              // Either server does not respond or does not have the file. Try the next.
                            }
                          }
                        }
                      }
                      try {
                        using (var rd = new BinaryReader(new BufferedStream(new FileStream(name, FileMode.Open, FileAccess.Read)))) {
                          for (int handle = 0; handle < typeNames.Length; handle++) {
                            if (typeNames[handle] != null) { 
                              var type = System.Type.GetType(typeNames[handle]);
                              var serial = SerializerFactory.Make(type);
                              var state = BigArray.Make(type, numUids);
                              for (long puid = 0; puid < numUids; puid++) {
                                state.SetValue(serial.Read(rd), puid);
                              }
                              serials[handle] = serial;
                              states[handle] = state;
                            }
                          }
                          if (rd.BaseStream.Position != rd.BaseStream.Length) {
                            throw new FileFormatException();
                            // If file is corrupted or does not exist, should get it from a peer 
                          }
                        }
                        channel.WriteInt32(0);
                      } catch {
                        channel.WriteInt32(-3);
                      }
                    }
                  } else {
                    channel.WriteInt32(-1); // Failure
                  }
                  channel.Flush();
                }
                break;
              }
              #endregion
            case (uint)OpCodes.SealStore:
              #region
              {
                var storeID = new Guid(channel.ReadBytes());
                if (!this.isLeader) {
                  channel.WriteInt32(-2);
                } else {
                  lock (this.stores) {
                    if (!this.stores.ContainsKey(storeID)) {
                      channel.WriteInt32(-3);
                    } else {
                      var smd = this.stores[storeID];
                      smd.rwlock.EnterUpgradeableReadLock();
                      try {
                        if (smd.isSealed) {
                          channel.WriteInt32(-1);
                        } else {
                          this.stores[storeID].isSealed = true;
                          this.WriteMetaData();
                          channel.WriteInt32(0);
                        }
                      } finally {
                        smd.rwlock.ExitUpgradeableReadLock();
                      }
                    }
                  }
                }
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.DeleteStore:
              #region
              {
                var storeID = new Guid(channel.ReadBytes());
                if (!this.isLeader) {
                  channel.WriteInt32(-2);
                } else {
                  lock (this.stores) {
                    if (!this.stores.ContainsKey(storeID)) {
                      channel.WriteInt32(-3);
                    } else {
                      this.stores.Remove(storeID);
                      this.WriteMetaData();
                      channel.WriteInt32(0);
                      // If any other client thread is currently using this store, bad things 
                      // will happen.  I need to come up with a protocol that ensures that the 
                      // store is not currently open.
                    }
                  }
                }
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.AddPageLinks:
              #region
              {
                var urlCellStrideLength = channel.ReadInt32();
                var fwdCellStrideLength = channel.ReadInt32();
                var bwdCellStrideLength = channel.ReadInt32();
                var fwdCompression = (LinkCompression)channel.ReadInt32();
                var bwdCompression = (LinkCompression)channel.ReadInt32();
                var primaries = new string[openPart.ping.numPartitions];
                for (int i = 0; i < primaries.Length; i++) {
                  primaries[i] = channel.ReadString();
                }

                StoreMetaData smd = null;
                if (this.isLeader) { 
                  lock (this.stores) {
                    smd = this.stores[openPart.spid.storeID];
                  }
                  smd.rwlock.EnterUpgradeableReadLock();
                }
                try {

                  Action urlCellFinishedClosure =
                    delegate() {
                      channel.WriteInt32(0);
                      channel.Flush();
                    };

                  Func<List<Tuple<long, long>>, List<Tuple<long, long>>> propagateBwdsClosure =
                    remotePairs => {
                      channel.WriteInt32(remotePairs.Count);
                      foreach (var pair in remotePairs) {
                        channel.WriteInt64(pair.Item1);
                        channel.WriteInt64(pair.Item2);
                      }
                      channel.Flush();
                      int n = channel.ReadInt32();
                      if (n == -1) {
                        return null;
                      } else {
                        var res = new List<Tuple<long, long>>(n);
                        for (int i = 0; i < n; i++) {
                          var uid1 = channel.ReadInt64();
                          var uid2 = channel.ReadInt64();
                          res.Add(new Tuple<long, long>(uid1, uid2));
                        }
                        return res;
                      }
                    };

                  Func<long, long[]> urlMergeCompletedClosure =
                    cmBaseUid => {
                      channel.WriteInt64(cmBaseUid);
                      channel.Flush();
                      var res = new long[openPart.ping.numPartitions];
                      for (int i = 0; i < res.Length; i++) {
                        res[i] = channel.ReadInt64();
                      }
                      return res;
                    };

                  Action cellFinishedClosure =
                    delegate() {
                      channel.WriteInt64(-1);
                      channel.Flush();
                      channel.ReadInt32();
                    };

                  // CommitClosure will throw a SocketException exception if any of the secondaries is not available.
                  // This exception should propagate all the way back to the client, causing it to retry the
                  // AddRow operation until it fully succeeds.
                  Action<List<int>> commitClosure =
                    delegate(List<int> epochs) {
                      channel.WriteInt32(0);
                      channel.Flush();
                      if (this.isLeader) {
                        var resp1 = channel.ReadInt32();
                        Contract.Assert(resp1 == 0);

                        // Acquire a write-lock on the store, to prevent interference between 
                        // AddRow-driven replication and AdvertiseService-driven replication.
                        smd.rwlock.EnterWriteLock();
                        try {
                          // Replicate the new cell on each primary to all secondaries that are currently
                          // believed to be available and actually responsive.  Rather than using the 
                          // heartbeat connections between this leader and the servers and having to deal 
                          // with locking issues, I open a new dedicated single-use connection to each server.
                          var channels = new Channel[smd.servers.Length]; // initially all null
                          lock (this.servers) {
                            for (int i = 0; i < smd.servers.Length; i++) {
                              var server = smd.servers[i];
                              try {
                                if (this.servers.Contains(smd.servers[i])) {
                                  channels[i] = new Channel(smd.servers[i], Service.PortNumber);
                                }
                              } catch (SocketException) {
                                // smd.servers[i] has crashed.  If it comes back online, it will readvertise 
                                // itself and pull any needed cells as part of the AdvertiseService protocol.
                              }
                            }
                          }

                          for (int i = 0; i < smd.servers.Length; i++) {
                            // what partitions does this server need to copy?
                            if (channels[i] != null) {
                              var partIDs = smd.PartitionsOnServer(smd.servers[i])
                                                .Except(primaries.Select((x, partID) => new { server = x, partID = partID })
                                                                .Where(x => x.server == smd.servers[i])
                                                                .Select(x => x.partID))
                                                .ToList();
                              try {
                                channels[i].WriteUInt32((uint)OpCodes.AddRowOnSecondary);
                                channels[i].WriteBytes(openPart.spid.storeID.ToByteArray());
                                channels[i].WriteInt32(partIDs.Count);
                                foreach (var partID in partIDs) {
                                  channels[i].WriteInt32(partID);
                                  channels[i].WriteString(primaries[partID]);
                                }
                                channels[i].Flush();
                              } catch (SocketException) {
                                channels[i] = null;
                              }
                            }
                          }
                          // Wait for each server to respond
                          for (int i = 0; i < smd.servers.Length; i++) {
                            if (channels[i] != null) {
                              try {
                                int resp2 = channels[i].ReadInt32();
                                Contract.Assert(resp2 == 0);
                              } catch (SocketException) {
                                channels[i] = null;
                              }
                            }
                          }

                          // All available servers reported that they have successfully pulled the replicas 
                          // they are supposed to have.  All TCP channels are in a state where they 
                          // have either been severed since (in which case the server will readvertise
                          // itself) or are waiting for a signal that the epoch can be advanced and
                          // superfluous cell files deleted.

                          // Update the meta-data on the leader, thereby committing the advance of the epoch
                          smd.epochs = epochs;
                          lock (this.stores) {
                            this.WriteMetaData();
                          }

                          for (int i = 0; i < smd.servers.Length; i++) {
                            if (channels[i] != null) {
                              try {
                                channels[i].WriteInt32(0);
                                channels[i].Flush();
                              } catch (SocketException) {
                                channels[i] = null;
                                // ignore -- server will readvertise itself and perform cleanup as part of it
                              }
                            }
                          }
                        } finally {
                          smd.rwlock.ExitWriteLock();
                        }

                        // Inform the client that commit is complete by sending it the new epoch
                        channel.WriteInt32(epochs.Last());
                        channel.Flush();
                      }
                      var resp = channel.ReadInt32();
                      Contract.Assert(resp == 0);
                    };

                  openPart.AddRow(openPTLS, primaries,
                                  urlCellStrideLength, fwdCellStrideLength, bwdCellStrideLength,
                                  fwdCompression, bwdCompression,
                                  ReceiveUrls(channel), ReceiveLinks(channel),
                                  urlCellFinishedClosure, propagateBwdsClosure,
                                  urlMergeCompletedClosure, cellFinishedClosure, commitClosure);
                  break;
                } finally {
                  if (this.isLeader) {
                    smd.rwlock.ExitUpgradeableReadLock();
                  }
                }
              }
              #endregion
            case (uint)OpCodes.MapOldToNewUids:
              #region
              {
                int n = channel.ReadInt32();
                for (int i = 0; i < n; i++) {
                  var uid = channel.ReadInt64();
                  channel.WriteInt64(openPart.OldToNew(uid));
                }
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.Request:
              #region
              {
                SubStore s = (SubStore)channel.ReadInt32();
                openPart.RequestSubStore(openPTLS, s);
                channel.WriteInt32(1);
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.Relinquish:
              #region
              {
                SubStore s = (SubStore)channel.ReadInt32();
                openPart.RelinquishSubStore(openPTLS, s);
                break;
              }
              #endregion
            case (uint)OpCodes.NumUrls:
              #region
              {
                int epoch;
                long numUids = openPart.NumUids(out epoch);
                channel.WriteInt32(epoch);
                channel.WriteInt64(numUids);
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.NumLinks:
              #region
              {
                int epoch;
                var numLinks = openPart.NumLinks(out epoch);
                channel.WriteInt32(epoch);
                channel.WriteInt64(numLinks);
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.MaxDegree:
              #region
              {
                int dir = channel.ReadInt32();
                Contract.Assert(dir == 0 || dir == 1);
                int epoch;
                var maxDegree = openPart.MaxDegree(openPTLS, dir, out epoch);
                channel.WriteInt32(epoch);
                channel.WriteInt32(maxDegree);
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.UrlToUid:
              #region
              {
                var urlBytes = channel.ReadBytes();
                int epoch;
                long uid = openPart.UrlToUid(openPTLS, urlBytes, out epoch, false);
                channel.WriteInt32(epoch);
                channel.WriteInt64(uid);
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.BatchedUrlToUid:
              #region
              {
                var privileged = channel.ReadInt32() != 0;
                int n = channel.ReadInt32();
                var urlBytes = new byte[n][];
                /* The reason for allocating this array is that otherwise the
                   advertisedServer may clog the TCP buffer on the client side, since
                   the client transmits all URLs before consuming the first UID.
                */
                for (int i = 0; i < n; i++) {
                  urlBytes[i] = channel.ReadBytes();
                }
                int epoch;
                var uids = openPart.UrlsToUids(openPTLS, urlBytes, out epoch, privileged);
                channel.WriteInt32(epoch);
                for (int i = 0; i < n; i++) {
                  channel.WriteInt64(uids[i]);
                }
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.UidToUrl:
              #region
              {
                int epoch;
                var urlBytes = openPart.UidToUrl(openPTLS, channel.ReadInt64(), out epoch);
                channel.WriteInt32(epoch);
                channel.WriteBytes(urlBytes);
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.BatchedUidToUrl:
              #region
              {
                int n = channel.ReadInt32();
                var uids = new long[n];
                for (int i = 0; i < n; i++) {
                  uids[i] = channel.ReadInt64();
                }
                /* The reason for allocating this array is that otherwise the
                   advertisedServer may clog the TCP buffer on the client side, since
                   the client transmits all UIDs before consuming the first URL.
                */
                int epoch;
                var urls = openPart.UidsToUrls(openPTLS, uids, out epoch);
                channel.WriteInt32(epoch);
                for (int i = 0; i < n; i++) {
                  channel.WriteBytes(urls[i]);
                }
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.GetDegree:
              #region
              {
                var dir = channel.ReadInt32();
                long uid = channel.ReadInt64();
                int epoch;
                var degree = openPart.GetLinks(openPTLS, uid, dir, out epoch).Count;
                channel.WriteInt32(epoch);
                channel.WriteInt32(degree);
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.BatchedGetDegree:
              #region
              {
                int dir = channel.ReadInt32();
                int n = channel.ReadInt32();
                var uids = new long[n];
                for (int i = 0; i < n; i++) {
                  uids[i] = channel.ReadInt64();
                }
                int epoch;
                var links = openPart.GetLinks(openPTLS, uids, dir, out epoch);
                channel.WriteInt32(epoch);
                for (int i = 0; i < n; i++) {
                  channel.WriteInt32(links[i].Count);
                }
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.SampleLinks:
              #region
              {
                var dir = channel.ReadInt32();
                int numSamples = channel.ReadInt32();
                bool consistent = channel.ReadInt32() != 0;
                long uid = channel.ReadInt64();
                int epoch;
                var links = openPart.GetLinks(openPTLS, uid, dir, out epoch);
                channel.WriteInt32(epoch);

                int len = links.Count;
                if (numSamples == -1 || len <= numSamples) {
                  channel.WriteInt32(len);
                  for (int i = 0; i < len; i++) {
                    channel.WriteInt64(links[i]);
                  }
                } else if (consistent) {
                  Int64[] samples = ConsistentSample(links, numSamples, perm);
                  channel.WriteInt32(numSamples);
                  for (int i = 0; i < numSamples; i++) {
                    channel.WriteInt64(samples[i]);
                  }
                } else {
                  bool[] sampled = new bool[len];
                  for (int i = 0; i < len; i++) sampled[i] = false;
                  for (int i = 0; i < numSamples;) {
                    int r = (int)((Int64)(random.NextUInt64() >> 1) % len);
                    if (sampled[r] == false) {
                      sampled[r] = true;
                      i++;
                    }
                  }
                  channel.WriteInt32(numSamples);
                  for (int i = 0; i < len; i++) {
                    if (sampled[i]) channel.WriteInt64(links[i]);
                  }
                }
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.BatchedSampleLinks:
              #region
              {
                var dir = channel.ReadInt32();
                var numSamples = channel.ReadInt32();
                var consistent = channel.ReadInt32() != 0;
                var n = channel.ReadInt32();
                var uids = new long[n];
                for (int i = 0; i < n; i++) {
                  uids[i] = channel.ReadInt64();
                }
                int epoch;
                var linkBatch = openPart.GetLinks(openPTLS, uids, dir, out epoch);
                channel.WriteInt32(epoch);
                for (int i = 0; i < n; i++) {
                  var links = linkBatch[i];
                  int len = links.Count;
                  if (numSamples == -1 || len <= numSamples) {
                    channel.WriteInt32(len);
                    for (int j = 0; j < len; j++) {
                      channel.WriteInt64(links[j]);
                    }
                  } else if (consistent) {
                    Int64[] samples = ConsistentSample(links, numSamples, perm);
                    channel.WriteInt32(numSamples);
                    for (int j = 0; j < numSamples; j++) {
                      channel.WriteInt64(samples[j]);
                    }
                  } else {
                    bool[] sampled = new bool[len];
                    for (int j = 0; j < len; j++) sampled[j] = false;
                    for (int j = 0; j < numSamples;) {
                      int r = (int)((Int64)(random.NextUInt64() >> 1) % len);
                      if (sampled[r] == false) {
                        sampled[r] = true;
                        j++;
                      }
                    }
                    channel.WriteInt32(numSamples);
                    for (int j = 0; j < len; j++) {
                      if (sampled[j]) channel.WriteInt64(links[j]);
                    }
                  }
                }
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.AllocateUidState:
              #region
              {
                int handle = channel.ReadInt32();
                string fullTypeName = System.Text.Encoding.UTF8.GetString(channel.ReadBytes());
                try {
                  var type = System.Type.GetType(fullTypeName);
                  serials[handle] = SerializerFactory.Make(type);
                  int epoch;
                  states[handle] = BigArray.Make(type, openPart.NumUids(out epoch));
                  channel.WriteString(null);
                  channel.Flush();
                } catch (System.Exception e) {
                  channel.WriteString(e.Message);
                  channel.Flush();
                }
                break;
              }
              #endregion
            case (uint)OpCodes.FreeUidState:
              #region
              {
                int handle = channel.ReadInt32();
                delHandles.Add(handle);
                break;
              }
              #endregion
            case (uint)OpCodes.GetUidState:
              #region
              {
                int handle = channel.ReadInt32();
                long uid = channel.ReadInt64();
                serials[handle].Write(states[handle].GetValue(openPart.ping.PUID(uid)), channel);
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.SetUidState:
              #region
              {
                int handle = channel.ReadInt32();
                long uid = channel.ReadInt64();
                states[handle].SetValue(serials[handle].Read(channel), openPart.ping.PUID(uid));
                break;
              }
              #endregion
            case (uint)OpCodes.BatchedGetUidState:
              #region
              {
                int handle = channel.ReadInt32();
                int n = channel.ReadInt32();
                long[] uids = new long[n];
                // Need to consume all incoming uids before responding
                for (int i = 0; i < n; i++) {
                  uids[i] = channel.ReadInt64();
                }
                var state = states[handle];
                var serial = serials[handle];
                for (int i = 0; i < n; i++) {

                  serial.Write(state.GetValue(openPart.ping.PUID(uids[i])), channel);
                }
                channel.Flush();
                break;
              }
              #endregion
            case (uint)OpCodes.BatchedSetUidState:
              #region
              {
                int handle = channel.ReadInt32();
                int n = channel.ReadInt32();
                var args = new object[]{null, channel};
                var state = states[handle];
                var serial = serials[handle];
                for (int i = 0; i < n; i++) {
                  long uid = channel.ReadInt64();
                  var val = serial.Read(channel);
                  state.SetValue(val, openPart.ping.PUID(uid));
                }
                break;
              }
              #endregion
            case (uint)OpCodes.CheckpointUidStates:
              #region
              {
                int chkptNum = channel.ReadInt32();
                var numHandles = channel.ReadInt32();
                int epoch;
                var numUids = openPart.NumUids(out epoch);
                var tmpName = new TempFileNames().New();
                using (var wr = new BinaryWriter(new BufferedStream(new FileStream(tmpName, FileMode.Create, FileAccess.Write)))) {
                  for (int handle = 0; handle < numHandles; handle++) {
                    if (!delHandles.Contains(handle)) {  // do not checkpoint UidStates that have been deleted
                      var state = states[handle];
                      for (long puid = 0; puid < numUids; puid++) {
                        serials[handle].Write(states[handle].GetValue(puid), wr);
                      }
                    }
                  }
                }
                File.Move(tmpName, SessionStateFileName(sessionID, openPart.spid.partID, chkptNum));
                channel.WriteInt32(0); // State has been written
                channel.Flush();

                //
                // Client waits for all servers to confirm that state has been written,
                // then asks leader to drive state replication, waits for leader to
                // confirm that replication is complete, and finally signals servers
                // that it's OK to delete the previous checkpoint file and to dispose
                // of any states that have been freed via FreeUidState
                if (this.isLeader) {
                  var resp1 = channel.ReadInt32();
                  Contract.Assert(resp1 == openPart.ping.numPartitions);
                  var primaries = new string[resp1];
                  for (int i = 0; i < primaries.Length; i++) {
                    primaries[i] = channel.ReadString();
                  }

                  // Drive replication of checkpoint file from each primary to all its seconaries
                  // This code is very similar to portions of CommitClosure in the AddRow opCode
                  // handler.
                  StoreMetaData smd;
                  lock (this.stores) {
                    smd = this.stores[openPart.spid.storeID];
                  }
                  var channels = new Channel[smd.servers.Length]; // initially all null
                  lock (this.servers) {
                    for (int i = 0; i < smd.servers.Length; i++) {
                      var server = smd.servers[i];
                      try {
                        if (this.servers.Contains(smd.servers[i])) {
                          channels[i] = new Channel(smd.servers[i], Service.PortNumber);
                        }
                      } catch (SocketException) {
                        // smd.servers[i] has crashed.  If it comes back online, it will 
                        // get the needed states as part of the OpenPartition protocol.
                      }
                    }
                  }
                  for (int i = 0; i < smd.servers.Length; i++) {
                    // what partitions does this server need to copy?
                    if (channels[i] != null) {
                      var partIDs = smd.PartitionsOnServer(smd.servers[i])
                                        .Except(primaries.Select((x, partID) => new { server = x, partID = partID })
                                                        .Where(x => x.server == smd.servers[i])
                                                        .Select(x => x.partID))
                                        .ToList();
                      try {
                        channels[i].WriteUInt32((uint)OpCodes.AddChkptOnSecondary);
                        channels[i].WriteBytes(sessionID.ToByteArray());
                        channels[i].WriteInt32(partIDs.Count);
                        foreach (var partID in partIDs) {
                          channels[i].WriteInt32(partID);
                          channels[i].WriteString(primaries[partID]);
                        }
                        channels[i].WriteInt32(chkptNum);
                        channels[i].Flush();
                      } catch (SocketException) {
                        channels[i] = null;
                      }
                    }
                  }
                  // Wait for each server to respond
                  for (int i = 0; i < smd.servers.Length; i++) {
                    if (channels[i] != null) {
                      try {
                        int resp2 = channels[i].ReadInt32();
                        Contract.Assert(resp2 == 0);
                      } catch (SocketException) {
                        channels[i] = null;
                      }
                    }
                  }

                  // Inform client that replication has happened
                  channel.WriteInt32(-9997);
                  channel.Flush();
                  var resp3 = channel.ReadInt32();
                  Contract.Assert(resp3 == -9996);

                  // Ask secondaries to clean up 
                  for (int i = 0; i < smd.servers.Length; i++) {
                    if (channels[i] != null) {
                      try {
                        channels[i].WriteInt32(0);
                        channels[i].Flush();
                      } catch (SocketException) {
                        channels[i] = null;
                        // ignore -- server will perform cleanup once state lease expires
                      }
                    }
                  }

                  // Tell client that cleanup of secondaries is complete
                  channel.WriteInt32(-9995);
                  channel.Flush();
                }
                var resp = channel.ReadInt32();
                Contract.Assert(resp == -9994);
                if (chkptNum > 0) {
                  File.Delete(SessionStateFileName(sessionID, openPart.spid.partID, chkptNum - 1));
                }
                for (int handle = 0; handle < numHandles; handle++) {
                  if (delHandles.Contains(handle)) {
                    states[handle] = null;
                  }
                }
                break;
              }
              #endregion
            default:
              var remote = Dns.GetHostEntry(((IPEndPoint)channel.Socket.RemoteEndPoint).Address).HostName;
              throw new Exception(string.Format("{0} transmitted unknown OpCode {1:X8}", remote, opCode));
          }
        } catch (SocketException) {
          // The SocketException may have been thrown while talking to a different 
          // Channel than "channel". Explicitly close "channel"
          channel.Close();
          if (openPart != null) openPart.DeregisterTLS(openPTLS);
          return;
        }
      }
    }

    private static string SessionStateFileName(Guid sessionID, int partID, int chkptNum) {
      return string.Format("ss-{0:N}-{1}-{2}.shs", sessionID, partID, chkptNum);
    }

    public static void Main(string[] args) {
      Thread.CurrentThread.Name = "Main";
      if (args.Length != 1 && args.Length != 2) {
        Console.Error.WriteLine("Usage: SHS.Server <leadername> [ <replaced-server> ]");
      } else {
        // For starters, delete any tmp files left behind by a crash.
        TempFileNames.CleanAll();

        var server = new Server(args[0]);
        var acc = new Acceptor(Service.PortNumber, 10);
        
        // Spawn a thread to advertise this server to the leader
        var th0 = new Thread(server.ReportToLeader);
        th0.Name = "LdTh";
        th0.Start(args.Length == 1 ? null : args[1]);

        // Successively accept connection requests and start a thread for each
        for (long i = 0;; i++) {
          var ch = acc.AcceptConnection();
          var th = new Thread(server.ProvideService);
          th.Name = i.ToString();
          th.Start(ch);
        }
      }
    }

    // ===== Private classes =====

    private class StoreMetaData {
      internal readonly Guid storeID;
      internal readonly string friendlyName;
      internal readonly Partitioning ping;
      internal bool isSealed;
      internal readonly string[] servers;  // invariant: servers.Length == ping.numPartitions + ping.numReplicas - 1
      internal List<int> epochs;
      internal readonly ReaderWriterLockSlim rwlock;

      internal StoreMetaData(Guid storeID, string friendlyName, Partitioning ping, string[] servers) {
        Contract.Assert(servers.Length == ping.numPartitions + ping.numReplicas - 1);
        this.storeID = storeID;
        this.friendlyName = friendlyName;
        this.ping = ping;
        this.isSealed = false;
        this.servers = servers;
        this.epochs = new List<int>();
        this.rwlock = new ReaderWriterLockSlim();
      }

      internal StoreMetaData(BinaryReader rd) {
        this.storeID = new Guid(rd.ReadBytes(16));
        this.friendlyName = rd.ReadString();
        var numPartitions = rd.ReadInt32();
        var numReplicas = rd.ReadInt32();
        var numPartitionBits = rd.ReadInt32();
        var numRelativeBits = rd.ReadInt32();
        this.ping = new Partitioning(numPartitions, numReplicas, numPartitions, numRelativeBits);
        this.isSealed = rd.ReadBoolean();
        this.servers = new string[numPartitions + numReplicas - 1];
        for (int i = 0; i < this.servers.Length; i++) {
          this.servers[i] = rd.ReadString();
        }
        int numEpochs = rd.ReadInt32();
        this.epochs = new List<int>();
        for (int i = 0; i < numEpochs; i++) {
          this.epochs.Add(rd.ReadInt32());
        }
        this.rwlock = new ReaderWriterLockSlim();
      }

      internal void Write(BinaryWriter wr) {
        wr.Write(this.storeID.ToByteArray());
        wr.Write(this.friendlyName);
        wr.Write(this.ping.numPartitions);
        wr.Write(this.ping.numReplicas);
        wr.Write(this.ping.numPartitionBits);
        wr.Write(this.ping.numRelativeBits);
        wr.Write(this.isSealed);
        foreach (var server in this.servers) {
          wr.Write(server);
        }
        wr.Write(this.epochs.Count);
        foreach (var epoch in this.epochs) {
          wr.Write(epoch);
        }
      }

      /// <summary>
      /// If it is possible to serve this store in the specified mode using the available 
      /// servers, return the advertisedServer names in partition-order (mode.e. result[mode] is the name 
      /// of a advertisedServer capable of serving partition mode); otherwise return null.
      /// </summary>
      /// <param name="availableServers">Set of available servers</param>
      /// <param name="mode">Normal or Degraded</param>
      /// <returns>advertisedServer names in partition-order</returns>
      internal string[] CoveringServers(List<string> availableServers) {
        // calling thread should hold a lock on this
        var list = new List<string>();
        foreach (var server in this.servers) {
          if (availableServers.Contains(server)) {
            list.Add(server);
          }
        }
        return list.Count < this.ping.numPartitions ? null : list.Take(this.ping.numPartitions).ToArray();
      }

      internal IEnumerable<int> PartitionsOnServer(string server) {
        for (int serverID = 0; serverID < this.servers.Length; serverID++) {
          if (this.servers[serverID].Equals(server)) {
            foreach (var partID in this.ping.PartitionsOnServer(serverID)) {
              yield return partID;
            }
            break;
          }
        }
      }

      internal string[] ServersOfPartition(int partID) {
        return this.ping.ServersOfPartition(partID).Select(i => this.servers[i]).ToArray();
      }
    }
  }
}
