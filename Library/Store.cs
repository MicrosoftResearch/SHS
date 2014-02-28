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
using System.Net.Sockets;
using System.Threading;

namespace SHS {
  public enum LinkCompression { None = 0, VarByte = 1, VarNybble = 2 };

  public enum SubStore { UrlStore = 0, FwdStore = 1, BwdStore = 2 };

  public enum Dir { Fwd = 0, Bwd = 1};

  public struct PageLinks {
    public string pageUrl;
    public string[] linkUrls;
  }

  public class EpochPassed : Exception {
  }

  public class ServerFailure : Exception {
  }

  public class Store {
    private readonly Service service;
    private readonly Guid storeID;      // Globally unique ID of store
    private readonly Guid sessionID;
    private readonly Partitioning ping;

    private readonly string[] primaries;  
    private readonly Channel[] channels;     
    private bool isClosed;
    private bool isSealed;

    private Hash64 hasher;
    private HashToUidCache hashToUidCache;  // Client-side urlhash-to-uid cache

    // These cached values are used by UidToLid and Uids.  
    private int numUidsEpoch = -2;  
    private long[] numUids = null;
    private long lastUID = -1;

    private int desiredEpoch = -1;

    private int connectCount = 0;  // used only for testing purposes

    private int chkptNum = 0;
    private int nextUidStateHandle = 0;
    private int chkptUidStateHandle = 0;
    private bool dirtyStates = false;
    private readonly List<UidState> states = new List<UidState>();
    private readonly List<int> delHandles = new List<int>();

    internal Store(Service service, Guid storeID, StoreInfo si) {
      Contract.Requires(service != null);
      Contract.Requires(si != null);

      this.service = service;
      this.storeID = storeID;
      this.sessionID = Guid.NewGuid();
      this.ping = new Partitioning(si.NumPartitions, si.NumReplicas, si.NumPartitionBits, si.NumRelativeBits);
      this.primaries = new string[si.NumPartitions];
      this.channels = new Channel[si.NumPartitions];
      this.hashToUidCache = null;
      this.hasher = null;
      this.isSealed = si.IsSealed;
      this.isClosed = false;
      this.ConnectToService();
    }

    internal Store(Guid storeID, Partitioning ping, string[] primaries) {
      Contract.Requires(ping != null);
      Contract.Requires(primaries != null);
      Contract.Requires(primaries.Length == ping.numPartitions);
      Contract.Requires(Contract.ForAll(primaries, primary => primary != null));

      this.service = null;
      this.storeID = storeID;
      this.sessionID = Guid.NewGuid();
      this.ping = ping;
      this.primaries = primaries;
      this.channels = new Channel[ping.numPartitions];
      this.hashToUidCache = null;
      this.hasher = null;
      this.isClosed = false;
      this.isSealed = false;
      this.ConnectToPrimaries();
    }

    private void ConnectToService() {
      while (true) {
        try {
          var leader = this.service.LeaderChannel;
          leader.WriteUInt32((uint)OpCodes.OpenStore);
          leader.WriteBytes(this.sessionID.ToByteArray());
          leader.WriteBytes(this.storeID.ToByteArray());
          leader.Flush();
          var resp = leader.ReadInt32();
          if (resp == -2) {
            throw new Exception(string.Format("{0} not the SHS service leader", service.leaderName));
          } else if (resp == -3) {
            throw new Exception(string.Format("{0:N} unknown to SHS service leader", storeID));
          } else if (resp == -1) {
            throw new SocketException(); // try again in 5 seconds
          } else {
            Contract.Assert(resp > 0);
            Contract.Assert(resp == ping.numPartitions);
            for (int i = 0; i < resp; i++) {
              this.primaries[i] = leader.ReadString();
            }
            Contract.Assert(this.primaries[0].StartsWith(this.service.leaderName, true, CultureInfo.InvariantCulture));
          }
          leader.Close();
          this.ConnectToPrimaries();

          if (this.dirtyStates) {
            foreach (var handle in this.delHandles) {
              this.states[handle].valid = true;
            }
            this.delHandles.Clear();  
            for (int i = this.chkptUidStateHandle; i < this.nextUidStateHandle; i++) {
              this.states[i].valid = false;
            }
            this.states.RemoveRange(this.chkptUidStateHandle, this.nextUidStateHandle - this.chkptUidStateHandle);
            this.nextUidStateHandle = this.chkptUidStateHandle;
            throw new ServerFailure();
          }
          break;
        } catch (SocketException) {
          Thread.Sleep(5000); // sleep and then try again
        }
      }
    }

    private void ConnectToPrimaries() {
      for (int i = 0; i < this.channels.Length; i++) {
        if (this.channels[i] != null) this.channels[i].Close();
        this.channels[i] = new Channel(this.primaries[i], Service.PortNumber);
        //this.channels[i].Socket.SendTimeout = 30000;    // experimental: time out after half a minute
        //this.channels[i].Socket.ReceiveTimeout = 30000; // experimental: time out after half a minute
      }

      for (int i = 0; i < ping.numPartitions; i++) {
        this.channels[i].WriteUInt32((uint)OpCodes.OpenPartition);
        this.channels[i].WriteBytes(this.sessionID.ToByteArray());
        this.channels[i].WriteBytes(storeID.ToByteArray());
        this.channels[i].WriteInt32(i);

        // Load any per-session state. 
        this.channels[i].WriteInt32(this.chkptNum-1);
        this.channels[i].WriteInt32(this.chkptUidStateHandle);
        for (int j = 0; j < this.chkptUidStateHandle; j++) {
          var type = this.states[j].type;
          this.channels[i].WriteString(type == null ? null : type.FullName);
        }
        this.channels[i].Flush();

        var resp2 = this.channels[i].ReadInt32();
        if (resp2 != 0) {
          throw new Exception(string.Format("Server {0} could not open {1:N}", this.primaries[i], storeID));
        }
      }
      if (service != null) { Console.Write("### [connect {0}] Opened partitions on", ++this.connectCount); foreach (var p in primaries) Console.Write(" {0}", p); Console.WriteLine(); }
    }

    public void Close() {
      for (int i = 0; i < this.channels.Length; i++) {
        this.channels[i] = null;
      }
      this.isClosed = true;
    }

    public void Seal() {
      while (true) {
        try {
          var leader = new Channel(this.service.leaderName, Service.PortNumber);
          leader.WriteUInt32((uint)OpCodes.SealStore);
          leader.WriteBytes(storeID.ToByteArray());
          leader.Flush();
          var resp = leader.ReadInt32();
          leader.Close();
          if (resp == -2) {
            throw new Exception(string.Format("{0} not the SHS service leader", this.service.leaderName));
          } else if (resp == -3) {
            throw new Exception(string.Format("Store {0:N} unknown to SHS service leader", this.storeID));
          } else if (resp == -1) {
            throw new Exception(string.Format("Store {0:N} is already sealed", this.storeID));
          } else {
            Contract.Assert(resp == 0);
          }
          this.isSealed = true;
          break;
        } catch (SocketException) {
          Thread.Sleep(5000); // sleep and then try again
        }
      }
    }

    public Guid ID { get { return this.storeID; } }

    public int AddPageLinks(IEnumerator<PageLinks> pageLinks,
                            int urlCellStrideLength = 32,
                            int fwdCellStrideLength = 32,
                            int bwdCellStrideLength = 32,
                            LinkCompression fwdCellCompression = LinkCompression.VarNybble,
                            LinkCompression bwdCellCompression = LinkCompression.VarNybble) 
    {
      if (this.isClosed) throw new Exception("Store already closed");
      if (this.isSealed) throw new Exception("Store already sealed");
      while (true) {
        try {
          // Send the AddRow opcode, the store id, and the cell compression and indexing parameters
          foreach (var ch in this.channels) {
            ch.WriteUInt32((uint)OpCodes.AddPageLinks);
            ch.WriteInt32(urlCellStrideLength);
            ch.WriteInt32(fwdCellStrideLength);
            ch.WriteInt32(bwdCellStrideLength);
            ch.WriteInt32((int)fwdCellCompression);
            ch.WriteInt32((int)bwdCellCompression);
            foreach (var primary in this.primaries) {
              ch.WriteString(primary);
            }
          }

          // Send each URLs to the responsible Server process
          pageLinks.Reset();
          long pageCnt = 0;
          long linkCnt = 0;
          while (pageLinks.MoveNext()) {
            var pl = pageLinks.Current;
            pageCnt++;
            this.channels[ping.PartitionID(pl.pageUrl)].WriteString(pl.pageUrl);
            foreach (var linkUrl in pl.linkUrls) {
              linkCnt++;
              this.channels[ping.PartitionID(linkUrl)].WriteString(linkUrl);
            }
          }
          // Send the string null to all servers and flush the channel.
          foreach (var ch in this.channels) {
            ch.WriteString(null);
            ch.Flush();
          }

          // Wait for all servers to respond with Int32 0 (OK).
          // Corresponds to urlCellFinishedClosure in Server.AddRow
          foreach (var ch in this.channels) {
            int response = ch.ReadInt32();
            if (response != 0) {
              throw new Exception("Unexpected return code");
            }
          }

          // Reset pageLinks to its beginning, then send each src-dst link pair to the responsible server
          // Corresponds to Server.ReadLinks
          pageLinks.Reset();
          pageCnt = 0;
          linkCnt = 0;
          long dupPageCnt = 0;
          long dupLinkCnt = 0;
          var smt = new ThriftySetMemberTest();
          while (pageLinks.MoveNext()) {
            var pl = pageLinks.Current;
            string pageUrl = pl.pageUrl;
            pageCnt++;
            int numLinks = pl.linkUrls.Length;
            if (smt.ContainsOrAdd(pageUrl)) {
              dupPageCnt++;
              dupLinkCnt += numLinks;
            } else {
              var ch = this.channels[ping.PartitionID(pageUrl)];
              ch.WriteString(pageUrl);
              ch.WriteInt32(numLinks);
              linkCnt += numLinks;
              foreach (var linkUrl in pl.linkUrls) {
                ch.WriteString(linkUrl);
              }
            }
          }
          foreach (var ch in this.channels) {
            ch.WriteString(null);
            ch.Flush();
          }

          //Console.Error.WriteLine("DEBUG: Added {0} pages and {1} links to store (plus {2} duplicate pages with {3} links)", pageCnt, linkCnt, dupPageCnt, dupLinkCnt);

          // Repeatedly receive a batch of UID pairs (denoting additions and deletions 
          // to the bwd store) from each server in turn, and propagate them to the server
          // responsible for "uid1". Once all servers have stopped transmitting, send a 
          // -1 to each server to indicate that it's OK to start building the bwd cell.
          // Corresponds to propagateBwdsClosure in Server.AddRow
          for (; ; ) {
            int numPairsInRound = 0;
            var buffers = new List<Tuple<long, long>>[this.channels.Length];
            for (int i = 0; i < buffers.Length; i++) {
              buffers[i] = new List<Tuple<long, long>>();
            }
            foreach (var ch in this.channels) {
              int n = ch.ReadInt32();
              numPairsInRound += n;
              for (int i = 0; i < n; i++) {
                long uid1 = ch.ReadInt64();  // specifies receiving server
                long uid2 = ch.ReadInt64();  // may have deleted-bit
                buffers[ping.PartitionID(uid1)].Add(new Tuple<long, long>(uid1, uid2));
              }
            }
            if (numPairsInRound == 0) {
              foreach (var ch in this.channels) {
                ch.WriteInt32(-1);
                ch.Flush();
              }
              break;
            } else {
              for (int i = 0; i < channels.Length; i++) {
                var buffer = buffers[i];
                channels[i].WriteInt32(buffer.Count);
                foreach (var tup in buffer) {
                  channels[i].WriteInt64(tup.Item1);
                  channels[i].WriteInt64(tup.Item2);
                }
                buffer.Clear();
                channels[i].Flush();
              }
            }
          }


          // Learn whether row merge is in progress, and if so propagate 
          // the minUrlUids of the bottom rows to all servers.
          var cmMinUrlUids = new long[channels.Length];
          for (int i = 0; i < cmMinUrlUids.Length; i++) {
            cmMinUrlUids[i] = channels[i].ReadInt64();
          }
          bool cmMerge = cmMinUrlUids[0] != -1;
          // Check invariant
          Contract.Assert(Contract.ForAll(cmMinUrlUids, uid => cmMerge ^ (uid == -1)));
          if (cmMerge) {
            foreach (var ch in channels) {
              foreach (var uid in cmMinUrlUids) {
                ch.WriteInt64(uid);
              }
              ch.Flush();
            }
            foreach (var ch in channels) {
              var resp = ch.ReadInt64();
              Contract.Assert(resp == -1);
            }
          }
          foreach (var ch in channels) {
            ch.WriteInt32(0);
            ch.Flush();
          }

          // Client-side counterpart of commitClosure 
          foreach (var ch in channels) {
            var resp = ch.ReadInt32();
            Contract.Assert(resp == 0);
          }
          this.channels[0].WriteInt32(0);
          this.channels[0].Flush();
          var newEpoch = this.channels[0].ReadInt32();
          for (int i = 0; i <this.primaries.Length; i++) {
            try {
              channels[i].WriteInt32(0);
              channels[i].Flush();
            } catch (SocketException) {
              // Once the leader has committed, the primaries are informed best-effort.
              // If a primary just died, it will readvertise itself and get the updated
              // information from the leader.  All living primaries must be informed of
              // the leader commit so their state is in sync.
            }
          }
          return newEpoch;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    internal long[] MapOldToNewUids(long[] uids) {
      int[] chIds = new int[uids.Length];
      int[] chCnt = new int[channels.Length];  // All 0 by default
      for (int i = 0; i < uids.Length; i++) {
        int chId = ping.PartitionID(uids[i]);
        chIds[i] = chId;
        chCnt[chId]++;
      }
      for (int chId = 0; chId < channels.Length; chId++) {
        if (chCnt[chId] > 0) {
          channels[chId].WriteUInt32((uint)OpCodes.MapOldToNewUids);
          channels[chId].WriteInt32(chCnt[chId]);
        }
      }
      for (int i = 0; i < uids.Length; i++) {
        channels[chIds[i]].WriteInt64(uids[i]);
      }
      for (int chId = 0; chId < channels.Length; chId++) {
        if (chCnt[chId] > 0) {
          channels[chId].Flush();
        }
      }
      var res = new long[uids.Length];
      for (int i = 0; i < uids.Length; i++) {
        int chId = chIds[i];
        long uid = channels[chId].ReadInt64();
        if (uid == -1) {
          throw new Exception(string.Format("UID {0,16:X} out of range", uids[i]));
        }
        res[i] = uid;
      }
      return res;

    }

    public void Request(SubStore s) {
      if (this.isClosed) throw new Exception("Store already closed");
      while (true) {
        try {
          foreach (var ch in this.channels) {
            ch.WriteUInt32((uint)OpCodes.Request);
            ch.WriteInt32((int)s);
            ch.Flush();
          }
          foreach (var ch in this.channels) {
            ch.ReadInt32();
          }
          return;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    public void Relinquish(SubStore s) {
      if (this.isClosed) throw new Exception("Store already closed");
      while (true) {
        try {
          foreach (var ch in this.channels) {
            ch.WriteUInt32((uint)OpCodes.Relinquish);
            ch.WriteInt32((int)s);
            ch.Flush();
          }
          // Return immediately, no acknowledgment required
          return;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    // Enable caching of the most popular URL-to-UID mappings
    public void SetUrlToUidCacheParams(int logCacheSize, int logSpineSize) {
      this.hashToUidCache = new HashToUidCache(logCacheSize, logSpineSize);
      this.hasher = new Hash64();
    }

    public void PrintCacheStats() {
      if (this.hashToUidCache != null) {
        Int64 probeCnt = this.hashToUidCache.GetProbeCount();
        Int64 hitCnt = this.hashToUidCache.GetHitCount();
        Console.Error.WriteLine
          ("URL-to-UID cache: {0} probes, {1} hits, {2}% hit rate",
           probeCnt, hitCnt, 100.0 * hitCnt / probeCnt);
      }
    }

    public void MarkAtomic() {
      this.desiredEpoch = -1;
    }

    public long NumUrls() {
      return this.NumUrls(ref this.desiredEpoch);
    }

    public long NumUrls(ref int desiredEpoch) {
      if (this.isClosed) throw new Exception("Store already closed");
      while (true) {
        try {
          foreach (var ch in this.channels) {
            ch.WriteUInt32((uint)OpCodes.NumUrls);
            ch.Flush();
          }

          bool epochPassed = false;
          foreach (var ch in this.channels) {
            int currEpoch = ch.ReadInt32();
            if (desiredEpoch != -1 && desiredEpoch != currEpoch) epochPassed = true;
            desiredEpoch = currEpoch;
          }

          long sum = 0;
          foreach (var ch in this.channels) {
            sum += ch.ReadInt64();
          }
          if (epochPassed) throw new EpochPassed();
          return sum;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    public long NumLinks() {
      return this.NumLinks(ref this.desiredEpoch);
    }

    public long NumLinks(ref int desiredEpoch) {
      if (this.isClosed) throw new Exception("Store already closed");
      while (true) {
        try {
          foreach (var ch in this.channels) {
            ch.WriteUInt32((uint)OpCodes.NumLinks);
            ch.Flush();
          }

          bool epochPassed = false;
          foreach (var ch in this.channels) {
            int currEpoch = ch.ReadInt32();
            if (desiredEpoch != -1 && desiredEpoch != currEpoch) epochPassed = true;
            desiredEpoch = currEpoch;
          }

          long sum = 0;
          foreach (var ch in this.channels) {
            sum += ch.ReadInt64();
          }
          if (epochPassed) throw new EpochPassed();
          return sum;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    public int MaxDegree(Dir dir) {
      return this.MaxDegree(dir, ref this.desiredEpoch);
    }

    public int MaxDegree(Dir dir, ref int desiredEpoch) {
      if (this.isClosed) throw new Exception("Store already closed");
      while (true) {
        try {
          foreach (var ch in this.channels) {
            ch.WriteUInt32((uint)OpCodes.MaxDegree);
            ch.WriteInt32((int)dir);
            ch.Flush();
          }

          bool epochPassed = false;
          foreach (var ch in this.channels) {
            int currEpoch = ch.ReadInt32();
            if (desiredEpoch != -1 && desiredEpoch != currEpoch) epochPassed = true;
            desiredEpoch = currEpoch;
          }

          int max = 0;
          foreach (var ch in this.channels) {
            int deg = ch.ReadInt32();
            if (deg > max) max = deg;
          }
          if (epochPassed) throw new EpochPassed();
          return max;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    private void SetSumUids(ref int epoch) {
      if (this.isClosed) throw new Exception("Store already closed");
    }

    public long UidToLid(long uid) {
      return this.UidToLid(uid, ref this.desiredEpoch);
    }

    private void RefreshNumUids(ref int desiredEpoch) {
      if (this.numUidsEpoch != desiredEpoch) {
        while (true) {
          try {
            this.numUids = new long[ping.numPartitions];

            foreach (var ch in this.channels) {
              ch.WriteUInt32((uint)OpCodes.NumUrls);
              ch.Flush();
            }

            bool epochPassed = false;
            for (int i = 0; i < this.channels.Length; i++) {
              int currEpoch = channels[i].ReadInt32();
              if (desiredEpoch != -1 && desiredEpoch != currEpoch) epochPassed = true;
              desiredEpoch = currEpoch;
              this.numUids[i] = channels[i].ReadInt64();
            }
            if (epochPassed) throw new EpochPassed();
            this.numUidsEpoch = desiredEpoch;
            var lastPartID = this.ping.numPartitions - 1;
            this.lastUID = this.ping.MakeUID(lastPartID, this.numUids[lastPartID] - 1);
            break;
          } catch (SocketException) {
            this.ConnectToService();
          }
        }
      }
    }

    public long UidToLid(long uid, ref int desiredEpoch) {
      if (this.isClosed) throw new Exception("Store already closed");
      if (uid == -1) return -1;
      this.RefreshNumUids(ref desiredEpoch);
      long res = 0;
      var n = ping.PartitionID(uid);
      for (int i = 0; i < n; i++) {
        res += this.numUids[i];
      }
      return res + ping.PUID(uid);
    }

    public IEnumerable<Int64> Uids() {
      return this.Uids(ref this.desiredEpoch);
    }

    public IEnumerable<Int64> Uids(ref int desiredEpoch) {
      if (this.isClosed) throw new Exception("Store already closed");
      this.RefreshNumUids(ref desiredEpoch);
      return this.InnerUids();
    }

    private IEnumerable<Int64> InnerUids() {
      for (int partID = 0; partID < this.ping.numPartitions; partID++) {
        for (Int64 puid = 0; puid < this.numUids[partID]; puid++) {
          yield return ping.MakeUID(partID, puid);
        }
      }
    }

    public bool IsLastUid(Int64 uid) {
      var partID = this.ping.numPartitions - 1;
      return uid == ping.MakeUID(partID, this.numUids[partID] - 1);
    }

    /// <summary>
    /// This method takes a URL.  If the current epoch of the store has advanced
    /// since the last call to MarkAtomic, throw EpochPassed; otherwise, 
    /// if the store contains the URL, return the corresponding UID, else 
    /// return -1.
    /// </summary>
    /// <param name="url">THE URL to look up in the store.</param>
    /// <returns>The UID corresponding to the URL, or -1 of the URL is not contained in this store.</returns>
    /// <exception cref="EpochPassed">The desired epoch has passed.</exception>
    public long UrlToUid(string url) {
      return this.UrlToUid(url, ref this.desiredEpoch);
    }

    /// <summary>
    /// This method takes a URL and an epoch.  If the epoch is not -1 and does 
    /// not match the current epoch of the store, throw EpochPassed; otherwise, 
    /// if the store contains the URL, return the corresponding UID, else 
    /// return -1.
    /// </summary>
    /// <param name="url">THE URL to look up in the store.</param>
    /// <param name="desiredEpoch">The desired epoch (-1 indicates no preference)</param>
    /// <returns>The UID corresponding to the URL, or -1 of the URL is not contained in this store.</returns>
    /// <exception cref="EpochPassed">The desired epoch has passed.</exception>
    public long UrlToUid(string url, ref int desiredEpoch) {
      if (this.isClosed) throw new Exception("Store already closed");
      while (true) {
        try {
          ulong hash = 0;
          if (this.hashToUidCache != null) {
            hash = this.hasher.Hash(url);
            long uid = this.hashToUidCache.Lookup(hash);
            if (uid != -1) return uid;
          }
          int chId = this.ping.PartitionID(url);
          var ch = this.channels[chId];
          ch.WriteUInt32((uint)OpCodes.UrlToUid);
          ch.WriteString(url);
          ch.Flush();
          int currEpoch = ch.ReadInt32();
          long res = ch.ReadInt64();
          if (desiredEpoch != -1 && desiredEpoch != currEpoch) throw new EpochPassed();
          desiredEpoch = currEpoch;
          if (this.hashToUidCache != null) this.hashToUidCache.Add(hash, res);
          return res;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    public long[] BatchedUrlToUid(string[] urls) {
      return this.BatchedUrlToUid(urls, ref this.desiredEpoch, false);
    }

    public long[] BatchedUrlToUid(string[] urls, ref int desiredEpoch) {
      return this.BatchedUrlToUid(urls, ref desiredEpoch, false);
    }

    internal long[] PrivilegedUrlToUid(string[] urls) {
      int desiredEpoch = -1;
      return this.InnerBatchedUrlToUid(urls, ref desiredEpoch, true);
    }

    private long[] BatchedUrlToUid(string[] urls, ref int desiredEpoch, bool privileged) {
      if (this.isClosed) throw new Exception("Store already closed");
      while (true) {
        try {
          return this.InnerBatchedUrlToUid(urls, ref desiredEpoch, privileged);
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    private long[] InnerBatchedUrlToUid(string[] urls, ref int desiredEpoch, bool privileged) {
      // Consult cache; record channel IDs for non-cached URLs
      var n = urls.Length;
      var uids = new long[n];
      var chIds = new int[n];
      var chCnt = new int[channels.Length];  // All 0 by default

      for (int i = 0; i < n; i++) {
        if (this.hashToUidCache != null) {
          var hash = this.hasher.Hash(urls[i]);
          var uid = this.hashToUidCache.Lookup(hash);
          if (uid != -1) {
            uids[i] = uid;
            chIds[i] = -1;
            continue;
          }
        }
        chIds[i] = ping.PartitionID(urls[i]);
        chCnt[chIds[i]]++;
      }

      // Send a batched request to each involved server
      for (int chId = 0; chId < channels.Length; chId++) {
        if (chCnt[chId] > 0) {
          channels[chId].WriteUInt32((uint)OpCodes.BatchedUrlToUid);
          channels[chId].WriteInt32(privileged ? 1 : 0);
          channels[chId].WriteInt32(chCnt[chId]);
        }
      }
      for (int i = 0; i < n; i++) {
        if (chIds[i] != -1) {
          channels[chIds[i]].WriteString(urls[i]);
        }
      }
      for (int chId = 0; chId < channels.Length; chId++) {
        if (chCnt[chId] > 0) {
          channels[chId].Flush();
        }
      }
      // Receive responses from each contacted server
      bool epochPassed = false;
      var debugEpochPassedIssue = new int[this.channels.Length];
      for (int chId = 0; chId < channels.Length; chId++) {
        if (chCnt[chId] > 0) {
          int currEpoch = channels[chId].ReadInt32();
          debugEpochPassedIssue[chId] = currEpoch;
          if (desiredEpoch != -1 && desiredEpoch != currEpoch) {
            epochPassed = true;
          }
          desiredEpoch = currEpoch;
        } else {
          debugEpochPassedIssue[chId] = -1;  // for debugging only
        }
      }

      for (int i = 0; i < n; i++) {
        if (chIds[i] != -1) {
          Int64 uid = channels[chIds[i]].ReadInt64();
          if (this.hashToUidCache != null) {
            UInt64 hash = this.hasher.Hash(urls[i]);
            // The same URL may occur multiple times in a batch.  Since it
            // is a checked run-time error to add an already cached hash to
            // the cache, we must check that hash is not yet cached.
            Int64 cuid = this.hashToUidCache.Lookup(hash);
            if (cuid != -1 && cuid != uid) {
              this.hashToUidCache.Add(hash, uid);
            }
          }
          uids[i] = uid;
        }
      }

      if (epochPassed) {
        Console.Write("### Unexpected EpochPassed: "); for (int i = 0; i < this.primaries.Length; i++) Console.Write(" {0}={1}", this.primaries[i], debugEpochPassedIssue[i]); Console.WriteLine();
          throw new EpochPassed();
      }

      // Return result
      return uids;
    }

    /// <summary>
    /// This method takes a UID.  If the current epoch of the store has advanced
    /// since the last call to MarkAtomic, throw EpochPassed; otherwise, if the 
    /// store contains the UID, return the corresponding URL, else throw an 
    /// Exception.
    /// </summary>
    /// <param name="uid">The UID to look up in the store.</param>
    /// <returns>The URL corresponding to the UID.</returns>
    /// <exception cref="EpochPassed">The desired epoch has passed.</exception>
    /// <exception cref="Exception">This store does not contain the UID.</exception>
    public string UidToUrl(long uid) {
      return this.UidToUrl(uid, ref this.desiredEpoch);
    }

    /// <summary>
    /// This method takes a UID and an epoch.  If the epoch is not -1 and does 
    /// not match the current epoch of the store, throw EpochPassed; otherwise, 
    /// if the store contains the UID, return the corresponding URL, else 
    /// throw an Exception.
    /// </summary>
    /// <param name="uid">The UID to look up in the store.</param>
    /// <param name="desiredEpoch">The desired epoch (-1 indicates no preference)</param>
    /// <returns>The URL corresponding to the UID.</returns>
    /// <exception cref="EpochPassed">The desired epoch has passed.</exception>
    /// <exception cref="Exception">This store does not contain the UID.</exception>
    public string UidToUrl(Int64 uid, ref int desiredEpoch) {
      if (this.isClosed) throw new Exception("Store already closed");
      while (true) {
        try {
          var ch = this.channels[ping.PartitionID(uid)];
          ch.WriteUInt32((uint)OpCodes.UidToUrl);
          ch.WriteInt64(uid);
          ch.Flush();
          int currentEpoch = ch.ReadInt32();
          var urlBytes = ch.ReadBytes();
          if (desiredEpoch != -1 && desiredEpoch != currentEpoch) throw new EpochPassed();
          desiredEpoch = currentEpoch;
          if (urlBytes == null) throw new Exception(string.Format("UID {0,16:X} out of range", uid));
          return System.Text.Encoding.UTF8.GetString(urlBytes);
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    public string[] BatchedUidToUrl(long[] uids) {
      return this.BatchedUidToUrl(uids, ref this.desiredEpoch);
    }

    public string[] BatchedUidToUrl(long[] uids, ref int desiredEpoch) {
      if (this.isClosed) throw new Exception("Store already closed");
      while (true) {
        try {
          var n = uids.Length;
          var urls = new string[n];
          var chIds = new int[n];
          var chCnt = new int[channels.Length];  // All 0 by default

          for (int i = 0; i < n; i++) {
            chIds[i] = ping.PartitionID(uids[i]);
            chCnt[chIds[i]]++;
          }

          // Send a batched request to each involved server
          for (int chId = 0; chId < channels.Length; chId++) {
            if (chCnt[chId] > 0) {
              channels[chId].WriteUInt32((uint)OpCodes.BatchedUidToUrl);
              channels[chId].WriteInt32(chCnt[chId]);
            }
          }
          for (int i = 0; i < n; i++) {
            channels[chIds[i]].WriteInt64(uids[i]);
          }
          for (int chId = 0; chId < channels.Length; chId++) {
            if (chCnt[chId] > 0) {
              channels[chId].Flush();
            }
          }

          // Receive responses from each contacted server
          bool epochPassed = false;
          for (int chId = 0; chId < channels.Length; chId++) {
            if (chCnt[chId] > 0) {
              int currEpoch = channels[chId].ReadInt32();
              if (desiredEpoch != -1 && desiredEpoch != currEpoch) epochPassed = true;
              desiredEpoch = currEpoch;
            }
          }

          for (int i = 0; i < n; i++) {
            var urlBytes = channels[chIds[i]].ReadBytes();
            if (urlBytes == null) {
              throw new Exception(string.Format("UID {0,16:X} out of range", uids[i]));
            }
            urls[i] = System.Text.Encoding.UTF8.GetString(urlBytes);
          }

          if (epochPassed) throw new EpochPassed();

          // Return result
          return urls;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    public int GetDegree(Int64 uid, Dir dir) {
      return this.GetDegree(uid, dir, ref this.desiredEpoch);
    }

    public int GetDegree(Int64 uid, Dir dir, ref int desiredEpoch) {
      if (this.isClosed) throw new Exception("Store already closed");
      while (true) {
        try {
          var ch = this.channels[ping.PartitionID(uid)];
          ch.WriteUInt32((uint)OpCodes.GetDegree);
          ch.WriteInt32((int)dir);
          ch.WriteInt64(uid);
          ch.Flush();
          int currentEpoch = ch.ReadInt32();
          int degree = ch.ReadInt32();
          if (desiredEpoch != -1 && desiredEpoch != currentEpoch) throw new EpochPassed();
          desiredEpoch = currentEpoch;
          if (degree == -1) {
            throw new Exception(string.Format("UID {0,16:X} out of range", uid));
          }
          return degree;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    public int[] BatchedGetDegree(Int64[] uids, Dir dir) {
      return this.BatchedGetDegree(uids, dir, ref this.desiredEpoch);
    }

    public int[] BatchedGetDegree(Int64[] uids, Dir dir, ref int desiredEpoch) {
      if (this.isClosed) throw new Exception("Store already closed");
      while (true) {
        try {
          int[] chIds = new int[uids.Length];
          int[] chCnt = new int[channels.Length];  // All 0 by default
          for (int i = 0; i < uids.Length; i++) {
            int chId = ping.PartitionID(uids[i]);
            chIds[i] = chId;
            chCnt[chId]++;
          }
          for (int chId = 0; chId < channels.Length; chId++) {
            if (chCnt[chId] > 0) {
              channels[chId].WriteUInt32((uint)OpCodes.BatchedGetDegree);
              channels[chId].WriteInt32((int)dir);
              channels[chId].WriteInt32(chCnt[chId]);
            }
          }
          for (int i = 0; i < uids.Length; i++) {
            channels[chIds[i]].WriteInt64(uids[i]);
          }
          for (int chId = 0; chId < channels.Length; chId++) {
            if (chCnt[chId] > 0) {
              channels[chId].Flush();
            }
          }

          bool epochPassed = false;
          for (int chId = 0; chId < channels.Length; chId++) {
            if (chCnt[chId] > 0) {
              int currEpoch = channels[chId].ReadInt32();
              if (desiredEpoch != -1 && desiredEpoch != currEpoch) epochPassed = true;
              desiredEpoch = currEpoch;
            }
          }

          var degrees = new int[uids.Length];
          for (int i = 0; i < uids.Length; i++) {
            int chId = chIds[i];
            int degree = channels[chId].ReadInt32();
            if (degree == -1) {
              throw new Exception(string.Format("UID {0,16:X} out of range", uids[i]));
            }
            degrees[i] = degree;
          }

          if (epochPassed) throw new EpochPassed();

          return degrees;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    public long[] GetLinks(long uid, Dir dir) {
      return this.GetLinks(uid, dir, ref this.desiredEpoch);
    }

    public long[] GetLinks(long uid, Dir dir, ref int desiredEpoch) {
      return this.SampleLinks(uid, dir, -1, false, ref desiredEpoch);
    }

    public long[][] BatchedGetLinks(long[] uids, Dir dir) {
      return this.BatchedGetLinks(uids, dir, ref this.desiredEpoch);
    }

    public long[][] BatchedGetLinks(long[] uids, Dir dir, ref int desiredEpoch) {
      return this.BatchedSampleLinks(uids, dir, -1, false, ref desiredEpoch);
    }

    public long[] SampleLinks(long uid, Dir dir, int numSamples, bool consistent) {
      return this.SampleLinks(uid, dir, numSamples, consistent, ref this.desiredEpoch);
    }

    public long[] SampleLinks(long uid, Dir dir, int numSamples, bool consistent, ref int desiredEpoch) {
      if (this.isClosed) throw new Exception("Store already closed");
      if (numSamples < -1) {
        throw new Exception("numSamples out of range");
      }
      while (true) {
        try {
          int id = ping.PartitionID(uid);
          var ch = this.channels[id];
          ch.WriteUInt32((uint)OpCodes.SampleLinks);
          ch.WriteInt32((int)dir);
          ch.WriteInt32(numSamples);
          ch.WriteInt32(consistent ? 1 : 0);
          ch.WriteInt64(uid);
          ch.Flush();
          int currentEpoch = ch.ReadInt32();
          int n = ch.ReadInt32();
          if (n == -1) {
            throw new Exception(string.Format("UID {0,16:X} out of range", uid));
          }
          Int64[] res = new Int64[n];
          for (int i = 0; i < n; i++) {
            res[i] = ch.ReadInt64();
          }
          if (desiredEpoch != -1 && desiredEpoch != currentEpoch) throw new EpochPassed();
          desiredEpoch = currentEpoch;
          return res;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    public long[][] BatchedSampleLinks(long[] uids, Dir dir, int numSamples, bool consistent) {
      return this.BatchedSampleLinks(uids, dir, numSamples, consistent, ref this.desiredEpoch);
    }

    public long[][] BatchedSampleLinks(long[] uids, Dir dir, int numSamples, bool consistent, ref int desiredEpoch) {
      if (this.isClosed) throw new Exception("Store already closed");
      if (numSamples < -1) {
        throw new Exception("numSamples out of range");
      }
      while (true) {
        try {
          int[] chIds = new int[uids.Length];
          int[] chCnt = new int[channels.Length];  // All 0 by default
          for (int i = 0; i < uids.Length; i++) {
            int chId = ping.PartitionID(uids[i]);
            chIds[i] = chId;
            chCnt[chId]++;
          }
          for (int chId = 0; chId < channels.Length; chId++) {
            if (chCnt[chId] > 0) {
              channels[chId].WriteUInt32((uint)OpCodes.BatchedSampleLinks);
              channels[chId].WriteInt32((int)dir);
              channels[chId].WriteInt32(numSamples);
              channels[chId].WriteInt32(consistent ? 1 : 0);
              channels[chId].WriteInt32(chCnt[chId]);
            }
          }
          for (int i = 0; i < uids.Length; i++) {
            channels[chIds[i]].WriteInt64(uids[i]);
          }
          for (int chId = 0; chId < channels.Length; chId++) {
            if (chCnt[chId] > 0) {
              channels[chId].Flush();
            }
          }

          bool epochPassed = false;
          for (int chId = 0; chId < channels.Length; chId++) {
            if (chCnt[chId] > 0) {
              int currEpoch = channels[chId].ReadInt32();
              if (desiredEpoch != -1 && desiredEpoch != currEpoch) epochPassed = true;
              desiredEpoch = currEpoch;
            }
          }

          Int64[][] res = new Int64[uids.Length][];
          for (int i = 0; i < uids.Length; i++) {
            int chId = chIds[i];
            int n = channels[chId].ReadInt32();
            if (n == -1) {
              throw new Exception(string.Format("UID {0,16:X} out of range", uids[i]));
            }
            res[i] = new Int64[n];
            for (int j = 0; j < n; j++) {
              res[i][j] = channels[chId].ReadInt64();
            }
          }

          if (epochPassed) throw new EpochPassed();

          return res;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    public UidState<T> AllocateUidState<T>() {
      if (this.isClosed) throw new Exception("Store already closed");
      if (!this.isSealed) throw new Exception("Store is not sealed");
      while (true) {
        try {
          this.dirtyStates = true;
          int handle = this.nextUidStateHandle++;
          foreach (var ch in this.channels) {
            ch.WriteUInt32((uint)OpCodes.AllocateUidState);
            ch.WriteInt32(handle);
            ch.WriteString(typeof(T).FullName);
            ch.Flush();
          }
          foreach (var ch in this.channels) {
            var resp = ch.ReadBytes();
            if (resp != null) throw new Exception(System.Text.Encoding.UTF8.GetString(resp));
          }
          var state = new UidState<T>(this, handle);
          this.states.Add(state);
          return state;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    public void Checkpoint() {
      while (true) {
        try {
          foreach (var ch in this.channels) {
            ch.WriteUInt32((uint)OpCodes.CheckpointUidStates);
            ch.WriteInt32(this.chkptNum);
            ch.WriteInt32(this.nextUidStateHandle);
            ch.Flush();
          }

          // Wait for confirmation that each primary has checkpointed its state
          foreach (var ch in this.channels) {
            var resp1 = ch.ReadInt32();
            Contract.Assert(resp1 == 0);
          }

          // Ask the leader to drive replication of state from primaries to secondaries.
          var leader = this.channels[0];
          leader.WriteInt32(this.primaries.Length);
          foreach (var primary in primaries) {
            leader.WriteString(primary);
          }
          leader.Flush();
          var resp2 = leader.ReadInt32();
          Contract.Assert(resp2 == -9997);
          
          // Data has been replicated as well as possible.  Commit checkpoint.
          this.chkptNum++;
          this.chkptUidStateHandle = this.nextUidStateHandle;
          this.dirtyStates = false;
          foreach (var handle in this.delHandles) {
            this.states[handle] = null;
          }
          delHandles.Clear();

          // Ask leader to ask secondaries to clean up
          leader.WriteInt32(-9996);
          leader.Flush();
          var resp3 = leader.ReadInt32();
          Contract.Assert(resp3 == -9995);

          // Ask primaries to clean up
          foreach (var ch in this.channels) {
            ch.WriteInt32(-9994);
            ch.Flush();
          }
          break;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    internal void FreeUidState(int handle) {
      if (this.isClosed) throw new Exception("Store already closed");
      if (!this.isSealed) throw new Exception("Store is not sealed");
      foreach (var ch in this.channels) {
        try {
          ch.WriteUInt32((uint)OpCodes.FreeUidState);
          ch.WriteInt32(handle);
          ch.Flush();
        } catch (SocketException) {
          // freeing state is best-effort
        }
      }
      this.delHandles.Add(handle);
    }

    internal T GetUidState<T>(int handle, Serializer serial, long uid) {
      if (this.isClosed) throw new Exception("Store already closed");
      if (!this.isSealed) throw new Exception("Store is not sealed");
      while (true) {
        try {
          var ch = this.channels[ping.PartitionID(uid)];
          ch.WriteUInt32((uint)OpCodes.GetUidState);
          ch.WriteInt32(handle);
          ch.WriteInt64(uid);
          ch.Flush();
          return (T)serial.Read(ch);
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    internal void SetUidState<T>(int handle, Serializer serial, long uid, T val) {
      if (this.isClosed) throw new Exception("Store already closed");
      if (!this.isSealed) throw new Exception("Store is not sealed");
      this.dirtyStates = true;
      while (true) {
        try {
          var ch = this.channels[ping.PartitionID(uid)];
          ch.WriteUInt32((uint)OpCodes.SetUidState);
          ch.WriteInt32(handle);
          ch.WriteInt64(uid);
          serial.Write(val, ch);
          break;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    internal T[] GetManyUidState<T>(int handle, Serializer serial, long[] uids) {
      if (this.isClosed) throw new Exception("Store already closed");
      if (!this.isSealed) throw new Exception("Store is not sealed");
      while (true) {
        try {
          var chIds = new int[uids.Length];
          var chCnt = new int[this.channels.Length];  // All 0 by default
          for (int i = 0; i < uids.Length; i++) {
            int chId = ping.PartitionID(uids[i]);
            chIds[i] = chId;
            chCnt[chId]++;
          }
          for (int chId = 0; chId < channels.Length; chId++) {
            if (this.isClosed) throw new Exception("Store already closed");
            if (chCnt[chId] > 0) {
              channels[chId].WriteUInt32((uint)OpCodes.BatchedGetUidState);
              channels[chId].WriteInt32(handle);
              channels[chId].WriteInt32(chCnt[chId]);
            }
          }
          for (int i = 0; i < uids.Length; i++) {
            channels[chIds[i]].WriteInt64(uids[i]);
          }
          for (int chId = 0; chId < channels.Length; chId++) {
            if (chCnt[chId] > 0) {
              channels[chId].Flush();
            }
          }
          var res = new T[uids.Length];
          for (int i = 0; i < uids.Length; i++) {
            res[i] = (T)serial.Read(channels[chIds[i]]);
          }
          return res;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    internal void SetManyUidState<T>(int handle, Serializer serial, long[] uids, T[] vals) {
      if (this.isClosed) throw new Exception("Store already closed");
      if (!this.isSealed) throw new Exception("Store is not sealed");
      this.dirtyStates = true;
      while (true) {
        try {
          var chIds = new int[uids.Length];
          var chCnt = new int[channels.Length];  // All 0 by default
          for (int i = 0; i < uids.Length; i++) {
            int chId = ping.PartitionID(uids[i]);
            chIds[i] = chId;
            chCnt[chId]++;
          }
          for (int chId = 0; chId < channels.Length; chId++) {
            if (chCnt[chId] > 0) {
              channels[chId].WriteUInt32((uint)OpCodes.BatchedSetUidState);
              channels[chId].WriteInt32(handle);
              channels[chId].WriteInt32(chCnt[chId]);
            }
          }
          for (int i = 0; i < uids.Length; i++) {
            channels[chIds[i]].WriteInt64(uids[i]);
            serial.Write(vals[i], channels[chIds[i]]);
          }
          break;
        } catch (SocketException) {
          this.ConnectToService();
        }
      }
    }

    internal IEnumerable<UidVal<T>> GetAllUidState<T>(int handle,
                                                      Serializer serial)
    {
      if (this.isClosed) throw new Exception("Store already closed");
      if (!this.isSealed) throw new Exception("Store is not sealed");
      Batch<long> batch = new Batch<long>(1 << 18);
      foreach (var uid in this.Uids()) {
        batch.Add(uid);
        if (batch.Full || this.IsLastUid(uid)) {
          long[] uids = batch;
          var items = this.GetManyUidState<T>(handle, serial, uids);
          for (int i = 0; i < uids.Length; i++) {
            yield return new UidVal<T>(uids[i], items[i]);
          }
          batch.Reset();
        }
      }
    }

    internal void SetAllUidState<T>(int handle, Serializer serial, Func<long,T> f) {
      if (this.isClosed) throw new Exception("Store already closed");
      if (!this.isSealed) throw new Exception("Store is not sealed");
      Batch<long> uidBatch = new Batch<long>(1 << 18);
      Batch<T> itemBatch = new Batch<T>(1 << 18);
      foreach (var uid in this.Uids()) {
        uidBatch.Add(uid);
        itemBatch.Add(f(uid));
        if (uidBatch.Full || this.IsLastUid(uid)) {
          this.SetManyUidState<T>(handle, serial, uidBatch, itemBatch);
          uidBatch.Reset();
          itemBatch.Reset();
        }
      }
    }
  }
}
