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
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace SHS {
  internal class Partition {

    internal class ThreadLocalState {
      internal readonly Partition partition;
      private int epoch;   // the epoch for which the two linkCellRds are valid; protected by a read-share of partition.rwlock
      internal bool urlLoaded;
      internal readonly bool[] linkLoaded;
      internal readonly Cell.LinkCellRd[][] linkCellRds;

      internal ThreadLocalState(Partition partition) {
        Contract.Requires(partition != null);

        this.partition = partition;
        this.epoch = -1;
        this.urlLoaded = false;
        this.linkLoaded = new bool[2];
        this.linkCellRds = new Cell.LinkCellRd[2][];
      }

      internal Cell.LinkCellRd[] GetLinkCellRds(int dir) {
        Contract.Requires(dir == 0 || dir == 1);
        Contract.Assert(this.linkLoaded[dir]);
        if (this.epoch < this.partition.Epoch) {
          this.linkCellRds[0] = null;
          this.linkCellRds[1] = null;
        }
        if (this.linkCellRds[dir] == null) {
          int n = this.partition.cells.Count;
          this.linkCellRds[dir] = new Cell.LinkCellRd[n];
          for (int i = 0; i < n; i++) {
            this.linkCellRds[dir][i] = new Cell.LinkCellRd(this.partition.cells[i].linkCell[dir]);
          }
        }
        return this.linkCellRds[dir];
      }
    }

    // Fields of Partition object ===============

    // The following fields are readonly after init and thus not protected by a lock
    internal readonly SPID spid;
    internal readonly Partitioning ping;

    // The following fields are protected by locking "this"
    private int urlRefCnt;                // Number of client threads expecting loaded URL substore
    private readonly int[] linkRefCnt;    // Number of client threads expecting loaded link substores
    private Cell newCell;

    // cells has integrated readers-writer lock machinery.  callers must ensure to hold the appropriate lock.
    private readonly Cells cells;

    // The following two fields are used during cell creation & merging.
    // They are immutable after creation, but they can be nulled out. 
    private UidSpaceMapperBundle mapperBundle;

    // require epochs to be in increasing sorted order
    internal Partition(SPID spid, Partitioning ping, List<Cell> cells) {
      this.spid = spid;
      this.ping = ping;

      this.urlRefCnt = 0;
      this.linkRefCnt = new int[2];

      this.mapperBundle = null;
      this.newCell = null;
      foreach (var cell in cells) {
        cell.part = this;
      }
      this.cells = new Cells(cells);
    }

    private void LoadUrls(ThreadLocalState tls) {
      if (!tls.urlLoaded) {
        lock (this) {
          if (this.urlRefCnt++ == 0) {
            for (int i = 0; i < cells.Count; i++) {
              cells[i].urlCell.Load();
            }
            if (this.newCell != null) this.newCell.urlCell.Load();
          }
          tls.urlLoaded = true;
        }
      }
    }

    private void UnloadUrls(ThreadLocalState tls) {
      if (tls.urlLoaded) {
        lock (this) {
          if (--this.urlRefCnt == 0) {
            for (int i = 0; i < cells.Count; i++) {
              cells[i].urlCell.Unload();
            }
            if (this.newCell != null) this.newCell.urlCell.Unload();
          }
          tls.urlLoaded = false;
        }
      }
    }


    private void LoadLinks(ThreadLocalState tls, int dir) {
      Contract.Requires(dir == 0 || dir == 1);
      if (!tls.linkLoaded[dir]) {
        lock (this) {
          if (this.linkRefCnt[dir]++ == 0) {
            for (int i = 0; i < cells.Count; i++) {
              cells[i].linkCell[dir].Load();
            }
            if (this.newCell != null) this.newCell.linkCell[dir].Load();
          }
          tls.linkLoaded[dir] = true;
        }
      }
    }

    private void UnloadLinks(ThreadLocalState tls, int dir) {
      Contract.Requires(dir == 0 || dir == 1);
      if (tls.linkLoaded[dir]) {
        lock (this) {
          if (--this.linkRefCnt[dir] == 0) {
            for (int i = 0; i < cells.Count; i++) {
              cells[i].linkCell[dir].Unload();
            }
            if (this.newCell != null) this.newCell.linkCell[dir].Unload();
          }
          tls.linkLoaded[dir] = false;
          tls.linkCellRds[dir] = null;
        }
      }
    }

    internal void DeregisterTLS(ThreadLocalState tls) {
      this.cells.EnterReadLock();
      try {
        this.UnloadUrls(tls);
        this.UnloadLinks(tls, 0);
        this.UnloadLinks(tls, 1);
      } finally {
        this.cells.ExitReadLock();
      }
    }

    internal void RequestSubStore(ThreadLocalState tls, SubStore s) {
      this.cells.EnterReadLock();
      try {
        switch (s) {
          case SubStore.UrlStore:
            this.LoadUrls(tls);   
            break;
          case SubStore.FwdStore:
            this.LoadLinks(tls, 0);
            break;
          case SubStore.BwdStore:
            this.LoadLinks(tls, 1);
            break;
        }
      } finally {
        this.cells.ExitReadLock();
      }
    }

    internal void RelinquishSubStore(ThreadLocalState tls, SubStore s) {
      this.cells.EnterReadLock();
      try {
        switch (s) {
          case SubStore.UrlStore:
            this.UnloadUrls(tls);
            break;
          case SubStore.FwdStore:
            this.UnloadLinks(tls, 0);
            break;
          case SubStore.BwdStore:
            this.UnloadLinks(tls, 1);
            break;
        }
      } finally {
        this.cells.ExitReadLock();
      }
    }

    internal int Epoch { 
      get {
        int n = this.cells.Count;
        return n == 0 ? -1 : this.cells[n-1].epoch;
      } 
    }

    [Pure]
    internal long BaseUID {
      get {
        return this.ping.MakeUID(this.spid.partID, 0);
      }
    }

    [Pure]
    internal long SupraUID {
      get {
        var n = this.cells.Count;
        return n == 0 ? BaseUID : this.cells[n - 1].supraUID;
      }
    }

    [Pure]
    private int CellIndex(long uid) {
      Contract.Requires(BaseUID <= uid && uid < SupraUID);
      for (int i = 0; i < this.cells.Count; i++) {
        var cell = this.cells[i];
        if (cell.baseUID <= uid && uid < cell.supraUID) return i;
      }
      throw new Exception(string.Format("UID {0:X16} not contained in {1}", uid, this.spid));
    }

    internal long NumUids(out int epoch) {
      this.cells.EnterReadLock();
      try {
        epoch = this.Epoch;
        long numUids = 0;
        for (int i = 0; i < this.cells.Count; i++) {
          numUids += cells[i].numUrls;
        }
        return numUids;
      } finally {
        this.cells.ExitReadLock();
      }
    }

    internal long NumLinks(out int epoch) {
      this.cells.EnterReadLock();
      try {
        epoch = this.Epoch;
        long numLinks = 0;
        for (int i = 0; i < this.cells.Count; i++) {
          numLinks += cells[i].linkCell[0].numLinks;
        }
        return numLinks;
      } finally {
        this.cells.ExitReadLock();
      }
    }

    internal int MaxDegree(ThreadLocalState partTLS, int dir, out int epoch) {
      this.cells.EnterReadLock();
      try {
        epoch = this.Epoch;
        this.LoadUrls(partTLS);
        this.LoadLinks(partTLS, dir);
        var linkCellRds = partTLS.GetLinkCellRds(dir);
        int res = 0;
        var supraUID = this.SupraUID;
        for (long uid = BaseUID; uid < supraUID; uid++) {
          var deg = this.GetLinks(linkCellRds, uid).Count;
          if (deg > res) res = deg;
        }
        return res;
      } finally {
        this.cells.ExitReadLock();
      }
    }

    internal long UrlToUid(ThreadLocalState tls, byte[] url, out int epoch, bool privileged) {
      this.cells.EnterReadLock();
      try {
        epoch = this.Epoch;
        this.LoadUrls(tls);
        return UrlToUid(url, privileged);
      } finally {
        this.cells.ExitReadLock();
      }
    }

    internal long[] UrlsToUids(ThreadLocalState tls, byte[][] urls, out int epoch, bool privileged) {
      this.cells.EnterReadLock();
      try {
        epoch = this.Epoch;
        this.LoadUrls(tls);
        var uids = new long[urls.Length];
        for (int k = 0; k < urls.Length; k++) {
          uids[k] = UrlToUid(urls[k], privileged);
        }
        return uids;
      } finally {
        this.cells.ExitReadLock();
      }
    }

    private long UrlToUid(byte[] url, bool privileged) {
      if (privileged) {
        try {
          long uid = this.newCell.urlCell.UrlToUid(url);
          if (uid != -1) return uid;
        } catch (NullReferenceException) {
          // If one of the servers dies during the course of an AddRow, the client 
          // closes the connections to all other primaries and establishes new connections.
          // When the connection to a surving primary is severed, the "finally" clause 
          // of AddRow responsible to clean up sets this.newCell to null. However, it 
          // is possible that a peer server has a privileged "UrlsToUids" request
          // in flight while this happening.  This may cause a NullReferenceException.
          // If that happens, throw a SocketException, which will cause the service 
          // thread to close the connection to the peer server (which is about to 
          // happen anyway since the system is in the process of reconfiguring itself)
          // and to terminate.
          throw new SocketException();
        }
      }
      for (int i = this.cells.Count - 1; i >= 0; i--) { // go from newest to oldest cell
        long uid = this.cells[i].urlCell.UrlToUid(url);
        if (uid != -1) return uid;
      }
      return -1;
    }

    internal byte[][] UidsToUrls(ThreadLocalState tls, long[] uids, out int epoch) {
      this.cells.EnterReadLock();
      try {
        epoch = this.Epoch;
        this.LoadUrls(tls);
        var urls = new byte[uids.Length][];
        for (int i = 0; i < uids.Length; i++) {
          Contract.Assert(this.ping.PartitionID(uids[i]) == this.spid.partID);
          urls[i] = this.cells[this.CellIndex(uids[i])].urlCell.UidToUrl(uids[i]);
        }
        return urls;
      } finally {
        this.cells.ExitReadLock();
      }
    }

    internal byte[] UidToUrl(ThreadLocalState tls, long uid, out int epoch) {
      Contract.Assert(this.ping.PartitionID(uid) == this.spid.partID);
      this.cells.EnterReadLock();
      try {
        epoch = this.Epoch;
        this.LoadUrls(tls);
        return this.cells[this.CellIndex(uid)].urlCell.UidToUrl(uid);
      } finally {
        this.cells.ExitReadLock();
      }
    }

    internal List<long>[] GetLinks(ThreadLocalState partTLS, long[] uids, int dir, out int epoch) {
      this.cells.EnterReadLock();
      try {
        epoch = this.Epoch;
        this.LoadLinks(partTLS, dir);
        var linkCellRds = partTLS.GetLinkCellRds(dir);
        var res = new List<long>[uids.Length];
        for (int k = 0; k < uids.Length; k++) {
          res[k] = this.GetLinks(linkCellRds, uids[k]);
        }
        return res;
      } finally {
        this.cells.ExitReadLock();
      }
    }

    internal List<long> GetLinks(ThreadLocalState partTLS, long uid, int dir, out int epoch) {
      Contract.Ensures(Contract.Result<List<long>>() != null);
      Contract.Ensures(Contract.ForAll(0, Contract.Result<List<long>>().Count, i => !UID.HasDeletedBit(Contract.Result<List<long>>()[i])));
      Contract.Ensures(Contract.Result<List<long>>().Count < 2 || Contract.ForAll(1, Contract.Result<List<long>>().Count, i => Contract.Result<List<long>>()[i - 1] < Contract.Result<List<long>>()[i]));

      this.cells.EnterReadLock();
      try {
        epoch = this.Epoch;
        this.LoadLinks(partTLS, dir);
        var linkCellRds = partTLS.GetLinkCellRds(dir);
        return this.GetLinks(linkCellRds, uid);
      } finally {
        this.cells.ExitReadLock();
      }
    }

    private List<long> GetLinks(Cell.LinkCellRd[] linkCellRds, long uid) {
      // start at lowest row that may actually contain prevUid
      var idx = this.CellIndex(uid);
      var n = this.cells.Count;
      var linkSet = new List<long>[n - idx];
      for (int i = idx; i < n; i++) {
        linkSet[i - idx] = linkCellRds[i].GetLinks(uid);
      }
      return CombineLinks(linkSet);
    }

    internal void Delete() {
      this.cells.EnterWriteLock();
      try {
        for (int i = 0; i < this.cells.Count; i++) {
          File.Delete(this.cells[i].fileName);
        }
      } finally {
        this.cells.ExitWriteLock();
      }
      // Should dispose of all unmanaged state
      // Should set a flag to make it impossible for other threads to susequently use this partition 
    }

    internal long OldToNew(long uid) {
      try {
        return this.mapperBundle.Map(uid);
      } catch (NullReferenceException) {
        // If one of the servers dies during the course of an AddRow, the client 
        // closes the connections to all other primaries and establishes new connections.
        // When the connection to a surving primary is severed, the "finally" clause 
        // of AddRow responsible to cleanup will set this.mapperBundle to null.
        // However, it is possible that a peer server has a "MapOldToNew" request
        // in flight while this happening.  This may cause a NullReferenceException.
        // If that happens, throw a SocketException, which will cause the service 
        // thread to close the connection to the peer server (which is about to 
        // happen anyway since the system is in the process of reconfiguring itself)
        // and to terminate.
        throw new SocketException();
      }
    }

    internal void AddRow(ThreadLocalState partTLS,
                         string[] primaries,
                         int urlStrideLen,
                         int fwdStrideLen,
                         int bwdStrideLen,
                         LinkCompression fwdCompression,
                         LinkCompression bwdCompression,
                         IEnumerable<byte[]> rowUrls,
                         IEnumerable<Tuple<byte[], byte[][]>> rowLinks,
                         Action UrlCellFinishedClosure,
                         Func<List<Tuple<long, long>>, List<Tuple<long, long>>> PropagateBwdsClosure,
                         Func<long, long[]> UrlMergeCompletedClosure,
                         Action CellFinalizedClosure,
                         Action<List<int>> CommitClosure) {
      this.cells.EnterUpgradeableReadLock();
      var tfn = new TempFileNames();
      try {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var newEpoch = this.Epoch + 1;
        var numCellsToMerge = this.NumCellsToMerge();
        var finalFileName = Cell.Name(this.spid, newEpoch);
        var newFileName = numCellsToMerge == 1 ? finalFileName : tfn.New();
        var newBaseUID = this.SupraUID;
        long urlCnt = 0;
        long newUrlCnt = 0;
        long patchPos1 = 0;
        long patchPos2 = 0;
        long patchPos3 = 0;
        long numUrls = 0;
        using (var urls = new DiskSorter<byte[]>(new EncString.Comparer(), EncString.Wr, EncString.Rd, 1 << 23)) {
          foreach (var urlBytes in rowUrls) {
            urlCnt++;
            // Check if the URL is already already in the partition, add it to urls if not
            int epoch;
            if (partTLS.partition.UrlToUid(partTLS, urlBytes, out epoch, false) == -1) {
              newUrlCnt++;
              urls.Add(urlBytes);
            }
          }
          //Console.Error.WriteLine("AddRow: URL receiving phase took {0} seconds; {1} URLs; {2} new", 0.001 * sw.ElapsedMilliseconds, urlCnt, newUrlCnt);
          sw.Restart();
          // Write out the partial cell, with the link subcells missing
          urls.Sort();

          using (var wr = new BinaryWriter(new BufferedStream(new FileStream(newFileName, FileMode.Create, FileAccess.Write)))) {
            wr.Write(newBaseUID);
            wr.Write(newEpoch);

            patchPos1 = wr.BaseStream.Position;
            wr.Write(0L);   // placeholder for urlCell.numUrls 
            wr.Write(-1L);  // placeholder for urlCell.numBytes
            wr.Write(urlStrideLen);

            patchPos2 = wr.BaseStream.Position;
            wr.Write(-1L); // placeholder for linkCell[0].numBytes
            wr.Write(0L);  // placeholder for linkCell[0].numLinks 
            wr.Write(fwdStrideLen);
            wr.Write((int)fwdCompression);

            patchPos3 = wr.BaseStream.Position;
            wr.Write(-1L); // placeholder for linkCell[1].numBytes
            wr.Write(0L);  // placeholder for linkCell[1].numLinks
            wr.Write(bwdStrideLen);
            wr.Write((int)bwdCompression);

            var strComp = new FrontCodingStringCompressor(wr.BaseStream);
            byte[] last = EncString.Empty;
            while (!urls.AtEnd()) {
              byte[] bytes = urls.Get();
              if (EncString.Compare(bytes, last) > 0) {
                strComp.WriteString(bytes);
                numUrls++;
              }
              last = bytes;
            }

            // Patch the persistings of urlCell.numUrls, urlCell.numSites, and urlCell.numBytes
            wr.BaseStream.Seek(patchPos1, SeekOrigin.Begin);
            wr.Write(numUrls);
            wr.Write(strComp.GetPosition());

            //Console.Error.WriteLine("AddRow: URL merge phase took {0} seconds; {1} unique URLs", 0.001 * sw.ElapsedMilliseconds, numUrls);
            sw.Restart();
          }
        }

        lock (this) {
          this.newCell = new Cell(this, newFileName, true);
          if (this.urlRefCnt > 0) this.newCell.urlCell.Load();
        }

        // Notify the client that the URL sorting phase is done and the partition has a partial cell for epoch nextGen
        UrlCellFinishedClosure();

        var store = new Store(this.spid.storeID, this.ping, primaries);
        //store.SetUrlToUidCacheParams(20, 16);

        long pairCnt = 0;
        // Receive pairs of strings from the client and add them to the buildPod.
        // Break when the client sends the string null. 

        var linkComparer = new UidPair.Comparer(ping);
        using (var fwds = new DiskSorter<UidPair>(linkComparer, UidPair.Write, UidPair.Read, 1 << 25)) {
          using (var bwds = new DiskSorter<UidPair>(linkComparer, UidPair.Write, UidPair.Read, 1 << 25)) {
            var batch = new LinkBatch(store, fwds);
            foreach (var tup in rowLinks) {
              int epoch;
              var urlBytes1 = tup.Item1;
              Contract.Assert(ping.PartitionID(Encoding.UTF8.GetString(urlBytes1)) == this.spid.partID);
              long uid1 = partTLS.partition.UrlToUid(partTLS, urlBytes1, out epoch, true);
              Contract.Assert(uid1 != -1);

              // Add the record (uid1,-1) to fwds, to indicate to WriteLinkCells that this page was crawled.
              fwds.Add(new UidPair { srcUid = uid1, dstUid = -1 });

              foreach (var urlBytes2 in tup.Item2) {
                pairCnt++;
                var url2 = Encoding.UTF8.GetString(urlBytes2);
                if (ping.PartitionID(url2) == this.spid.partID) {
                  var uid2 = partTLS.partition.UrlToUid(partTLS, urlBytes2, out epoch, true);
                  fwds.Add(new UidPair { srcUid = uid1, dstUid = uid2 });
                } else {
                  batch.Add(uid1, url2);
                }
              }
            }
            //Console.Error.WriteLine("AddRow: client transmitted {0} URL pairs", pairCnt);
            batch.Process();

            //Console.Error.WriteLine("AddRow: Link collection phase took {0} seconds", 0.001 * sw.ElapsedMilliseconds);
            sw.Restart();

            var bwdProp = new BwdsPropagator(this, bwds, PropagateBwdsClosure);

            using (var wr = new BinaryWriter(new BufferedStream(new FileStream(newFileName, FileMode.Open, FileAccess.Write)))) {
              var numFwds = this.WriteLinkCell(wr, partTLS, newBaseUID, numUrls, bwdProp, 0, fwds, patchPos2, fwdStrideLen, IntStreamCompressor.New(fwdCompression));
              var numBwds = this.WriteLinkCell(wr, partTLS, newBaseUID, numUrls, null,    1, bwds, patchPos3, bwdStrideLen, IntStreamCompressor.New(bwdCompression));
              //Console.Error.WriteLine("AddRow: Link merge phase took {0} seconds, {1} fwd links, {2} bwd links",
              //                        0.001 * sw.ElapsedMilliseconds, numFwds, numBwds);
              sw.Restart();
            }
          }
        }

        lock (this) {
          this.newCell.Reload();
          // newCell.urlCell is known to be loaded. If necessary load the fwd and bwd LinkCells.
          Contract.Assert(this.urlRefCnt > 0 && newCell.urlCell.IsLoaded);
          if (this.linkRefCnt[0] > 0) newCell.linkCell[0].Load();
          if (this.linkRefCnt[1] > 0) newCell.linkCell[1].Load();
        }

        if (numCellsToMerge == 1) {
          // Invoking "CellFinalizedClosure" signals the clerk that this advertisedServer
          // has finalized the merged cell and is ready to advance the epoch.
          // "CellFinalizedClosure" returns after all servers have signaled
          // the clerk, and the clerk has replied to all advertisedServer to proceed.
          // This needs to happen before acquiring rwlock, since otherwise 
          // there is a potential for deadlock, since other servers may still
          // need to resolve URLs (calling "PrivilegedUrlToUid" on this advertisedServer),
          // which requires a read-share of rwlock.
          CellFinalizedClosure();
          this.cells.EnterWriteLock();
          try {
            // Notify the leader that this.rwlock is held and update can be committed
            CommitClosure(this.cells.EpochsPostUpdate(0, this.newCell));

            this.cells.Update(0, this.newCell);
            this.newCell = null;
          } finally {
            this.cells.ExitWriteLock();
          }
        } else {
          Contract.Assert(this.mapperBundle == null);
          // Verify that the input cells meet the code contract 
          var mergeCells = new Cell[numCellsToMerge];
          for (int i = 0; i < numCellsToMerge - 1; i++) {
            mergeCells[i] = this.cells[this.cells.Count - (numCellsToMerge - 1) + i];
          }
          mergeCells[numCellsToMerge - 1] = this.newCell;

          Contract.Assert(Contract.ForAll(1, numCellsToMerge, i => mergeCells[i - 1].supraUID == mergeCells[i].baseUID));

          // Write out the header portion of the new cell
          using (var wr = new BinaryWriter(new BufferedStream(new FileStream(finalFileName, FileMode.Create, FileAccess.Write)))) {
            wr.Write(mergeCells[0].baseUID);
            wr.Write(mergeCells[numCellsToMerge - 1].epoch);

            patchPos1 = wr.BaseStream.Position;
            wr.Write(0L);   // placeholder for urlCell.numUrls 
            wr.Write(-1L);  // placeholder for urlCell.numBytes
            wr.Write(mergeCells[numCellsToMerge - 1].urlCell.indexStride);

            patchPos2 = wr.BaseStream.Position;
            var fwdStride = mergeCells[numCellsToMerge - 1].linkCell[0].indexStride;
            var fwdComprCode = mergeCells[numCellsToMerge - 1].linkCell[0].compressionCode;
            wr.Write(-1L); // placeholder for linkCell[0].numBytes
            wr.Write(0L);  // placeholder for linkCell[0].numLinks 
            wr.Write(fwdStride);
            wr.Write((int)fwdComprCode);

            patchPos3 = wr.BaseStream.Position;
            var bwdStride = mergeCells[numCellsToMerge - 1].linkCell[1].indexStride;
            var bwdComprCode = mergeCells[numCellsToMerge - 1].linkCell[1].compressionCode;
            wr.Write(-1L); // placeholder for linkCell[1].numBytes
            wr.Write(0L);  // placeholder for linkCell[1].numLinks
            wr.Write(bwdStride);
            wr.Write((int)bwdComprCode);

            // Create an array of temp file names for the old-to-new-UID map writers
            var tmpNames = new string[numCellsToMerge];
            for (int i = 0; i < numCellsToMerge; i++) {
              tmpNames[i] = tfn.New();
            }

            var heap = new UrlCellHeapElem[numCellsToMerge];
            var pos = 0;
            for (int i = 0; i < numCellsToMerge; i++) {
              var oldNewMapWr = new BinaryWriter(new FileStream(tmpNames[i], FileMode.Create, FileAccess.Write));
              var oldNewMapCompr = new VarNybbleIntStreamCompressor();
              oldNewMapCompr.SetWriter(oldNewMapWr);
              var elem = new UrlCellHeapElem {
                cellIdx = i,
                urls = mergeCells[i].urlCell.Urls().GetEnumerator(),
                cnt = 0,
                numUrls = mergeCells[i].numUrls,
                currCuid = 0,
                prevOldNewGap = 0,
                oldNewMapWr = oldNewMapWr,
                oldNewMapCompr = oldNewMapCompr
              };
              if (elem.urls.MoveNext()) {
                heap[pos] = elem;
                int x = pos;
                while (x > 0) {
                  int p = (x - 1) / 2;
                  if (EncString.Compare(heap[p].urls.Current, heap[x].urls.Current) < 0) break;
                  var e = heap[p]; heap[p] = heap[x]; heap[x] = e;
                  x = p;
                }
                pos++;
              } else {
                Contract.Assert(elem.cnt == elem.numUrls);
                elem.urls = null;
                elem.oldNewMapWr.Close();
              }
            }
            // Iterate until heap is empty
            long newCuid = 0;
            const long mapStrideLength = 1024;

            var strComp = new FrontCodingStringCompressor(wr.BaseStream);
            var idxFile = tfn.New();
            using (var idxWr = new BinaryWriter(new FileStream(idxFile, FileMode.Create, FileAccess.Write))) {
              while (pos > 0) {
                // Get the top heap element 
                var elem = heap[0];

                // Write the url to the URL cell portion of the new cell, adjust 
                // cnt, currCuid and prevOldNewGap, and write uid-to-uid 
                // translation entries
                strComp.WriteString(elem.urls.Current);
                var oldNewGap = newCuid - elem.currCuid;  // the difference between this URL in the old and the new cell-local UID space
                Contract.Assert(oldNewGap >= 0);
                var delta = oldNewGap - elem.prevOldNewGap;
                Contract.Assert(delta >= 0);
                elem.oldNewMapCompr.PutUInt64((ulong)delta);
                elem.prevOldNewGap = oldNewGap;
                elem.currCuid++;
                elem.cnt++;
                newCuid++;
                if (elem.cnt % mapStrideLength == 0) elem.oldNewMapCompr.Align();
                idxWr.Write(elem.cellIdx);

                // Advance the URL enumerator, and remove heap[0] from the heap 
                if (!elem.urls.MoveNext()) {
                  Contract.Assert(elem.cnt == elem.numUrls);
                  elem.urls = null;
                  elem.oldNewMapCompr.Align();
                  elem.oldNewMapWr.Close();
                  heap[0] = heap[--pos];
                  heap[pos] = null;
                }
                // re-establish the heap property
                int x = 0;
                while (x < pos) {
                  int c = 2 * x + 1;
                  if (c >= pos) {
                    break;
                  } else if (c + 1 < pos) {
                    if (EncString.Compare(heap[c].urls.Current, heap[c + 1].urls.Current) > 0) c = c + 1;
                  }
                  if (EncString.Compare(heap[x].urls.Current, heap[c].urls.Current) > 0) {
                    var e = heap[x]; heap[x] = heap[c]; heap[c] = e;
                    x = c;
                  } else {
                    break;
                  }
                }

              }
            }
            wr.BaseStream.Seek(patchPos1, SeekOrigin.Begin);
            wr.Write(newCuid);
            wr.Write(strComp.GetPosition());

            this.mapperBundle = new UidSpaceMapperBundle(mapStrideLength, mergeCells, tmpNames);

            // Signal the clerk that the URL cells have been merged, and transmit 
            // the minimal minUirlUid; receive a vector of minUrlUids for all 
            // partitions of the store
            var partBaseUids = UrlMergeCompletedClosure(mergeCells[0].baseUID);

            // Merge the forward and backward link cells
            this.MergeLinkCells(mergeCells, 0, partTLS, partBaseUids, store, wr, idxFile, patchPos2, fwdStride, IntStreamCompressor.New((LinkCompression)fwdComprCode));
            this.MergeLinkCells(mergeCells, 1, partTLS, partBaseUids, store, wr, idxFile, patchPos3, bwdStride, IntStreamCompressor.New((LinkCompression)bwdComprCode));
          }


          // Invoking "CellFinalizedClosure" signals the clerk that this advertisedServer
          // has finalized the merged cell and is ready to advance the epoch.
          // "CellFinishedClosure" returns after all servers have signaled
          // the clerk, and the clerk has replied to all advertisedServer to proceed.
          // This needs to happen before acquiring rwlock, since otherwise 
          // there is a potential for deadlock, since other servers may still
          // need to resolve URLs (calling "PrivilegedUrlToUid" on this advertisedServer),
          // which requires a read-share of rwlock.
          CellFinalizedClosure();
          this.cells.EnterWriteLock();  // Acquire the Write share of the RW lock 
          try {
            // Notify the leader that this.rwlock is held and update can be committed
            this.newCell = null;
            var mergedCell = new Cell(this, finalFileName, false);
            CommitClosure(this.cells.EpochsPostUpdate(numCellsToMerge - 1, mergedCell));
            lock (this) {
              this.cells.Update(numCellsToMerge - 1, mergedCell);
              if (this.urlRefCnt > 0) mergedCell.urlCell.Load();
              if (this.linkRefCnt[0] > 0) mergedCell.linkCell[0].Load();
              if (this.linkRefCnt[1] > 0) mergedCell.linkCell[1].Load();
            }


            foreach (var cell in mergeCells) {
              File.Delete(cell.fileName);  // delete the underlying file
            }
          } finally {
            this.cells.ExitWriteLock();  // Release the write share of the RW lock
          }
        }
        //Console.Error.WriteLine("AddRow completed; took {0} seconds total", 0.001 * sw.ElapsedMilliseconds);
      } finally {
        tfn.CleanThis();           // Delete temporary files produced during this AddRow
        this.mapperBundle = null;  // Making mapperBundle unreachable allows GC
        this.newCell = null;
        this.cells.ExitUpgradeableReadLock();
      }
    }

    internal string NextCellName() {
      this.cells.EnterReadLock();
      try {
        return Cell.Name(this.spid, this.Epoch + 1);
      } finally {
        this.cells.ExitReadLock();
      }
    }

    internal void AdvanceEpoch() {
      this.cells.EnterWriteLock();
      try {
        var newEpoch = this.Epoch + 1;
        var numCellsToMerge = this.NumCellsToMerge();
        var finalFileName = Cell.Name(this.spid, newEpoch);
        var cell = new Cell(this, finalFileName, false);
        lock (this) {
          if (this.urlRefCnt > 0) cell.urlCell.Load();
          if (this.linkRefCnt[0] > 0) cell.linkCell[0].Load();
          if (this.linkRefCnt[1] > 0) cell.linkCell[1].Load();
        }
        this.cells.Update(numCellsToMerge - 1, cell);
      } finally {
        this.cells.ExitWriteLock();
      }
    }

    // Methods private to Partition

    private int NumCellsToMerge() {
      var epoch = this.Epoch + 2;
      int res = 1;
      for (int i = 0; i < sizeof(int) * 8; i++) {
        if ((epoch & 1) == 1) break;
        epoch >>= 1;
        res++;
      }
      return res;
    }

    private static List<long> CombineLinks(List<long>[] uidsSeq) {
      Contract.Requires(uidsSeq != null);
      Contract.Requires(Contract.ForAll(uidsSeq, uids => uids != null));
      Contract.Ensures(Contract.Result<List<long>>() != null);
      Contract.Ensures(UID.LinksAreSorted(Contract.Result<List<long>>()));

      Contract.Assert(Contract.ForAll(0, uidsSeq.Length, i => UID.LinksAreSorted(uidsSeq[i])));

      if (uidsSeq.Length == 0) {
        return new List<long>();
      } else {
        var res = uidsSeq[0];
        for (int i = 1; i < uidsSeq.Length; i++) {
          res = CombineLinks(res, uidsSeq[i]);
        }
        return res;
      }
    }

    private static List<long> CombineLinks(List<long> uids1, List<long> uids2) {
      Contract.Requires(uids1 != null);
      Contract.Requires(uids2 != null);
      Contract.Ensures(Contract.Result<List<long>>() != null);
      Contract.Ensures(Contract.Result<List<long>>().Count <= uids1.Count + uids2.Count);
      Contract.Ensures(UID.LinksAreSorted(Contract.Result<List<long>>()));

      Contract.Assert(UID.LinksAreSorted(uids1));
      Contract.Assert(UID.LinksAreSorted(uids2));

      var uids = new List<long>(uids1.Count + uids2.Count);
      int p1 = 0;
      int p2 = 0;
      while (p1 < uids1.Count && p2 < uids2.Count) {
        var uid1 = uids1[p1];
        var uid2 = uids2[p2];
        if (UID.ClrDeletedBit(uid1) < UID.ClrDeletedBit(uid2)) {
          uids.Add(uid1);
          p1++;
        } else if (UID.ClrDeletedBit(uid1) > UID.ClrDeletedBit(uid2)) {
          uids.Add(uid2);
          p2++;
        } else if (UID.ClrDeletedBit(uid1) == UID.ClrDeletedBit(uid2)) {
          Contract.Assert(UID.HasDeletedBit(uid1) ^ UID.HasDeletedBit(uid2));
          p1++;
          p2++;
        }
      }
      while (p1 < uids1.Count) {
        uids.Add(uids1[p1++]);
      }
      while (p2 < uids2.Count) {
        uids.Add(uids2[p2++]);
      }

      Contract.Assert(UID.LinksAreSorted(uids));
      return uids;
    }

    /// <summary>
    /// DiffLinks takes two arrays of UIDs.  It requires that neither array contains
    /// any UIDs with its deleted-bit set, and that both arrays are in sorted order.
    /// It returns a variant of set difference: Those UIDs in "uids2" that are not in 
    /// "uids1", with their deleted-bit clear; plus those UIDs in "uids1" that are 
    /// not in "uids2", with their deleted-bit set.  It ensures that the result is 
    /// non-null, and (ignoring any deleted-bits) in sorted order.
    /// </summary>
    /// <param name="uids1">The link-set of a page that is currently in the store.</param>
    /// <param name="uids2">The updated link-set that is to replace it.</param>
    /// <returns>The set of additions and deletions required to get from uids1 to uids2.</returns>
    private static List<long> DiffLinks(List<long> uids1, List<long> uids2) {
      Contract.Requires(uids1 != null);
      Contract.Requires(uids2 != null);
      Contract.Ensures(Contract.Result<List<long>>() != null);
      Contract.Ensures(Contract.Result<List<long>>().Count <= uids1.Count + uids2.Count);
      Contract.Ensures(UID.LinksAreSorted(Contract.Result<List<long>>()));

      Contract.Assert(UID.NoLinksAreDeleted(uids1));
      Contract.Assert(UID.NoLinksAreDeleted(uids1));
      Contract.Assert(UID.LinksAreSorted(uids1));
      Contract.Assert(UID.LinksAreSorted(uids2));

      var uids = new List<long>(uids1.Count + uids2.Count);
      int p1 = 0;
      int p2 = 0;
      while (p1 < uids1.Count && p2 < uids2.Count) {
        var uid1 = uids1[p1];
        var uid2 = uids2[p2];
        if (uid1 < uid2) {
          uids.Add(UID.SetDeletedBit(uid1));
          p1++;
        } else if (uid1 > uid2) {
          uids.Add(uid2);
          p2++;
        } else {
          p1++;
          p2++;
        }
      }
      while (p1 < uids1.Count) {
        uids.Add(UID.SetDeletedBit(uids1[p1++]));
      }
      while (p2 < uids2.Count) {
        uids.Add(uids2[p2++]);
      }
      return uids;
    }

    private void AddToUidToUidTable(long uid, UidToUidTable remUidTbl, long[] partBaseUids) {
      var uidPid = this.ping.PartitionID(uid);
      if (uidPid != this.spid.partID && partBaseUids[uidPid] <= uid) {
        remUidTbl.Add(uid);
      }
    }

    private long TranslateUID(long uid, UidToUidTable remUidTbl, long[] partBaseUids) {
      var del = UID.HasDeletedBit(uid);
      uid = UID.ClrDeletedBit(uid);
      var uidPid = this.ping.PartitionID(uid);
      if (uidPid == this.spid.partID) {
        uid = this.mapperBundle.Map(uid);
      } else if (partBaseUids[uidPid] <= uid) {
        uid = remUidTbl.OldToNew(uid);
      } else {
        // leave unchanged
      }
      if (del) uid = UID.SetDeletedBit(uid);
      return uid;
    }

    private void ProcessPageLinksBatch(List<MlcPLs> batch, long[] partBaseUids, Store store) {
      // The link UIDs in the batch need to be translated, which in general 
      // will require calls to other servers.

      // Create a UidToUidTable and fill it with the remote UIDs that need 
      // to be translated, and then update the table to contain the translation
      var remUidTbl = new UidToUidTable(1000000);
      foreach (var pl in batch) {
        this.AddToUidToUidTable(pl.pageUid, remUidTbl, partBaseUids);
        foreach (var link in pl.linkUids) {
          this.AddToUidToUidTable(UID.ClrDeletedBit(link), remUidTbl, partBaseUids);
        }
      }
      remUidTbl.SetNewUids(store.MapOldToNewUids(remUidTbl.GetOldUids()));
      // Translate all UIDs 
      foreach (var pl in batch) {
        pl.pageUid = TranslateUID(pl.pageUid, remUidTbl, partBaseUids);
        var linkUids = pl.linkUids;
        for (int j = 0; j < linkUids.Count; j++) {
          linkUids[j] = this.TranslateUID(linkUids[j], remUidTbl, partBaseUids);
        }
        linkUids.Sort(UID.CompareUidsForSort);
      }
    }

    private IEnumerable<MlcPLs> TranslateMlcPLs(IEnumerable<MlcPLs> pageLinks, long[] partBaseUids, Store store) {
      var batch = new List<MlcPLs>();
      foreach (var pl in pageLinks) {
        if (batch.Count == 10000) {
          this.ProcessPageLinksBatch(batch, partBaseUids, store);
          foreach (var pl2 in batch) {
            yield return pl2;
          }
          batch.Clear();
        }
        batch.Add(pl);
      }
      this.ProcessPageLinksBatch(batch, partBaseUids, store);
      foreach (var pl2 in batch) {
        yield return pl2;
      }
    }

    private IEnumerable<MlcPLs> MergeLinkCells(Cell[] mergeCells, int dir, string idxFile) {
      Contract.Assert(Contract.ForAll(mergeCells, cell => cell.linkCell[dir].bytes != null));

      // The fist step is to simply yield the link-combinations of all 
      // linkCell entries whose (implicit) pageUid values are below 
      // mergeCells[0].minUrlUid, mode.e. whose pageUid values are not 
      // affected by the remapping.
      var linkCellRds1 = new Cell.LinkCellRd[mergeCells.Length];
      for (int i = 0; i < mergeCells.Length; i++) {
        Contract.Assert(mergeCells[i].linkCell[dir].bytes != null);
        linkCellRds1[i] = new Cell.LinkCellRd(mergeCells[i].linkCell[dir]);
      }
      for (long pageUid = this.BaseUID; pageUid < mergeCells[0].baseUID; pageUid++) {
        var linkSeq = new List<long>[mergeCells.Length];
        for (int i = 0; i < mergeCells.Length; i++) {
          linkSeq[i] = linkCellRds1[i].GetLinks(pageUid);
          Contract.Assert(UID.LinksAreSorted(linkSeq[i]));
        }
        yield return new MlcPLs(pageUid, CombineLinks(linkSeq));
      }

      // Now, yield the link-combinations of all linkCell entries whose 
      // (implicit) pagePuid values are in the range that is affected by
      // the remaining, in the order prescribed by idxFile, which will 
      // return them in sorted order.
      var linkCellRds2 = new Cell.LinkCellRd[mergeCells.Length, mergeCells.Length];
      for (int i = 0; i < mergeCells.Length; i++) {
        for (int j = 0; j <= i; j++) {
          linkCellRds2[i, j] = new Cell.LinkCellRd(mergeCells[i].linkCell[dir]);
        }
      }
      var pageUIDs = new long[mergeCells.Length];
      for (int i = 0; i < mergeCells.Length; i++) {
        pageUIDs[i] = mergeCells[i].baseUID;
      }
      foreach (var j in MergeIndices(idxFile)) {
        var pageUid = pageUIDs[j];
        var linkSeq = new List<long>[mergeCells.Length-j];
        for (int i = j; i < mergeCells.Length; i++) {
          linkSeq[i - j] = linkCellRds2[i, j].GetLinks(pageUid);
        }
        yield return new MlcPLs(pageUid, CombineLinks(linkSeq));
        pageUIDs[j]++;
      }
      Contract.Assert(Contract.ForAll(0, mergeCells.Length, i => pageUIDs[i] == mergeCells[i].supraUID));
    }

    private static IEnumerable<int> MergeIndices(string idxFile) {
      using (var rd = new BinaryReader(new FileStream(idxFile, FileMode.Open, FileAccess.Read))) {
        for (; ; ) {
          int j;
          try {
            j = rd.ReadInt32();
          } catch (EndOfStreamException) {
            break;
          }
          yield return j;
        }
      }
    }

    private int NetNumLinks(List<long> links) {
      int netCnt = 0;
      foreach (var uid in links) {
        netCnt += UID.HasDeletedBit(uid) ? -1 : +1;
      }
      return netCnt;
    }

    private long WriteLinkCell(BinaryWriter wr,
                               Partition.ThreadLocalState partTLS,
                               long baseUID,
                               long numUrls,
                               BwdsPropagator bwdProp, 
                               int dir,
                               DiskSorter<UidPair> sorter, 
                               long patchPos, 
                               int indexStride, 
                               IntStreamCompressor compr) 
    {
      sorter.Sort();
      wr.BaseStream.Seek(0, SeekOrigin.End);
      compr.SetWriter(wr);
      Int64 numLinks = 0;
      for (var uid = BaseUID; uid < this.newCell.supraUID; uid++) {
        var srcUid = uid;
        if ((this.ping.PUID(uid) % indexStride) == 0) compr.Align();
        var linkUids = new List<long>();
        bool crawledPage = false;
        while (!sorter.AtEnd()) {
          UidPair rec = sorter.Peek();
          if (rec.srcUid != srcUid) break;
          if (rec.dstUid == -1) {
            Contract.Assert(dir == 0 && !crawledPage);
            crawledPage = true;
          } else if (rec.dstUid == srcUid) {
            // Omit reflexive links -- this is a debatable choice
          } else if (linkUids.Count > 0 && rec.dstUid == linkUids[linkUids.Count - 1]) {
            // Omit duplicate links 
          } else {
            linkUids.Add(rec.dstUid);
          }
          sorter.Get();
        }
        if (crawledPage && uid < baseUID) {
          int epoch;
          linkUids = DiffLinks(this.GetLinks(partTLS, srcUid, dir, out epoch), linkUids);
        }

        numLinks += NetNumLinks(linkUids);
        compr.PutUInt32((UInt32)linkUids.Count);
        Contract.Assert(UID.LinksAreSorted(linkUids));

        for (int i = 0; i < linkUids.Count; i++) {
          if (i == 0) {
            compr.PutInt64(linkUids[i] - srcUid);
          } else {
            compr.PutUInt64((UInt64)(linkUids[i] - linkUids[i - 1]));
          }
        }

        if (bwdProp != null) {
          for (int i = 0; i < linkUids.Count; i++) {
            if (UID.HasDeletedBit(linkUids[i])) {
              bwdProp.Add(UID.ClrDeletedBit(linkUids[i]), UID.SetDeletedBit(srcUid));
            } else {
              bwdProp.Add(linkUids[i], srcUid);
            }
          }
        }
      }
      if (bwdProp != null) bwdProp.Finish();
      long numBytes = compr.Align();  // Align flushes data to wr, so do this before Seek!
      wr.BaseStream.Seek(patchPos, SeekOrigin.Begin);
      wr.Write(numBytes);
      wr.Write(numLinks);
      Contract.Assert(sorter.AtEnd());
      sorter.Dispose();
      return numLinks;
    }

    // MergeLinkCells is similar to WriteLinkCell. Maybe it is possible to factor out the common parts...
    private long MergeLinkCells(Cell[] mergeCells,
                                int dir,
                                Partition.ThreadLocalState partTLS,
                                long[] partBaseUids,
                                Store store,
                                BinaryWriter wr,
                                string idxFile,
                                long patchPos,
                                int indexStride,
                                IntStreamCompressor compr) {
      this.LoadLinks(partTLS, dir);

      wr.BaseStream.Seek(0, SeekOrigin.End);
      compr.SetWriter(wr);
      long numLinks = 0;
      long puid = 0;
      foreach (var pl in TranslateMlcPLs(MergeLinkCells(mergeCells, dir, idxFile), partBaseUids, store)) {
        if ((puid % indexStride) == 0) compr.Align();
        numLinks += NetNumLinks(pl.linkUids);
        compr.PutUInt32((uint)pl.linkUids.Count);
        Contract.Assert(UID.LinksAreSorted(pl.linkUids));
        for (int j = 0; j < pl.linkUids.Count; j++) {
          if (j == 0) {
            compr.PutInt64(pl.linkUids[j] - pl.pageUid);
          } else {
            compr.PutUInt64((ulong)(pl.linkUids[j] - pl.linkUids[j - 1]));
          }
        }
        puid++;
      }

      long numBytes = compr.Align();  // Align flushes data to wr, so do this before Seek!
      wr.BaseStream.Seek(patchPos, SeekOrigin.Begin);
      wr.Write(numBytes);
      wr.Write(numLinks);
      return numLinks;
    }

    private class MlcPLs {
      internal long pageUid;
      internal List<long> linkUids;

      internal MlcPLs(long pageUid, List<long> linkUids) {
        this.pageUid = pageUid;
        this.linkUids = linkUids;
      }
    }

    internal struct Cells {
      private readonly ReaderWriterLockSlim rwlock;
      private readonly List<Cell> cells;
      internal Cells(List<Cell> cells) {
        this.cells = cells;
        this.rwlock = new ReaderWriterLockSlim();
      }
      internal int Count {
        get {
          Contract.Assert(this.rwlock.IsReadLockHeld || this.rwlock.IsUpgradeableReadLockHeld || this.rwlock.IsWriteLockHeld);
          return this.cells.Count; 
        }
      }
      internal Cell this[int i] {
        get {
          Contract.Assert(this.rwlock.IsReadLockHeld || this.rwlock.IsUpgradeableReadLockHeld || this.rwlock.IsWriteLockHeld);
          return this.cells[i];
        }
      }
      internal List<int> EpochsPostUpdate(int numToRemove, Cell cellToAdd) {
        var epochs = this.cells.Select(x => x.epoch).ToList();
        epochs.RemoveRange(this.cells.Count - numToRemove, numToRemove);
        epochs.Add(cellToAdd.epoch);
        return epochs;
      }
      internal void Update(int numToRemove, Cell cellToAdd) {
        Contract.Assert(this.rwlock.IsWriteLockHeld);
        this.cells.RemoveRange(this.cells.Count - numToRemove, numToRemove);
        this.cells.Add(cellToAdd);
      }
      internal void EnterReadLock() {
        this.rwlock.EnterReadLock();
      }
      internal void ExitReadLock() {
        this.rwlock.ExitReadLock();
      }
      internal void EnterUpgradeableReadLock() {
        this.rwlock.EnterUpgradeableReadLock();
      }
      internal void ExitUpgradeableReadLock() {
        this.rwlock.ExitUpgradeableReadLock();
      }
      internal void EnterWriteLock() {
        this.rwlock.EnterWriteLock();
      }
      internal void ExitWriteLock() {
        this.rwlock.ExitWriteLock();
      }
    }

    private class LinkBatch {
      private readonly Store store;
      private readonly DiskSorter<UidPair> fwds;
      private int cnt;
      private string[] urls;
      private long[] uids;

      internal LinkBatch(Store store, DiskSorter<UidPair> fwds) {
        this.store = store;
        this.fwds = fwds;
        this.cnt = 0;
        this.urls = new string[100000];
        this.uids = new Int64[100000];
      }

      internal void Add(long uid, string url) {
        Contract.Requires(uid != -1);
        if (this.cnt == this.urls.Length) {
          this.Process();
        }
        this.uids[this.cnt] = uid;
        this.urls[this.cnt] = url;
        this.cnt++;
      }

      internal void Process() {
        var remUrls = this.urls.Length == this.cnt ? this.urls : RightSize(this.urls, this.cnt);
        var remUids = store.PrivilegedUrlToUid(remUrls);
        for (int i = 0; i < this.cnt; i++) {
          Contract.Assert(remUids[i] != -1);
          fwds.Add(new UidPair { srcUid = this.uids[i], dstUid = remUids[i] });
          this.urls[i] = null;
        }
        this.cnt = 0;
      }

      private static T[] RightSize<T>(T[] a, int n) {
        var tmp = new T[n];
        Array.Copy(a, tmp, n);
        return tmp;
      }
    }

    private class BwdsPropagator {
      private Partition partition;
      private Func<List<Tuple<long, long>>, List<Tuple<long, long>>> propagateBwdsClosure;
      private DiskSorter<UidPair> bwds;
      private List<Tuple<long, long>> buffer;

      internal BwdsPropagator(Partition partition, DiskSorter<UidPair> bwds, Func<List<Tuple<long, long>>, List<Tuple<long, long>>> propagateBwdsClosure) {
        this.partition = partition;
        this.propagateBwdsClosure = propagateBwdsClosure;
        this.bwds = bwds;
        this.buffer = new List<Tuple<long, long>>(1000);
      }

      internal void Add(long uid1, long uid2) {
        int id = this.partition.ping.PartitionID(uid1);
        if (id == this.partition.spid.partID) {
          this.bwds.Add(new UidPair { srcUid = uid1, dstUid = uid2 });
        } else {
          if (this.buffer.Count == this.buffer.Capacity) {
            this.FlushBuffer();
          }
          this.buffer.Add(new Tuple<long, long>(uid1, uid2));
        }
      }

      internal void Finish() {
        while (this.FlushBuffer()) ;
        this.buffer = null;
      }

      internal bool FlushBuffer() {
        var localPairs = propagateBwdsClosure(this.buffer);
        this.buffer.Clear();
        if (localPairs != null) {
          foreach (var pair in localPairs) {
            bwds.Add(new UidPair { srcUid = pair.Item1, dstUid = pair.Item2 });
          }
          return true;
        } else {
          return false;
        }
      }
    }

    private struct UidPair {
      internal Int64 srcUid;
      internal Int64 dstUid;

      internal static UidPair Read(BinaryReader rd) {
        var srcUid = rd.ReadInt64();
        var dstUid = rd.ReadInt64();
        return new UidPair { srcUid = srcUid, dstUid = dstUid };
      }

      internal static void Write(BinaryWriter wr, UidPair pair) {
        wr.Write(pair.srcUid);
        wr.Write(pair.dstUid);
      }

      internal class Comparer : System.Collections.Generic.Comparer<UidPair> {
        private Partitioning ping;

        internal Comparer(Partitioning ping) {
          this.ping = ping;
        }

        public override int Compare(UidPair a, UidPair b) {
          var aSrc = ping.PUID(a.srcUid);
          var bSrc = ping.PUID(b.srcUid);
          if (aSrc < bSrc) {
            return -1;
          } else if (aSrc > bSrc) {
            return +1;
          } else {
            var aDst = UID.ClrDeletedBit(a.dstUid);
            var bDst = UID.ClrDeletedBit(b.dstUid);
            if (aDst < bDst) {
              return -1;
            } else if (aDst > bDst) {
              return +1;
            } else {
              return 0;
            }
          }
        }
      }
    }

    private class UrlCellHeapElem {
      internal int cellIdx;   // index of this cell in the array of cells to be merged 
      internal IEnumerator<byte[]> urls;
      internal long cnt;
      internal long numUrls;  // for debugging only
      internal long currCuid;
      internal long prevOldNewGap;
      internal BinaryWriter oldNewMapWr;
      internal IntStreamCompressor oldNewMapCompr;
    }
  }
}
