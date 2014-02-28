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
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.IO;

// Merging multiple rows of Cells changes the mapping between the URLs contained
// in these cells and the UIDs used to represent these URLs in link sets. This
// source file contains two classes providing a mechanism for mapping UIDs from
// the old to the new numbering space.
// 
// UidSpaceMapper implements a mechansim for mapping the UIDs of the URLs of a 
// single cell from the old to the new numbering space.  UidSpaceMapperBundle 
// encapsulates the UidSpaceMappers of all to-be-mereged cells in a partition. 

namespace SHS {
  internal class UidSpaceMapperBundle {
    private readonly long baseUID;
    internal readonly long supraUID;    // needed only for code contract assertions
    private readonly UidSpaceMapper[] mappers;

    internal UidSpaceMapperBundle(long strideLength, Cell[] cells, string[] fileNames) {
      Contract.Requires(cells.Length == fileNames.Length && cells.Length > 0);
      this.mappers = new UidSpaceMapper[fileNames.Length];
      this.baseUID = cells[0].baseUID;
      this.supraUID = cells[cells.Length - 1].supraUID;
      for (int i = 0; i < fileNames.Length; i++) {
        Contract.Assert(i == 0 || cells[i].baseUID == cells[i - 1].supraUID);
        this.mappers[i] = new UidSpaceMapper(strideLength, baseUID, cells[i].baseUID, cells[i].numUrls, fileNames[i]);
      }
    }

    internal long Map(long uid) {
      if (uid < this.baseUID) {
        return uid;
      } else {
        foreach (var mapper in this.mappers) {
          if (mapper.Owns(uid)) {
            var res = mapper.Map(uid);
            Contract.Assert(this.baseUID <= res && res < this.supraUID);
            return res;
          }
        }
        throw new Exception(string.Format("UID {0:X16} out of range", uid));
      }
    }
  }

  internal class UidSpaceMapper {
    private readonly long strideLength;
    private readonly long newBaseUid;
    internal readonly long oldBaseUid;       // internal for debugging
    internal readonly long numUrls;          // internal for debugging
    internal CachedStream bytes;         // internal only for Code Contracts
    private readonly IntStreamDecompressor deco;
    private readonly ulong[] idxPosition;    // position in this.bytes
    private readonly ulong[] idxGapSum;      // sum of the gaps up to this point

    internal UidSpaceMapper(long strideLength, long newBaseUid, long oldBaseUid, long numUrls, string fileName) {
      this.oldBaseUid = oldBaseUid;
      this.newBaseUid = newBaseUid;
      this.numUrls = numUrls;
      this.strideLength = strideLength;
      using (var stream = new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read))) {
        this.bytes = new CachedStream(stream, (ulong)stream.Length);
        this.deco = new VarNybbleIntStreamDecompressor(this.bytes);
      }
      // Construct the index by parsing this.bytes
      long numIdxItems = (numUrls / strideLength) + 1;
      this.idxPosition = new ulong[numIdxItems];
      this.idxGapSum = new ulong[numIdxItems];
      ulong gapSum = 0;
      int p = 0;
      for (long i = 0; i <= numUrls; i++) {
        if (i % strideLength == 0) {
          this.idxPosition[p] = deco.GetPosition();
          this.idxGapSum[p] = gapSum;
          p++;
        }
        if (i < numUrls) gapSum += deco.GetUInt64();
      }
      Contract.Assert(p == numIdxItems);
      Contract.Assert(deco.AtEnd());
    }

    internal bool Owns(long uid) {
      return this.oldBaseUid <= uid && uid < this.oldBaseUid + this.numUrls;
    }

    internal long Map(long uid) {
      Contract.Requires(this.bytes != null);
      Contract.Assert(this.oldBaseUid <= uid && uid < this.oldBaseUid + this.numUrls);
      var cuid = uid - this.oldBaseUid;
      var div = (cuid+1) / this.strideLength;
      var mod = (cuid+1) % this.strideLength;
      var gap = this.idxGapSum[div];
      // this.deco is stateful and used by multiple threads, so it has to be 
      // locked. This raises the possibility of lock contention. A possible 
      // optimization would be to have a separate decompressor object per
      // thread and pass this decompressor in as an argument.
      lock (this.deco) {
        this.deco.SetPosition(this.idxPosition[div]);    
        for (int i = 0; i < mod; i++) {
          gap += this.deco.GetUInt64();
        }
      }
      return this.newBaseUid + cuid + (long)gap;
    }
  }

  internal class UidToUidTable {
    private long[][] oldUids;
    private long[][] newUids;
    private long count;
    private Permuter perm;

    internal UidToUidTable(int spineLength) {
      this.oldUids = new long[spineLength][];
      this.newUids = new long[spineLength][];
      this.count = 0;
      this.perm = new Permuter();
    }

    internal void Add(long oldUid) {
      var i = this.perm.Permute((ulong)oldUid) % (ulong)this.oldUids.Length;
      var bucket = this.oldUids[i];
      int n = bucket == null ? 0 : bucket.Length;
      for (int j = 0; j < n; j++) {
        if (bucket[j] == oldUid) return;
      }
      var tmp = new long[n + 1];
      for (int j = 0; j < n; j++) {
        tmp[j] = bucket[j];
      }
      tmp[n] = oldUid;
      this.oldUids[i] = tmp;
      this.count++;
    }

    internal long Count {
      get { return this.count; }
    }

    internal long[] GetOldUids() {
      var olds = new long[this.count];
      long p = 0;
      for (int i = 0; i < this.oldUids.Length; i++) {
        var bucket = this.oldUids[i];
        int n = bucket == null ? 0 : bucket.Length;
        for (int j = 0; j < n; j++) {
          olds[p++] = bucket[j];
        }
      }
      Contract.Assert(p == this.count);
      return olds;
    }

    internal void SetNewUids(long[] news) {
      long p = 0;
      for (int i = 0; i < this.oldUids.Length; i++) {
        var bucket = this.oldUids[i];
        if (bucket != null) {
          this.newUids[i] = new long[bucket.Length];
          for (int j = 0; j < bucket.Length; j++) {
            this.newUids[i][j] = news[p++];
          }
        }
      }
      Contract.Assert(p == this.count);
    }

    internal long OldToNew(long oldUid) {
      var i = this.perm.Permute((ulong)oldUid) % (ulong)this.oldUids.Length;
      var bucket = this.oldUids[i];
      int n = bucket == null ? 0 : bucket.Length;
      for (int j = 0; j < n; j++) {
        if (bucket[j] == oldUid) return this.newUids[i][j];
      }
      throw new Exception("Key not found");
    }

    internal void Clear() {
      for (int i = 0; i < this.oldUids.Length; i++) {
        this.oldUids[i] = null;
        this.newUids[i] = null;
      }
      this.count = 0;
    }
  }
}
