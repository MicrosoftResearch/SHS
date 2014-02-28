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
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Text;

namespace SHS {
  internal class Cell {
    internal Partition part;            // the partition containing this cell
    internal readonly string fileName;  // the name of the file persisting this cell
    internal readonly int epoch;        // the epoch during which this cell was created
    internal readonly long baseUID;     // the UID of the first URL in the URL subcell
    internal readonly long supraUID;    // the UID above the last URL in the URL subcell
    internal readonly long numUrls;     // the number of URLs in the URL subcell
    internal readonly UrlCell urlCell;  // the URL subcell
    internal readonly LinkCell[] linkCell;          // the fwd and bwd link subcells

    internal static string Name(SPID spid, int epoch) {
      return string.Format("{0:N}-{1}-{2}.shs", spid.storeID, spid.partID, epoch);
    }

    internal const string NamePattern = @"^([\dabcdef]{32})-(\d+)-(\d+).shs";

    /// <summary>
    /// Load a cell from disk into memory.
    /// </summary>
    /// <param name="part">The partition where this cell belongs</param>
    /// <param name="fileName">The name of the file holding the persisted cell</param>
    /// <exception cref="FileFormatException">The cell file is malformed</exception>
    /// <exception cref="EndOfStreamException">The cell file is too short</exception>
    internal Cell(Partition part, string fileName, bool partial) {
      this.part = part;
      this.fileName = fileName;
      var fileLength = new FileInfo(fileName).Length;
      using (var br = new BinaryReader(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read)))) {
        this.baseUID   = br.ReadInt64();
        this.epoch     = br.ReadInt32();
        this.numUrls   = br.ReadInt64();
        this.supraUID  = this.baseUID + this.numUrls;

        this.urlCell = new UrlCell(this, br);
        this.linkCell = new LinkCell[2];
        this.linkCell[0] = new LinkCell(this, br);
        this.linkCell[1] = new LinkCell(this, br);

        long numHdrBytes = br.BaseStream.Position;
        this.urlCell.startPos = numHdrBytes;
        this.linkCell[0].startPos = this.urlCell.startPos + this.urlCell.numBytes;
        this.linkCell[1].startPos = this.linkCell[0].startPos + this.linkCell[0].numBytes;
        var expectedSize = numHdrBytes + this.urlCell.numBytes + 
          (partial ? 0L : this.linkCell[0].numBytes + this.linkCell[1].numBytes);
        if (fileLength != expectedSize) {
          throw new FileFormatException(fileName + " is wrong size");
        }
      }
    }

    /// <summary>
    /// Reload a "partial" cell once it has been "completed".
    /// </summary>
    internal void Reload() {
      Contract.Assert(this.linkCell[0].numBytes == -1);
      Contract.Assert(this.linkCell[1].numBytes == -1);
      var tmp = new Cell(this.part, this.fileName, false);
      this.linkCell[0] = tmp.linkCell[0];
      this.linkCell[1] = tmp.linkCell[1];
      // No need to dispose of tmp
    }

    [Pure]
    internal bool UidInUrlCell(long uid) {
      return this.baseUID <= uid && uid < supraUID;
    }

    [Pure]
    internal bool UidInLinkCell(long uid) {
      return this.part.BaseUID <= uid && uid < supraUID;
    }

    internal class UrlCell {
      internal readonly long numBytes;
      internal readonly int indexStride;  // internal only for debugging, otherwise could be private
      internal readonly Cell cell;
      internal long startPos;
      private byte[][] idxUrls;           // each byte[] is logically a UTF8-encoded string
      private ulong[] idxOffsets;
      private CachedStream bytes;    
      private readonly Hash64 hasher;
      private HashToUidCache hashToUidCache;  // Needs to be protected by a lock

      internal UrlCell(Cell cell, BinaryReader rd) {
        this.numBytes = rd.ReadInt64();
        this.indexStride = rd.ReadInt32();
        this.cell = cell;
        this.startPos = -1;
        this.idxUrls = null;
        this.idxOffsets = null;
        this.bytes = null;
        this.hashToUidCache = null;
        this.hasher = new Hash64();
      }

      internal bool IsLoaded { get { return this.bytes != null && this.idxUrls != null && this.idxOffsets != null; } }

      internal void Unload() {
        this.bytes = null;
        this.idxUrls = null;
        this.idxOffsets = null;
      }

      internal void Load() {
        Contract.Assert(this.numBytes >= 0);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using (var stream = new BufferedStream(new FileStream(this.cell.fileName, FileMode.Open, FileAccess.Read))) {
          stream.Seek(this.startPos, SeekOrigin.Begin);
          this.bytes = new CachedStream(stream, (ulong)this.numBytes);
        }
        var secs1 = 0.001 * sw.ElapsedMilliseconds;
        sw.Restart();

        // Given the data in this.bytes, construct idxUrls and idxOffsets
        var numIdxItems = (int)((this.cell.numUrls + this.indexStride - 1) / this.indexStride);
        // Note that for numUrls=0 ^^this^^ is NOT the same as (int)((this.cell.numUrls - 1) / this.indexStride) + 1 !!
        this.idxUrls = new byte[numIdxItems][];
        this.idxOffsets = new ulong[numIdxItems];
        ulong pos = 0;    // position in this.bytes -- starts at 0
        ulong lastPos = 0;
        var res = new byte[0];
        int resLen = 0;
        long cuid = 0;

        while (cuid < this.cell.numUrls) {
          int prefLen = this.ReadCompressedSize(ref pos);
          int suffLen = this.ReadCompressedSize(ref pos);
          resLen = prefLen + suffLen;

          // Enlarge the result buffer if necessary 
          if (resLen > res.Length) {
            var tmp = new byte[resLen];
            for (int i = 0; i < prefLen; i++) {  // care only about first prefLen bytes
              tmp[i] = res[i];
            }
            res = tmp;
          }

          // Read the suffix
          for (int i = prefLen; i < resLen; i++) {
            res[i] = this.bytes.GetUInt8(pos + (ulong)(i - prefLen));
          }
          pos += (ulong)suffLen;

          if (++cuid % indexStride == 0) {
            // Save an index item
            int idx = (int)(cuid / indexStride) - 1;
            idxUrls[idx] = SubArray(res, 0, resLen);
            idxOffsets[idx] = lastPos;
            lastPos = pos;
          }
        }
        // Finally, store a sentinel
        if (cuid % indexStride != 0) {
          idxUrls[numIdxItems - 1] = SubArray(res, 0, resLen);
          idxOffsets[numIdxItems - 1] = lastPos;
        }
        var secs2 = 0.001 * sw.ElapsedMilliseconds;
        //Console.Error.WriteLine("PERF: Cell {0} url portion: Loading took {1} seconds, indexing took {2} seconds", this.cell.fileName, secs1, secs2);

        Contract.ForAll(this.idxUrls, url => url != null);
      }

      internal void SetUrlToUidCacheParams(int logCacheSize, int logSpineSize) {
        this.hashToUidCache = new HashToUidCache(logCacheSize, logSpineSize);
      }

      internal void PrintCacheStats() {
        if (this.hashToUidCache != null) {
          long probeCnt = this.hashToUidCache.GetProbeCount();
          long hitCnt = this.hashToUidCache.GetHitCount();
          Console.Error.WriteLine("URL-to-UID cache: {0} probes, {1} hits, {2}% hit rate",
                                  probeCnt, hitCnt, 100.0 * hitCnt / probeCnt);
        }
      }

      internal long UrlToUid(byte[] url) {
        Contract.Requires(this.IsLoaded);
        Contract.Requires(url != null);

        lock (this) {
          /* This lock really only needs to protect the state of the
             HashToUidCache object that is part of the UrlCell object.
             So, it could be moved further down into the UrlCell object. */
          ulong hash = 0;

          if (this.hashToUidCache != null) {
            hash = this.hasher.Hash(url);
            long uid = this.hashToUidCache.Lookup(hash);
            if (uid != -1) return uid;
          }

          int lo = 0;
          int hi = this.idxUrls.Length;
          /* Loop invariant: If urlBytes is in the UrlCell, it is in a segment whose
             number is in the interval [lo,hi].  Initially, hi is numIdxItems
             (one past the last segment), reflecting the possibility that urlBytes
             is greater than the last URL in the store. 
             It is possible that this UrlCell contains no URLs (numUrls==0). In 
             this case, this.idxUrls.Length==0, causing the while loop below to 
             iterate zero times and the subsequent conditional to return -1.
           */

          long cuid;
          while (lo < hi) {
            int mid = (lo + hi) / 2;
            int cmp = EncString.Compare(url, this.idxUrls[mid]);
            if (cmp > 0) {
              lo = mid + 1;
            } else if (cmp < 0) {
              hi = mid;
            } else {
              // Hit -- no need to go to main file
              cuid = ((long)mid + 1) * this.indexStride - 1;
              if (cuid >= this.cell.numUrls) cuid = this.cell.numUrls - 1; // last seg underfull
              var uid = cuid + this.cell.baseUID;
              if (this.hashToUidCache != null) this.hashToUidCache.Add(hash, uid);
              return uid;
            }
          }
          if (hi == this.idxUrls.Length) {
            return -1;  // urlBytes is greater than largest URL
          }

          cuid = ((long)lo) * this.indexStride - 1;
          int len = url.Length;
          byte[] suffix = lo == 0 ? EncString.Empty : this.idxUrls[lo - 1];
          ulong pos = this.idxOffsets[lo];
          long candPos = -1;
          int prefLen = 0;
          int suffLen = suffix.Length;
          int p = 0;
          for (; ; ) {
            for (int i = 0; p < len && i < suffLen; i++) {
              byte c = candPos == -1
                ? suffix[i]
                : this.bytes.GetUInt8((ulong)(candPos + i));
              if (c > url[p]) {
                return -1;
              }
              if (c < url[p]) break;
              p++;
            }
            if (p == len) {
              if (len == prefLen + suffLen) {
                var uid = cuid + this.cell.baseUID;
                if (this.hashToUidCache != null) this.hashToUidCache.Add(hash, uid);
                return uid;
              } else {
                return -1;
              }
            }
            for (; ; ) {
              prefLen = this.ReadCompressedSize(ref pos);
              suffLen = this.ReadCompressedSize(ref pos);
              candPos = (long)pos;
              pos += (ulong)suffLen;
              cuid++;
              if (prefLen > p) {
                // the current and the target URL still differ at position partID,
                // so we can just continue the loop
              } else if (prefLen < p) {
                return -1;  // urlBytes is not contained in stream
              } else { // we know that prefLen == partID
                break;
              }
            }
          }
        }
      }

      internal byte[] UidToUrl(long uid) {
        Contract.Requires(this.cell.baseUID <= uid && uid < this.cell.supraUID);

        lock (this) {
          /* This lock really only needs to protect the state of the
             HashToUidCache object that is part of the UrlCell object.
             So, it could be moved further down into the UrlCell object. */

          long cuid = uid - this.cell.baseUID;
          int idx = (int)(cuid / this.indexStride);

          // First, figure out how long the result string will be
          ulong pos = this.idxOffsets[idx];
          int prefLen = 0;
          int suffLen = 0;
          for (long p = idx * this.indexStride; p <= cuid; p++) {
            prefLen = this.ReadCompressedSize(ref pos);
            suffLen = this.ReadCompressedSize(ref pos);
            pos += (ulong)suffLen;
            if (pos > this.bytes.Size) {
              throw new FileFormatException("Trying to read past end of stream");
            }
          }
          // Second, create the result string and copy the index URL into it
          int resLen = prefLen + suffLen;
          var res = new byte[resLen];
          if (idx > 0) {
            for (int i = 0; i < resLen && i < this.idxUrls[idx - 1].Length; i++) {
              res[i] = this.idxUrls[idx - 1][i];
            }
          }

          // Finally, build the result string
          pos = this.idxOffsets[idx];
          for (long p = idx * this.indexStride; p <= cuid; p++) {
            // Decompress the prefix length and the suffix length
            prefLen = this.ReadCompressedSize(ref pos);
            suffLen = this.ReadCompressedSize(ref pos);
            int len = prefLen + suffLen;
            if (len > resLen) len = resLen;
            for (int i = prefLen; i < len; i++) {
              res[i] = this.bytes.GetUInt8(pos + (ulong)(i - prefLen));
            }
            pos += (ulong)suffLen;
          }
          return res;
        }
      }

      internal IEnumerable<byte[]> Urls() {
        ulong pos = 0;
        var prevRes = new byte[0];
        for (long luid = 0; luid < this.cell.numUrls; luid++) {
          int prefLen = this.ReadCompressedSize(ref pos);
          int suffLen = this.ReadCompressedSize(ref pos);
          var res = new byte[prefLen + suffLen];
          for (int i = 0; i < prefLen; i++) {
            res[i] = prevRes[i];
          }
          for (int i = prefLen; i < res.Length; i++) {
            res[i] = this.bytes.GetUInt8(pos + (ulong)(i - prefLen));
          }
          pos += (ulong)suffLen;
          prevRes = res;
          yield return res;
        }
        Contract.Assert(pos == this.bytes.Size);
      }

      private int ReadCompressedSize(ref ulong pos) {
        int res = 0;
        byte x;
        int ls = 0;
        do {
          x = this.bytes.GetUInt8(pos);
          pos += sizeof(byte);
          res = res | (x & 0x7f) << ls;
          ls += 7;
        } while ((x & 0x80) == 0);
        return res;
      }

      private static byte[] SubArray(byte[] bytes, int start, int length) {
        var res = new byte[length];
        for (int i = 0; i < length; i++) {
          res[i] = bytes[start++];
        }
        return res;
      }
    }

    internal class LinkCell {
      internal readonly long numBytes;
      internal readonly long numLinks;
      internal readonly int indexStride;
      internal readonly LinkCompression compressionCode;
      internal readonly Cell cell;
      internal long startPos;
      internal ulong[] idxOffsets;
      internal CachedStream bytes;


      internal LinkCell(Cell cell, BinaryReader rd) {
        this.numBytes = rd.ReadInt64();
        this.numLinks = rd.ReadInt64();
        this.indexStride = rd.ReadInt32();
        this.compressionCode = (LinkCompression)rd.ReadUInt32();
        this.cell = cell;
        this.startPos = -1;
        this.idxOffsets = null;
        this.bytes = null;
      }

      internal void Unload() {
        this.bytes = null;
        this.idxOffsets = null;
      }

      internal void Load() {
        var sw = Stopwatch.StartNew();
        // Somewhat of a hack: If this LinkCell is not yet sealed, return immediately.
        // This is OK as long as noone tries to use the cell subsequently.
        // A better solution would be not to call Load on unsealed LinkCell objects.
        if (this.numBytes == -1) return;

        using (var rd = new BinaryReader(new BufferedStream(new FileStream(this.cell.fileName, FileMode.Open, FileAccess.Read, FileShare.Read)))) {
          rd.BaseStream.Seek(this.startPos, SeekOrigin.Begin);
          this.bytes = new CachedStream(rd.BaseStream, (ulong)this.numBytes);
        }
        var secs1 = 0.001 * sw.ElapsedMilliseconds;
        sw.Restart();

        // Construct idxOffsets from main
        var supraPuid = this.cell.part.ping.PUID(this.cell.supraUID);
        long numIdxItems = (int)((supraPuid - 1) / this.indexStride) + 1;
        this.idxOffsets = new ulong[numIdxItems];
        var decompressor = this.NewDecompressor();
        int idx = 0;
        for (long puid = 0; puid < supraPuid; puid++) {
          if (puid % this.indexStride == 0) {
            idxOffsets[idx++] = decompressor.GetPosition();
          }
          uint m = decompressor.GetUInt32();
          for (uint j = 0; j < m; j++) {
            var linkUid = decompressor.GetUInt64();  // don't care that first gap is signed
          }
        }
        var secs2 = 0.001 * sw.ElapsedMilliseconds;
        //Console.Error.WriteLine("PERF: Cell {0} {1} portion: Loading took {2} seconds, indexing took {3} seconds", this.cell.fileName, this == this.cell.fwdCell ? "fwd" : "bwd", secs1, secs2);
      }

      internal IntStreamDecompressor NewDecompressor() {
        Contract.Requires(this.bytes != null);
        switch (this.compressionCode) {
          case LinkCompression.None:
            return new DummyIntStreamDecompressor(this.bytes);
          case LinkCompression.VarByte:
            return new VarByteIntStreamDecompressor(this.bytes);
          case LinkCompression.VarNybble:
            return new VarNybbleIntStreamDecompressor(this.bytes);
          default:
            throw new FileFormatException("Unknown LinkCell compression scheme");
        }
      }
    }

    internal class LinkCellRd {
      internal readonly LinkCell linkCell;  
      private readonly IntStreamDecompressor decompressor;  // stateful
      private long nextPuid;                                // stateful

      internal LinkCellRd(LinkCell linkCell) {
        this.linkCell = linkCell;
        this.decompressor = this.linkCell.NewDecompressor();
        this.nextPuid = 0;
      }

      // GetLinks takes a UID instead of a PUID because it uses the uid to decomress the 
      // UID's first link, and an on-the-fly translation would require the PUID-to-UID 
      // translation table to be loaded at all times.
      internal List<long> GetLinks(long uid) {
        Contract.Requires(this.linkCell.cell.UidInLinkCell(uid));
        Contract.Ensures(Contract.Result<List<long>>() != null);
        Contract.Ensures(UID.LinksAreSorted(Contract.Result<List<long>>()));

        long puid = this.linkCell.cell.part.ping.PUID(uid);
        if (puid != this.nextPuid) {
          long block = puid / this.linkCell.indexStride;
          this.decompressor.SetPosition(this.linkCell.idxOffsets[block]);
          for (long pos = block * this.linkCell.indexStride; pos < puid; pos++) {
            uint m = this.decompressor.GetUInt32();
            for (uint j = 0; j < m; j++) {
              // First gap (i=0) is actually an Int64, but I don't care
              this.decompressor.GetUInt64();
            }
          }
        }
        uint n = this.decompressor.GetUInt32();
        var linkUids = new List<long>((int)n);
        var linkUid = uid;
        for (uint j = 0; j < n; j++) {
          long gap = j == 0
            ? this.decompressor.GetInt64()
            : (long)this.decompressor.GetUInt64();
          linkUid += gap;
          linkUids.Add(linkUid);
        }
        this.nextPuid = puid + 1;
        return linkUids;
      }
    }
  }
}
