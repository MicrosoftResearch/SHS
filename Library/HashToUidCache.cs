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

namespace SHS {
  internal class HashToUidCache {
    private int     next;         // in range [0 .. cacheSize-1]
    private bool    full;         // true if cache contains cacheSize entries
    private ulong[] hashes;       // array of length cacheSize
    private ulong[] bitsAndUids;  // array of length cacheSize
    private int     spineLen;
    private int[][] indexes;      // array of length spineLen, points into hashes, bitsAndUids
    private long    probeCnt;     // For measurement purposes
    private long    hitCnt;       // For measurement purposes

    // Creates a new cache that can hold up to 2^logSpineLen items
    internal HashToUidCache(int logCacheSize, int logSpineLen) {
      int cacheSize = 1 << logCacheSize;
      this.next = 0;
      this.full = false;
      this.hashes = new UInt64[cacheSize];
      this.bitsAndUids = new UInt64[cacheSize];
      for (int i = 0; i < cacheSize; i++) {
        this.hashes[i] = 0;
        this.bitsAndUids[i] = 0;
      }
      this.spineLen = 1 << logSpineLen;
      this.indexes = new Int32[this.spineLen][];
      this.probeCnt = 0;
      this.hitCnt = 0;
    }

    // Returns the UID if it is in the cache, -1 otherwise
    internal long Lookup(ulong hash) {
      this.probeCnt++;
      uint i = (uint)(hash & (ulong)(this.spineLen - 1));
      int n = this.indexes[i] == null ? 0 : this.indexes[i].Length;
      for (int j = 0; j < n; j++) {
        int k = this.indexes[i][j];
        if (this.hashes[k] == hash) {
          this.hitCnt++;
          this.bitsAndUids[k] |= 0x8000000000000000UL;  // set dont-evict bit
          return (long)(this.bitsAndUids[k] & 0x7fffffffffffffffUL);
        }
      }
      return -1;
    }

    // Adds the hash and its associated UID to the cache
    internal void Add(UInt64 hash, Int64 uid) {
      if (this.full) {
        // Find victim for eviction
        while ((this.bitsAndUids[this.next] & 0x8000000000000000UL) != 0) {
          this.bitsAndUids[this.next] &= 0x7fffffffffffffffUL;
          this.next++;
          if (this.next == this.bitsAndUids.Length) this.next = 0;
        }
        // Remove this.next from hash table
        this.RemoveFromTable(this.hashes[this.next], this.next);
      }
      this.hashes[this.next] = hash;
      this.bitsAndUids[this.next] = (UInt64)uid;  // dont-evict bit not set
      uint i = (uint)(hash & (UInt64)(this.spineLen - 1));
      int n = this.indexes[i] == null ? 0 : this.indexes[i].Length;
      Int32[] tmp = new Int32[n+1];
      for (int j = 0; j < n; j++) {
        int k = this.indexes[i][j];
        if (this.hashes[k] == hash) {
          throw new Exception("hash is already in cache");
        }
        tmp[j] = k;
      }
      tmp[n] = this.next;
      this.indexes[i] = tmp;
      this.next++;
      if (this.next == this.bitsAndUids.Length) {
        this.next = 0;
        this.full = true;
      }
    }

    // Returns the number of calls to this.Lookup
    internal Int64 GetProbeCount() {
      return this.probeCnt;
    }

    // Return the number of times this.Lookup returned not -1
    internal Int64 GetHitCount() {
      return this.hitCnt;
    }

    private void RemoveFromTable(ulong hash, int idx) {
      uint i = (uint)(hash & (UInt64)(this.spineLen - 1));
      int n = this.indexes[i] == null ? 0 : this.indexes[i].Length;
      for (int j = 0; j < n; j++) {
        if (this.indexes[i][j] == idx) {
          var tmp = new int[n-1];
          for (int k = 0; k < j; k++) {
            tmp[k] = this.indexes[i][k];
          }
          for (int k = j+1; k < n; k++) {
            tmp[k-1] = this.indexes[i][k];
          }
          this.indexes[i] = tmp;
          return;
        }
      }
      throw new Exception(string.Format("Item ({0:X16},{1}) not in HashToUidCache; bucket ID={2} len={3}", hash, idx, i, n));
    }
  }
}
