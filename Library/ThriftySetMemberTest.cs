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

using System.Runtime.InteropServices;

namespace SHS {
  public class ThriftySetMemberTest {
    private readonly Hash64 hasher;
    private readonly UInt40[][] hashes;
    private long  count;

    public ThriftySetMemberTest(int logSpineLen) {
      int spineLen = 1 << logSpineLen;
      this.hashes = new UInt40[spineLen][];
      for (int i = 0; i < spineLen; i++) {
        this.hashes[i] = null;
      }
      this.count = 0;
      this.hasher = new Hash64();
    }

    public ThriftySetMemberTest() : this(24) {}

    public long Count { get { return this.count; } }

    public bool ContainsOrAdd(string s) {
      return this.ContainsOrAdd(this.hasher.Hash(s));
    }

    public bool ContainsOrAdd(ulong hash) {
      UInt40 h = new UInt40(hash);
      int i = (int)(hash & (ulong)(this.hashes.Length - 1));
      int n = this.hashes[i] == null ? 0 : this.hashes[i].Length;
      for (int j = 0; j < n; j++) {
        if (this.hashes[i][j].Equals(h)) return true;
      }
      UInt40[] tmp = new UInt40[n+1];
      for (int j = 0; j < n; j++) {
        tmp[j] = this.hashes[i][j];
      }
      tmp[n] = h;
      this.hashes[i] = tmp;
      this.count++;
      return false;
    }

    public bool Contains(ulong hash) {
      UInt40 h = new UInt40(hash);
      int i = (int)(hash & (ulong)(this.hashes.Length - 1));
      int n = this.hashes[i] == null ? 0 : this.hashes[i].Length;
      for (int j = 0; j < n; j++) {
        if (this.hashes[i][j].Equals(h)) return true;
      }
      return false;
    }

    public void Add(ulong hash) {
      UInt40 h = new UInt40(hash);
      int i = (int)(hash & (ulong)(this.hashes.Length - 1));
      int n = this.hashes[i] == null ? 0 : this.hashes[i].Length;
      for (int j = 0; j < n; j++) {
        if (this.hashes[i][j].Equals(h)) return;
      }
      UInt40[] tmp = new UInt40[n+1];
      for (int j = 0; j < n; j++) {
        tmp[j] = this.hashes[i][j];
      }
      tmp[n] = h;
      this.hashes[i] = tmp;
      this.count++;
    }

    [StructLayout(LayoutKind.Sequential,Pack=1)]
    private struct UInt40 {
      private readonly byte b7;
      private readonly byte b6;
      private readonly byte b5;
      private readonly byte b4;
      private readonly byte b3;

      internal UInt40(ulong hash) {
        this.b7 = (byte)((hash >> 56) & 0xff);
        this.b6 = (byte)((hash >> 48) & 0xff);
        this.b5 = (byte)((hash >> 40) & 0xff);
        this.b4 = (byte)((hash >> 32) & 0xff);
        this.b3 = (byte)((hash >> 24) & 0xff);
      }

      ulong Get(ulong lower24) {
        return (((ulong)this.b7) << 56)
          | (((ulong)this.b6) << 48)
          | (((ulong)this.b5) << 40)
          | (((ulong)this.b4) << 32)
          | (((ulong)this.b3) << 24)
          | (lower24 & 0xffffff);
      }

      bool Equals(UInt40 that) {
        return this.b7 == that.b7
          && this.b6 == that.b6
          && this.b5 == that.b5
          && this.b4 == that.b4
          && this.b3 == that.b3;
      }
    }
  }
}
