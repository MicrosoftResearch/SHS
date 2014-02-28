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
using System.IO;

/* This class has mutable state.  Clients are responsible for guaranteeing
   thread-safety, either by locking this object or (preferredly) by
   having a separate decompressor object per thread. */

namespace SHS {
  internal class VarByteIntStreamDecompressor : IntStreamDecompressor {
    private readonly CachedStream main;

    // mutable state
    private UInt64 pos;
    private UInt64 word;      // bytes 7 (highest-order byte) to lastContBit are payload
    // byte 0 (lowest-order byte) unused
    // byte 1 contains underfullness [1..6] if contBit 7 is set
    private UInt64 contBits;  // contBit 7 is used to indicate underfull word
    private int nextContBit;  // -1 indicates that word, contBits do not have valid data
    private int lastContBit;

    internal VarByteIntStreamDecompressor(CachedStream main) {
      this.main = main;
      this.pos = 0;

      UInt64 len = main.Size;
      if ((len % 8) != 0) {
        throw new FileFormatException("stream length (" + len + ") is not a multiple of 8");
      }
      this.nextContBit = -1;
      this.lastContBit = 0;
    }

    internal override void SetPosition(UInt64 pos) {
      this.pos = pos;
      this.nextContBit = -1;
      this.lastContBit = 0;
    }

    internal override UInt64 GetPosition() {
      return this.pos;
    }

    internal override Int32 GetInt32() {
      Int64 x = this.GetInt64();
      Int32 res = (Int32)x;
      if (res != x) {
        throw new FileFormatException("decompressed a non-INT32 " + x);
      }
      return res;
    }

    internal override UInt32 GetUInt32() {
      UInt64 x = this.GetUInt64();
      UInt32 res = (UInt32)x;
      if (res != x) {
        throw new FileFormatException("decompressed a non-UINT32 " + x);
      }
      return res;
    }

    internal override Int64 GetInt64() {
      UInt64 ures = this.GetUInt64();
      return (ures & 1) != 0 ? (Int64)(~(ures >> 1)) : (Int64)(ures >> 1);
    }

    internal override UInt64 GetUInt64() {
      if (this.nextContBit < this.lastContBit) this.LoadWord();
      int k = 8;
      UInt64 hiRes = 0;
      while (((UInt64)1 << this.nextContBit & this.contBits) == 0) {
        this.nextContBit--;
        if (this.nextContBit == -1) {
          hiRes =  this.word >> (64 - k);
          this.LoadWord();
          k = 8;
        } else {
          k += 8;
        }
      }
      UInt64 res = this.word >> (64 - k) | hiRes << k;
      this.word <<= k;
      this.nextContBit--;
      return res;
    }

    internal override bool AtEnd() {
      return this.pos == this.main.Size && this.nextContBit < this.lastContBit;
    }

    private void LoadWord() {
      UInt64 w = this.main.GetUInt64(this.pos);
      this.pos += sizeof(UInt64);
      this.word = w & 0xffffffffffffff00UL;
      this.contBits = w & 0x00000000000000ffUL;
      this.nextContBit = 6;
      this.lastContBit = (this.contBits & 1 << 7) != 0
        ? (int)(w >> 8) & 0xff
        : 0;
    }
  }
}
