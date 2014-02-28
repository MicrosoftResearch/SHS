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

using System.IO;

namespace SHS {
  internal class VarNybbleIntStreamCompressor : IntStreamCompressor {
    private BinaryWriter wr;
    private long pos;
    private int freeNybbles;  // in [12..1]
    private ulong word;  // Used to accumulate the next word to write to ob

    internal VarNybbleIntStreamCompressor() {
      this.wr = null;
      this.pos = 0;
      this.freeNybbles = 12;
      this.word = 0;
    }

    internal override void SetWriter(BinaryWriter wr) {
      this.wr = wr;
      this.pos = 0;
      this.freeNybbles = 12;
      this.word = 0;
    }

    internal override void PutInt32(int x) {
      this.PutInt64(x);
    }

    internal override void PutUInt32(uint x) {
      this.PutUInt64(x);
    }

    internal override void PutInt64(long x) {
      // Change the encoding of the sign of x
      ulong ux = x < 0 ? (ulong)(~x) << 1 | 1 : (ulong)x << 1;
      this.PutUInt64(ux);
    }

    internal override void PutUInt64(ulong x) {
      // Determine how many nybbles we need to represent x
      ulong mask   = 0xf000000000000000UL;
      int numNybbles = 16;
      for (; numNybbles > 1 && (x & mask) == 0; numNybbles--) {
        mask >>= 4;  // "logical shift" -- higher-order bits are 0
      }
      // Does x fit into word?
      while (this.freeNybbles < numNybbles) {
        // Write out the higher-order bytes that fit; flush; adjust numNybbles and x
        numNybbles -= this.freeNybbles;
        this.word |= (x >> numNybbles * 4) << 16;  // Relies on logical shift
        this.FlushWord();
        x &= ((1UL << numNybbles * 4) - 1);
      }
      this.freeNybbles -= numNybbles;
      this.word |= 1UL << this.freeNybbles;
      this.word |= x << (this.freeNybbles + 4) * 4;
      if (this.freeNybbles == 0) this.FlushWord();
    }

    internal override long Align() {
      if (this.freeNybbles < 12) {
        this.word |= (ulong)this.freeNybbles << 12;
        this.FlushWord();
      }
      return this.pos << 3;
    }

    internal override LinkCompression Identify() {
      return LinkCompression.VarNybble;
    }

    private void FlushWord() {
      this.wr.Write(this.word);
      this.pos++;
      this.freeNybbles = 12;
      this.word = 0;
    }
  }
}
