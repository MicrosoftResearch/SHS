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
  internal class DummyIntStreamCompressor : IntStreamCompressor {
    private BinaryWriter wr;
    private long pos;

    /* Creates a (non-)compressor on top of wr. */
    internal DummyIntStreamCompressor() {
      this.wr = null;
      this.pos = 0L;
    }

    /* Provide implementations for all pure virtual functions of base class. */
    internal override void SetWriter(BinaryWriter wr) {
      this.wr = wr;
      this.pos = 0L;
    }

    internal override void PutInt32(int x) {
      this.wr.Write(x);
      this.pos += sizeof(int);
    }

    internal override void PutUInt32(uint x) {
      this.wr.Write(x);
      this.pos += sizeof(uint);
    }

    internal override void PutInt64(long x) {
      this.wr.Write(x);
      this.pos += sizeof(long);
    }

    internal override void PutUInt64(ulong x) {
      this.wr.Write(x);
      this.pos += sizeof(ulong);
    }

    internal override long Align() {
      return this.pos;
    }

    internal override LinkCompression Identify() {
      return LinkCompression.None;
    }
  }
}
