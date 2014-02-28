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

namespace SHS {
  internal class FrontCodingStringCompressor {
    private Stream stream;
    private byte[] last;
    private Int64 pos;

    internal FrontCodingStringCompressor(Stream stream) {
      this.stream = stream;
      this.last = new byte[0];
      this.pos = 0;
    }

    internal void WriteString(byte[] bytes) {
      // Determine the length of the common prefix between bytes and last
      int minLen = Math.Min(this.last.Length, bytes.Length);
      int prefLen = 0;
      while (prefLen < minLen && this.last[prefLen] == bytes[prefLen]) prefLen++;
      int suffLen = bytes.Length - prefLen;

      // Write out prefLen and suffLen using a variable-length encoding
      this.pos += this.WriteCompressedSize(prefLen);
      this.pos += this.WriteCompressedSize(suffLen);

      // Write out the suffix
      this.stream.Write(bytes, prefLen, suffLen);

      // Update pos and last
      this.pos += suffLen;
      this.last = bytes;
    }

    internal Int64 GetPosition() {
      return this.pos;
    }

    private int WriteCompressedSize(int x) {
      int numWritten = 0;
      do {
        byte z = (byte)(x & 0x7f);
        x >>= 7;
        if (x == 0) z |= 0x80;
        this.stream.WriteByte(z);
        numWritten++;
      } while (x != 0);
      return numWritten;
    }
  }
}
