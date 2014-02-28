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
using System.Runtime.InteropServices; // to bring DllImport into scope

namespace SHS {
  public unsafe class CachedStream : System.Runtime.ConstrainedExecution.CriticalFinalizerObject {
    private readonly IntPtr ph;   // Native process heap
    private readonly byte* bytes; // Unmanaged byte array
    private readonly ulong size;

    /// <summary>
    /// Read a number of bytes from a stream into unmanaged main memory, and construct 
    /// a container object around it.
    /// </summary>
    /// <param name="stream">The stream to read from</param>
    /// <param name="numBytes">The number of bytes to read</param>
    public CachedStream(Stream stream, ulong numBytes) {
      this.size = numBytes;

      // Allocate unmanaged byte array
      unsafe {
        /* This recipe is taken from chapter 18.9 of "The C#  Programming
           Language" (3rd edition) by Hejlsberg et al.  According to Yuan Yu,
           an alternative and possibly better way would be to use
           System.Runtime.InteropServices.AllocHGlobal. */
        this.ph = GetProcessHeap();
        this.bytes = (byte*)HeapAlloc(ph, 0, (UIntPtr)this.size);
        if (this.bytes == null) throw new OutOfMemoryException();
      }

      const int chunkSize = 8192;
      byte[] buf = new byte[chunkSize];
      ulong pos = 0;
      ulong rem = numBytes;
      while (rem > 0) {
        int max = rem < chunkSize ? (int)rem : chunkSize;
        int n = stream.Read(buf, 0, max);
        if (n == 0) throw new FileFormatException(string.Format("Stream too short, expected {0} extra bytes", rem));
        rem -= (ulong)n;
        for (int i = 0; i < n; i++) {
          this.bytes[pos++] = buf[i];
        }
      }
      if (pos != size) throw new Exception();
    }

    ~CachedStream() {
      HeapFree(this.ph, 0, this.bytes);
    }

    public int GetInt32(ulong bo) {
      if (bo + sizeof(int) > this.size) throw new IndexOutOfRangeException();
      return *((int*)(this.bytes + bo));
    }

    public long GetInt64(ulong bo) {
      if (bo + sizeof(long) > this.size) throw new IndexOutOfRangeException();
      return *((long*)(this.bytes + bo));
    }

    public byte GetUInt8(ulong bo) {
      if (bo + sizeof(byte) > this.size) throw new IndexOutOfRangeException(string.Format("bo={0} size={1}", bo, size));
      return *((byte*)(this.bytes + bo));
    }

    public uint GetUInt32(ulong bo) {
      if (bo + sizeof(uint) > this.size) throw new IndexOutOfRangeException();
      return *((uint*)(this.bytes + bo));
    }

    public ulong GetUInt64(ulong bo) {
      if (bo + sizeof(ulong) > this.size) throw new IndexOutOfRangeException();
      return *((ulong*)(this.bytes + bo));
    }

    // ... same for other types, as needed

    public ulong Size {
      get { return this.size; } 
    }

    [DllImport("kernel32", SetLastError = true)]
    static extern IntPtr GetProcessHeap();

    [DllImport("kernel32", SetLastError = true)]
    static extern void* HeapAlloc(IntPtr hHeap, int flags, UIntPtr numBytes);

    [DllImport("kernel32", SetLastError = true)]
    static extern bool HeapFree(IntPtr hHeap, int flags, void* block);
  }
}
