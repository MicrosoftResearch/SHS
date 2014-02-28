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

namespace SHS {
  /// <summary>
  /// The DiskSorter class implements a generic method for sorting data sets 
  /// that are too large to fit into main memory.  Given that it may be useful 
  /// to client code, I decided to put it into SHS.Library instead of SHS.Server.
  /// </summary>
  /// <typeparam name="T">Type of objects to be sorted</typeparam>
  public class DiskSorter<T> : IDisposable {
    private struct Elem {
      internal BinaryReader rd;
      internal T item;
    }

    private IComparer<T> Comparer;
    private Writer Write;
    private Reader Read;

    private readonly List<string> tmpFiles;
    private T[] buffer;
    private int cnt;
    private long total;
    private Elem[] heap;
    private int pos;

    [ContractInvariantMethod]
    private void ObjectInvariant() {
      Contract.Invariant(this.buffer == null || (0 <= this.cnt && this.cnt <= this.buffer.Length));
      Contract.Invariant(this.heap == null || (0 <= this.pos && this.pos <= this.heap.Length));
      Contract.Invariant(this.tmpFiles != null);
    }

    public delegate void Writer(BinaryWriter wr, T item);
    public delegate T Reader(BinaryReader rd);

    /// <summary>
    /// Create a new DiskSorter
    /// </summary>
    /// <param name="compare">An IComparer for comparing pairs of elememts</param>
    /// <param name="write">A method for writing elements to a BinaryWriter</param>
    /// <param name="read">A method for reading elements from a BinaryReader</param>
    /// <param name="bufSz">Maximum number of elements to keep in main memory</param>
    public DiskSorter(IComparer<T> compare, Writer write, Reader read, int bufSz) {
      this.Comparer = compare;
      this.Write = write;
      this.Read = read;
      this.buffer = new T[bufSz];
      this.tmpFiles = new List<string>();
      this.cnt = 0;
      this.total = 0;
      this.heap = null;
      this.pos = 0;
    }

    /// <summary>
    ///  Total number of elements added to the DiskSorter
    /// </summary>
    public long Total { get { return this.total; } }

    /// <summary>
    /// Dispose the DiskSorter
    /// </summary>
    public void Dispose() {
      foreach (var tmpFile in this.tmpFiles) {
        try {
          File.Delete(tmpFile);
        } catch (IOException) {
          Console.Error.WriteLine("WARNING: Could not delete file " + tmpFile);
        }
      }
    }

    /// <summary>
    /// Add an element to the DiskSorter
    /// </summary>
    /// <param name="item">The element to be added</param>
    public void Add(T item) {
      Contract.Assume(this.buffer != null);
      Contract.Assert(0 <= this.cnt && this.cnt <= this.buffer.Length);
      if (this.cnt == this.buffer.Length) this.SortAndWriteBuffer();
      this.buffer[this.cnt++] = item;
      this.total++;
    }

    /// <summary>
    /// Sort all elements in the DiskSorter
    /// </summary>
    public void Sort() {
      if (this.cnt > 0) this.SortAndWriteBuffer();
      this.buffer = null;
      this.heap = new Elem[tmpFiles.Count];
      this.pos = 0;
      //foreach (var tmpFile in tmpFiles) {
      for (int i = 0; i < tmpFiles.Count; i++) {
        var tmpFile = tmpFiles[i];
        var rd = new BinaryReader(new BufferedStream(new FileStream(tmpFile, FileMode.Open, FileAccess.Read)));
        // Per invariance, each temporary file contains at least one item, so I
        // don't need to worry about an EndOfStreamException when reading the 
        // first item from each file and caching it in the heap data structure.
        this.heap[this.pos].rd = rd;
        this.heap[this.pos].item = this.Read(rd); 

        int x = this.pos;
        while (x > 0) {
          int p = (x - 1) / 2;
          if (this.Comparer.Compare(this.heap[p].item, this.heap[x].item) < 0) break;
          var e = this.heap[p]; this.heap[p] = this.heap[x]; this.heap[x] = e;
          x = p;
        }
        this.pos++;
      }
    }

    /// <summary>
    /// True if and only if the last element has been consumed 
    /// </summary>
    /// <returns>True if the last element has been consumed, false otherwise</returns>
    public bool AtEnd() {
      return this.pos == 0;
    }

    /// <summary>
    /// Return the next element in sorted order, but do not consume it
    /// </summary>
    /// <returns>The next unconsumed element</returns>
    public T Peek() {
      Contract.Assert(this.heap != null && this.pos > 0);
      return this.heap[0].item;
    }

    /// <summary>
    /// Return and consume the next element in sorted order
    /// </summary>
    /// <returns>The next unconsumed element</returns>
    public T Get() {
      Contract.Assert(this.heap != null && this.pos > 0);
      T res = this.heap[0].item;
      try {
        this.heap[0].item = Read(heap[0].rd);
      } catch (EndOfStreamException) {
        this.heap[0].rd.Close();
        this.heap[0] = this.heap[--this.pos];
      }
      int x = 0;
      while (x < this.pos) {
        int c = 2 * x + 1;
        if (c >= this.pos) {
          break;
        } else if (c + 1 < this.pos) {
          if (this.Comparer.Compare(this.heap[c].item, this.heap[c + 1].item) > 0) c = c + 1;
        }
        if (this.Comparer.Compare(this.heap[x].item, this.heap[c].item) > 0) {
          var e = this.heap[x]; this.heap[x] = this.heap[c]; this.heap[c] = e;
          x = c;
        } else {
          break;
        }
      }
      return res;
    }

    private void SortAndWriteBuffer() {
      Contract.Assert(this.buffer != null);
      Array.Sort<T>(this.buffer, 0, this.cnt, this.Comparer);
      var tmpFile = string.Format("tmp-{0:N}.shs", Guid.NewGuid());
      using (var wr = new BinaryWriter(new BufferedStream(new FileStream(tmpFile, FileMode.Create, FileAccess.Write)))) {
        for (int i = 0; i < this.cnt; i++) {
          this.Write(wr, this.buffer[i]);
        }
      }
      tmpFiles.Add(tmpFile);
      this.cnt = 0;
    }
  }
}
