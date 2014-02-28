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

namespace SHS {
  public class UidMap {
    private long[] vals;
    private int pos;
    private bool sorted;

    public UidMap() {
      this.vals = new long[0];
      this.pos = 0;
      this.sorted = true;
    }

    public UidMap(int sz) {
      this.vals = new long[sz];
      this.pos = 0;
      this.sorted = true;
    }

    public UidMap(List<long> items) {
      this.vals = new long[items.Count];
      for (int i = 0; i < this.vals.Length; i++) {
        this.vals[i] = items[i];
      }
      this.pos = this.vals.Length;
      this.sorted = false;
    }

    public UidMap(long[] items) {
      this.vals = new long[items.Length];
      for (int i = 0; i < this.vals.Length; i++) {
        this.vals[i] = items[i];
      }
      this.pos = this.vals.Length;
      this.sorted = false;
    }

    public UidMap(long[][] items) {
      int n = 0;
      for (int i = 0; i < items.Length; i++) {
        n += items[i].Length;
      }
      this.vals = new long[n];
      this.pos = 0;
      for (int i = 0; i < items.Length; i++) {
        for (int j = 0; j < items[i].Length; j++) {
          this.vals[pos++] = items[i][j];
        }
      }
      this.sorted = false;
    }

    public void Add(long val) {
      if (this.pos == this.vals.Length) {
        Array.Resize<long>(ref this.vals, (int)((this.pos + 1) * 1.2));
      }
      this.vals[pos++] = val;
      this.sorted = false;
    }

    public void Add(long[] items) {
      if (this.pos + items.Length > this.vals.Length) {
        Array.Resize<long>(ref this.vals, this.pos + items.Length);
      }
      for (int i = 0; i < items.Length; i++) {
        this.vals[pos++] = items[i];
      }
      this.sorted = false;
    }

    public int this[long item] {
      get {
        if (!this.sorted) this.SortAndDedup();
        int lo = 0;
        int hi = this.pos;
        while (lo < hi) {
          int mid = (lo + hi) / 2;
          if (item > vals[mid]) {
            lo = mid + 1;
          } else if (item < vals[mid]) {
            hi = mid;
          } else {
            return mid;
          }
        }
        throw new KeyNotFoundException(item.ToString());
      }
    }

    public static implicit operator long[] (UidMap x) {
      return x.ToArray();
    }

    private long[] ToArray() {
      if(!this.sorted) this.SortAndDedup();
      return this.vals;
    }

    private void SortAndDedup() {
      Sort(this.vals, 0, this.pos);
      this.sorted = true;
      if (this.pos > 0) {
        int x = 0;
        int y = 1;
        while (y < this.pos) {
          if (this.vals[y] == this.vals[x]) {
            y++;
          } else {
            this.vals[++x] = this.vals[y++];
          }
        }
        this.pos = x + 1;
      }
      long[] tmp = new long[this.pos];
      System.Array.Copy(this.vals, tmp, this.pos);
      this.vals = tmp;
    }

    private static void Sort(long[] items, int lo, int hi) {
      QuickSort(items, lo, hi-1);
      InsertionSort(items, lo, hi-1);
    }

    private static void QuickSort(long[] items, int lo, int hi) {
      if (hi - lo <= 10) return;
      long mid = (lo + hi) / 2;
      long item = items[mid]; items[mid] = items[hi-1]; items[hi-1] = item;
      if (items[lo] > items[hi-1]) {
        item = items[lo]; items[lo] = items[hi-1]; items[hi-1] = item;
      }
      if (items[lo] > items[hi]) {
        item = items[lo]; items[lo] = items[hi]; items[hi] = item;
      }
      if (items[hi-1] > items[hi]) {
        item = items[hi-1]; items[hi-1] = items[hi]; items[hi] = item;
      }
      int i = Partition(items, lo+1, hi-1);
      QuickSort(items, lo, i-1);
      QuickSort(items, i+1, hi);
    }

    private static int Partition(long[] items, int lo, int hi) {
      int i = lo - 1, j = hi;
      long value = items[hi];
      while (true) {
        while (items[++i] < value) /*SKIP*/;
        while (value < items[--j]) { if (j == lo) break; }
        if (i >= j) break;
        long item = items[i]; items[i] = items[j]; items[j] = item;
      }
      long item1 = items[i]; items[i] = items[hi]; items[hi] = item1;
      return i;
    }

    private static void InsertionSort(long[] items, int lo, int hi) {
      for (int i = lo + 1; i <= hi; i++) {
        if (items[lo] > items[i]) {
          long item = items[lo]; items[lo] = items[i]; items[i] = item;
        }
      }
      for (long i = lo + 2; i <= hi; i++) {
        long j = i;
        long item = items[i];
        while (item < items[j-1]) {
          items[j] = items[j-1];
          j--;
        }
        items[j] = item;
      }
    }
  }
}
