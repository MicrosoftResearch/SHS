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

public class BitVector {
  private ulong[] bits;

  public BitVector(long n) {
    this.bits = new ulong[((n - 1) / 64) + 1];
  }

  public bool this[long i] {
    get {
      return ((this.bits[i >> 6] >> (int)(i & 0x3f)) & 1) == 1;
    }
    set {
      long p = i >> 6;
      ulong m = (ulong)1 << (int)(i & 0x3f);
      if (value) {
        this.bits[p] |= m;
      } else {
        this.bits[p] &= ~m;
      }
    }
  }

  public void SetAll(bool val) {
    ulong m = val ? ulong.MaxValue : ulong.MinValue;
    for (int i = 0; i < this.bits.Length; i++) this.bits[i] = m;
  }
}
