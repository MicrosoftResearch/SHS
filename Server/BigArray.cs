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
  public abstract class BigArray {
    public static BigArray Make(Type t, long n) {
      if (t == typeof(Boolean)) {
        return new BigBooleanArray(n);
      } else if (t == typeof(Byte)) {
        return new BigByteArray(n);
      } else if (t == typeof(SByte)) {
        return new BigSByteArray(n);
      } else if (t == typeof(UInt16)) {
        return new BigUInt16Array(n);
      } else if (t == typeof(Int16)) {
        return new BigInt16Array(n);
      } else if (t == typeof(UInt32)) {
        return new BigUInt32Array(n);
      } else if (t == typeof(Int32)) {
        return new BigInt32Array(n);
      } else if (t == typeof(UInt64)) {
        return new BigUInt64Array(n);
      } else if (t == typeof(Int64)) {
        return new BigInt64Array(n);
      } else if (t == typeof(Single)) {
        return new BigSingleArray(n);
      } else if (t == typeof(Double)) {
        return new BigDoubleArray(n);
      } else if (t == typeof(Decimal)) {
        return new BigDecimalArray(n);
      } else if (t == typeof(Char)) {
        return new BigCharArray(n);
      } else {
        // The implementation of the Big<T>Arrays above uses the "sizeof"
        // operator, which applies only to "unmanaged-types". Above are the
        // thirteen language-defined unmanaged types.
        return new WrappedArray(t, n);
      }
    }
    public abstract long Length {get; }
    public abstract object GetValue(long i);
    public abstract void SetValue(object o, long i);
  }

  public class WrappedArray : BigArray {
    private System.Array inner;

    public WrappedArray(Type t, long n) {
      this.inner = Array.CreateInstance(t, (int)n);
    }

    public override long Length {
      get {
        return inner.LongLength;
      }
    }

    public override object GetValue(long i) {
      return inner.GetValue(i);
    }

    public override void SetValue(object o, long i) {
      inner.SetValue(o, i);
    }
  }
}
