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
using System.Diagnostics.Contracts;
using System.Collections.Generic;

namespace SHS {
  public abstract class UidState {
    protected readonly Store shs;
    protected readonly int handle;
    internal readonly Type type;
    protected readonly Serializer serial;
    internal bool valid;

    protected UidState(Store shs, int handle, Type type) {
      this.shs = shs;
      this.handle = handle;
      this.type = type;
      this.serial = SerializerFactory.Make(type);
      this.valid = true;
    }
    public void Dispose() {
      if (!this.valid) throw new Exception("Invalid UidState");
      shs.FreeUidState(handle);
      this.valid = false;
    }
  }
  public class UidState<T> : UidState {

    internal UidState(Store shs, int handle) : base(shs, handle, typeof(T)) {
      Contract.Requires(shs != null);
    }

    [ContractInvariantMethod]
    private void ObjectInvariant() {
      Contract.Invariant(this.shs != null);
      Contract.Invariant(this.serial != null);
    }

    public T Get(long uid) {
      if (!this.valid) throw new Exception("Invalid UidState");
      return shs.GetUidState<T>(handle, serial, uid);
    }

    public void Set(long uid, T val) {
      if (!this.valid) throw new Exception("Invalid UidState");
      shs.SetUidState<T>(handle, serial, uid, val);
    }

    public T[] GetMany(long[] uids) {
      if (!this.valid) throw new Exception("Invalid UidState");
      return shs.GetManyUidState<T>(handle, serial, uids);
    }

    public void SetMany(long[] uids, T[] vals) {
      if (!this.valid) throw new Exception("Invalid UidState");
      shs.SetManyUidState<T>(handle, serial, uids, vals);
    }

    public IEnumerable<UidVal<T>> GetAll() {
      if (!this.valid) throw new Exception("Invalid UidState");
      return shs.GetAllUidState<T>(handle, serial);
    }

    public void SetAll(Func<long,T> f) {
      if (!this.valid) throw new Exception("Invalid UidState");
      shs.SetAllUidState<T>(handle, serial, f);
    }
  }

  public struct UidVal<T> {
    public readonly long uid;
    public readonly T val;

    public UidVal(long uid, T val) {
      this.uid = uid;
      this.val = val;
    }
  }
}
