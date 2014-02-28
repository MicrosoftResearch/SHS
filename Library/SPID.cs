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
  internal struct SPID : IEquatable<SPID> {
    internal readonly Guid storeID;
    internal readonly int partID;

    internal SPID(Guid storeID, int partID) {
      this.storeID = storeID;
      this.partID = partID;
    }

    public override string ToString() {
      return string.Format("SPID(storeID={0:N},partID={2})", this.storeID, this.partID);
    }

    public override int GetHashCode() {
      return this.storeID.GetHashCode() ^ this.partID.GetHashCode();
    }

    public bool Equals(SPID that) {
      return this.storeID == that.storeID && this.partID == that.partID;
    }
  }
}
