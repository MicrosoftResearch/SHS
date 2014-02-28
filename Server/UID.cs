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

namespace SHS {
  internal static class UID {
    private const int UidDeletedBit = 62;

    [Pure]
    internal static bool HasDeletedBit(long uid) {
      return (uid & (1L << UidDeletedBit)) != 0;
    }

    [Pure]
    internal static long SetDeletedBit(long uid) {
      return uid | (1L << UidDeletedBit);
    }

    [Pure]
    internal static long ClrDeletedBit(long uid) {
      return uid & ~(1L << UidDeletedBit);
    }

    [Pure]
    internal static int CompareUidsForSort(long uid1, long uid2) {
      return ClrDeletedBit(uid1).CompareTo(ClrDeletedBit(uid2));
    }

    [Pure]
    internal static bool LinksAreSorted(List<long> uids) {
      for (int i = 1; i < uids.Count; i++) {
        if (UID.ClrDeletedBit(uids[i - 1]) >= UID.ClrDeletedBit(uids[i])) return false;
      }
      return true;
    }

    [Pure]
    internal static bool NoLinksAreDeleted(List<long> uids) {
      for (int i = 0; i < uids.Count; i++) {
        if (UID.HasDeletedBit(uids[i])) return false;
      }
      return true;
    }
  }
}
