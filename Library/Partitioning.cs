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
  internal class Partitioning {
    private static readonly Hash64 hasher = new Hash64();

    internal readonly int numPartitions;
    internal readonly int numReplicas;
    internal readonly int numPartitionBits;  // only non-private use is writing to file or channel
    internal readonly int numRelativeBits;   // only non-private use is writing to file or channel
    private readonly int partMask;
    private readonly long puidMask;

    internal Partitioning(int numPartitions, int numReplicas, int numPartitionBits, int numRelativeBits) {
      this.numPartitions = numPartitions;
      this.numReplicas = numReplicas;
      this.numPartitionBits = numPartitionBits;
      this.numRelativeBits = numRelativeBits;
      this.partMask = (1 << numPartitionBits) - 1;
      this.puidMask = (1L << numRelativeBits) - 1;
    }

    internal int PartitionID(string url) {
      return (int)(hasher.Hash(UrlUtils.HostOf(url)) % (ulong)this.numPartitions);
    }

    internal int PartitionID(long uid) {
      return (int)(uid >> this.numRelativeBits) & this.partMask;
    }

    internal long PUID(long uid) {
      return uid & this.puidMask;
    }

    internal long MakeUID(int partID, long puid) {
      return ((long)partID << this.numRelativeBits) | puid;
    }

    // Assume a store is configured to have 4 partitions (numPartitions=4), each 
    // replicated 3 times (numReplicas=3), and therefore requires 6 servers
    // (servers.Length=4+3-1=6).  The following diagram illustrates how the 
    // 4 partitions are spread across the 6 servers.  All indices are zero-based;
    // the horizontal coordinates identify servers (i.e. each server is a column
    // in the table), the vertical coordinates identify partitions, and an X 
    // indicates that a server holds a replica of a partition (i.e. each row 
    // has 3 X's):
    // 
    //                        0  1  2  3  4  5
    //                     0  X  X  X
    //                     1     X  X  X  
    //                     2        X  X  X
    //                     3           X  X  X
    //
    // Formally, partition p is on servers { p+r | 0 <= r < numReplicas } 
    // (there is no modulus-wraparound), and server s holds partitions 
    // { s-r | 0 <= r < numReplicas && 0 <= s-r < numPartitions)

    internal IEnumerable<int> ServersOfPartition(int partID) {
      for (int i = 0; i < this.numReplicas; i++) {
        yield return partID + i;
      }
    }

    internal IEnumerable<int> PartitionsOnServer(int serverID) {
      for (int i = 0; i < this.numReplicas; i++) {
        int partID = serverID - i;
        if (partID >= 0 && partID < this.numPartitions) {
          yield return partID;
        }
      }
    }
  }
}
