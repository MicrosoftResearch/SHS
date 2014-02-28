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
  public class StoreInfo {
    public readonly Guid StoreID;
    public readonly string FriendlyName;
    public readonly int NumPartitions;
    public readonly int NumReplicas;
    public readonly int NumPartitionBits;
    public readonly int NumRelativeBits;
    public readonly bool IsSealed;
    public readonly bool IsAvailable;

    internal StoreInfo(
      Guid storeID,
      string friendlyName,
      int numPartitions,
      int numReplicas,
      int numPartitionBits,
      int numRelativeBits,
      bool isSealed,
      bool isAvailable) 
    {
      this.StoreID = storeID;
      this.FriendlyName = friendlyName;
      this.NumPartitions = numPartitions;
      this.NumReplicas = numReplicas;
      this.NumPartitionBits = numPartitionBits;
      this.NumRelativeBits = numRelativeBits;
      this.IsSealed = isSealed;
      this.IsAvailable = isAvailable;
    }
  }
}
