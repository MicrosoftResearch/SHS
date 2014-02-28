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
using System.Net.Sockets;
using System.Threading;

namespace SHS {
  /// <summary>
  /// This class provides the client-side API for creating, listing, opening, 
  /// and deleting SHS stores.
  /// </summary>
  public class Service {
    internal const int PortNumber = 4100;

    internal readonly string leaderName;

    public Service(string leaderName) {
      this.leaderName = leaderName;
    }

    public int NumServers() {
      while (true) {
        try {
          var leader = new Channel(this.leaderName, Service.PortNumber);
          leader.WriteUInt32((uint)OpCodes.NumAvailableServers);
          leader.Flush();
          var res = leader.ReadInt32();
          leader.Close();
          if (res == -2) throw new Exception(string.Format("{0} not the SHS service leader", this.leaderName));
          return res;
        } catch (SocketException) {
          Thread.Sleep(5000); // sleep and then try again
        }
      }
    }

    public StoreInfo[] ListStores() {
      while (true) {
        try {
          var leader = new Channel(this.leaderName, Service.PortNumber);
          leader.WriteUInt32((uint)OpCodes.ListStores);
          leader.Flush();
          int n = leader.ReadInt32();
          var res = new StoreInfo[n];
          for (int i = 0; i < n; i++) {
            var storeID = new Guid(leader.ReadBytes());
            var friendlyName = leader.ReadString();
            var numPartitions = leader.ReadInt32();
            var numReplicas = leader.ReadInt32();
            var numPartitionBits = leader.ReadInt32();
            var numRelativeBits = leader.ReadInt32();
            var isSealed = leader.ReadBoolean();
            var isAvailable = leader.ReadBoolean();
            res[i] = new StoreInfo(storeID, friendlyName, numPartitions, numReplicas, numPartitionBits, numRelativeBits, isSealed, isAvailable);
          }
          leader.Close();
          return res;
        } catch (SocketException) {
          Thread.Sleep(5000); // sleep and then try again
        }
      }
    }

    internal Channel LeaderChannel {
      get { return new Channel(this.leaderName, Service.PortNumber); } 
    }

    public Store CreateStore() {
      return this.CreateStore(this.NumServers() - 1, 2);
    }

    public Store CreateStore(int numPartitions,
                             int numReplicas,
                             string friendlyName = "<unnamed store>", 
                             int numPartitionBits = 4,
                             int numRelativeBits = 32) 
    {
      Contract.Requires(numPartitions > 0);
      Contract.Requires(numReplicas > 0);
      // Make sure that uidNumPartitionBits and uidNumRelativeBits are in range
      if (numPartitions <= 0) {
        throw new ArgumentOutOfRangeException("numPartitions must be at least 1");
      } else if (numReplicas <= 0) {
        throw new ArgumentOutOfRangeException("numReplicas must be at least 1");
      } else if (numPartitionBits <= 0 || numPartitionBits > 32) {
        throw new ArgumentOutOfRangeException("uidNumPartitionBits must be between 1 and 31");
      } else if ((1L << numPartitionBits) < numPartitions) {
        throw new ArgumentOutOfRangeException("uidNumPartitionBits too small to encode numPartitions");
      } else if (numRelativeBits <= 0) {
        throw new ArgumentOutOfRangeException("uidNumRelativeBits must be positive");
      } else if (numPartitionBits + numRelativeBits > 61) {
        throw new ArgumentOutOfRangeException("uidNumPartitionBits + uidNumRelativeBits must be <= 61");
      }
      var storeID = Guid.NewGuid();
      while (true) {
        try {
          var leader = new Channel(this.leaderName, Service.PortNumber);
          leader.WriteUInt32((uint)OpCodes.CreateStore);
          leader.WriteBytes(storeID.ToByteArray());
          leader.WriteString(friendlyName);
          leader.WriteInt32(numPartitionBits);
          leader.WriteInt32(numRelativeBits);
          leader.WriteInt32(numPartitions);
          leader.WriteInt32(numReplicas);
          leader.Flush();
          var resp = leader.ReadInt32();
          leader.Close();
          if (resp == -2) {
            throw new Exception(string.Format("{0} not the SHS service leader", this.leaderName));
          } else if (resp == -1) {
            throw new SocketException();  // try again
          } else {
            Contract.Assert(resp == 0);
            return new Store(this, storeID, new StoreInfo(storeID, friendlyName, numPartitions, numReplicas, numPartitionBits, numRelativeBits, false, true));
          }
        } catch (SocketException) {
          Thread.Sleep(5000); // sleep and then try again
        }
      }
    }

    public Store OpenStore(Guid storeID) {
      return new Store(this, storeID, PingStore(storeID));
    }

    private StoreInfo PingStore(Guid storeID) {
      while (true) {
        try {
          var leader = this.LeaderChannel;
          leader.WriteUInt32((uint)OpCodes.StatStore);
          leader.WriteBytes(storeID.ToByteArray());
          leader.Flush();

          var resp = leader.ReadInt32();
          if (resp == -2) {
            leader.Close();
            throw new Exception(string.Format("{0} not the SHS service leader", this.leaderName));
          } else if (resp == -3) {
            leader.Close();
            throw new Exception(string.Format("{0:N} unknown to SHS service leader", storeID));
          } else {
            Contract.Assert(resp == 0);
            var friendlyName = leader.ReadString();
            var numPartitions = leader.ReadInt32();
            var numReplicas = leader.ReadInt32();
            var numPartitionBits = leader.ReadInt32();
            var numRelativeBits = leader.ReadInt32();
            var isSealed = leader.ReadBoolean();
            var isAvailable = leader.ReadBoolean();
            leader.Close();
            return new StoreInfo(storeID, friendlyName, numPartitions, numReplicas, numPartitionBits, numRelativeBits, isSealed, isAvailable);

          }
        } catch (SocketException) {
          Thread.Sleep(5000); // sleep and then try again
        }
      }
    }

    public void DeleteStore(Guid storeID) {
      while (true) {
        try {
          var leader = new Channel(this.leaderName, Service.PortNumber);
          leader.WriteUInt32((uint)OpCodes.DeleteStore);
          leader.WriteBytes(storeID.ToByteArray());
          leader.Flush();
          var resp = leader.ReadInt32();
          leader.Close();
          if (resp == -2) {
            throw new Exception(string.Format("{0} not the SHS service leader", this.leaderName));
          } else if (resp == -3) {
            throw new Exception(string.Format("Store {0:N} unknown to SHS service leader", storeID));
          } else if (resp == -1) {
            throw new Exception(string.Format("Store {0:N} is currently open", storeID));
          } else {
            Contract.Assert(resp == 0);
          }
          break;
        } catch (SocketException) {
          Thread.Sleep(5000); // sleep and then try again
        }
      }
    }
  }
}
