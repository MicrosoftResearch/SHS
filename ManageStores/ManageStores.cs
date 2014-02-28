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
using System.Text;

namespace SHS {
  class ManageStores {
    static void Main(string[] args) {
      if (args.Length == 2 && args[1] == "list") {
        foreach (var si in new Service(args[0]).ListStores()) {
          Console.WriteLine("{0:N} {1,3} {2,2} {3,2} {4,2} {5} {6} {7}", 
            si.StoreID, 
            si.NumPartitions, 
            si.NumReplicas, 
            si.NumPartitionBits,
            si.NumRelativeBits,
            si.IsSealed ? "S" : "O",
            si.IsAvailable ? "+" : "-",
            si.FriendlyName);
        }
      } else if (args.Length == 3 && args[1] == "delete") {
        new Service(args[0]).DeleteStore(Guid.Parse(args[2]));
      } else {
        Console.WriteLine("Usage: SHS.ManageStores <leader> <command>");
        Console.WriteLine("where command can be:");
        Console.WriteLine("    list");
        Console.WriteLine("    delete <storename>");
      }
    }
  }
}
