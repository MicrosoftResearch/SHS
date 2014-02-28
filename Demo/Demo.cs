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
  public class Demo {
    private static void List(Int64 uid, Store store, int dist) {
      if (dist == 0) {
        Console.WriteLine(store.UidToUrl(uid));
      } else {
        Int64[] uids = store.GetLinks(uid, dist > 0 ? Dir.Fwd : Dir.Bwd);
        for (int i = 0; i < uids.Length; i++) {
          List(uids[i], store, dist > 0 ? dist - 1 : dist + 1);
        }
      }
    }

    public static void Main(string[] args) {
      if (args.Length != 4) {
        Console.Error.WriteLine("Usage: SHS.Demo <servers.txt> <store> <dist> <urlBytes>");
      } else {
        int dist = Int32.Parse(args[2]);
        var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
        long uid = store.UrlToUid(args[3]);
        if (uid == -1) {
          Console.Error.WriteLine("URL {0} not in store", args[3]);
        } else {
          List(uid, store, dist);
        }
      }
    }
  }
}
