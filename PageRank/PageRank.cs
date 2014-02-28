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
using System.Diagnostics;
using System.IO;
using SHS;

public class ShsPageRank {
  public static void Main(string[] args) {
    if (args.Length != 4) {
      Console.Error.WriteLine("Usage: SHS.PageRank <leader> <store> <d> <iters>");
    } else {
      var sw = Stopwatch.StartNew();
      var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
      double d = double.Parse(args[2]);
      int numIters = int.Parse(args[3]);
      long n = store.NumUrls();
      using (var wr = new BinaryWriter(new BufferedStream(new FileStream("pr-scores-" + 0 + ".bin", FileMode.Create, FileAccess.Write)))) {
        for (long i = 0; i < n; i++) wr.Write(1.0 / n);
      }
      var scores = store.AllocateUidState<double>();
      var uidBatch = new Batch<long>(50000);
      for (int k = 0; k < numIters; k++) {
        scores.SetAll(x => d / n);
        using (var rd = new BinaryReader(new BufferedStream(new FileStream("pr-scores-" + k + ".bin", FileMode.Open, FileAccess.Read)))) {
          foreach (long u in store.Uids()) {
            uidBatch.Add(u);
            if (uidBatch.Full || store.IsLastUid(u)) {
              var linkBatch = store.BatchedGetLinks(uidBatch, Dir.Fwd);
              var uniqLinks = new UidMap(linkBatch);
              var scoreArr = scores.GetMany(uniqLinks);
              foreach (var links in linkBatch) {
                double f = (1.0 - d) * rd.ReadDouble() / links.Length;
                foreach (var link in links) {
                  scoreArr[uniqLinks[link]] += f;
                }
              }
              scores.SetMany(uniqLinks, scoreArr);
              uidBatch.Reset();
            }
          }
        }
        using (var wr = new BinaryWriter(new BufferedStream(new FileStream("pr-scores-" + (k + 1) + ".bin", FileMode.Create, FileAccess.Write)))) {
          foreach (var us in scores.GetAll()) wr.Write(us.val);
        }
        File.Delete("pr-scores-" + k + ".bin");
        Console.WriteLine("Iteration {0} complete", k);
      }
      Console.WriteLine("Done. {0} iterations took {1} seconds.", numIters, 0.001 * sw.ElapsedMilliseconds);
    }
  }
}
