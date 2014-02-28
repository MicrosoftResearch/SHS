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

public class PageRankFT {
  public static void Main(string[] args) {
    if (args.Length != 4) {
      Console.Error.WriteLine("Usage: SHS.PageRankFT <leader> <store> <d> <iters>");
    } else {
      var sw = Stopwatch.StartNew();
      var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));

      Action<Action> Checkpointed = delegate(Action checkpointedBlock) {
        while (true) {
          try {
            checkpointedBlock();
            store.Checkpoint();
            break;
          } catch (ServerFailure) {
            Console.Error.WriteLine("Restarting from checkpoint");
            // go again
          }
        }
      };

      double d = double.Parse(args[2]);
      int numIters = int.Parse(args[3]);
      long n = store.NumUrls();

      UidState<double> oldScores = null, newScores = null;

      Checkpointed(delegate() {
        newScores = store.AllocateUidState<double>();
        oldScores = store.AllocateUidState<double>();
        oldScores.SetAll(uid => 1.0 / n);
      });

      for (int k = 0; k < numIters; k++) {
        Checkpointed(delegate() {
          var uidBatch = new Batch<long>(50000);
          newScores.SetAll(x => d / n);
          foreach (long u in store.Uids()) {
            uidBatch.Add(u);
            if (uidBatch.Full || store.IsLastUid(u)) {
              var linkBatch = store.BatchedGetLinks(uidBatch, Dir.Fwd);
              var newMap = new UidMap(linkBatch);
              var oldSc = oldScores.GetMany(uidBatch);
              var newSc = newScores.GetMany(newMap);
              for (int i = 0; i < uidBatch.Count; i++) {
                var links = linkBatch[i];
                double f = (1.0 - d) * oldSc[i] / links.Length;
                foreach (var link in links) {
                  newSc[newMap[link]] += f;
                }
              }
              newScores.SetMany(newMap, newSc);
              uidBatch.Reset();
            }
          }
        });
        var tmp = newScores; newScores = oldScores; oldScores = tmp;
        Console.WriteLine("Done with iteration {0}", k);
      }
      using (var wr = new BinaryWriter(new BufferedStream(new FileStream("pr-scores.bin", FileMode.Create, FileAccess.Write)))) {
        foreach (var us in newScores.GetAll()) wr.Write(us.val);
      }
      Console.WriteLine("Done. {0} iterations took {1} seconds.", numIters, 0.001 * sw.ElapsedMilliseconds);
    }
  }
}
