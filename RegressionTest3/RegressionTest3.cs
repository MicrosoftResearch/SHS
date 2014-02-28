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
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using SHS;

namespace RegressionTest3 {
  class RegressionTest3 {
    static void Main(string[] args) {
      if (args.Length != 4) {
        Console.Error.WriteLine("Usage: SHS.RegressionTest3 <servers.txt> <numPages> <numIterations> <numValThreads>");
      } else {
        int numPages = int.Parse(args[1]);
        int numIters = int.Parse(args[2]);
        int numValidators = int.Parse(args[3]);
        var sw = Stopwatch.StartNew();
        var svc = new Service(args[0]);
        Console.WriteLine("Service currently provides {0} servers", svc.NumServers());
        var store = svc.CreateStore();
        Console.WriteLine("Created new store with GUID {0:N}", store.ID);
        
        // Create a validation graph with "numPages" vertices and (for now) no edges
        var rand = new Random(123456);
        var pages = new string[numPages];
        var fwds = new List<int>[numPages];
        var bwds = new List<int>[numPages];
        for (int i = 0; i < pages.Length; i++) {
          var r = rand.Next();
          pages[i] = string.Format("http://www.{0:D2}.com/{1}", rand.Next(1, 100), RandomString(rand));
          fwds[i] = new List<int>();
          bwds[i] = new List<int>();
        }
        Array.Sort(pages);
        Console.WriteLine("Synthesized {0} URLs ({1} duplicates)", pages.Length, pages.Length - pages.Distinct().Count());

        for (int iter = 0; iter < numIters; iter++) {
          var batchSize = rand.Next(10, 50);
          var plBatch = new List<PL>();
          long addCtr = 0;
          long delCtr = 0;

          while (plBatch.Count < batchSize) {
            // Pick a page
            var src = rand.Next(0, pages.Length);
            if (plBatch.Exists(x => x.src == src)) continue;
            var list = fwds[src];
            foreach (var dst in list) {
              bwds[dst].Remove(src);
            }
            if (list.Count == 0) {
              // If this page has no links, create between 20 and 50 links, with bias towards the "neighborhood"
              var numAdds = rand.Next(20, 51);
              while (numAdds > 0) {
                var dst = (int)RandomNormal(rand, src, 100);
                if (dst >= 0 && dst < pages.Length && !list.Contains(dst) && dst != src) {
                  list.Add(dst);
                  addCtr++;
                  numAdds--;
                }
              }
            } else {
              // Otherwise, choose about half of the links to delete, and add about the same number of new links
              var dels = list.Where(x => rand.Next(0, 2) == 0).ToList();
              delCtr += dels.Count;
              var numAdds = rand.Next(dels.Count - 3, dels.Count + 4);
              while (numAdds > 0) {
                var dst = (int)RandomNormal(rand, src, 100);
                if (dst >= 0 && dst < pages.Length && !list.Contains(dst) && dst != src) {
                  list.Add(dst);
                  addCtr++;
                  numAdds--;
                }
              }
              list = list.Except(dels).ToList();
            }
            foreach (var dst in list) {
              bwds[dst].Add(src);
            }
            fwds[src] = list;
            plBatch.Add(new PL { src = src, dsts = CloneList(list) });
          }
          var pageLinksBatch = plBatch.Select(x => new PageLinks { pageUrl = pages[x.src], linkUrls = x.dsts.Select(y => pages[y]).ToArray() }).ToList();

          int epoch = store.AddPageLinks(pageLinksBatch.GetEnumerator());
          //store.MarkAtomic();

          var snapFwds = CloneLists(fwds);
          var snapBwds = CloneLists(bwds);
          for (int i = 0; i < numValidators; i++) {
            var vs = new ValidatorState(args[0], store.ID, epoch, plBatch, pages, snapFwds, snapBwds);
            new Thread(vs.DoWork).Start();
          }

          var srcUrls = plBatch.Select(x => pages[x.src]).ToArray();
          var srcUids = store.BatchedUrlToUid(srcUrls, ref epoch);
          var fwdLinkUids = store.BatchedGetLinks(srcUids, Dir.Fwd, ref epoch);
          for (int i = 0; i < fwdLinkUids.Length; i++) {
            var fwdLinkUrlsR = store.BatchedUidToUrl(fwdLinkUids[i], ref epoch);
            var fwdLinkUrlsL = fwds[plBatch[i].src].Select(x => pages[x]).ToArray();
            if (!SameSets(fwdLinkUrlsR, fwdLinkUrlsL)) {
              lock (Console.Out) {
                Console.WriteLine("Detected inconsistenty! srcURL[{0}]={1}", i, srcUrls[i]);
                Console.WriteLine("{0} fwd link URLs according to SHS", fwdLinkUrlsR.Length);
                for (int k = 0; k < fwdLinkUrlsR.Length; k++) {
                  Console.WriteLine("  fwdLinkUrlsR[{0}]={1}", k, fwdLinkUrlsR[k]);
                }
                Console.WriteLine("{0} fwd link URLs according to local state", fwdLinkUrlsL.Length);
                for (int k = 0; k < fwdLinkUrlsL.Length; k++) {
                  Console.WriteLine("  fwdLinkUrlsL[{0}]={1}", k, fwdLinkUrlsL[k]);
                }
              }
              throw new Exception();
            }
            var bwdLinkUids = store.BatchedGetLinks(fwdLinkUids[i], Dir.Bwd, ref epoch);
            for (int j = 0; j < bwdLinkUids.Length; j++) {
              var bwdLinkUrlsR = store.BatchedUidToUrl(bwdLinkUids[j], ref epoch);
              var bwdLinkUrlsL = bwds[Idx(fwdLinkUrlsR[j], pages, plBatch[i].dsts)].Select(x => pages[x]).ToArray();
              if (!SameSets(bwdLinkUrlsR, bwdLinkUrlsL)) {
                lock (Console.Out) {
                  Console.WriteLine("Detected inconsistenty!");
                  Console.WriteLine("  srcURL[{0}]={1}", i, srcUrls[i]);
                  Console.WriteLine("  dstURL[{0}]={1}", j, fwdLinkUrlsR[j]);
                  Console.WriteLine("{0} bwd link URLs according to SHS", bwdLinkUrlsR.Length);
                  for (int k = 0; k < bwdLinkUrlsR.Length; k++) {
                    Console.WriteLine("  bwdLinkUrlsR[{0}]={1}", k, bwdLinkUrlsR[k]);
                  }
                  Console.WriteLine("{0} bwd link URLs according to local state", bwdLinkUrlsL.Length);
                  for (int k = 0; k < bwdLinkUrlsL.Length; k++) {
                    Console.WriteLine("  bwdLinkUrlsL[{0}]={1}", k, bwdLinkUrlsL[k]);
                  }
                }
                throw new Exception();
              }
            }
          }
          Console.WriteLine("Iteration {0}: Put {1} PageLinks into store, Adding {2} and deleting {3} links. Validation passed!", iter, batchSize, addCtr, delCtr);
        }

        Console.WriteLine("{0} of {1} non-mutating validation threads were exempted, validated {2} of graph on average",
          counters.numEpochPassed, counters.numChecks, counters.sumFractionChecked / counters.numChecks);
        Console.WriteLine("Done. RegressionTest3 took {0} seconds", 0.001 * sw.ElapsedMilliseconds);
      }
    }

    private static List<int>[] CloneLists(List<int>[] lists) {
      var res = new List<int>[lists.Length];
      for (int i = 0; i < lists.Length; i++) {
        res[i] = CloneList(lists[i]);
      }
      return res;
    }

    private static List<int> CloneList(List<int> list) {
      var res = new List<int>(list.Count);
      for (int i = 0; i < list.Count; i++) {
        res.Add(list[i]);
      }
      return res;
    }

    private class Counters {
      internal double sumFractionChecked = 0;
      internal long numChecks = 0;
      internal long numEpochPassed = 0;
    }

    private static readonly Counters counters = new Counters();

    private class ValidatorState {
      private string serviceName;
      private Guid storeId;
      private int epoch;
      private List<PL> plBatch;
      private string[] pages;
      private List<int>[] fwds;
      private List<int>[] bwds;

      internal ValidatorState(string serviceName, Guid storeId, int epoch, List<PL> plBatch, string[] pages, List<int>[] fwds, List<int>[] bwds) {
        this.serviceName = serviceName;
        this.storeId = storeId;
        this.epoch = epoch;
        this.plBatch = plBatch;
        this.pages = pages;
        this.fwds = fwds;
        this.bwds = bwds;
      }

      internal void DoWork() {
        double fractionChecked = 0.0;
        try {
          var shs = new Service(serviceName);
          var store = shs.OpenStore(storeId);
          var srcUrls = plBatch.Select(x => pages[x.src]).ToArray();
          var srcUids = store.BatchedUrlToUid(srcUrls, ref epoch);
          var fwdLinkUids = store.BatchedGetLinks(srcUids, Dir.Fwd, ref epoch);
          double eps = 1.0 / fwdLinkUids.Length;
          for (int i = 0; i < fwdLinkUids.Length; i++) {
            var fwdLinkUrlsR = store.BatchedUidToUrl(fwdLinkUids[i], ref epoch);
            var fwdLinkUrlsL = fwds[plBatch[i].src].Select(x => pages[x]).ToArray();
            if (!SameSets(fwdLinkUrlsR, fwdLinkUrlsL)) {
              lock (Console.Out) {
                Console.WriteLine("Detected inconsistenty! srcURL[{0}]={1}", i, srcUrls[i]);
                Console.WriteLine("{0} fwd link URLs according to SHS", fwdLinkUrlsR.Length);
                for (int k = 0; k < fwdLinkUrlsR.Length; k++) {
                  Console.WriteLine("  fwdLinkUrlsR[{0}]={1}", k, fwdLinkUrlsR[k]);
                }
                Console.WriteLine("{0} fwd link URLs according to local state", fwdLinkUrlsL.Length);
                for (int k = 0; k < fwdLinkUrlsL.Length; k++) {
                  Console.WriteLine("  fwdLinkUrlsL[{0}]={1}", k, fwdLinkUrlsL[k]);
                }
              }
              throw new Exception();
            }
            var bwdLinkUids = store.BatchedGetLinks(fwdLinkUids[i], Dir.Bwd, ref epoch);
            for (int j = 0; j < bwdLinkUids.Length; j++) {
              var bwdLinkUrlsR = store.BatchedUidToUrl(bwdLinkUids[j], ref epoch);
              var bwdLinkUrlsL = bwds[Idx(fwdLinkUrlsR[j], pages, plBatch[i].dsts)].Select(x => pages[x]).ToArray();
              if (!SameSets(bwdLinkUrlsR, bwdLinkUrlsL)) {
                lock (Console.Out) {
                  Console.WriteLine("Detected inconsistenty!");
                  Console.WriteLine("  srcURL[{0}]={1}", i, srcUrls[i]);
                  Console.WriteLine("  dstURL[{0}]={1}", j, fwdLinkUrlsR[j]);
                  Console.WriteLine("{0} bwd link URLs according to SHS", bwdLinkUrlsR.Length);
                  for (int k = 0; k < bwdLinkUrlsR.Length; k++) {
                    Console.WriteLine("  bwdLinkUrlsR[{0}]={1}", k, bwdLinkUrlsR[k]);
                  }
                  Console.WriteLine("{0} bwd link URLs according to local state", bwdLinkUrlsL.Length);
                  for (int k = 0; k < bwdLinkUrlsL.Length; k++) {
                    Console.WriteLine("  bwdLinkUrlsL[{0}]={1}", k, bwdLinkUrlsL[k]);
                  }
                }
                throw new Exception();
              }
            }
            fractionChecked += eps;
          }
        } catch (EpochPassed) {
          lock (counters) {
            counters.numEpochPassed++;
          }
        } finally {
          lock (counters) {
            counters.sumFractionChecked += fractionChecked;
            counters.numChecks++;
          }
        }
      }
    }

    private static int Idx(string url, string[] pages, List<int> cands) {
      foreach (var x in cands) {
        if (pages[x] == url) return x;
      }
      throw new Exception();
    }

    private static bool SameSets(string[] set1, string[] set2) {
      set1 = (string[])set1.Clone();
      set2 = (string[])set2.Clone();
      if (set1.Length != set2.Length) {
        return false;
      }
      Array.Sort(set1);
      Array.Sort(set2);
      for (int i = 0; i < set1.Length; i++) {
        if (set1[i] != set2[i]) return false;
      }
      return true;
    }

    private class PL {
      internal int src;
      internal List<int> dsts;
    }

    private static string RandomString(Random rand) {
      var len = rand.Next(5, 10);
      var res = new char[len];
      for (int i = 0; i < len; i++) {
        res[i] = (char)('a' + rand.Next(0, 26));
      }
      return new string(res);
    }

    private static double RandomNormal(Random rand, double mean, double stdDev) {
      return mean + stdDev * Math.Sqrt(-2.0 * Math.Log(rand.NextDouble())) * Math.Sin(2.0 * Math.PI * rand.NextDouble());
    }
  }
}
