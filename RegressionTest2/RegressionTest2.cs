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
using System.Linq;

using SHS;

namespace RegressionTest2 {
  class RegressionTest2 {
    static void Main(string[] args) {
      if (args.Length != 3) {
        Console.Error.WriteLine("Usage: SHS.RegressionTest2 <leader> <numPages> <numIterations>");
      } else {
        int numPages = int.Parse(args[1]);
        int numIters = int.Parse(args[2]);
        var sw = System.Diagnostics.Stopwatch.StartNew();
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
        Console.Error.WriteLine("Synthesized {0} URLs ({1} duplicates)", pages.Length, pages.Length - pages.Distinct().Count());

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
            plBatch.Add(new PL { src = src, dsts = list });
          }
          var pageLinksBatch = plBatch.Select(x => new PageLinks{pageUrl = pages[x.src], linkUrls = x.dsts.Select(y => pages[y]).ToArray()}).ToList();

          store.AddPageLinks(pageLinksBatch.GetEnumerator());
          store.MarkAtomic();

          var srcUrls = plBatch.Select(x => pages[x.src]).ToArray();
          var srcUids = store.BatchedUrlToUid(srcUrls);
          var fwdLinkUids = store.BatchedGetLinks(srcUids, Dir.Fwd);
          for (int i = 0; i < fwdLinkUids.Length; i++) {
            var fwdLinkUrlsR = store.BatchedUidToUrl(fwdLinkUids[i]);
            var fwdLinkUrlsL = fwds[plBatch[i].src].Select(x => pages[x]).ToArray();
            AssertSameSets(fwdLinkUrlsR, fwdLinkUrlsL);
            var bwdLinkUids = store.BatchedGetLinks(fwdLinkUids[i], Dir.Bwd);
            for (int j = 0; j < bwdLinkUids.Length; j++) {
              var bwdLinkUrlsR = store.BatchedUidToUrl(bwdLinkUids[j]);
              var bwdLinkUrlsL = bwds[Idx(fwdLinkUrlsR[j], pages, plBatch[i].dsts)].Select(x => pages[x]).ToArray();
              AssertSameSets(bwdLinkUrlsR, bwdLinkUrlsL);
            }
          }
          Console.Error.WriteLine("Iteration {0}: Put {1} PageLinks into store, Adding {2} and deleting {3} links. Validation passed!", iter, batchSize, addCtr, delCtr);
        }

        Console.WriteLine("Done. RegressionTest2 took {0} seconds", 0.001 * sw.ElapsedMilliseconds);
      }
    }

    private static int Idx(string url, string[] pages, List<int> cands) {
      foreach (var x in cands) {
        if (pages[x] == url) return x;
      }
      throw new Exception();
    }

    private static void AssertSameSets(string[] set1, string[] set2) {
      set1 = (string[])set1.Clone();
      set2 = (string[])set2.Clone();
      if (set1.Length != set2.Length) {
        for (int i = 0; i < set1.Length; i++) {
          Console.Error.WriteLine("   set1[{0}]={1}", i, set1[i]);
        }
        for (int i = 0; i < set2.Length; i++) {
          Console.Error.WriteLine("   set2[{0}]={1}", i, set2[i]);
        }
        throw new Exception(string.Format("Assertion failed; len1={0} len2={1}", set1.Length, set2.Length));
      }
      Array.Sort(set1);
      Array.Sort(set2);
      for (int i = 0; i < set1.Length; i++) {
        if (set1[i] != set2[i]) throw new Exception("Assertion failed");
      }
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
