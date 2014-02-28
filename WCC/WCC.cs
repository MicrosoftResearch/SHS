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

public class WCC {
  private static long[] GetRoots(UidState<long> roots, long[] uids) {
    var reprs = roots.GetMany(uids);
    var map = new UidMap();
    bool recur = false;
    for (int i = 0; i < uids.Length; i++) {
      if (reprs[i] != uids[i]) {
        map.Add(reprs[i]);
        recur = true;
      }
    }
    if (recur) {
      var change = false;
      var reprClos = GetRoots(roots, map);
      for (int i = 0; i < uids.Length; i++) {
        if (reprs[i] != uids[i]) {
          var rep = reprClos[map[reprs[i]]];
          if (reprs[i] != rep) {
            reprs[i] = rep;
            change = true;
          }
        }
      }
      if (change) roots.SetMany(uids, reprs);
    }
    return reprs;
  }

  internal static UidVal<long> Read(BinaryReader rd) {
    var uid = rd.ReadInt64();
    var val = rd.ReadInt64();
    return new UidVal<long>(uid, val);
  }

  internal static void Write(BinaryWriter wr, UidVal<long> uv) {
    wr.Write(uv.uid);
    wr.Write(uv.val);
  }

  internal class Comparer : System.Collections.Generic.Comparer<UidVal<long>> {
    public override int Compare(UidVal<long> a, UidVal<long> b) {
      if (a.val < b.val) {
        return -1;
      } else if (a.val > b.val) {
        return +1;
      } else if (a.uid < b.uid) {
        return -1;
      } else if (a.uid > b.uid) {
        return +1;
      } else {
        return 0;
      }
    }
  }

  public static void Main(string[] args) {
    if (args.Length != 2) {
      Console.Error.WriteLine("Usage: SHS.WCC <leader> <store>");
    } else {
      var sw = Stopwatch.StartNew();
      var store = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
      var roots = store.AllocateUidState<long>();
      roots.SetAll(x => x);
      var batch = new Batch<long>(10000);
      foreach (long u in store.Uids()) {
        batch.Add(u);
        if (batch.Full || store.IsLastUid(u)) {
          long[] uids = batch;
          long[][] fwds = store.BatchedGetLinks(uids, Dir.Fwd);
          var map = new UidMap(fwds);
          map.Add(uids);
          var xRoots = GetRoots(roots, map);
          for (int i = 0; i < fwds.Length; i++) {
            uids[i] = xRoots[map[uids[i]]];
            for (int j = 0; j < fwds[i].Length; j++) {
              fwds[i][j] = xRoots[map[fwds[i][j]]];
            }
          }
          map = new UidMap(fwds);
          map.Add(uids);
          long[] reprs = roots.GetMany(map);
          for (int i = 0; i < fwds.Length; i++) {
            long A = uids[i];
            long a = map[A];
            while (A != reprs[a]) {
              A = reprs[a];
              a = map[A];
            }
            for (int j = 0; j < fwds[i].Length; j++) {
              long B = fwds[i][j];
              long b = map[B];
              while (B != reprs[b]) {
                B = reprs[b];
                b = map[B];
              }
              if (reprs[a] < reprs[b]) {
                reprs[b] = reprs[a];
              } else {
                reprs[a] = reprs[b];
                a = b;
              }
            }
          }
          roots.SetMany(map, reprs);
          batch.Reset();
        }
      }
      batch = new Batch<long>(400000);
      foreach (long u in store.Uids()) {
        batch.Add(u);
        if (batch.Full || store.IsLastUid(u)) {
          GetRoots(roots, batch);
          batch.Reset();
        }
      }
      using (var sorter = new DiskSorter<UidVal<long>>(new Comparer(), Write, Read, 100000000)) {
        foreach (var uv in roots.GetAll()) sorter.Add(uv);
        sorter.Sort();
        using (var wccWr = new BinaryWriter(new BufferedStream(new FileStream("wcc-main.bin", FileMode.Create, FileAccess.Write)))) {
          using (var idxWr = new BinaryWriter(new BufferedStream(new FileStream("wcc-index.bin", FileMode.Create, FileAccess.Write)))) {
            long last = 0;
            long lastRoot = -1;
            for (long i = 0; i < sorter.Total; i++) {
              var uv = sorter.Get();
              wccWr.Write(uv.uid);
              if (i == 0) {
                lastRoot = uv.val;
              } else if (uv.val != lastRoot) {
                idxWr.Write(i - last);
                idxWr.Write(last);
                last = i;
                lastRoot = uv.val;
              }
            }
            Debug.Assert(sorter.AtEnd());
            if (sorter.Total > 0) {
              idxWr.Write(sorter.Total - last);
              idxWr.Write(last);
            }
          }
        }
      }
      var dict = new System.Collections.Generic.Dictionary<long, long>();
      using (var rd = new BinaryReader(new BufferedStream(new FileStream("wcc-index.bin", FileMode.Open, FileAccess.Read)))) {
        while (true) {
          try {
            long size = rd.ReadInt64();
            long pos = rd.ReadInt64();
            if (!dict.ContainsKey(size)) dict[size] = 0;
            dict[size]++;
          } catch (EndOfStreamException) {
            break;
          }
        }
      }
      long maxSize = 0;
      long numWCCs = 0;
      foreach (var kv in dict) {
        if (kv.Key > maxSize) maxSize = kv.Key;
        numWCCs += kv.Value;
      }
      Console.WriteLine("Done. {0} weakly connected components, largest has {1} nodes. Job took {2} seconds.", numWCCs, maxSize, 0.001 * sw.ElapsedMilliseconds);
    }
  }
}