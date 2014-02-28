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
using System.IO.Compression;
using System.Linq;
using SHS;

public class ASP {
  private static void ProcessBatch(Store shs, UidState<byte> dists, UidState<long> seeds, long[] uids, byte dist, Dir dir) {
    var uidSeeds = seeds.GetMany(uids);
    var nborUids = shs.BatchedGetLinks(uids, dir);
    var map = new UidMap(nborUids);
    var distChunk = dists.GetMany(map);
    var seedChunk = seeds.GetMany(map);
    for (int i = 0; i < nborUids.Length; i++) {
      for (int j = 0; j < nborUids[i].Length; j++) {
        int x = map[nborUids[i][j]];
        if (distChunk[x] > dist) {
          distChunk[x] = dist;
          seedChunk[x] = uidSeeds[i];
        }
      }
    }
    dists.SetMany(map, distChunk);
    seeds.SetMany(map, seedChunk);
  }
  public static void Main(string[] args) {
    if (args.Length != 3) {
      Console.Error.WriteLine("Usage: SHS.ASP <leader> <store> [f|b|u]");
    } else {
      var sw = Stopwatch.StartNew();
      var shs = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
      var dists = shs.AllocateUidState<byte>();
      var seeds = shs.AllocateUidState<long>();
      var cands = shs.AllocateUidState<bool>();
      var batch = new Batch<long>(1000000);
      long numCands = 0;
      bool fwd = args[2] == "f" || args[2] == "u";
      bool bwd = args[2] == "b" || args[2] == "u";
      foreach (long u in shs.Uids()) {
        batch.Add(u);
        if (batch.Full || shs.IsLastUid(u)) {
          int[] fwdDegs = shs.BatchedGetDegree(batch, Dir.Fwd);
          int[] bwdDegs = shs.BatchedGetDegree(batch, Dir.Bwd);
          bool[] isCands = new bool[batch.Count];
          for (int i = 0; i < batch.Count; i++) {
            isCands[i] = fwd && bwd ? fwdDegs[i] + bwdDegs[i] > 1 : fwdDegs[i] > 0 && bwdDegs[i] > 0;
            if (isCands[i]) numCands++;
          }
          cands.SetMany(batch, isCands);
          batch.Reset();
        }
      }
      System.Random rand = new System.Random(12345);
      int dim = 0;
      batch = new Batch<long>(1000);
      for (; (long)1 << dim <= numCands; dim++) {
        long numSeeds = (long)1 << dim;
        dists.SetAll(x => 0xff);
        seeds.SetAll(x => -1);
        double remainingSamps = numSeeds;
        double remainingCands = numCands;
        foreach (var uc in cands.GetAll()) {
          if (uc.val) {
            if (rand.NextDouble() < remainingSamps / remainingCands) {
              batch.Add(uc.uid);
              remainingSamps--;
            }
            remainingCands--;
          }
          if (batch.Full || shs.IsLastUid(uc.uid)) {
            dists.SetMany(batch, ((long[])batch).Select(x => (byte)0).ToArray());
            seeds.SetMany(batch, batch);
            batch.Reset();
          }
        }
        for (byte k = 0; k < 0xff; k++) {
          long hits = 0;
          foreach (var x in dists.GetAll()) {
            if (x.val == k) {
              batch.Add(x.uid);
              hits++;
            }
            if (batch.Full || shs.IsLastUid(x.uid)) {
              if (bwd) ProcessBatch(shs, dists, seeds, batch, (byte)(k + 1), Dir.Fwd);
              if (fwd) ProcessBatch(shs, dists, seeds, batch, (byte)(k + 1), Dir.Bwd);
              batch.Reset();
            }
          }
          if (hits == 0) break;
        }
        using (var wr = new BinaryWriter(new GZipStream(new BufferedStream(new FileStream("sketchslice-" + args[2] + "-" + dim.ToString("d2") + ".bin", FileMode.Create, FileAccess.Write)), CompressionMode.Compress))) {
          long rch = 0;  // Number of reachable URls
          foreach (var x in dists.GetAll().Zip(seeds.GetAll(), (d, s) => System.Tuple.Create(d, s))) {
            if (x.Item1.val < 0xff) rch++;
            wr.Write(x.Item1.val);
            wr.Write(x.Item2.val);
          }
        }
      }
      using (var wr = new BinaryWriter(new GZipStream(new BufferedStream(new FileStream("sketches-" + args[2] + ".bin", FileMode.Create, FileAccess.Write)), CompressionMode.Compress))) {
        wr.Write(dim);
        var readers = new BinaryReader[dim];
        for (int i = 0; i < dim; i++) {
          readers[i] = new BinaryReader(new GZipStream(new BufferedStream(new FileStream("sketchslice-" + args[2] + "-" + i.ToString("d2") + ".bin", FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
        }
        while (true) {
          try {
            for (int i = 0; i < dim; i++) {
              wr.Write(readers[i].ReadByte());
              wr.Write(readers[i].ReadInt64());
            }
          } catch (EndOfStreamException) {
            break;
          }
        }
        for (int i = 0; i < dim; i++) {
          readers[i].Close();
        }
      }
      Console.WriteLine("Done. Job took {0} seconds.", 0.001 * sw.ElapsedMilliseconds);
    }
  }
}
