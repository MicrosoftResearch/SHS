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

public class SCC2 {
  private class Frame {
    internal Frame parent;
    internal int id;
    internal uint ctr;
    internal Frame(Frame parent, int id, uint[] pastLast) {
      this.parent = parent;
      this.id = id;
      this.ctr = id == 0 ? 0 : pastLast[id - 1];
    }
  }
  private static long[] Flatten(long[][] nbors) {
    int c = 0;
    for (int i = 0; i < nbors.Length; i++) {
      c += nbors[i].Length;
    }
    long[] res = new long[c];
    int p = 0;
    for (int i = 0; i < nbors.Length; i++) {
      for (int j = 0; j < nbors[i].Length; j++) {
        res[p++] = nbors[i][j];
      }
    }
    return res;
  }
  public static void Main(string[] args) {
    if (args.Length != 2) {
      Console.Error.WriteLine("Usage: SHS.SCC2 <leader> <store>");
    } else {
      var sw = Stopwatch.StartNew();
      var shs = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
      var map = shs.AllocateUidState<int>();  // Mapping from UID to local ID
      int numVerts = 0;  // Number of core vertices
      var batch = new Batch<long>(500000);
      foreach (long u in shs.Uids()) {
        batch.Add(u);
        if (batch.Full || shs.IsLastUid(u)) {
          int[] fwdDegs = shs.BatchedGetDegree(batch, Dir.Fwd);
          int[] bwdDegs = shs.BatchedGetDegree(batch, Dir.Bwd);
          var mapChunk = new int[batch.Count];
          for (int i = 0; i < batch.Count; i++) {
            mapChunk[i] = fwdDegs[i] == 0 || bwdDegs[i] == 0 ? -1 : numVerts++;
          }
          map.SetMany(batch, mapChunk);
          batch.Reset();
        }
      }
      uint numEdges = 0;
      foreach (var up in map.GetAll()) {
        if (up.val != -1) batch.Add(up.uid);
        if (batch.Full || shs.IsLastUid(up.uid)) {
          long[][] nbors = shs.BatchedGetLinks(batch, Dir.Fwd);
          int[] mappedNbors = map.GetMany(Flatten(nbors));
          int q = 0;
          for (int i = 0; i < nbors.Length; i++) {
            for (int j = 0; j < nbors[i].Length; j++) {
              if (mappedNbors[q++] != -1) numEdges++;
            }
          }
          batch.Reset();
        }
      }
      uint[] pastLast = new uint[numVerts]; // one past last link of that page
      var links = new int[numEdges];
      int p = 0;
      uint r = 0;
      foreach (var up in map.GetAll()) {
        if (up.val != -1) batch.Add(up.uid);
        if (batch.Full || shs.IsLastUid(up.uid)) {
          long[][] nbors = shs.BatchedGetLinks(batch, Dir.Fwd);
          int[] mappedNbors = map.GetMany(Flatten(nbors));
          int q = 0;
          for (int i = 0; i < nbors.Length; i++) {
            for (int j = 0; j < nbors[i].Length; j++) {
              int id = mappedNbors[q++];
              if (id != -1) links[r++] = id;
            }
            pastLast[p++] = r;
          }
          batch.Reset();
        }
      }
      var bv = new BitVector(numVerts);  // All false at creation
      int[] stk = new int[numVerts];
      int stkPtr = stk.Length;
      for (int u = 0; u < numVerts; u++) {
        if (!bv[u]) {
          bv[u] = true;
          Frame frame = new Frame(null, u, pastLast);
          while (frame != null) {
            while (frame.ctr < pastLast[frame.id]) {
              int v = links[frame.ctr++];
              if (!bv[v]) {
                bv[v] = true;
                frame = new Frame(frame, v, pastLast);
              }
            }
            stk[--stkPtr] = frame.id;
            frame = frame.parent;
          }
        }
      }
      p = 0;
      r = 0;
      foreach (var up in map.GetAll()) {
        if (up.val != -1) batch.Add(up.uid);
        if (batch.Full || shs.IsLastUid(up.uid)) {
          long[][] nbors = shs.BatchedGetLinks(batch, Dir.Bwd);
          int[] mappedNbors = map.GetMany(Flatten(nbors));
          int q = 0;
          for (int i = 0; i < nbors.Length; i++) {
            for (int j = 0; j < nbors[i].Length; j++) {
              int id = mappedNbors[q++];
              if (id != -1) links[r++] = id;
            }
            pastLast[p++] = r;
          }
          batch.Reset();
        }
      }
      var pam = new long[numVerts];
      p = 0;
      foreach (var up in map.GetAll()) {
        if (up.val != -1) pam[p++] = up.uid;
      }
      using (var sccWr = new BinaryWriter(new BufferedStream(new FileStream("scc-main.bin", FileMode.Create, FileAccess.Write)))) {
        using (var idxWr = new BinaryWriter(new BufferedStream(new FileStream("scc-index.bin", FileMode.Create, FileAccess.Write)))) {
          long sccPos = 0;
          bv.SetAll(false);
          for (int i = 0; i < stk.Length; i++) {
            int u = stk[i];
            if (!bv[u]) {
              long sccSize = 0;
              bv[u] = true;
              Frame frame = new Frame(null, u, pastLast);
              while (frame != null) {
                while (frame.ctr < pastLast[frame.id]) {
                  int v = links[frame.ctr++];
                  if (!bv[v]) {
                    bv[v] = true;
                    frame = new Frame(frame, v, pastLast);
                  }
                }
                sccWr.Write(pam[frame.id]);
                sccSize++;
                frame = frame.parent;
              }
              idxWr.Write(sccSize);
              idxWr.Write(sccPos);
              sccPos += sccSize;
            }
          }
          foreach (var up in map.GetAll()) {
            if (up.val == -1) {
              sccWr.Write(up.uid);
              idxWr.Write(1L);
              idxWr.Write(sccPos++);
            }
          }
        }
      }
      var dict = new System.Collections.Generic.Dictionary<long, long>();
      using (var ib = new BinaryReader(new BufferedStream(new FileStream("scc-index.bin", FileMode.Open, FileAccess.Read)))) {
        while (true) {
          try {
            long size = ib.ReadInt64();
            long pos = ib.ReadInt64();
            if (!dict.ContainsKey(size)) dict[size] = 0;
            dict[size]++;
          } catch (EndOfStreamException) {
            break;
          }
        }
      }
      long maxSize = 0;
      long numSCCs = 0;
      foreach (var kv in dict) {
        if (kv.Key > maxSize) maxSize = kv.Key;
        numSCCs += kv.Value;
      }
      Console.WriteLine("Done. Job took {0} seconds.", 0.001 * sw.ElapsedMilliseconds);
    }
  }
}