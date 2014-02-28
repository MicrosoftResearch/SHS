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
using System.IO;
using SHS;

public class Salsa {
  public static void Main(string[] args) {
    var shs = new Service(args[0]).OpenStore(Guid.Parse(args[1]));
    using (var rd = new BinaryReader(new BufferedStream(new FileStream(args[2], FileMode.Open, FileAccess.Read)))) {
      int bs = int.Parse(args[3]);
      int fs = int.Parse(args[4]);
      while (true) {
        try {
          int queryId = rd.ReadInt32();
          int numUrls = rd.ReadInt32();
          var urls = new string[numUrls];
          for (int i = 0; i < numUrls; i++) urls[i] = rd.ReadString();
          var uids = shs.BatchedUrlToUid(urls);
          var tbl = new UidMap(uids);
          var bwdUids = shs.BatchedSampleLinks(tbl, Dir.Bwd, bs, true);
          var fwdUids = shs.BatchedSampleLinks(tbl, Dir.Fwd, fs, true);
          foreach (long[] x in bwdUids) tbl.Add(x);
          foreach (long[] x in fwdUids) tbl.Add(x);
          long[] srcUids = tbl;
          var dstUids = shs.BatchedGetLinks(srcUids, Dir.Fwd);
          int n = dstUids.Length;
          var srcId = new List<int>[n];
          var dstId = new List<int>[n];
          for (int i = 0; i < n; i++) {
            srcId[i] = new List<int>();
            dstId[i] = new List<int>();
          }
          for (int i = 0; i < n; i++) {
            int sid = tbl[srcUids[i]];
            for (int j = 0; j < dstUids[i].Length; j++) {
              int did = tbl[dstUids[i][j]];
              if (did != -1) {
                srcId[sid].Add(did);
                dstId[did].Add(sid);
              }
            }
          }
          int numAuts = 0;
          for (int i = 0; i < n; i++) {
            if (dstId[i].Count > 0) numAuts++;
          }
          double initAut = 1.0 / numAuts;
          var aut = new double[n];
          var tmp = new double[n];
          for (int i = 0; i < n; i++) {
            aut[i] = dstId[i].Count > 0 ? initAut : 0.0;
          }
          for (int k = 0; k < 100; k++) {
            for (int u = 0; u < n; u++) {
              foreach (var id in dstId[u]) {
                tmp[id] += (aut[u] / dstId[u].Count);
              }
              aut[u] = 0.0;
            }
            for (int u = 0; u < n; u++) {
              foreach (var id in srcId[u]) {
                aut[id] += (tmp[u] / srcId[u].Count);
              }
              tmp[u] = 0.0;
            }
          }
          var scores = new double[urls.Length];
          for (int i = 0; i < scores.Length; i++) {
            scores[i] = uids[i] == -1 ? 0.0 : aut[tbl[uids[i]]];
          }
          double bestScore = double.MinValue;
          string bestUrl = null;
          for (int i = 0; i < urls.Length; i++) {
            if (scores[i] > bestScore) {
              bestScore = scores[i];
              bestUrl = urls[i];
            }
          }
          System.Console.Error.WriteLine("{0} {1}", queryId, bestUrl);
        } catch (EndOfStreamException) {
          break;
        }
      }
    }
  }
}
