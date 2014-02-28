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
using System.IO;
using System.IO.Compression;

namespace SHS {
  class ConvertCwGraphToShsInput {
    private static readonly char[] Sep = new char[]{' '};

    public static void Main(string[] args) {
      if (args.Length != 3) {
        Console.Error.WriteLine("Usage: ConvertCwGraphToShsInput <in-nodes.gz> <in-graph.gz> <out.bin.gz>");
      } else {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using (var lps = new DiskSorter<IdxIdx>(new IdxIdx.Comparer(), IdxIdx.Write, IdxIdx.Read, 1 << 25)) {
          using (var rd = new StreamReader(new GZipStream(new FileStream(args[1], FileMode.Open, FileAccess.Read), CompressionMode.Decompress))) {
            var numExpected = long.Parse(rd.ReadLine());
            if (numExpected == 4780950903) numExpected += 8;  // Hack: Deal with CW09 Cat A graph file being flawed
            Console.WriteLine("Expecting {0} body lines", numExpected);
            long srcIdx = 0;
            long numLinks = 0;
            for (; ; ) {
              var line = rd.ReadLine();
              if (line == null) {
                break;
              }
              var outLinks = line.Split(Sep, StringSplitOptions.RemoveEmptyEntries);
              var deg = outLinks.Length;
              for (int i = 0; i < deg; i++) {
                var link = long.Parse(outLinks[i]);
                if (link >= numExpected) throw new Exception(string.Format("Line {0} has out-of-range link {1}", srcIdx, link));
                lps.Add(new IdxIdx { srcIdx = srcIdx, dstIdx = link });
                numLinks++;
              }
              srcIdx++;
            }
            Console.Error.WriteLine("Read graph file, found {0} nodes and {1} edges.", srcIdx, numLinks);
          }
          lps.Sort();

          using (var lss = new DiskSorter<IdxUrl>(new IdxUrl.Comparer(), IdxUrl.Write, IdxUrl.Read, 1 << 20)) {
            // Next, read the nodes file and the destination-sorted graph file in lock-step, and write
            // it back out, having replaced the second index by the URL
            using (var nodeRd = new BinaryReader(new GZipStream(new FileStream(args[0], FileMode.Open, FileAccess.Read), CompressionMode.Decompress))) {
              long urlIdx = -1;
              string url = null;
              while (!lps.AtEnd()) {
                var pair = lps.Get();
                while (urlIdx < pair.dstIdx) {
                  var line = nodeRd.ReadLine();
                  if (line == null) throw new Exception(string.Format("Unexpected end of {0}", args[0]));
                  url = NormalizeUrlSchemeAndHost(line);
                  urlIdx++;
                }
                lss.Add(new IdxUrl { srcIdx = pair.srcIdx, dstUrl = url });
              }
            }
            lss.Sort();

            // Finally, read the nodes file and the destination-sorted graph file in lock-step, 
            // and write it back out in the format expected by ShsBuilder.
            using (var nodeRd = new BinaryReader(new GZipStream(new FileStream(args[0], FileMode.Open, FileAccess.Read), CompressionMode.Decompress))) {
              using (var wr = new BinaryWriter(new GZipStream(new FileStream(args[2], FileMode.OpenOrCreate, FileAccess.Write), CompressionMode.Compress))) {
                long urlIdx = 0;
                long lstIdx = -1;
                var list = new List<string>();
                while (!lss.AtEnd()) {
                  var pair = lss.Get();
                  if (lstIdx == -1) {
                    lstIdx = pair.srcIdx;
                  }
                  if (lstIdx == pair.srcIdx) {
                    list.Add(pair.dstUrl);
                  } else {
                    while (urlIdx <= lstIdx) {
                      var line = nodeRd.ReadLine();
                      if (line == null) throw new Exception(string.Format("Unexpected end of {0}", args[0]));
                      var srcUrl = NormalizeUrlSchemeAndHost(line);
                      wr.Write(srcUrl);
                      if (urlIdx != lstIdx) {
                        wr.Write(0);
                      } else {
                        wr.Write(list.Count);
                        foreach (var x in list) {
                          wr.Write(x);
                        }
                      }
                      urlIdx++;
                    }
                    list.Clear();
                    list.Add(pair.dstUrl);
                    lstIdx = pair.srcIdx;
                  }
                }
                while (true) {
                  var line = nodeRd.ReadLine();
                  if (line == null) break;
                  var srcUrl = NormalizeUrlSchemeAndHost(line);
                  wr.Write(srcUrl);
                  if (urlIdx != lstIdx) {
                    wr.Write(0);
                  } else {
                    wr.Write(list.Count);
                    foreach (var x in list) {
                      wr.Write(x);
                    }
                  }
                  urlIdx++;
                }
              }
            }
          }
        }
        Console.Error.WriteLine("Wrote out result file; starting sanity check.");
        using (var rd = new BinaryReader(new GZipStream(new FileStream(args[2], FileMode.Open, FileAccess.Read), CompressionMode.Decompress))) {
          long pageCnt = 0;
          long linkCnt = 0;
          for (; ; ) {
            try {
              var srcUrl = rd.ReadString();
              pageCnt++;
              var numLinks = rd.ReadInt32();
              for (int i = 0; i < numLinks; i++) {
                var dstUrl = rd.ReadString();
                linkCnt++;
              }
            } catch (EndOfStreamException) {
              break;
            }
          }
          Console.Error.WriteLine("Found {0} pages and {1} links. Job took {2} seconds.", pageCnt, linkCnt, 0.001 * sw.ElapsedMilliseconds);
        }
      }
    }

    public static string NormalizeUrlSchemeAndHost(string url) {
      int slashesSeen = 0;
      var res = new char[url.Length];
      for (int i = 0; i < res.Length; i++) {
        if (url[i] == '/') slashesSeen++;
        res[i] = slashesSeen > 2 ? url[i] : char.ToLowerInvariant(url[i]);
      }
      return new string(res);
    }
  }

  public static class ExtensionMethods {
    private static readonly Encoding enco = new UTF8Encoding();

    public static string ReadLine(this BinaryReader rd) {
      var list = new List<byte>();
      for (; ; ) {
        try {
          var b = rd.ReadByte();
          if (b == '\n') {
            return enco.GetString(list.ToArray());
          }
          list.Add(b);
        } catch (EndOfStreamException) {
          return null;
        }
      }
    }

    private static readonly string[] KnownUrlPrefixes = new string[]{"http://", "https://"};  // must be lower-cased

    public static bool KnownUrlPrefix(this string url) {
      var impossible = new bool[KnownUrlPrefixes.Length]; // auto-initialized to false
      var numPoss = impossible.Length;
      for (int i = 0; numPoss > 0 && i < url.Length; i++) {
        var c = char.ToLowerInvariant(url[i]);
        for (int j = 0; j < impossible.Length; j++) {
          if (!impossible[j]) {
            if (c != KnownUrlPrefixes[j][i]) {
              impossible[j] = true;
              numPoss--;
            } else if (i == KnownUrlPrefixes[j].Length - 1) {
              return true;
            }
          }
        }
      }
      return false;
    }
  }

  internal struct IdxIdx {
    internal long srcIdx;
    internal long dstIdx;

    internal static IdxIdx Read(BinaryReader rd) {
      var srcUid = rd.ReadInt64();
      var dstUid = rd.ReadInt64();
      return new IdxIdx { srcIdx = srcUid, dstIdx = dstUid };
    }

    internal static void Write(BinaryWriter wr, IdxIdx a) {
      wr.Write(a.srcIdx);
      wr.Write(a.dstIdx);
    }

    internal class Comparer : System.Collections.Generic.Comparer<IdxIdx> {
      public override int Compare(IdxIdx a, IdxIdx b) {
        if (a.dstIdx < b.dstIdx) {
          return -1;
        } else if (a.dstIdx > b.dstIdx) {
          return +1;
        } else if (a.srcIdx < b.srcIdx) {
          return -1;
        } else if (a.srcIdx > b.srcIdx) {
          return +1;
        } else {
          return 0;
        }
      }
    }
  }

  internal struct IdxUrl {
    internal long srcIdx;
    internal string dstUrl;

    internal static IdxUrl Read(BinaryReader rd) {
      long srcIdx = rd.ReadInt64();
      string dstUrl = rd.ReadString();
      return new IdxUrl { srcIdx = srcIdx, dstUrl = dstUrl };
    }

    internal static void Write(BinaryWriter wr, IdxUrl a) {
      wr.Write(a.srcIdx);
      wr.Write(a.dstUrl);
    }

    internal class Comparer : System.Collections.Generic.Comparer<IdxUrl> {
      public override int Compare(IdxUrl a, IdxUrl b) {
        if (a.srcIdx < b.srcIdx) {
          return -1;
        } else if (a.srcIdx > b.srcIdx) {
          return +1;
        } else {
          return string.CompareOrdinal(a.dstUrl, b.dstUrl);
        }
      }
    }
  }
}
