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
using System.IO.Compression;
using SHS;

class RegressionTest0 {
  static void Main(string[] args) {
    bool rfl = false;  // Can LinkDB contain reflexive edges?
    bool dup = false;  // Can LinkDB contain duplicate edges?
    if (args.Length == 4 && string.CompareOrdinal(args[0], "refdup") == 0) {
      rfl = true;
      dup = true;
    } else if (args.Length == 4 && string.CompareOrdinal(args[0], "refnodup") == 0) {
      rfl = true;
      dup = false;
    } else if (args.Length == 4 && string.CompareOrdinal(args[0], "dupnoref") == 0) {
      rfl = false;
      dup = true;
    } else if (args.Length == 4 && string.CompareOrdinal(args[0], "norefdup") == 0) {
      rfl = false;
      dup = false;
    } else {
      Console.Error.WriteLine("Usage: SHS.RegressionTest0 [refdup|refnodup|dupnoref|norefdup] <rawlinks.bin> <servers.txt> <store>");
      System.Environment.Exit(1);
    }
    var sw = System.Diagnostics.Stopwatch.StartNew();
    var smt = new ThriftySetMemberTest();
    var shs = new Service(args[2]).OpenStore(Guid.Parse(args[3]));
    long pageCnt = 0;
    foreach (var fileName in File.ReadAllLines(args[1])) {
      using (var rd = new BinaryReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress))) {
        while (true) {
          try {
            string pageUrl = rd.ReadString();
            long pageUid = shs.UrlToUid(pageUrl);
            string pageUidUrl = shs.UidToUrl(pageUid);
            if (string.CompareOrdinal(pageUrl, pageUidUrl) != 0) {
              Console.Error.WriteLine();
              Console.Error.WriteLine("Regression test failed for page {0} (self-test)", pageCnt);
              Console.Error.WriteLine("Original URL: {0}", pageUrl);
              Console.Error.WriteLine("UrlToUid UID: {0,16:X}", pageUid);
              Console.Error.WriteLine("UidToUrl URL: {0}", pageUidUrl);
              System.Environment.Exit(1);
            }
            int numLinks = rd.ReadInt32();
            var linkUrls = new string[numLinks];
            for (int i = 0; i < numLinks; i++) {
              linkUrls[i] = rd.ReadString();
            }
            if (!smt.ContainsOrAdd(pageUrl)) {
              // Ignore self-links
              var nonReflectiveL = new List<string>();
              for (int i = 0; i < numLinks; i++) {
                if (string.CompareOrdinal(pageUrl, linkUrls[i]) != 0) nonReflectiveL.Add(linkUrls[i]);
              }
              var nonReflectiveA = nonReflectiveL.ToArray();
              var linkUids = shs.BatchedUrlToUid(nonReflectiveA);
              var bwdLinkUids = shs.BatchedGetLinks(linkUids, Dir.Bwd);

              // Backward test first
              for (int i = 0; i < bwdLinkUids.Length; i++) {
                bool found = false;
                for (int j = 0; j < bwdLinkUids[i].Length && !found; j++) {
                  if (pageUid == bwdLinkUids[i][j]) found = true;
                }
                if (!found) {
                  var bwdLinkUrls = shs.BatchedUidToUrl(bwdLinkUids[i]);
                  Console.Error.WriteLine();
                  Console.Error.WriteLine("Regression test failed after {0} seconds for page {1} (bwd test)",
                                          0.001 * sw.ElapsedMilliseconds, pageCnt);
                  Console.Error.WriteLine("pageUrl={0}", pageUrl);
                  Console.Error.WriteLine("linkUrl[{0}]={1}", i, linkUrls[i]);
                  Console.Error.WriteLine("bwdLinkUrls.Length={0}", bwdLinkUrls.Length);
                  for (int j = 0; j < bwdLinkUrls.Length; j++) {
                    Console.Error.WriteLine("bwdLinkUrl[{0}]={1}", j, bwdLinkUrls[j]);
                  }
                  System.Environment.Exit(1);
                }
              }

              // Forward test second
              var fwdLinkUrls = shs.BatchedUidToUrl(shs.GetLinks(pageUid, Dir.Fwd));
              Array.Sort<string>(linkUrls, string.CompareOrdinal);
              Array.Sort<string>(fwdLinkUrls, string.CompareOrdinal);
              int a = 0;
              int b = 0;
              while (a < numLinks && b < fwdLinkUrls.Length) {
                // Skip any reflexive and/or duplicate links, if desired
                while ((!rfl && a < numLinks && string.CompareOrdinal(pageUrl, linkUrls[a]) == 0)
                       || (!dup && a < numLinks - 1 && string.CompareOrdinal(linkUrls[a], linkUrls[a + 1]) == 0)) {
                  a++;
                }
                if (a >= numLinks) {
                  break;
                }
                if (string.CompareOrdinal(linkUrls[a], fwdLinkUrls[b]) != 0) {
                  Console.Error.WriteLine();
                  Console.Error.WriteLine("Regression test failed for page {0} (fwd test1)",
                          pageCnt);
                  Console.Error.WriteLine("raw links file has {0} links:", numLinks);
                  for (int i = 0; i < numLinks; i++) {
                    Console.Error.WriteLine("  link[{0}]={1}", i, linkUrls[i]);
                  }
                  Console.Error.WriteLine("fwd db has {0} links:", fwdLinkUrls.Length);
                  for (int i = 0; i < fwdLinkUrls.Length; i++) {
                    Console.Error.WriteLine("  link[{0}]={1}", i, fwdLinkUrls[i]);
                  }
                  System.Environment.Exit(1);
                }
                a++;
                b++;
              }
              if (a < numLinks || b < fwdLinkUrls.Length) {
                Console.Error.WriteLine("\n");
                Console.Error.WriteLine("Regression test failed for page [0} (fwd test2)",
                        pageCnt);
                Console.Error.WriteLine("raw links file has [0} links:", numLinks);
                for (int i = 0; i < numLinks; i++) {
                  Console.Error.WriteLine("  link[{0}]={1}", i, linkUrls[i]);
                }
                Console.Error.WriteLine("fwd db has {0} links:", fwdLinkUrls.Length);
                for (int i = 0; i < fwdLinkUrls.Length; i++) {
                  Console.Error.WriteLine("  link[{0}]={1}", i, fwdLinkUrls[i]);
                }
                System.Environment.Exit(1);
              }
            }
            if ((++pageCnt % 10000) == 0) {
              Console.Error.WriteLine("\rProcessed {0} pages in {1:f1} seconds",
                                      pageCnt, 0.001 * sw.ElapsedMilliseconds);
            }
          } catch (EndOfStreamException) {
            break;
          }
        }
      }
    }
    Console.Error.WriteLine();
    Console.Error.WriteLine("Regression test completed; tested {0} pages in {1:f1} seconds",
                            pageCnt, 0.001 * sw.ElapsedMilliseconds);
  }
}
