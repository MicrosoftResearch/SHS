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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.IO.Compression;
using System.Diagnostics;

using SHS;

namespace RegressionTest1 {
  class RegressionTest1 {
    static void Main(string[] args) {
      if (args.Length != 2) {
        Console.Error.WriteLine("SHS.RegressionTest1 <leader> <linkfiles.txt>");
      } else {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var svc = new Service(args[0]);
        Console.WriteLine("Service currently provides {0} servers", svc.NumServers());
        var store = svc.CreateStore();
        Console.WriteLine("Created new store with GUID {0:N}", store.ID);
        var linkFiles = File.ReadAllLines(args[1]);
        for (int i = 0; i < linkFiles.Length; i++) {
          store.AddPageLinks(new PageLinksFileEnumerator(Slice(linkFiles, i, 1)));
          store.MarkAtomic();
          Console.WriteLine("Added batch {0}. Current store has {1} URLs, {2} links, {3} max in-degree, {4} max out-degree", 
                            i, store.NumUrls(), store.NumLinks(), store.MaxDegree(Dir.Bwd), store.MaxDegree(Dir.Fwd));
          ValidateStore(store, new PageLinksFileEnumerator(Slice(linkFiles, 0, i+1)));
        }
        Console.WriteLine("Deleting store {0:N}", store.ID);
        store.Close();
        svc.DeleteStore(store.ID);
        Console.WriteLine("Done. RegressionTest1 took {0} seconds", 0.001 * sw.ElapsedMilliseconds);
      }
    }

    private static void ValidateStore(Store store, PageLinksFileEnumerator plEnum) {
      var sw = Stopwatch.StartNew();
      long pageCnt = 0;
      while (plEnum.MoveNext()) {
        pageCnt++;
        var pl = plEnum.Current;
        string pageUrl = pl.pageUrl;
        long pageUid = store.UrlToUid(pageUrl);
        string pageUidUrl = store.UidToUrl(pageUid);
        if (string.CompareOrdinal(pageUrl, pageUidUrl) != 0) {
          Console.Error.WriteLine();
          Console.Error.WriteLine("Regression test failed for page {0} (self-test)", pageCnt);
          Console.Error.WriteLine("Original URL: {0}", pageUrl);
          Console.Error.WriteLine("UrlToUid UID: {0,16:X}", pageUid);
          Console.Error.WriteLine("UidToUrl URL: {0}", pageUidUrl);
          System.Environment.Exit(1);
        }
        int numLinks = pl.linkUrls.Length;
        // Ignore self-links
        var nonReflectiveL = new List<string>();
        for (int j = 0; j < numLinks; j++) {
          if (string.CompareOrdinal(pageUrl, pl.linkUrls[j]) != 0) nonReflectiveL.Add(pl.linkUrls[j]);
        }
        var nonReflectiveA = nonReflectiveL.ToArray();
        var linkUids = store.BatchedUrlToUid(nonReflectiveA);
        var bwdLinkUids = store.BatchedGetLinks(linkUids, Dir.Bwd);

        // Backward test first
        for (int j = 0; j < bwdLinkUids.Length; j++) {
          bool found = false;
          for (int k = 0; k < bwdLinkUids[j].Length && !found; k++) {
            if (pageUid == bwdLinkUids[j][k]) found = true;
          }
          if (!found) {
            var bwdLinkUrls = store.BatchedUidToUrl(bwdLinkUids[j]);
            Console.Error.WriteLine();
            Console.Error.WriteLine("Regression test failed after {0} seconds for page {1} (bwd test)",
                                    0.001 * sw.ElapsedMilliseconds, pageCnt);
            Console.Error.WriteLine("page: UID={0:X} URL={1}", pageUid, pageUrl);
            Console.Error.WriteLine("link[{0}]: UID={1:X} URL={2}", j, linkUids[j], pl.linkUrls[j]);
            Console.Error.WriteLine("bwdLinkUids[{0}].Length={1}", j, bwdLinkUrls.Length);
            for (int k = 0; k < bwdLinkUrls.Length; k++) {
              Console.Error.WriteLine("bwdLink[{0}]: UID={1:X} URL={2}", k, bwdLinkUids[j][k], bwdLinkUrls[k]);
            }
            System.Environment.Exit(1);
          }
        }

        // Forward test second
        var fwdLinkUrls = store.BatchedUidToUrl(store.GetLinks(pageUid, Dir.Fwd));
        Array.Sort<string>(pl.linkUrls, string.CompareOrdinal);
        Array.Sort<string>(fwdLinkUrls, string.CompareOrdinal);
        int a = 0;
        int b = 0;
        while (a < numLinks && b < fwdLinkUrls.Length) {
          while ((a < numLinks && string.CompareOrdinal(pageUrl, pl.linkUrls[a]) == 0)
                 || (a < numLinks - 1 && string.CompareOrdinal(pl.linkUrls[a], pl.linkUrls[a + 1]) == 0)) {
            a++;
          }
          if (a >= numLinks) {
            break;
          }
          if (string.CompareOrdinal(pl.linkUrls[a], fwdLinkUrls[b]) != 0) {
            Console.Error.WriteLine();
            Console.Error.WriteLine("Regression test (fwd test1) failed for page {0} uid={1,16:x} urlBytes={2}", pageCnt, pageUid, pl.pageUrl);
            Console.Error.WriteLine("raw links file has {0} links:", numLinks);
            for (int j = 0; j < numLinks; j++) {
              Console.Error.WriteLine("  link[{0}]={1}", j, pl.linkUrls[j]);
            }
            Console.Error.WriteLine("fwd db has {0} links:", fwdLinkUrls.Length);
            for (int j = 0; j < fwdLinkUrls.Length; j++) {
              Console.Error.WriteLine("  link[{0}]={1}", j, fwdLinkUrls[j]);
            }
            System.Environment.Exit(1);
          }
          a++;
          b++;
        }
        if (a < numLinks || b < fwdLinkUrls.Length) {
          Console.Error.WriteLine("Regression test (fwd test2) failed for page {0} uid={1,16:x} urlBytes={2}", pageCnt, pageUid, pl.pageUrl);
          Console.Error.WriteLine("raw links file has {0} links:", numLinks);
          for (int j = 0; j < numLinks; j++) {
            Console.Error.WriteLine("  link[{0}]={1}", j, pl.linkUrls[j]);
          }
          Console.Error.WriteLine("fwd db has {0} links:", fwdLinkUrls.Length);
          for (int j = 0; j < fwdLinkUrls.Length; j++) {
            Console.Error.WriteLine("  link[{0}]={1}", j, fwdLinkUrls[j]);
          }
          System.Environment.Exit(1);
        }
      }
      Console.WriteLine("Validated {0} PageLink records against store, taking {1} seconds", pageCnt, 0.001 * sw.ElapsedMilliseconds);
    }

    private static T[] Slice<T>(T[] a, int start, int len) {
      var res = new T[len];
      Array.Copy(a, start, res, 0, len);
      return res;
    }
  }

  internal class PageLinksFileEnumerator : IEnumerator<PageLinks> {
    private readonly string[] fileList;
    private int nextFile;
    private BinaryReader rd;
    private PageLinks current;

    internal PageLinksFileEnumerator(string[] fileList) {
      this.fileList = fileList;
      this.nextFile = 0;
      this.rd = null;
    }

    public PageLinks Current { get { return this.current; } }

    object IEnumerator.Current { get { return this.current; } }

    public bool MoveNext() {
      while (true) {
        try {
          if (this.rd == null) {
            if (this.nextFile < this.fileList.Length) {
              this.rd = new BinaryReader(new GZipStream(new BufferedStream(new FileStream(this.fileList[this.nextFile++], FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
            } else {
              return false;
            }
          }
          this.current.pageUrl = rd.ReadString();
          this.current.linkUrls = new string[rd.ReadInt32()];
          for (int i = 0; i < this.current.linkUrls.Length; i++) {
            this.current.linkUrls[i] = rd.ReadString();
          }
          return true;
        } catch (EndOfStreamException) {
          this.rd.Close();
          this.rd = null;
        }
      }
    }

    public void Reset() {
      if (this.rd != null) {
        this.rd.Close();
        this.rd = null;
      }
      this.nextFile = 0;
    }

    public void Dispose() {
      this.rd.Close();
    }
  }
}
