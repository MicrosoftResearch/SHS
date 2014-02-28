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
using System.Diagnostics;
using System.IO;
using System.IO.Compression;

namespace SHS {
  internal class PageLinksFileEnumerator : IEnumerator<PageLinks> {
    private readonly string fileName;
    private BinaryReader rd;
    private PageLinks current;

    internal PageLinksFileEnumerator(string fileName) {
      this.fileName = fileName;
      this.rd = new BinaryReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
    }

    public PageLinks Current { get { return this.current; } }

    object IEnumerator.Current { get { return this.current; } }

    public bool MoveNext() {
      try {
        this.current.pageUrl = rd.ReadString();
        this.current.linkUrls = new string[rd.ReadInt32()];
        for (int i = 0; i < this.current.linkUrls.Length; i++) {
          this.current.linkUrls[i] = rd.ReadString();
        }
        return true;
      } catch (EndOfStreamException) {
        return false;
      }
    }

    public void Reset() {
      this.rd.Close();
      this.rd = new BinaryReader(new GZipStream(new BufferedStream(new FileStream(fileName, FileMode.Open, FileAccess.Read)), CompressionMode.Decompress));
    }

    public void Dispose() {
      this.rd.Close();
    }
  }

  class Builder {
    public static void Main(string[] args) {
      if (args.Length != 3) {
        Console.Error.WriteLine("Usage: SHS.Builder <leader> <linkfile.bin.gz> <friendly-name>");
        Environment.Exit(1);
      }
      var sw = Stopwatch.StartNew();
      var service = new Service(args[0]);
      var numServers = service.NumServers();
      Console.WriteLine("SHS service is currently running on {0} servers", numServers);
      var store = service.CreateStore(numServers-1, 2, args[2]);
      var plEnum = new PageLinksFileEnumerator(args[1]);
      store.AddPageLinks(plEnum);
      store.Seal();
      Console.Error.WriteLine("Done. Building store {0:N} took {1} seconds.", store.ID, 0.001 * sw.ElapsedMilliseconds);
    }
  }
}
