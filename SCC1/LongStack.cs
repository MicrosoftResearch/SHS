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

using System.IO;
public class LongStack {
    private string baseName;
    private long[] buf;
    private int pos;
    private int exclHiFileId;

    public LongStack(int bufSz, string prefix) {
      this.baseName = prefix + "_" +  System.DateTime.Now.Ticks.ToString("X16");
      this.buf = new long[bufSz];
      this.pos = 0;
      this.exclHiFileId = 0;
    }

    private string Name(int id) {
      return baseName + "." + id.ToString("x8");
    }

    public void Push(long item) {
      if (pos >= buf.Length) {
        // Write buffer to disk and use newly freed buffer.
        using (var wr = new BinaryWriter(new BufferedStream(new FileStream(Name(exclHiFileId++), FileMode.Create, FileAccess.Write)))) {
          for (int i = 0; i < buf.Length; i++) {
            wr.Write(buf[i]);
          }
        }
        pos = 0;
      }
      buf[pos++] = item;
    }

    public long Pop() {
      if (pos == 0) {
        if (Empty) throw new System.Exception("Stack is empty");
        string name = Name(--exclHiFileId);
        using (var rd = new BinaryReader(new BufferedStream(new FileStream(name, FileMode.Open, FileAccess.Read)))) {
          for (int i = 0; i < buf.Length; i++) {
            buf[i] = rd.ReadInt64();
          }
        }
        System.IO.File.Delete(name);
        pos = buf.Length;
      }
      return buf[--pos];
    }

    public bool Empty {
      get {
        return pos == 0 && exclHiFileId == 0;
      }
    }
  }
