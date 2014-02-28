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
using System.IO;

namespace SHS {
  internal class AtomicStream : FileStream {
    private string name;

    internal AtomicStream(string name, FileMode mode, FileAccess access)
      : base(name + ".new", mode, access) 
    {
      this.name = name;
    }

    protected override void Dispose(bool disposing) {
      if (disposing) {
        base.Dispose(disposing);
        if (File.Exists(this.name)) {
          // Maybe File.Replace is atomic than doing two File.Move calls?
          File.Move(this.name, this.name + ".old");
          File.Move(this.name + ".new", this.name);
          File.Delete(this.name + ".old");
        } else {
          File.Move(this.name + ".new", this.name);
        }
      }
    }
  }
}
