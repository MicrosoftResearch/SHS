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
using System.Linq;
using System.Text.RegularExpressions;

namespace SHS {
  internal class TempFileNames {
    private readonly List<string> names;

    internal static void CleanAll() {
      var tmpFileRegex = new Regex(@"^tmp-([\dabcdef]{32}).shs$", RegexOptions.IgnoreCase);
      var tmpFiles = Directory.GetFiles(Directory.GetCurrentDirectory())
                              .Select(x => Path.GetFileName(x))
                              .Where(x => tmpFileRegex.IsMatch(x))
                              .ToList();
      foreach (var x in tmpFiles) {
        File.Delete(x);
      }
    }

    internal TempFileNames() {
      this.names = new List<string>();
    }

    internal void CleanThis() {
      foreach (var name in this.names) {
        if (File.Exists(name)) {
          File.Delete(name);
        }
      }
      this.names.Clear();
    }

    internal string New() {
      var name = string.Format("tmp-{0:N}.shs", Guid.NewGuid());
      names.Add(name);
      return name;
    }
  }
}
