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

namespace SHS {
  public class UrlUtils {
    public static string InvertHost(string url) {
      int a = PrefixLength(url);
      int b = HostLength(url, a);
      return url.Substring(0, a)
        + HostUtils.InvertHost(url.Substring(a, b))
        + url.Substring(a+b);
    }

    public static string HostOf(string url) {
      int a = PrefixLength(url);
      int b = HostLength(url, a);
      return url.Substring(a, b);
    }

    public static string DomainOf(string url) {
      return HostUtils.DomainOf(UrlUtils.HostOf(url));
    }

    private static int PrefixLength(string url) {
      if (url.SafeStartsWith("http://")) {
        return 7;
      } else if (url.SafeStartsWith("https://")) {
        return 8;
      } else {
        throw new Exception("URL " + url + " does not start with http:// or https://");
      }
    }

    private static int HostLength(string url, int prefixLen) {
      int i = prefixLen;
      while (i < url.Length && url[i] != ':' && url[i] != '/') i++;
      return i - prefixLen;
    }
  }

  public static class ExtensionMethods {
    public static bool SafeStartsWith(this string full, string pref) {
      if (full.Length < pref.Length) return false;
      for (int i = 0; i < pref.Length; i++) {
        if (full[i] != pref[i]) return false;
      }
      return true;
    }
  }
}
