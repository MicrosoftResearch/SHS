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

namespace SHS {
  /* I am using a Mersenne Twister here. This could be replaced by a wrapper 
     around the standard C# random-number generator. */
  internal sealed class Random {
    private const int NN = 312;
    private const int MM = 156;
    private const ulong UM = 0xFFFFFFFF80000000UL; // Most significant 33 bits
    private const ulong LM = 0x000000007FFFFFFFUL; // Least significant 31 bits
    private static readonly ulong[] mag01 = {0UL, 0xB5026F5AA96619E9UL};

    // mt and mti are stateful, so calls to NextUInt64 must be synchronized
    private readonly ulong[] mt;
    private uint mti;

    internal Random() {
      mt = new ulong[NN];
      mt[0] = 5489UL;
      for (mti = 1; mti < NN; mti++) {
        mt[mti] = 6364136223846793005UL * (mt[mti-1] ^ (mt[mti-1] >> 62)) + (ulong)mti;
      }
    }

    /* returns a random number between 0 and 2^64-1 */
    internal ulong NextUInt64() {
      ulong x = 0;
      lock (this) {
        if (mti >= NN) {
          for (int i=0; i < NN-MM; i++) {
            x = (mt[i] & UM)|(mt[i+1] & LM);
            mt[i] = mt[i+MM] ^ (x>>1) ^ mag01[(int)(x & 1UL)];
          }
          for (int i = NN-MM; i < NN-1; i++) {
            x = (mt[i] & UM)|(mt[i+1] & LM);
            mt[i] = mt[i+(MM-NN)] ^ (x>>1) ^ mag01[(int)(x & 1UL)];
          }
          x = (mt[NN-1] & UM)|(mt[0] & LM);
          mt[NN-1] = mt[MM-1] ^ (x>>1) ^ mag01[(int)(x & 1UL)];

          mti = 0;
        }
        x = mt[mti++];
      }
      x ^= (x >> 29) & 0x5555555555555555UL;
      x ^= (x << 17) & 0x71D67FFFEDA60000UL;
      x ^= (x << 37) & 0xFFF7EEE000000000UL;
      x ^= (x >> 43);
      return x;
    }
  }
}
