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
  public class Hash64 {
    private readonly UInt64   primPoly;
    private readonly UInt64[] bmt;

    public Hash64() : this(0x911498AE0E66BAD6) {}

    public Hash64(UInt64 primPoly) {
      this.primPoly = primPoly;
      this.bmt = new UInt64[256];
      UInt64 t = this.primPoly;
      this.bmt[0] = 0;
      for (int i = 0x80; i != 0; i >>= 1) {
        this.bmt[i] = t;
        t = (t >> 1) ^ (((t & 1) != 0) ? this.primPoly : 0);
      }
      for (int i = 1; i < 256; i <<= 1) {
        for (int j = 1; j < i; j++) {
          this.bmt[i ^ j] = this.bmt[i] ^ this.bmt[j];
        }
      }
    }

    public UInt64 Hash(string s) {
      UInt64 fp = this.primPoly;
      var bytes = System.Text.Encoding.UTF8.GetBytes(s);
      for (int i = 0; i < bytes.Length; i++) {
        fp = (fp >> 8) ^ this.bmt[(bytes[i] ^ fp) & 0xffUL];
      }
      return fp;
    }

    public UInt64 Hash(byte[] bytes) {
      UInt64 fp = this.primPoly;
      for (int i = 0; i < bytes.Length; i++) {
        fp = (fp >> 8) ^ this.bmt[(((int)bytes[i]) ^ ((int)fp)) & 0xff];
      }
      return fp;
    }

    public static readonly UInt64[] PP = {
      0xb40ab24e49737109,
      0xc0398760d3108fd6,
      0xd869093f2ebec587,
      0xa6ab08f800c128c9,
      0xa629a9c460a8edfb,
      0xd422e28678b47614,
      0x93facdf9bc1363a2,
      0x93caa3c5dd40d768,
      0xaa53204a7969914e,
      0xe2415fb3440a16bb,
      0xa05f3d0295be208f,
      0xb1e611886ec27c88,
      0xd6d2bc63c91d290e,
      0xf80f25b8c1930ecc,
      0x97dc1fd115e0e70e,
      0xe17f23cd55fe08ae,
      0xd309c54ae0d66600,
      0xb55bd69117e20f21,
      0x9b19a5d4d4f5ccbe,
      0xcbca35d9ab901b9b,
      0x889417ed965534dd,
      0x8f27c100bd898837,
      0x930fc2d34cc207e3,
      0xba0920c3f1c7b364,
      0x80d46b49cfadf5cc,
      0xb45b9d252b5d6071,
      0x9fe4d82f5fd432d2,
      0xa97d6763d5f818b3,
      0xe8d6b0be7c43649d,
      0xbc673c33fbe55129,
      0xec03ce27f7509ae5,
      0x808401d440abf627,
      0x95c51b3d387ce64b,
      0xa5a59bd27d3f452d,
      0xe429f8be22291027,
      0xe4764c26913308e0,
      0xafd52ea135797bda,
      0xeb04bdfea0163482,
      0x9e81f8b8d63a6b87,
      0xd320f803485563ae,
      0x8af88fe409983363,
      0xd66102fef6ccfe37,
      0xa93e47043985cda0,
      0x88bf43af43565fa7,
      0xbebb7241360adb47,
      0xd399e12dea25d131,
      0xd03a3d3c20aa87f4,
      0x8111202d77c4b0a8,
      0xc62d960fccc5ba7f,
      0x9d94edd9e31c0833,
      0xa926bc8010d838e0,
      0xf3c8b80989f6395a,
      0x99824e83b5562fba,
      0xd87d11f3a1ae7f31,
      0xadb9b99e5d44d4ea,
      0xaef654bb644fe26a,
      0xcbf16d7ac4a259e8,
      0x8a1a38ce068a8e79,
      0xfc5207dc711c0a9f,
      0xd30ddb1ba0f02884,
      0xd48fc688376f2998,
      0xa79f0024e168fb6e,
      0x80709fe6a7dd8d6f,
      0xc8771453abb9e8e3,
      0xc9e8268efb9fd8a3,
      0xc994dbf7c566278e,
      0xddd80109c37bd67b,
      0xa9cc55348f13c673,
      0xa36d7a45d27bc907,
      0xd7e2a78c66663257,
      0xdd426ee67c908039,
      0xc80996c7916f5fc8,
      0xf9a6c5153d62dc96,
      0x8267aaa0c80b20a6,
      0xdeb59e2db3e430a8,
      0xa03fa2802d0318a9,
      0x83b7afb5c47e0dfc,
      0x8752b710e740bfa9,
      0xa6ee843c1df1006e,
      0x814705bf21a7a80e,
      0xf3feedba611a554d,
      0xdbe78addf2daa748,
      0x961e7a41615851cc,
      0xdb85afd5496a1c1d,
      0xbadd6e782e2ba8ce,
      0xaf93ef6d2abed356,
      0xc645141ad5794d6c,
      0xd86e9600582cb555,
      0xc39d12b425fe98a3,
      0x8c3467629a5f7296,
      0x9f373e3c90100d71,
      0xb00c9e7b68d20287,
      0x9f6f838b293b2e4a,
      0xcbd55e6bb5990fdc,
      0xc9ca494c50fcc7c8,
      0xe7e36ad968b357d0,
      0x88f27f83c0204576,
      0x9b17ad6f4c8a74b2,
      0xe0cfbf085660db1c,
      0x982f15079f214ce0
    };
  }
}
