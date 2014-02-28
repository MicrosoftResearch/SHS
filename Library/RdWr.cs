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

  public interface Rd {
    bool ReadBoolean();
    byte ReadByte();
    sbyte ReadSByte();
    ushort ReadUInt16();
    short ReadInt16();
    uint ReadUInt32();
    int ReadInt32();
    ulong ReadUInt64();
    long ReadInt64();
    float ReadSingle();
    double ReadDouble();
    decimal ReadDecimal();
    char ReadChar();
    byte[] ReadBytes();
    string ReadString();
  }

  public interface Wr {
    void WriteBoolean(bool x);
    void WriteUInt8(byte x);
    void WriteInt8(sbyte x);
    void WriteUInt16(ushort x);
    void WriteInt16(short x);
    void WriteUInt32(uint x);
    void WriteInt32(int x);
    void WriteUInt64(ulong x);
    void WriteInt64(long x);
    void WriteSingle(float x);
    void WriteDouble(double x);
    void WriteDecimal(decimal x);
    void WriteChar(char x);
    void WriteBytes(byte[] x);
    void WriteString(string s);
  }

  public struct Int {
    int val;
    Int(int x) {
      this.val = x;
    }
    public static implicit operator int (Int x) {
      return x.val;
    }
    public static implicit operator Int (int x) {
      return new Int(x);
    }
    public static Int Read(Int val, Rd rd) {
      return rd.ReadInt32();
    }
    public static void Write(Int val, Wr wr) {
      wr.WriteInt32(val);
    }
  }

  public static class RdWrExtensions {
    public static bool Read(this bool x, Rd rd) {
      return rd.ReadBoolean();
    }
    public static byte Read(this byte x, Rd rd) {
      return rd.ReadByte();
    }
    public static sbyte Read(this sbyte x, Rd rd) {
      return rd.ReadSByte();
    }
    public static ushort Read(this ushort x, Rd rd) {
      return rd.ReadUInt16();
    }
    public static short Read(this short x, Rd rd) {
      return rd.ReadInt16();
    }
    public static uint Read(this uint x, Rd rd) {
      return rd.ReadUInt32();
    }
    public static int Read(this int x, Rd rd) {
      return rd.ReadInt32();
    }
    public static ulong Read(this ulong x, Rd rd) {
      return rd.ReadUInt64();
    }
    public static long Read(this long x, Rd rd) {
      return rd.ReadInt64();
    }
    public static float Read(this float x, Rd rd) {
      return rd.ReadSingle();
    }
    public static double Read(this double x, Rd rd) {
      return rd.ReadDouble();
    }
    public static decimal Read(this decimal x, Rd rd) {
      return rd.ReadDecimal();
    }
    public static char Read(this char x, Rd rd) {
      return rd.ReadChar();
    }
    public static byte[] Read(this byte[] x, Rd rd) {
      return rd.ReadBytes();
    }
    public static string Read(this string x, Rd rd) {
      return rd.ReadString();
    }
    public static void Write(this bool x, Wr wr) {
      wr.WriteBoolean(x);
    }
    public static void Write(this byte x, Wr wr) {
      wr.WriteUInt8(x);
    }
    public static void Write(this sbyte x, Wr wr) {
      wr.WriteInt8(x);
    }
    public static void Write(this ushort x, Wr wr) {
      wr.WriteUInt16(x);
    }
    public static void Write(this short x, Wr wr) {
      wr.WriteInt16(x);
    }
    public static void Write(this uint x, Wr wr) {
      wr.WriteUInt32(x);
    }
    public static void Write(this int x, Wr wr) {
      wr.WriteInt32(x);
    }
    public static void Write(this ulong x, Wr wr) {
      wr.WriteUInt64(x);
    }
    public static void Write(this long x, Wr wr) {
      wr.WriteInt64(x);
    }
    public static void Write(this float x, Wr wr) {
      wr.WriteSingle(x);
    }
    public static void Write(this double x, Wr wr) {
      wr.WriteDouble(x);
    }
    public static void Write(this decimal x, Wr wr) {
      wr.WriteDecimal(x);
    }
    public static void Write(this char x, Wr wr) {
      wr.WriteChar(x);
    }
    public static void Write(this byte[] x, Wr wr) {
      wr.WriteBytes(x);
    }
    public static void Write(this string x, Wr wr) {
      wr.WriteString(x);
    }
  }
}
