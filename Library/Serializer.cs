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
using System.Reflection;

namespace SHS {
  public static class SerializerFactory {
    public static Serializer Make(Type t) {
      if (t == typeof(Boolean)) {
        return new BooleanSerializer();
      } else if (t == typeof(Byte)) {
        return new ByteSerializer();
      } else if (t == typeof(SByte)) {
        return new SByteSerializer();
      } else if (t == typeof(UInt16)) {
        return new UInt16Serializer();
      } else if (t == typeof(Int16)) {
        return new Int16Serializer();
      } else if (t == typeof(UInt32)) {
        return new UInt32Serializer();
      } else if (t == typeof(Int32)) {
        return new Int32Serializer();
      } else if (t == typeof(UInt64)) {
        return new UInt64Serializer();
      } else if (t == typeof(Int64)) {
        return new Int64Serializer();
      } else if (t == typeof(Single)) {
        return new SingleSerializer();
      } else if (t == typeof(Double)) {
        return new DoubleSerializer();
      } else if (t == typeof(Decimal)) {
        return new DecimalSerializer();
      } else if (t == typeof(Char)) {
        return new CharSerializer();
      } else if (t == typeof(String)) {
        return new StringSerializer();
      } else {
        return new GeneralSerializer(t);
      }
    }
  }

  public abstract class Serializer {
    public abstract object Read(Rd rd);
    public abstract void Write(object val, Wr wr);

    public abstract object Read(BinaryReader rd);

    public abstract void Write(object val, BinaryWriter wr);
  }

  public class GeneralSerializer : Serializer {
    private MethodInfo rdChMeth;
    private MethodInfo wrChMeth;
    private MethodInfo rdBrMeth;
    private MethodInfo wrBwMeth;

    public GeneralSerializer(Type t) {
      this.rdChMeth = t.GetMethod("Read", new Type[]{t, typeof(Rd)});
      if (this.rdChMeth == null) {
        throw new Exception(t + " does not have a Read(Rd) method");
      }
      this.wrChMeth = t.GetMethod("Write", new Type[]{t, typeof(Wr)});
      if (this.wrChMeth == null) {
        throw new Exception(t + " does not have a Write(Wr) method");
      }
      this.rdBrMeth = t.GetMethod("Read", new Type[] { t, typeof(BinaryReader) });
      if (this.rdBrMeth == null) {
        throw new Exception(t + " does not have a Read(BinaryReader) method");
      }
      this.wrBwMeth = t.GetMethod("Write", new Type[] { t, typeof(BinaryWriter) });
      if (this.wrBwMeth == null) {
        throw new Exception(t + " does not have a Write(BinaryWriter) method");
      }
    }
    public override object Read(Rd rd) {
      return this.rdChMeth.Invoke(null, new object[]{null, rd});
    }
    public override object Read(BinaryReader rd) {
      return this.rdBrMeth.Invoke(null, new object[]{null, rd});
    }
    public override void Write(object val, Wr wr) {
      wrChMeth.Invoke(null, new object[]{val, wr});
    }
    public override void Write(object val, BinaryWriter wr) {
      wrBwMeth.Invoke(null, new object[]{val, wr});
    }
  }

  public class BooleanSerializer : Serializer {
    public override object Read(Rd rd) {
      return rd.ReadBoolean();
    }
    public override object Read(BinaryReader rd) {
      return rd.ReadBoolean();
    }
    public override void Write(object val, Wr wr) {
      wr.WriteBoolean((Boolean)val);
    }
    public override void Write(object val, BinaryWriter wr) {
      wr.Write((Boolean)val);
    }
  }

  public class ByteSerializer : Serializer {
    public override object Read(Rd rd) {
      return rd.ReadByte();
    }
    public override object Read(BinaryReader rd) {
      return rd.ReadByte();
    }
    public override void Write(object val, Wr wr) {
      wr.WriteUInt8((Byte)val);
    }
    public override void Write(object val, BinaryWriter wr) {
      wr.Write((Byte)val);
    }
  }

  public class SByteSerializer : Serializer {
    public override object Read(Rd rd) {
      return rd.ReadSByte();
    }
    public override object Read(BinaryReader rd) {
      return rd.ReadSByte();
    }
    public override void Write(object val, Wr wr) {
      wr.WriteInt8((SByte)val);
    }
    public override void Write(object val, BinaryWriter wr) {
      wr.Write((SByte)val);
    }
  }

  public class UInt16Serializer : Serializer {
    public override object Read(Rd rd) {
      return rd.ReadUInt16();
    }
    public override object Read(BinaryReader rd) {
      return rd.ReadUInt16();
    }
    public override void Write(object val, Wr wr) {
      wr.WriteUInt16((UInt16)val);
    }
    public override void Write(object val, BinaryWriter wr) {
      wr.Write((UInt16)val);
    }
  }

  public class Int16Serializer : Serializer {
    public override object Read(Rd rd) {
      return rd.ReadInt16();
    }
    public override object Read(BinaryReader rd) {
      return rd.ReadInt16();
    }
    public override void Write(object val, Wr wr) {
      wr.WriteInt16((Int16)val);
    }
    public override void Write(object val, BinaryWriter wr) {
      wr.Write((Int16)val);
    }
  }

  public class UInt32Serializer : Serializer {
    public override object Read(Rd rd) {
      return rd.ReadUInt32();
    }
    public override object Read(BinaryReader rd) {
      return rd.ReadUInt32();
    }
    public override void Write(object val, Wr wr) {
      wr.WriteUInt32((UInt32)val);
    }
    public override void Write(object val, BinaryWriter wr) {
      wr.Write((UInt32)val);
    }
  }

  public class Int32Serializer : Serializer {
    public override object Read(Rd rd) {
      return rd.ReadInt32();
    }
    public override object Read(BinaryReader rd) {
      return rd.ReadInt32();
    }
    public override void Write(object val, Wr wr) {
      wr.WriteInt32((Int32)val);
    }
    public override void Write(object val, BinaryWriter wr) {
      wr.Write((Int32)val);
    }
  }

  public class UInt64Serializer : Serializer {
    public override object Read(Rd rd) {
      return rd.ReadUInt64();
    }
    public override object Read(BinaryReader rd) {
      return rd.ReadUInt64();
    }
    public override void Write(object val, Wr wr) {
      wr.WriteUInt64((UInt64)val);
    }
    public override void Write(object val, BinaryWriter wr) {
      wr.Write((UInt64)val);
    }
  }

  public class Int64Serializer : Serializer {
    public override object Read(Rd rd) {
      return rd.ReadInt64();
    }
    public override object Read(BinaryReader rd) {
      return rd.ReadInt64();
    }
    public override void Write(object val, Wr wr) {
      wr.WriteInt64((Int64)val);
    }
    public override void Write(object val, BinaryWriter wr) {
      wr.Write((Int64)val);
    }
  }

  public class SingleSerializer : Serializer {
    public override object Read(Rd rd) {
      return rd.ReadSingle();
    }
    public override object Read(BinaryReader rd) {
      return rd.ReadSingle();
    }
    public override void Write(object val, Wr wr) {
      wr.WriteSingle((Single)val);
    }
    public override void Write(object val, BinaryWriter wr) {
      wr.Write((Single)val);
    }
  }

  public class DoubleSerializer : Serializer {
    public override object Read(Rd rd) {
      return rd.ReadDouble();
    }
    public override object Read(BinaryReader rd) {
      return rd.ReadDouble();
    }
    public override void Write(object val, Wr wr) {
      wr.WriteDouble((Double)val);
    }
    public override void Write(object val, BinaryWriter wr) {
      wr.Write((Double)val);
    }
  }

  public class DecimalSerializer : Serializer {
    public override object Read(Rd rd) {
      return rd.ReadDecimal();
    }
    public override object Read(BinaryReader rd) {
      return rd.ReadDecimal();
    }
    public override void Write(object val, Wr wr) {
      wr.WriteDecimal((Decimal)val);
    }
    public override void Write(object val, BinaryWriter wr) {
      wr.Write((Decimal)val);
    }
  }

  public class CharSerializer : Serializer {
    public override object Read(Rd rd) {
      return rd.ReadChar();
    }
    public override object Read(BinaryReader rd) {
      return rd.ReadChar();
    }
    public override void Write(object val, Wr wr) {
      wr.WriteChar((Char)val);
    }
    public override void Write(object val, BinaryWriter wr) {
      wr.Write((Char)val);
    }
  }

  public class StringSerializer : Serializer {
    public override object Read(Rd rd) {
      return rd.ReadString();
    }
    public override object Read(BinaryReader rd) {
      return rd.ReadString();
    }
    public override void Write(object val, Wr wr) {
      wr.WriteString((String)val);
    }
    public override void Write(object val, BinaryWriter wr) {
      wr.Write((String)val);
    }
  }
}
