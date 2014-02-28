/* Potential clients of this class: InBuf, OutBuf, Tcp, and IncoreCachedFile. */

using Debug = System.Diagnostics.Debug;
using Float32 = System.Single;
using Float64 = System.Double;
using Int16 = System.Int16;
using Int32 = System.Int32;
using Int64 = System.Int64;
using Int8 = System.SByte;
using UInt16 = System.UInt16;
using UInt32 = System.UInt32;
using UInt64 = System.UInt64;
using UInt8 = System.Byte;

namespace Najork {
  public class Punning {
    unsafe public static Int8 GetInt8(byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(Int8));
      fixed (byte* sb = b) {
        return *((Int8*)(sb + pos));
      }
    }

    unsafe public static Int16 GetInt16(byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(Int16));
      fixed (byte* sb = b) {
        return *((Int16*)(sb + pos));
      }
    }

    unsafe public static Int32 GetInt32(byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(Int32));
      fixed (byte* sb = b) {
        return *((Int32*)(sb + pos));
      }
    }

    unsafe public static Int64 GetInt64(byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(Int64));
      fixed (byte* sb = b) {
        return *((Int64*)(sb + pos));
      }
    }

    unsafe public static UInt8 GetUInt8(byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(UInt8));
      fixed (byte* sb = b) {
        return *((UInt8*)(sb + pos));
      }
    }

    unsafe public static UInt16 GetUInt16(byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(UInt16));
      fixed (byte* sb = b) {
        return *((UInt16*)(sb + pos));
      }
    }

    unsafe public static UInt32 GetUInt32(byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(UInt32));
      fixed (byte* sb = b) {
        return *((UInt32*)(sb + pos));
      }
    }

    unsafe public static UInt64 GetUInt64(byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(UInt64));
      fixed (byte* sb = b) {
        return *((UInt64*)(sb + pos));
      }
    }

    unsafe public static Float32 GetFloat32(byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(Float32));
      fixed (byte* sb = b) {
        return *((Float32*)(sb + pos));
      }
    }

    unsafe public static Float64 GetFloat64(byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(Float64));
      fixed (byte* sb = b) {
        return *((Float64*)(sb + pos));
      }
    }

    unsafe public static void PutInt8(Int8 x, byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(Int8));
      fixed (byte* sb = b) {
        *((Int8*)(sb + pos)) = x;
      }
    }

    unsafe public static void PutInt16(Int16 x, byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(Int16));
      fixed (byte* sb = b) {
        *((Int16*)(sb + pos)) = x;
      }
    }

    unsafe public static void PutInt32(Int32 x, byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(Int32));
      fixed (byte* sb = b) {
        *((Int32*)(sb + pos)) = x;
      }
    }

    unsafe public static void PutInt64(Int64 x, byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(Int64));
      fixed (byte* sb = b) {
        *((Int64*)(sb + pos)) = x;
      }
    }

    unsafe public static void PutUInt8(UInt8 x, byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(UInt8));
      fixed (byte* sb = b) {
        *((UInt8*)(sb + pos)) = x;
      }
    }

    unsafe public static void PutUInt16(UInt16 x, byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(UInt16));
      fixed (byte* sb = b) {
        *((UInt16*)(sb + pos)) = x;
      }
    }

    unsafe public static void PutUInt32(UInt32 x, byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(UInt32));
      fixed (byte* sb = b) {
        *((UInt32*)(sb + pos)) = x;
      }
    }

    unsafe public static void PutUInt64(UInt64 x, byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(UInt64));
      fixed (byte* sb = b) {
        *((UInt64*)(sb + pos)) = x;
      }
    }

    unsafe public static void PutFloat32(Float32 x, byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(Float32));
      fixed (byte* sb = b) {
        *((Float32*)(sb + pos)) = x;
      }
    }

    unsafe public static void PutFloat64(Float64 x, byte[] b, int pos) {
      Debug.Assert(b.Length - pos  >= sizeof(Float64));
      fixed (byte* sb = b) {
        *((Float64*)(sb + pos)) = x;
      }
    }
  }
}
