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
using System.Diagnostics.Contracts;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace SHS {

  public class Channel : Rd, Wr {
    private string name;  // may be null
    private Socket socket;
    private readonly byte[] sendBuf;
    private readonly byte[] recvBuf;
    private readonly byte[] serBuf;
    private int    sendPos;
    private int    recvPos;
    private int    recvLen;

    private static Socket MakeSocket(string hostName, int port) {
      Socket s = new Socket(AddressFamily.InterNetwork,
                            SocketType.Stream,
                            ProtocolType.Tcp);
      s.Connect(hostName, port);
      return s;
    }

    public Channel(string name, int port) : this(MakeSocket(name, port)) {
      Contract.Requires(name != null);
      Contract.Requires(1 <= port && port < 65535);

      this.name = name;
    }

    public Channel(Socket s) {
      Contract.Requires(s != null);
      this.socket = s;
      this.socket.LingerState = new LingerOption(true, 1);
      this.socket.SendBufferSize = 1 << 16;
      this.socket.ReceiveBufferSize = 1 << 16;
      this.socket.NoDelay = false;
      this.serBuf = new byte[8];
      this.recvBuf = new byte[this.socket.ReceiveBufferSize];
      this.sendBuf = new byte[this.socket.SendBufferSize];
      this.sendPos = 0;
      this.recvPos = 0;
      this.recvLen = 0;
    }

    public Socket Socket {
      get { return this.socket; }
    }

    public string Name {
      get { return this.name; }
    }

    unsafe public void WriteBoolean(Boolean x) {
      if (this.sendPos + sizeof(Boolean) < this.sendBuf.Length) {
        fixed (byte* sb = this.sendBuf) {
          *((Boolean*)(sb + this.sendPos)) = x;
          this.sendPos += sizeof(Boolean);
        }
      } else {
        fixed (byte* sb = this.serBuf) {
          *((Boolean*)sb) = x;
        }
        this.WriteNumBytes(this.serBuf, sizeof(Boolean));
      }
    }

    unsafe public Boolean ReadBoolean() {
      if (this.recvPos + sizeof(Boolean) <= this.recvLen) {
        fixed (byte* sb = this.recvBuf) {
          int pos = recvPos;
          recvPos += sizeof(Boolean);
          return *((Boolean*)(sb + pos));
        }
      } else {
        this.ReadNumBytes(this.serBuf, sizeof(Boolean));
        fixed (byte* sb = this.serBuf) {
          return *((Boolean*)sb);
        }
      }
    }

    unsafe public void WriteChar(Char x) {
      if (this.sendPos + sizeof(Char) < this.sendBuf.Length) {
        fixed (byte* sb = this.sendBuf) {
          *((Char*)(sb + this.sendPos)) = x;
          this.sendPos += sizeof(Char);
        }
      } else {
        fixed (byte* sb = this.serBuf) {
          *((Char*)sb) = x;
        }
        this.WriteNumBytes(this.serBuf, sizeof(Char));
      }
    }

    unsafe public Char ReadChar() {
      if (this.recvPos + sizeof(Char) <= this.recvLen) {
        fixed (byte* sb = this.recvBuf) {
          int pos = recvPos;
          recvPos += sizeof(Char);
          return *((Char*)(sb + pos));
        }
      } else {
        this.ReadNumBytes(this.serBuf, sizeof(Char));
        fixed (byte* sb = this.serBuf) {
          return *((Char*)sb);
        }
      }
    }

    unsafe public void WriteInt8(sbyte x) {
      if (this.sendPos + sizeof(sbyte) < this.sendBuf.Length) {
        fixed (byte* sb = this.sendBuf) {
          *((sbyte*)(sb + this.sendPos)) = x;
          this.sendPos += sizeof(sbyte);
        }
      } else {
        fixed (byte* sb = this.serBuf) {
          *((sbyte*)sb) = x;
        }
        this.WriteNumBytes(this.serBuf, sizeof(sbyte));
      }
    }

    unsafe public sbyte ReadSByte() {
      if (this.recvPos + sizeof(sbyte) <= this.recvLen) {
        fixed (byte* sb = this.recvBuf) {
          int pos = recvPos;
          recvPos += sizeof(sbyte);
          return *((sbyte*)(sb + pos));
        }
      } else {
        this.ReadNumBytes(this.serBuf, sizeof(sbyte));
        fixed (byte* sb = this.serBuf) {
          return *((sbyte*)sb);
        }
      }
    }

    unsafe public void WriteUInt8(byte x) {
      if (this.sendPos + sizeof(byte) < this.sendBuf.Length) {
        fixed (byte* sb = this.sendBuf) {
          *((byte*)(sb + this.sendPos)) = x;
          this.sendPos += sizeof(byte);
        }
      } else {
        fixed (byte* sb = this.serBuf) {
          *((byte*)sb) = x;
        }
        this.WriteNumBytes(this.serBuf, sizeof(byte));
      }
    }

    unsafe public byte ReadByte() {
      if (this.recvPos + sizeof(byte) <= this.recvLen) {
        fixed (byte* sb = this.recvBuf) {
          int pos = recvPos;
          recvPos += sizeof(byte);
          return *((byte*)(sb + pos));
        }
      } else {
        this.ReadNumBytes(this.serBuf, sizeof(byte));
        fixed (byte* sb = this.serBuf) {
          return *((byte*)sb);
        }
      }
    }

    unsafe public void WriteInt16(Int16 x) {
      if (this.sendPos + sizeof(Int16) < this.sendBuf.Length) {
        fixed (byte* sb = this.sendBuf) {
          *((Int16*)(sb + this.sendPos)) = x;
          this.sendPos += sizeof(Int16);
        }
      } else {
        fixed (byte* sb = this.serBuf) {
          *((Int16*)sb) = x;
        }
        this.WriteNumBytes(this.serBuf, sizeof(Int16));
      }
    }

    unsafe public Int16 ReadInt16() {
      if (this.recvPos + sizeof(Int16) <= this.recvLen) {
        fixed (byte* sb = this.recvBuf) {
          int pos = recvPos;
          recvPos += sizeof(Int16);
          return *((Int16*)(sb + pos));
        }
      } else {
        this.ReadNumBytes(this.serBuf, sizeof(Int16));
        fixed (byte* sb = this.serBuf) {
          return *((Int16*)sb);
        }
      }
    }

    unsafe public void WriteUInt16(UInt16 x) {
      if (this.sendPos + sizeof(UInt16) < this.sendBuf.Length) {
        fixed (byte* sb = this.sendBuf) {
          *((UInt16*)(sb + this.sendPos)) = x;
          this.sendPos += sizeof(UInt16);
        }
      } else {
        fixed (byte* sb = this.serBuf) {
          *((UInt16*)sb) = x;
        }
        this.WriteNumBytes(this.serBuf, sizeof(UInt16));
      }
    }

    unsafe public UInt16 ReadUInt16() {
      if (this.recvPos + sizeof(UInt16) <= this.recvLen) {
        fixed (byte* sb = this.recvBuf) {
          int pos = recvPos;
          recvPos += sizeof(UInt16);
          return *((UInt16*)(sb + pos));
        }
      } else {
        this.ReadNumBytes(this.serBuf, sizeof(UInt16));
        fixed (byte* sb = this.serBuf) {
          return *((UInt16*)sb);
        }
      }
    }

    unsafe public void WriteInt32(Int32 x) {
      if (this.sendPos + sizeof(Int32) < this.sendBuf.Length) {
        fixed (byte* sb = this.sendBuf) {
          *((Int32*)(sb + this.sendPos)) = x;
          this.sendPos += sizeof(Int32);
        }
      } else {
        fixed (byte* sb = this.serBuf) {
          *((Int32*)sb) = x;
        }
        this.WriteNumBytes(this.serBuf, sizeof(Int32));
      }
    }

    unsafe public Int32 ReadInt32() {
      if (this.recvPos + sizeof(Int32) <= this.recvLen) {
        fixed (byte* sb = this.recvBuf) {
          int pos = recvPos;
          recvPos += sizeof(Int32);
          return *((Int32*)(sb + pos));
        }
      } else {
        this.ReadNumBytes(this.serBuf, sizeof(Int32));
        fixed (byte* sb = this.serBuf) {
          return *((Int32*)sb);
        }
      }
    }

    unsafe public void WriteUInt32(UInt32 x) {
      fixed (byte* sb = this.serBuf) {
        *((UInt32*)sb) = x;
      }
      this.WriteNumBytes(this.serBuf, sizeof(UInt32));
    }

    unsafe public UInt32 ReadUInt32() {
      this.ReadNumBytes(this.serBuf, sizeof(UInt32));
      fixed (byte* sb = this.serBuf) {
        return *((UInt32*)sb);
      }
    }

    unsafe public void WriteInt64(Int64 x) {
      if (this.sendPos + sizeof(Int64) < this.sendBuf.Length) {
        fixed (byte* sb = this.sendBuf) {
          *((Int64*)(sb + this.sendPos)) = x;
          this.sendPos += sizeof(Int64);
        }
      } else {
        fixed (byte* sb = this.serBuf) {
          *((Int64*)sb) = x;
        }
        this.WriteNumBytes(this.serBuf, sizeof(Int64));
      }
    }

    unsafe public Int64 ReadInt64() {
      if (this.recvPos + sizeof(Int64) <= this.recvLen) {
        fixed (byte* sb = this.recvBuf) {
          int pos = recvPos;
          recvPos += sizeof(Int64);
          return *((Int64*)(sb + pos));
        }
      } else {
        this.ReadNumBytes(this.serBuf, sizeof(Int64));
        fixed (byte* sb = this.serBuf) {
          return *((Int64*)sb);
        }
      }
    }

    unsafe public void WriteUInt64(UInt64 x) {
      fixed (byte* sb = this.serBuf) {
        *((UInt64*)sb) = x;
      }
      this.WriteNumBytes(this.serBuf, sizeof(UInt64));
    }

    unsafe public UInt64 ReadUInt64() {
      this.ReadNumBytes(this.serBuf, sizeof(UInt64));
      fixed (byte* sb = this.serBuf) {
        return *((UInt64*)sb);
      }
    }

    unsafe public void WriteSingle(float x) {
      fixed (byte* sb = this.serBuf) {
        *((float*)sb) = x;
      }
      this.WriteNumBytes(this.serBuf, sizeof(float));
    }

    unsafe public float ReadSingle() {
      this.ReadNumBytes(this.serBuf, sizeof(float));
      fixed (byte* sb = this.serBuf) {
        return *((float*)sb);
      }
    }

    unsafe public void WriteDouble(double x) {
      fixed (byte* sb = this.serBuf) {
        *((double*)sb) = x;
      }
      this.WriteNumBytes(this.serBuf, sizeof(double));
    }

    unsafe public double ReadDouble() {
      this.ReadNumBytes(this.serBuf, sizeof(double));
      fixed (byte* sb = this.serBuf) {
        return *((double*)sb);
      }
    }

    unsafe public void WriteDecimal(decimal x) {
      fixed (byte* sb = this.serBuf) {
        *((decimal*)sb) = x;
      }
      this.WriteNumBytes(this.serBuf, sizeof(decimal));
    }

    unsafe public decimal ReadDecimal() {
      this.ReadNumBytes(this.serBuf, sizeof(decimal));
      fixed (byte* sb = this.serBuf) {
        return *((decimal*)sb);
      }
    }

    public void WriteString(string s) {
      this.WriteBytes(s == null ? null : Encoding.UTF8.GetBytes(s));
    }

    public string ReadString() {
      var bytes = this.ReadBytes();
      return bytes == null ? null : Encoding.UTF8.GetString(bytes);
    }

    public void WriteBytes(byte[] buf) {
      if (buf == null) {
        this.WriteInt32(-1);
      } else {
        this.WriteInt32(buf.Length);
        int i = 0;
        while (i < buf.Length) {
          int end = Math.Min(buf.Length, i + this.sendBuf.Length - this.sendPos);
          while (i < end) {
            this.sendBuf[this.sendPos++] = buf[i++];
          }
          if (this.sendBuf.Length == this.sendPos) this.FlushSendBuffer();
        }
      }
    }

    public byte[] ReadBytes() {
      int n = this.ReadInt32();
      if (n == -1) return null;
      if (n > 0 && this.recvLen == 0) throw new Exception("stream too short");
      var buf = new byte[n];
      for (int k = 0; k < n; k++) {
        if (this.recvPos == this.recvLen) this.FillRecvBuffer();
        buf[k] = this.recvBuf[this.recvPos++];
      }
      return buf;
    }


    public void WriteNumBytes(byte[] b, int num) {
      int start = 0;
      while (num > 0) {
        int cnt = Math.Min(num, this.sendBuf.Length - this.sendPos);
        for (int i = 0; i < cnt; i++) {
          this.sendBuf[this.sendPos+i] = b[start+i];
        }
        this.sendPos += cnt; start += cnt; num -= cnt;
        if (this.sendBuf.Length == this.sendPos) this.FlushSendBuffer();
      }
    }

    public void ReadNumBytes(byte[] b, int num) {
      int start = 0;
      while (num > 0) {
        if (this.recvPos == this.recvLen) this.FillRecvBuffer();
        int cnt = Math.Min(num, this.recvLen - this.recvPos);
        for (int i = 0; i < cnt; i++) {
          b[start+i] = this.recvBuf[this.recvPos+i];
        }
        this.recvPos += cnt; start += cnt; num -= cnt;
      }
    }

    /* Possible exceptions:
       System.Net.Sockets.SocketException
       System.ObjectDisposedException
     */
    public void Close() {
      try {
        this.FlushSendBuffer();
        this.socket.Shutdown(SocketShutdown.Both);
        this.socket.Close();
        this.socket.Dispose();
      } catch (SocketException) {
        // ignore
      }
      this.socket = null;
    }

    /* Possible exceptions:
       System.ObjectDisposedException
       System.Net.Sockets.SocketException
    */
    public void Flush() {
      // Disable Nagle to ensure flushed data is sent right away
      this.socket.NoDelay = true;
      this.FlushSendBuffer();
      // Re-enable Nagle to fully utilize MTUs
      this.socket.NoDelay = false;
    }

    private void FlushSendBuffer() {
      this.socket.Send(this.sendBuf, this.sendPos, SocketFlags.None);
      this.sendPos = 0;
    }

    private void FillRecvBuffer() {
      this.recvPos = 0;
      this.recvLen = this.socket.Receive(this.recvBuf);
      if (this.recvLen == 0) throw new SocketException();
    }
  }

  class Acceptor {
    private Socket listenSocket;

    public Acceptor(int port, int queueLen) {
      this.listenSocket = new Socket(AddressFamily.InterNetwork,
                                     SocketType.Stream,
                                     ProtocolType.Tcp);
      this.listenSocket.Bind(new IPEndPoint(IPAddress.Any, port));
      this.listenSocket.Listen(queueLen);
    }

    public void Dispose() {
      this.listenSocket.Shutdown(SocketShutdown.Both);
      this.listenSocket.Close();
      this.listenSocket = null;
    }

    public Channel AcceptConnection(){
      return new Channel(this.listenSocket.Accept());
    }
  }
}
