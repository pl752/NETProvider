/*
 *    The contents of this file are subject to the Initial
 *    Developer's Public License Version 1.0 (the "License");
 *    you may not use this file except in compliance with the
 *    License. You may obtain a copy of the License at
 *    https://github.com/FirebirdSQL/NETProvider/raw/master/license.txt.
 *
 *    Software distributed under the License is distributed on
 *    an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either
 *    express or implied. See the License for the specific
 *    language governing rights and limitations under the License.
 *
 *    All Rights Reserved.
 */

//$Authors = Jiri Cincura (jiri@cincura.net)

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.CsProj;
using BenchmarkDotNet.Toolchains.InProcess.NoEmit;
using FirebirdSql.Data.Common;

namespace Perf;

sealed class InProcessMemoryConfig : ManualConfig
{
	public InProcessMemoryConfig()
	{
		AddDiagnoser(MemoryDiagnoser.Default);
		AddJob(Job.Default
			.WithToolchain(CsProjCoreToolchain.NetCoreApp80));
		AddJob(Job.Default
			.WithToolchain(CsProjCoreToolchain.NetCoreApp10_0));
	}
}

static class XdrSynthetic
{
	public const int StackallocThreshold = 1024;
	public static ArrayPool<byte> Fixed16BytePool { get; } = ArrayPool<byte>.Create(maxArrayLength: 16, maxArraysPerBucket: 128);

	public static Charset Utf8Charset { get; } = GetCharsetOrDefault("UTF8");

	static Charset GetCharsetOrDefault(string name)
	{
		return Charset.TryGetByName(name, out var charset) ? charset : Charset.DefaultCharset;
	}

	public static byte[] CreateAsciiBytes(int length)
	{
		if (length <= 0)
			return Array.Empty<byte>();
		var result = new byte[length];
		for (var i = 0; i < result.Length; i++)
		{
			result[i] = (byte)('a' + (i % 26));
		}
		return result;
	}

	public static void ReadInto(Span<byte> destination, ReadOnlySpan<byte> source, ref int offset, int length)
	{
		if (length <= 0)
			return;
		source.Slice(offset, length).CopyTo(destination);
		offset += length;
	}

	public static void ReadPad(byte[] smallBuffer, ReadOnlySpan<byte> source, ref int offset, int length)
	{
		if (length <= 0)
			return;
		ReadInto(smallBuffer.AsSpan(0, length), source, ref offset, length);
	}

	public static byte[] EncodeNetworkInt32(int value) => TypeEncoder.EncodeInt32(value);
}

[Config(typeof(InProcessMemoryConfig))]
public class XdrReadStringBenchmark
{
	[Params(16, 8192)] //[Params(0, 16, 128, 1024, 2048)]
	public int Length { get; set; }

	readonly byte[] _smallBuffer = new byte[16];
	byte[] _source = Array.Empty<byte>();
	Charset _charset = Charset.DefaultCharset;

	[GlobalSetup]
	public void GlobalSetup()
	{
		_charset = XdrSynthetic.Utf8Charset;
		var payload = XdrSynthetic.CreateAsciiBytes(Length);
		var pad = (4 - Length) & 3;
		_source = new byte[Length + pad];
		payload.CopyTo(_source, 0);
	}

	[Benchmark(Description = "master ReadString(Charset,int)")]
	public string Master()
	{
		var offset = 0;
		// master ReadString(Charset,int) => charset.GetString(ReadOpaque(length), 0, length)
		var buffer = new byte[Length];
		XdrSynthetic.ReadInto(buffer, _source, ref offset, Length);
		XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - Length) & 3);
		return _charset.GetString(buffer, 0, buffer.Length);
	}

	[Benchmark(Description = "local_opt2 ReadString(Charset,int)")]
	public string LocalOpt2()
	{
		if (Length <= 0)
			return string.Empty;

		var offset = 0;
		if (Length <= XdrSynthetic.StackallocThreshold)
		{
			Span<byte> buffer = stackalloc byte[Length];
			XdrSynthetic.ReadInto(buffer, _source, ref offset, Length);
			XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - Length) & 3);
			return _charset.GetString(buffer);
		}
		else
		{
			var rented = ArrayPool<byte>.Shared.Rent(Length);
			try
			{
				XdrSynthetic.ReadInto(rented.AsSpan(0, Length), _source, ref offset, Length);
				XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - Length) & 3);
				return _charset.GetString(rented.AsSpan(0, Length));
			}
			finally
			{
				ArrayPool<byte>.Shared.Return(rented);
			}
		}
	}

	[Benchmark(Description = "local_opt2 ReadString(Charset,int) (rent always)")]
	public string LocalOpt2_RentAlways()
	{
		if (Length <= 0)
			return string.Empty;

		var offset = 0;
		var rented = ArrayPool<byte>.Shared.Rent(Length);
		try
		{
			XdrSynthetic.ReadInto(rented.AsSpan(0, Length), _source, ref offset, Length);
			XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - Length) & 3);
			return _charset.GetString(rented.AsSpan(0, Length));
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented);
		}
	}

	[Benchmark(Description = "local_opt2 ReadString(Charset,int) (stackalloc, clear)")]
	public string LocalOpt2_StackC()
	{
		if (Length <= 0)
			return string.Empty;

		var offset = 0;
		if (Length <= XdrSynthetic.StackallocThreshold)
		{
			Span<byte> buffer = stackalloc byte[Length];
			XdrSynthetic.ReadInto(buffer, _source, ref offset, Length);
			XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - Length) & 3);
			var s = _charset.GetString(buffer);
			buffer.Clear();
			return s;
		}
		else
		{
			var rented = ArrayPool<byte>.Shared.Rent(Length);
			try
			{
				XdrSynthetic.ReadInto(rented.AsSpan(0, Length), _source, ref offset, Length);
				XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - Length) & 3);
				return _charset.GetString(rented.AsSpan(0, Length));
			}
			finally
			{
				ArrayPool<byte>.Shared.Return(rented, clearArray: true);
			}
		}
	}

	[Benchmark(Description = "local_opt2 ReadString(Charset,int) (rent always, clear)")]
	public string LocalOpt2_RentAlwaysC()
	{
		if (Length <= 0)
			return string.Empty;

		var offset = 0;
		var rented = ArrayPool<byte>.Shared.Rent(Length);
		try
		{
			XdrSynthetic.ReadInto(rented.AsSpan(0, Length), _source, ref offset, Length);
			XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - Length) & 3);
			return _charset.GetString(rented.AsSpan(0, Length));
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented, clearArray: true);
		}
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class XdrReadOpaqueBenchmark
{
	[Params(0, 1, 16, 128, 1024)]
	public int Length { get; set; }

	readonly byte[] _smallBuffer = new byte[16];
	byte[] _source = Array.Empty<byte>();
	byte[] _last = Array.Empty<byte>();
	byte[] _preallocated = Array.Empty<byte>();

	[GlobalSetup]
	public void GlobalSetup()
	{
		var payload = XdrSynthetic.CreateAsciiBytes(Length);
		var pad = (4 - Length) & 3;
		_source = new byte[Length + pad];
		payload.CopyTo(_source, 0);
		_preallocated = Length > 0 ? new byte[Length] : Array.Empty<byte>();
	}

	[Benchmark(Description = "master ReadOpaque(int)")]
	public int Master()
	{
		var offset = 0;
		var buffer = new byte[Length];
		XdrSynthetic.ReadInto(buffer, _source, ref offset, Length);
		XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - Length) & 3);
		_last = buffer;
		return buffer.Length;
	}

	[Benchmark(Description = "local_opt2 ReadOpaque(int)")]
	public int LocalOpt2()
	{
		var offset = 0;
		var buffer = Length > 0 ? new byte[Length] : Array.Empty<byte>();
		XdrSynthetic.ReadInto(buffer, _source, ref offset, Length);
		XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - Length) & 3);
		_last = buffer;
		return buffer.Length;
	}

	[Benchmark(Description = "ReadOpaque(int) (preallocated)")]
	public int Preallocated()
	{
		var offset = 0;
		XdrSynthetic.ReadInto(_preallocated, _source, ref offset, Length);
		XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - Length) & 3);
		_last = _preallocated;
		return _preallocated.Length;
	}

	[Benchmark(Description = "ReadOpaque(int) (preallocated, clear)")]
	public int PreallocatedC()
	{
		var len = Preallocated();
		if (len > 0)
			Array.Clear(_preallocated, 0, len);
		return len;
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class XdrReadBufferBenchmark
{
	[Params(0, 16, 128, 1024)]
	public int Length { get; set; }

	readonly byte[] _smallBuffer = new byte[16];
	byte[] _source = Array.Empty<byte>();
	byte[] _last = Array.Empty<byte>();
	byte[] _preallocated = Array.Empty<byte>();

	[GlobalSetup]
	public void GlobalSetup()
	{
		var payload = XdrSynthetic.CreateAsciiBytes(Length);
		var pad = (4 - Length) & 3;
		var lengthBytes = XdrSynthetic.EncodeNetworkInt32(Length);
		_source = new byte[4 + Length + pad];
		lengthBytes.CopyTo(_source, 0);
		payload.CopyTo(_source, 4);
		_preallocated = Length > 0 ? new byte[Length] : Array.Empty<byte>();
	}

	[Benchmark(Description = "master ReadBuffer()")]
	public int Master()
	{
		var offset = 0;
		XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 4), _source, ref offset, 4);
		var length = (ushort)TypeDecoder.DecodeInt32(_smallBuffer);
		var buffer = new byte[length];
		XdrSynthetic.ReadInto(buffer, _source, ref offset, length);
		XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - length) & 3);
		_last = buffer;
		return buffer.Length;
	}

	[Benchmark(Description = "local_opt2 ReadBuffer()")]
	public int LocalOpt2()
	{
		var offset = 0;
		XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 4), _source, ref offset, 4);
		var length = (ushort)TypeDecoder.DecodeInt32(_smallBuffer);
		var buffer = length > 0 ? new byte[length] : Array.Empty<byte>();
		XdrSynthetic.ReadInto(buffer, _source, ref offset, length);
		XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - length) & 3);
		_last = buffer;
		return buffer.Length;
	}

	[Benchmark(Description = "ReadBuffer() (preallocated)")]
	public int Preallocated()
	{
		var offset = 0;
		XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 4), _source, ref offset, 4);
		var length = (ushort)TypeDecoder.DecodeInt32(_smallBuffer);
		XdrSynthetic.ReadInto(_preallocated.AsSpan(0, length), _source, ref offset, length);
		XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - length) & 3);
		_last = _preallocated;
		return length;
	}

	[Benchmark(Description = "ReadBuffer() (preallocated, clear)")]
	public int PreallocatedC()
	{
		var len = Preallocated();
		if (len > 0)
			Array.Clear(_preallocated, 0, len);
		return len;
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class XdrReadGuidBenchmark
{
	[Params(false, true)]
	public bool Varying { get; set; }

	readonly byte[] _smallBuffer = new byte[16];
	byte[] _source = Array.Empty<byte>();

	[GlobalSetup]
	public void GlobalSetup()
	{
		var guidBytes = new byte[16];
		for (var i = 0; i < guidBytes.Length; i++)
			guidBytes[i] = (byte)(i + 1);

		if (Varying)
		{
			var lengthBytes = XdrSynthetic.EncodeNetworkInt32(16);
			_source = new byte[4 + 16];
			lengthBytes.CopyTo(_source, 0);
			guidBytes.CopyTo(_source, 4);
		}
		else
		{
			_source = guidBytes;
		}
	}

	[Benchmark(Description = "master ReadGuid(int)")]
	public Guid Master()
	{
		var offset = 0;
		if (Varying)
		{
			XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 4), _source, ref offset, 4);
			var length = (ushort)TypeDecoder.DecodeInt32(_smallBuffer);
			var buffer = new byte[length];
			XdrSynthetic.ReadInto(buffer, _source, ref offset, length);
			XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - length) & 3);
			return TypeDecoder.DecodeGuid(buffer);
		}
		else
		{
			var buffer = new byte[16];
			XdrSynthetic.ReadInto(buffer, _source, ref offset, 16);
			return TypeDecoder.DecodeGuid(buffer);
		}
	}

	[Benchmark(Description = "local_opt2 ReadGuid(int)")]
	public Guid LocalOpt2()
	{
		var offset = 0;
		if (Varying)
		{
			XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 4), _source, ref offset, 4);
			var length = (ushort)TypeDecoder.DecodeInt32(_smallBuffer);
			var buffer = new byte[length];
			XdrSynthetic.ReadInto(buffer, _source, ref offset, length);
			XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - length) & 3);
			return TypeDecoder.DecodeGuid(buffer);
		}
		else
		{
			Span<byte> buffer = stackalloc byte[16];
			XdrSynthetic.ReadInto(buffer, _source, ref offset, 16);
			return TypeDecoder.DecodeGuidSpan(buffer);
		}
	}

	[Benchmark(Description = "local_opt2 ReadGuid(int) (shared _smallBuffer)")]
	public Guid LocalOpt2_SmallBuffer()
	{
		var offset = 0;
		if (Varying)
		{
			XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 4), _source, ref offset, 4);
			var length = (ushort)TypeDecoder.DecodeInt32(_smallBuffer);
			var buffer = new byte[length];
			XdrSynthetic.ReadInto(buffer, _source, ref offset, length);
			XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - length) & 3);
			return TypeDecoder.DecodeGuid(buffer);
		}
		else
		{
			XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 16), _source, ref offset, 16);
			return TypeDecoder.DecodeGuid(_smallBuffer);
		}
	}

	[Benchmark(Description = "local_opt2 ReadGuid(int) (shared _smallBuffer, clear)")]
	public Guid LocalOpt2_SmallBufferC()
	{
		var offset = 0;
		if (Varying)
		{
			XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 4), _source, ref offset, 4);
			var length = (ushort)TypeDecoder.DecodeInt32(_smallBuffer);
			var buffer = new byte[length];
			XdrSynthetic.ReadInto(buffer, _source, ref offset, length);
			XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - length) & 3);
			var g = TypeDecoder.DecodeGuid(buffer);
			Array.Clear(buffer);
			return g;
		}
		else
		{
			XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 16), _source, ref offset, 16);
			var g = TypeDecoder.DecodeGuid(_smallBuffer);
			ClearArray(_smallBuffer, 16);
			return g;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static void ClearArray(byte[] array, int len) {
		array.AsSpan(0, len).Clear();
	}

	[Benchmark(Description = "local_opt2 ReadGuid(int) (rent always)")]
	public Guid LocalOpt2_RentAlways()
	{
		var offset = 0;
		if (Varying)
		{
			XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 4), _source, ref offset, 4);
			var length = (ushort)TypeDecoder.DecodeInt32(_smallBuffer);
			var buffer = new byte[length];
			XdrSynthetic.ReadInto(buffer, _source, ref offset, length);
			XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - length) & 3);
			return TypeDecoder.DecodeGuid(buffer);
		}
		else
		{
			var rented = ArrayPool<byte>.Shared.Rent(16);
			try
			{
				XdrSynthetic.ReadInto(rented.AsSpan(0, 16), _source, ref offset, 16);
				return TypeDecoder.DecodeGuidSpan(rented.AsSpan(0, 16));
			}
			finally
			{
				ArrayPool<byte>.Shared.Return(rented);
			}
		}
	}

	[Benchmark(Description = "local_opt2 ReadGuid(int) (stackalloc, clear)")]
	public Guid LocalOpt2_StackC()
	{
		var offset = 0;
		if (Varying)
		{
			XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 4), _source, ref offset, 4);
			var length = (ushort)TypeDecoder.DecodeInt32(_smallBuffer);
			var buffer = new byte[length];
			XdrSynthetic.ReadInto(buffer, _source, ref offset, length);
			XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - length) & 3);
			var g = TypeDecoder.DecodeGuid(buffer);
			Array.Clear(buffer);
			return g;
		}
		else
		{
			Span<byte> buffer = stackalloc byte[16];
			XdrSynthetic.ReadInto(buffer, _source, ref offset, 16);
			var g = TypeDecoder.DecodeGuidSpan(buffer);
			buffer.Clear();
			return g;
		}
	}

	[Benchmark(Description = "local_opt2 ReadGuid(int) (rent always, clear)")]
	public Guid LocalOpt2_RentAlwaysC()
	{
		var offset = 0;
		if (Varying)
		{
			XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 4), _source, ref offset, 4);
			var length = (ushort)TypeDecoder.DecodeInt32(_smallBuffer);
			var buffer = new byte[length];
			XdrSynthetic.ReadInto(buffer, _source, ref offset, length);
			XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, (4 - length) & 3);
			var g = TypeDecoder.DecodeGuid(buffer);
			Array.Clear(buffer);
			return g;
		}
		else
		{
			var rented = ArrayPool<byte>.Shared.Rent(16);
			try
			{
				XdrSynthetic.ReadInto(rented.AsSpan(0, 16), _source, ref offset, 16);
				return TypeDecoder.DecodeGuidSpan(rented.AsSpan(0, 16));
			}
			finally
			{
				ArrayPool<byte>.Shared.Return(rented, clearArray: true);
			}
		}
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class XdrReadBooleanBenchmark
{
	readonly byte[] _smallBuffer = new byte[16];
	readonly byte[] _source = [1, 0, 0, 0];

	[Benchmark(Description = "master ReadBoolean()")]
	public bool Master()
	{
		var offset = 0;
		// master ReadBoolean => DecodeBoolean(ReadOpaque(1))
		var buffer = new byte[1];
		XdrSynthetic.ReadInto(buffer, _source, ref offset, 1);
		XdrSynthetic.ReadPad(_smallBuffer, _source, ref offset, 3);
		return TypeDecoder.DecodeBoolean(buffer);
	}

	[Benchmark(Description = "local_opt2 ReadBoolean() (shared)")]
	public bool LocalOpt2() {
		var offset = 0;
		XdrSynthetic.ReadInto(_smallBuffer, _source, ref offset, 4);
		return TypeDecoder.DecodeBoolean(_smallBuffer);
	}

	[Benchmark(Description = "local_opt2 ReadBoolean() (stackalloc)")]
	public bool LocalOpt2_Stack()
	{
		var offset = 0;
		Span<byte> bytes = stackalloc byte[4];
		XdrSynthetic.ReadInto(bytes, _source, ref offset, 4);
		return TypeDecoder.DecodeBoolean(bytes);
	}

	[Benchmark(Description = "local_opt2 ReadBoolean() (rent always)")]
	public bool LocalOpt2_RentAlways()
	{
		var offset = 0;
		var rented = ArrayPool<byte>.Shared.Rent(4);
		try
		{
			XdrSynthetic.ReadInto(rented.AsSpan(0, 4), _source, ref offset, 4);
			return TypeDecoder.DecodeBoolean(rented.AsSpan(0, 4));
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented);
		}
	}

	[Benchmark(Description = "local_opt2 ReadBoolean() (stackalloc, clear)")]
	public bool LocalOpt2_StackC() {
		var offset = 0;
		Span<byte> bytes = stackalloc byte[4];
		XdrSynthetic.ReadInto(bytes, _source, ref offset, 4);
		bool res = TypeDecoder.DecodeBoolean(bytes);
		bytes.Clear();
		return res;
	}

	[Benchmark(Description = "local_opt2 ReadBoolean() (rent always, clear)")]
	public bool LocalOpt2_RentAlwaysC() {
		var offset = 0;
		var rented = ArrayPool<byte>.Shared.Rent(4);
		try {
			XdrSynthetic.ReadInto(rented.AsSpan(0, 4), _source, ref offset, 4);
			return TypeDecoder.DecodeBoolean(rented.AsSpan(0, 4));
		}
		finally {
			ArrayPool<byte>.Shared.Return(rented, clearArray: true);
		}
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class XdrReadDec34Benchmark
{
	readonly byte[] _smallBuffer = new byte[16];
	readonly byte[] _source = new byte[16];

	[GlobalSetup]
	public void GlobalSetup()
	{
		for (var i = 0; i < _source.Length; i++)
			_source[i] = (byte)(i + 1);
	}

	[Benchmark(Description = "master ReadDec34()")]
	public FirebirdSql.Data.Types.FbDecFloat Master()
	{
		var offset = 0;
		var buffer = new byte[16];
		XdrSynthetic.ReadInto(buffer, _source, ref offset, 16);
		return TypeDecoder.DecodeDec34(buffer);
	}

	[Benchmark(Description = "local_opt2 ReadDec34() using shared _smallBuffer")]
	public FirebirdSql.Data.Types.FbDecFloat LocalOpt2_SmallBuffer()
	{
		var offset = 0;
		XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 16), _source, ref offset, 16);
		return TypeDecoder.DecodeDec34(_smallBuffer);
	}

	[Benchmark(Description = "local_opt2 ReadDec34() renting buffer")]
	public FirebirdSql.Data.Types.FbDecFloat LocalOpt2_Rent()
	{
		var offset = 0;
		var rented = XdrSynthetic.Fixed16BytePool.Rent(16);
		try
		{
			XdrSynthetic.ReadInto(rented.AsSpan(0, 16), _source, ref offset, 16);
			return TypeDecoder.DecodeDec34(rented);
		}
		finally
		{
			XdrSynthetic.Fixed16BytePool.Return(rented);
		}
	}

	[Benchmark(Description = "local_opt2 ReadDec34() using shared _smallBuffer, clear")]
	public FirebirdSql.Data.Types.FbDecFloat LocalOpt2_SmallBufferC()
	{
		var offset = 0;
		XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 16), _source, ref offset, 16);
		var v = TypeDecoder.DecodeDec34(_smallBuffer);
		Array.Clear(_smallBuffer, 0, 16);
		return v;
	}

	[Benchmark(Description = "local_opt2 ReadDec34() renting buffer, clear")]
	public FirebirdSql.Data.Types.FbDecFloat LocalOpt2_RentC()
	{
		var offset = 0;
		var rented = XdrSynthetic.Fixed16BytePool.Rent(16);
		try
		{
			XdrSynthetic.ReadInto(rented.AsSpan(0, 16), _source, ref offset, 16);
			var v = TypeDecoder.DecodeDec34(rented);
			Array.Clear(rented, 0, 16);
			return v;
		}
		finally
		{
			XdrSynthetic.Fixed16BytePool.Return(rented);
		}
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class XdrReadInt32Benchmark
{
	readonly byte[] _smallBuffer = new byte[16];
	readonly byte[] _source;

	public XdrReadInt32Benchmark()
	{
		_source = XdrSynthetic.EncodeNetworkInt32(123456789);
	}

	[Benchmark(Description = "master ReadInt32() using shared _smallBuffer")]
	public int Master()
	{
		var offset = 0;
		XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 4), _source, ref offset, 4);
		return TypeDecoder.DecodeInt32(_smallBuffer);
	}

	[Benchmark(Description = "local_opt2 ReadInt32() (stackalloc)")]
	public int LocalOpt2_Stack()
	{
		var offset = 0;
		Span<byte> bytes = stackalloc byte[4];
		XdrSynthetic.ReadInto(bytes, _source, ref offset, 4);
		return TypeDecoder.DecodeInt32(bytes);
	}

	//[Benchmark(Description = "local_opt2 ReadInt32() renting buffer")]
	public int LocalOpt2_Rent()
	{
		var offset = 0;
		var rented = ArrayPool<byte>.Shared.Rent(4);
		try
		{
			XdrSynthetic.ReadInto(rented.AsSpan(0, 4), _source, ref offset, 4);
			return TypeDecoder.DecodeInt32(rented);
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented);
		}
	}

	//[Benchmark(Description = "local_opt2 ReadInt32() using shared _smallBuffer, clear")]
	public int LocalOpt2_SmallBufferC()
	{
		var offset = 0;
		XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 4), _source, ref offset, 4);
		var v = TypeDecoder.DecodeInt32(_smallBuffer);
		Array.Clear(_smallBuffer, 0, 4);
		return v;
	}

	[Benchmark(Description = "local_opt2 ReadInt32() using shared _smallBuffer, clear with AsSpan")]
	public int LocalOpt2_SmallBufferC2() {
		var offset = 0;
		XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 4), _source, ref offset, 4);
		var v = TypeDecoder.DecodeInt32(_smallBuffer);
		ClearArray(_smallBuffer, 4);
		return v;
	}

	[Benchmark(Description = "local_opt3 ReadInt32() (MemoryMarshal.AsBytes + CreateSpan)")]
	public int LocalOpt3_MemoryMarshal()
	{
		var offset = 0;
		int value = default;
		var bytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref value, 1));
		XdrSynthetic.ReadInto(bytes, _source, ref offset, 4);
		return BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(value) : value;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static void ClearArray(byte[] array, int len) {
		array.AsSpan(0, len).Clear();
	}

	//[Benchmark(Description = "local_opt2 ReadInt32() (stackalloc, clear)")]
	public int LocalOpt2_StackC()
	{
		var offset = 0;
		Span<byte> bytes = stackalloc byte[4];
		XdrSynthetic.ReadInto(bytes, _source, ref offset, 4);
		var v = TypeDecoder.DecodeInt32(bytes);
		bytes.Clear();
		return v;
	}

	//[Benchmark(Description = "local_opt2 ReadInt32() renting buffer, clear")]
	public int LocalOpt2_RentC()
	{
		var offset = 0;
		var rented = ArrayPool<byte>.Shared.Rent(4);
		try
		{
			XdrSynthetic.ReadInto(rented.AsSpan(0, 4), _source, ref offset, 4);
			return TypeDecoder.DecodeInt32(rented);
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented, clearArray: true);
		}
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class XdrReadInt64Benchmark
{
	readonly byte[] _smallBuffer = new byte[16];
	readonly byte[] _source = new byte[8];

	[GlobalSetup]
	public void GlobalSetup()
	{
		for (var i = 0; i < _source.Length; i++)
			_source[i] = (byte)(i + 1);
	}

	[Benchmark(Description = "master ReadInt64() using shared _smallBuffer")]
	public long Master()
	{
		var offset = 0;
		XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 8), _source, ref offset, 8);
		return TypeDecoder.DecodeInt64(_smallBuffer);
	}

	[Benchmark(Description = "local_opt2 ReadInt64() (stackalloc)")]
	public long LocalOpt2_Stack()
	{
		var offset = 0;
		Span<byte> bytes = stackalloc byte[8];
		XdrSynthetic.ReadInto(bytes, _source, ref offset, 8);
		return TypeDecoder.DecodeInt64(bytes);
	}

	[Benchmark(Description = "local_opt3 ReadInt64() (MemoryMarshal.AsBytes + CreateSpan)")]
	public long LocalOpt3_MemoryMarshal()
	{
		var offset = 0;
		long value = default;
		var bytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref value, 1));
		XdrSynthetic.ReadInto(bytes, _source, ref offset, 8);
		return BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(value) : value;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static void ClearArray(byte[] array, int len)
	{
		array.AsSpan(0, len).Clear();
	}

	[Benchmark(Description = "local_opt2 ReadInt64() renting buffer")]
	public long LocalOpt2_Rent()
	{
		var offset = 0;
		var rented = ArrayPool<byte>.Shared.Rent(8);
		try
		{
			XdrSynthetic.ReadInto(rented.AsSpan(0, 8), _source, ref offset, 8);
			return TypeDecoder.DecodeInt64(rented);
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented);
		}
	}

	[Benchmark(Description = "local_opt2 ReadInt64() using shared _smallBuffer, clear")]
	public long LocalOpt2_SmallBufferC()
	{
		var offset = 0;
		XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 8), _source, ref offset, 8);
		var v = TypeDecoder.DecodeInt64(_smallBuffer);
		Array.Clear(_smallBuffer, 0, 8);
		return v;
	}

	[Benchmark(Description = "local_opt2 ReadInt64() using shared _smallBuffer, clear with AsSpan")]
	public long LocalOpt2_SmallBufferC2() {
		var offset = 0;
		XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 8), _source, ref offset, 8);
		var v = TypeDecoder.DecodeInt64(_smallBuffer);
		ClearArray(_smallBuffer, 8);
		return v;
	}

	[Benchmark(Description = "local_opt2 ReadInt64() (stackalloc, clear)")]
	public long LocalOpt2_StackC()
	{
		var offset = 0;
		Span<byte> bytes = stackalloc byte[8];
		XdrSynthetic.ReadInto(bytes, _source, ref offset, 8);
		var v = TypeDecoder.DecodeInt64(bytes);
		bytes.Clear();
		return v;
	}

	[Benchmark(Description = "local_opt2 ReadInt64() renting buffer, clear")]
	public long LocalOpt2_RentC()
	{
		var offset = 0;
		var rented = ArrayPool<byte>.Shared.Rent(8);
		try
		{
			XdrSynthetic.ReadInto(rented.AsSpan(0, 8), _source, ref offset, 8);
			return TypeDecoder.DecodeInt64(rented);
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented, clearArray: true);
		}
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class XdrReadInt128Benchmark
{
	readonly byte[] _smallBuffer = new byte[16];
	readonly byte[] _source = new byte[16];

	[GlobalSetup]
	public void GlobalSetup()
	{
		for (var i = 0; i < _source.Length; i++)
			_source[i] = (byte)(i + 1);
	}

	[Benchmark(Description = "master ReadInt128()")]
	public System.Numerics.BigInteger Master()
	{
		var offset = 0;
		var buffer = new byte[16];
		XdrSynthetic.ReadInto(buffer, _source, ref offset, 16);
		return TypeDecoder.DecodeInt128(buffer);
	}

	[Benchmark(Description = "local_opt2 ReadInt128() using shared _smallBuffer")]
	public System.Numerics.BigInteger LocalOpt2_SmallBuffer()
	{
		var offset = 0;
		XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 16), _source, ref offset, 16);
		return TypeDecoder.DecodeInt128(_smallBuffer);
	}

	[Benchmark(Description = "local_opt2 ReadInt128() renting buffer")]
	public System.Numerics.BigInteger LocalOpt2_Rent()
	{
		var offset = 0;
		var rented = XdrSynthetic.Fixed16BytePool.Rent(16);
		try
		{
			XdrSynthetic.ReadInto(rented.AsSpan(0, 16), _source, ref offset, 16);
			return TypeDecoder.DecodeInt128(rented);
		}
		finally
		{
			XdrSynthetic.Fixed16BytePool.Return(rented);
		}
	}

	[Benchmark(Description = "local_opt2 ReadInt128() using shared _smallBuffer, clear")]
	public System.Numerics.BigInteger LocalOpt2_SmallBufferC()
	{
		var offset = 0;
		XdrSynthetic.ReadInto(_smallBuffer.AsSpan(0, 16), _source, ref offset, 16);
		var v = TypeDecoder.DecodeInt128(_smallBuffer);
		Array.Clear(_smallBuffer, 0, 16);
		return v;
	}

	[Benchmark(Description = "local_opt2 ReadInt128() renting buffer, clear")]
	public System.Numerics.BigInteger LocalOpt2_RentC()
	{
		var offset = 0;
		var rented = XdrSynthetic.Fixed16BytePool.Rent(16);
		try
		{
			XdrSynthetic.ReadInto(rented.AsSpan(0, 16), _source, ref offset, 16);
			var v = TypeDecoder.DecodeInt128(rented);
			Array.Clear(rented, 0, 16);
			return v;
		}
		finally
		{
			XdrSynthetic.Fixed16BytePool.Return(rented);
		}
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class XdrWriteInt32Benchmark
{
	int _sink;

	[Params(123456789)]
	public int Value { get; set; }

	[Benchmark(Description = "master Write(int) (alloc)")]
	public int Master()
	{
		var bytes = TypeEncoder.EncodeInt32(Value);
		_sink ^= bytes[0];
		return _sink;
	}

	[Benchmark(Description = "local_opt2 Write(int) (stackalloc)")]
	public int LocalOpt2()
	{
		Span<byte> bytes = stackalloc byte[4];
		TypeEncoder.EncodeInt32(Value, bytes);
		_sink ^= bytes[0];
		return _sink;
	}

	[Benchmark(Description = "local_opt3 Write(int) (MemoryMarshal.AsBytes + CreateSpan)")]
	public int LocalOpt3_MemoryMarshal()
	{
		var v = BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(Value) : Value;
		_sink ^= MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref v, 1))[0];
		return _sink;
	}

	[Benchmark(Description = "local_opt2 Write(int) (rent always)")]
	public int LocalOpt2_RentAlways()
	{
		var rented = ArrayPool<byte>.Shared.Rent(4);
		try
		{
			TypeEncoder.EncodeInt32(Value, rented.AsSpan(0, 4));
			_sink ^= rented[0];
			return _sink;
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented);
		}
	}

	[Benchmark(Description = "local_opt2 Write(int) (stackalloc, clear)")]
	public int LocalOpt2_StackC()
	{
		Span<byte> bytes = stackalloc byte[4];
		TypeEncoder.EncodeInt32(Value, bytes);
		_sink ^= bytes[0];
		bytes.Clear();
		return _sink;
	}

	[Benchmark(Description = "local_opt2 Write(int) (rent always, clear)")]
	public int LocalOpt2_RentAlwaysC()
	{
		var rented = ArrayPool<byte>.Shared.Rent(4);
		try
		{
			TypeEncoder.EncodeInt32(Value, rented.AsSpan(0, 4));
			_sink ^= rented[0];
			return _sink;
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented, clearArray: true);
		}
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class XdrWriteInt64Benchmark
{
	int _sink;

	[Params(1234567890123456789L)]
	public long Value { get; set; }

	[Benchmark(Description = "master Write(long) (alloc)")]
	public int Master()
	{
		var bytes = TypeEncoder.EncodeInt64(Value);
		_sink ^= bytes[0];
		return _sink;
	}

	[Benchmark(Description = "local_opt2 Write(long) (stackalloc)")]
	public int LocalOpt2()
	{
		Span<byte> bytes = stackalloc byte[8];
		TypeEncoder.EncodeInt64(Value, bytes);
		_sink ^= bytes[0];
		return _sink;
	}

	[Benchmark(Description = "local_opt3 Write(long) (MemoryMarshal.AsBytes + CreateSpan)")]
	public int LocalOpt3_MemoryMarshal()
	{
		var v = BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(Value) : Value;
		_sink ^= MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref v, 1))[0];
		return _sink;
	}

	[Benchmark(Description = "local_opt2 Write(long) (rent always)")]
	public int LocalOpt2_RentAlways()
	{
		var rented = ArrayPool<byte>.Shared.Rent(8);
		try
		{
			TypeEncoder.EncodeInt64(Value, rented.AsSpan(0, 8));
			_sink ^= rented[0];
			return _sink;
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented);
		}
	}

	[Benchmark(Description = "local_opt2 Write(long) (stackalloc, clear)")]
	public int LocalOpt2_StackC()
	{
		Span<byte> bytes = stackalloc byte[8];
		TypeEncoder.EncodeInt64(Value, bytes);
		_sink ^= bytes[0];
		bytes.Clear();
		return _sink;
	}

	[Benchmark(Description = "local_opt2 Write(long) (rent always, clear)")]
	public int LocalOpt2_RentAlwaysC()
	{
		var rented = ArrayPool<byte>.Shared.Rent(8);
		try
		{
			TypeEncoder.EncodeInt64(Value, rented.AsSpan(0, 8));
			_sink ^= rented[0];
			return _sink;
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented, clearArray: true);
		}
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class XdrWriteBoolBenchmark
{
	int _sink;

	[Params(true)]
	public bool Value { get; set; }

	static readonly byte[] PadArray = [0, 0, 0, 0];

	[Benchmark(Description = "master Write(bool) (alloc)")]
	public int Master()
	{
		// master Write(bool) => WriteOpaque(EncodeBoolean(value))
		var buffer = TypeEncoder.EncodeBoolean(Value);
		_sink ^= buffer[0];
		_sink ^= PadArray[0]; // simulate WritePad for (4 - 1) & 3
		return _sink;
	}

	[Benchmark(Description = "local_opt2 Write(bool) (stackalloc)")]
	public int LocalOpt2()
	{
		Span<byte> buffer = stackalloc byte[1];
		TypeEncoder.EncodeBoolean(Value, buffer);
		_sink ^= buffer[0];
		_sink ^= PadArray[0]; // simulate WritePad for (4 - 1) & 3
		return _sink;
	}

	[Benchmark(Description = "local_opt2 Write(bool) (rent always)")]
	public int LocalOpt2_RentAlways()
	{
		var rented = ArrayPool<byte>.Shared.Rent(1);
		try
		{
			TypeEncoder.EncodeBoolean(Value, rented.AsSpan(0, 1));
			_sink ^= rented[0];
			_sink ^= PadArray[0]; // simulate WritePad for (4 - 1) & 3
			return _sink;
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented);
		}
	}

	[Benchmark(Description = "local_opt2 Write(bool) (stackalloc, clear)")]
	public int LocalOpt2_StackC()
	{
		Span<byte> buffer = stackalloc byte[1];
		TypeEncoder.EncodeBoolean(Value, buffer);
		_sink ^= buffer[0];
		_sink ^= PadArray[0];
		buffer.Clear();
		return _sink;
	}

	[Benchmark(Description = "local_opt2 Write(bool) (rent always, clear)")]
	public int LocalOpt2_RentAlwaysC()
	{
		var rented = ArrayPool<byte>.Shared.Rent(1);
		try
		{
			TypeEncoder.EncodeBoolean(Value, rented.AsSpan(0, 1));
			_sink ^= rented[0];
			_sink ^= PadArray[0];
			return _sink;
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented, clearArray: true);
		}
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class XdrWriteStringBenchmark
{
	int _sink;

	[Params(16, 8192)]
	public int CharLength { get; set; }

	Charset _charset = Charset.DefaultCharset;
	string _value = string.Empty;

	[GlobalSetup]
	public void GlobalSetup()
	{
		_charset = XdrSynthetic.Utf8Charset;
		_value = CharLength <= 0 ? string.Empty : new string('a', CharLength);
	}

	static readonly byte[] PadArray = [0, 0, 0, 0];

	[Benchmark(Description = "master Write(string) (GetBytes alloc)")]
	public int Master()
	{
		var buffer = _charset.GetBytes(_value);
		var lengthBytes = TypeEncoder.EncodeInt32(buffer.Length);
		_sink ^= lengthBytes[0];
		if (buffer.Length > 0)
			_sink ^= buffer[0];
		_sink ^= PadArray[0]; // simulate WritePad
		return _sink;
	}

	[Benchmark(Description = "local_opt2 Write(string) (stackalloc/rent)")]
	public int LocalOpt2()
	{
		if (string.IsNullOrEmpty(_value))
		{
			Span<byte> lengthBytes = stackalloc byte[4];
			TypeEncoder.EncodeInt32(0, lengthBytes);
			_sink ^= lengthBytes[0];
			return _sink;
		}

		var encoding = _charset.Encoding;
		var maxBytes = encoding.GetMaxByteCount(_value.Length);

		if (maxBytes <= XdrSynthetic.StackallocThreshold)
		{
			Span<byte> span = stackalloc byte[maxBytes];
			var written = encoding.GetBytes(_value.AsSpan(), span);
			Span<byte> lengthBytes = stackalloc byte[4];
			TypeEncoder.EncodeInt32(written, lengthBytes);
			_sink ^= lengthBytes[0];
			_sink ^= span[0];
			_sink ^= PadArray[0]; // simulate WritePad
			return _sink;
		}
		else
		{
			var rented = ArrayPool<byte>.Shared.Rent(maxBytes);
			try
			{
				var written = encoding.GetBytes(_value.AsSpan(), rented.AsSpan());
				Span<byte> lengthBytes = stackalloc byte[4];
				TypeEncoder.EncodeInt32(written, lengthBytes);
				_sink ^= lengthBytes[0];
				_sink ^= rented[0];
				_sink ^= PadArray[0]; // simulate WritePad
				return _sink;
			}
			finally
			{
				ArrayPool<byte>.Shared.Return(rented);
			}
		}
	}

	[Benchmark(Description = "local_opt2 Write(string) (rent always)")]
	public int LocalOpt2_RentAlways()
	{
		if (string.IsNullOrEmpty(_value))
		{
			Span<byte> lengthBytes = stackalloc byte[4];
			TypeEncoder.EncodeInt32(0, lengthBytes);
			_sink ^= lengthBytes[0];
			return _sink;
		}

		var encoding = _charset.Encoding;
		var maxBytes = encoding.GetMaxByteCount(_value.Length);
		var rented = ArrayPool<byte>.Shared.Rent(maxBytes);
		try
		{
			var written = encoding.GetBytes(_value.AsSpan(), rented.AsSpan());
			Span<byte> lengthBytes = stackalloc byte[4];
			TypeEncoder.EncodeInt32(written, lengthBytes);
			_sink ^= lengthBytes[0];
			_sink ^= rented[0];
			_sink ^= PadArray[0]; // simulate WritePad
			return _sink;
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented);
		}
	}

	[Benchmark(Description = "local_opt2 Write(string) (stackalloc/rent, clear)")]
	public int LocalOpt2_C()
	{
		if (string.IsNullOrEmpty(_value))
		{
			Span<byte> lengthBytes = stackalloc byte[4];
			TypeEncoder.EncodeInt32(0, lengthBytes);
			_sink ^= lengthBytes[0];
			lengthBytes.Clear();
			return _sink;
		}

		var encoding = _charset.Encoding;
		var maxBytes = encoding.GetMaxByteCount(_value.Length);

		if (maxBytes <= XdrSynthetic.StackallocThreshold)
		{
			Span<byte> span = stackalloc byte[maxBytes];
			var written = encoding.GetBytes(_value.AsSpan(), span);
			Span<byte> lengthBytes = stackalloc byte[4];
			TypeEncoder.EncodeInt32(written, lengthBytes);
			_sink ^= lengthBytes[0];
			_sink ^= span[0];
			_sink ^= PadArray[0];
			span.Clear();
			lengthBytes.Clear();
			return _sink;
		}
		else
		{
			var rented = ArrayPool<byte>.Shared.Rent(maxBytes);
			try
			{
				var written = encoding.GetBytes(_value.AsSpan(), rented.AsSpan());
				Span<byte> lengthBytes = stackalloc byte[4];
				TypeEncoder.EncodeInt32(written, lengthBytes);
				_sink ^= lengthBytes[0];
				_sink ^= rented[0];
				_sink ^= PadArray[0];
				lengthBytes.Clear();
				return _sink;
			}
			finally
			{
				ArrayPool<byte>.Shared.Return(rented, clearArray: true);
			}
		}
	}

	[Benchmark(Description = "local_opt2 Write(string) (rent always, clear)")]
	public int LocalOpt2_RentAlwaysC()
	{
		if (string.IsNullOrEmpty(_value))
		{
			Span<byte> lengthBytes = stackalloc byte[4];
			TypeEncoder.EncodeInt32(0, lengthBytes);
			_sink ^= lengthBytes[0];
			lengthBytes.Clear();
			return _sink;
		}

		var encoding = _charset.Encoding;
		var maxBytes = encoding.GetMaxByteCount(_value.Length);
		var rented = ArrayPool<byte>.Shared.Rent(maxBytes);
		try
		{
			var written = encoding.GetBytes(_value.AsSpan(), rented.AsSpan());
			Span<byte> lengthBytes = stackalloc byte[4];
			TypeEncoder.EncodeInt32(written, lengthBytes);
			_sink ^= lengthBytes[0];
			_sink ^= rented[0];
			_sink ^= PadArray[0];
			lengthBytes.Clear();
			return _sink;
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented, clearArray: true);
		}
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class XdrWriteGuidBenchmark
{
	int _sink;

	[Params(false, true)]
	public bool Varying { get; set; }

	readonly Guid _value = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");

	static readonly byte[] PadArray = [0, 0, 0, 0];

	[Benchmark(Description = "master Write(Guid,int) (alloc)")]
	public int Master()
	{
		var bytes = TypeEncoder.EncodeGuid(_value);
		if (Varying)
		{
			var lengthBytes = TypeEncoder.EncodeInt32(bytes.Length);
			_sink ^= lengthBytes[0];
		}
		_sink ^= bytes[0];
		_sink ^= PadArray[0];
		return _sink;
	}

	[Benchmark(Description = "local_opt2 Write(Guid,int) (stackalloc)")]
	public int LocalOpt2()
	{
		Span<byte> bytes = stackalloc byte[16];
		TypeEncoder.EncodeGuid(_value, bytes);
		if (Varying)
		{
			Span<byte> lengthBytes = stackalloc byte[4];
			TypeEncoder.EncodeInt32(16, lengthBytes);
			_sink ^= lengthBytes[0];
		}
		_sink ^= bytes[0];
		_sink ^= PadArray[0];
		return _sink;
	}

	[Benchmark(Description = "local_opt2 Write(Guid,int) (rent always)")]
	public int LocalOpt2_RentAlways()
	{
		var rented = ArrayPool<byte>.Shared.Rent(16);
		try
		{
			TypeEncoder.EncodeGuid(_value, rented.AsSpan(0, 16));
			if (Varying)
			{
				Span<byte> lengthBytes = stackalloc byte[4];
				TypeEncoder.EncodeInt32(16, lengthBytes);
				_sink ^= lengthBytes[0];
			}
			_sink ^= rented[0];
			_sink ^= PadArray[0];
			return _sink;
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented);
		}
	}

	[Benchmark(Description = "local_opt2 Write(Guid,int) (stackalloc, clear)")]
	public int LocalOpt2_StackC()
	{
		Span<byte> bytes = stackalloc byte[16];
		TypeEncoder.EncodeGuid(_value, bytes);
		if (Varying)
		{
			Span<byte> lengthBytes = stackalloc byte[4];
			TypeEncoder.EncodeInt32(16, lengthBytes);
			_sink ^= lengthBytes[0];
			lengthBytes.Clear();
		}
		_sink ^= bytes[0];
		_sink ^= PadArray[0];
		bytes.Clear();
		return _sink;
	}

	[Benchmark(Description = "local_opt2 Write(Guid,int) (rent always, clear)")]
	public int LocalOpt2_RentAlwaysC()
	{
		var rented = ArrayPool<byte>.Shared.Rent(16);
		try
		{
			TypeEncoder.EncodeGuid(_value, rented.AsSpan(0, 16));
			if (Varying)
			{
				Span<byte> lengthBytes = stackalloc byte[4];
				TypeEncoder.EncodeInt32(16, lengthBytes);
				_sink ^= lengthBytes[0];
				lengthBytes.Clear();
			}
			_sink ^= rented[0];
			_sink ^= PadArray[0];
			return _sink;
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(rented, clearArray: true);
		}
	}
}
