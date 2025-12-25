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
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using FirebirdSql.Data.Client.Managed;
using Org.BouncyCastle.Crypto.Engines;
using Org.BouncyCastle.Crypto.Parameters;

namespace Perf;

[Config(typeof(InProcessMemoryConfig))]
public class FirebirdNetworkHandlingWrapperReadBenchmark
{
	const int TotalBytesPerInvoke = 256 * 1024;

	[Params(32, 8 * 1024, 64 * 1024)]
	public int ChunkSize { get; set; }

	[Params(false, true)]
	public bool Encrypted { get; set; }

	byte[] _key = Array.Empty<byte>();
	byte[] _plain = Array.Empty<byte>();
	byte[] _wire = Array.Empty<byte>();
	byte[] _buffer = Array.Empty<byte>();

	BenchmarkDataProvider _providerLegacy = null!;
	BenchmarkDataProvider _providerRing = null!;
	FirebirdNetworkHandlingWrapperLegacy _legacy = null!;
	FirebirdNetworkHandlingWrapper _ring = null!;

	[GlobalSetup]
	public void GlobalSetup()
	{
		_key = new byte[16];
		for (var i = 0; i < _key.Length; i++)
			_key[i] = (byte)(i + 1);

		_plain = new byte[TotalBytesPerInvoke];
		for (var i = 0; i < _plain.Length; i++)
			_plain[i] = (byte)(i * 31 + 7);

		_wire = new byte[_plain.Length];
		Buffer.BlockCopy(_plain, 0, _wire, 0, _plain.Length);
		if (Encrypted)
		{
			var cipher = new RC4Engine();
			cipher.Init(forEncryption: false, new KeyParameter(_key));
			cipher.ProcessBytes(_wire, 0, _wire.Length, _wire, 0);
		}

		_buffer = new byte[ChunkSize];

		_providerLegacy = new BenchmarkDataProvider(_wire);
		_providerRing = new BenchmarkDataProvider(_wire);
		_legacy = new FirebirdNetworkHandlingWrapperLegacy(_providerLegacy);
		_ring = new FirebirdNetworkHandlingWrapper(_providerRing);

		if (Encrypted)
		{
			_legacy.StartEncryption(_key);
			_ring.StartEncryption(_key);
		}
	}

	[Benchmark(Baseline = true, Description = "legacy ReadBytes()")]
	public int Legacy()
	{
		var checksum = 0;
		var chunks = TotalBytesPerInvoke / ChunkSize;
		for (var i = 0; i < chunks; i++)
		{
			ReadFully(_legacy, _buffer, ChunkSize);
			checksum ^= _buffer[0];
		}
		return checksum;
	}

	[Benchmark(Description = "ring+bypass ReadBytes()")]
	public int Ring()
	{
		var checksum = 0;
		var chunks = TotalBytesPerInvoke / ChunkSize;
		for (var i = 0; i < chunks; i++)
		{
			ReadFully(_ring, _buffer, ChunkSize);
			checksum ^= _buffer[0];
		}
		return checksum;
	}

	static void ReadFully(IDataProvider provider, byte[] buffer, int count)
	{
		var toRead = count;
		var offset = 0;
		while (toRead > 0)
		{
			var read = provider.Read(buffer, offset, toRead);
			if (read == 0)
				throw new IOException($"Missing {toRead} bytes to fill total {count}.");
			offset += read;
			toRead -= read;
		}
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class FirebirdNetworkHandlingWrapperWriteBenchmark
{
	const int TotalBytesPerInvoke = 256 * 1024;

	[Params(32, 8 * 1024, 64 * 1024)]
	public int ChunkSize { get; set; }

	[Params(false, true)]
	public bool Encrypted { get; set; }

	byte[] _key = Array.Empty<byte>();
	byte[] _payload = Array.Empty<byte>();

	BenchmarkDataProvider _providerLegacy = null!;
	BenchmarkDataProvider _providerRing = null!;
	FirebirdNetworkHandlingWrapperLegacy _legacy = null!;
	FirebirdNetworkHandlingWrapper _ring = null!;

	[GlobalSetup]
	public void GlobalSetup()
	{
		_key = new byte[16];
		for (var i = 0; i < _key.Length; i++)
			_key[i] = (byte)(i + 1);

		_payload = new byte[ChunkSize];
		for (var i = 0; i < _payload.Length; i++)
			_payload[i] = (byte)(i * 13 + 3);

		_providerLegacy = new BenchmarkDataProvider(Array.Empty<byte>());
		_providerRing = new BenchmarkDataProvider(Array.Empty<byte>());
		_legacy = new FirebirdNetworkHandlingWrapperLegacy(_providerLegacy);
		_ring = new FirebirdNetworkHandlingWrapper(_providerRing);

		if (Encrypted)
		{
			_legacy.StartEncryption(_key);
			_ring.StartEncryption(_key);
		}
	}

	[Benchmark(Baseline = true, Description = "legacy Write()+Flush()")]
	public long Legacy()
	{
		var writes = TotalBytesPerInvoke / ChunkSize;
		for (var i = 0; i < writes; i++)
		{
			_legacy.Write(_payload, 0, _payload.Length);
		}
		_legacy.Flush();
		return _providerLegacy.BytesWritten;
	}

	[Benchmark(Description = "ring+bypass Write()+Flush()")]
	public long Ring()
	{
		var writes = TotalBytesPerInvoke / ChunkSize;
		for (var i = 0; i < writes; i++)
		{
			_ring.Write(_payload, 0, _payload.Length);
		}
		_ring.Flush();
		return _providerRing.BytesWritten;
	}
}

sealed class BenchmarkDataProvider : IDataProvider
{
	readonly byte[] _data;
	int _index;

	public long BytesWritten { get; private set; }

	public BenchmarkDataProvider(byte[] data)
	{
		_data = data ?? Array.Empty<byte>();
	}

	public void Reset()
	{
		_index = 0;
		BytesWritten = 0;
	}

	public void ResetCounters()
	{
		BytesWritten = 0;
	}

	public int Read(byte[] buffer, int offset, int count)
	{
		if (count <= 0)
			return 0;

		if (_data.Length == 0)
			return 0;

		var data = _data;
		var index = _index;
		var first = Math.Min(count, data.Length - index);
		Buffer.BlockCopy(data, index, buffer, offset, first);
		index += first;
		if (index == data.Length)
			index = 0;

		var remaining = count - first;
		if (remaining > 0)
		{
			while (remaining > 0)
			{
				var chunk = Math.Min(remaining, data.Length);
				Buffer.BlockCopy(data, 0, buffer, offset + (count - remaining), chunk);
				remaining -= chunk;
				index = chunk == data.Length ? 0 : chunk;
			}
		}

		_index = index;
		return count;
	}

	public int Read(Span<byte> buffer, int offset, int count)
	{
		if (count <= 0)
			return 0;

		if (_data.Length == 0)
			return 0;

		var data = _data;
		var index = _index;
		var first = Math.Min(count, data.Length - index);
		data.AsSpan(index, first).CopyTo(buffer.Slice(offset, first));
		index += first;
		if (index == data.Length)
			index = 0;

		var remaining = count - first;
		if (remaining > 0)
		{
			while (remaining > 0)
			{
				var chunk = Math.Min(remaining, data.Length);
				data.AsSpan(0, chunk).CopyTo(buffer.Slice(offset + (count - remaining), chunk));
				remaining -= chunk;
				index = chunk == data.Length ? 0 : chunk;
			}
		}

		_index = index;
		return count;
	}

	public ValueTask<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default)
	{
		return new ValueTask<int>(Read(buffer, offset, count));
	}

	public ValueTask<int> ReadAsync(Memory<byte> buffer, int offset, int count, CancellationToken cancellationToken = default)
	{
		return new ValueTask<int>(Read(buffer.Span, offset, count));
	}

	public void Write(ReadOnlySpan<byte> buffer)
	{
		BytesWritten += buffer.Length;
	}

	public void Write(byte[] buffer, int offset, int count)
	{
		BytesWritten += count;
	}

	public ValueTask WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default)
	{
		BytesWritten += count;
		return ValueTask.CompletedTask;
	}

	public ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, int offset, int count, CancellationToken cancellationToken = default)
	{
		BytesWritten += count;
		return ValueTask.CompletedTask;
	}

	public void Flush()
	{
	}

	public ValueTask FlushAsync(CancellationToken cancellationToken = default)
	{
		return ValueTask.CompletedTask;
	}
}

sealed class FirebirdNetworkHandlingWrapperLegacy : IDataProvider, ITracksIOFailure
{
	const int PreferredBufferSize = 32 * 1024;

	readonly IDataProvider _dataProvider;

	readonly Queue<byte> _outputBuffer;
	readonly Queue<byte> _inputBuffer;
	readonly byte[] _readBuffer;

	byte[] _compressionBuffer = Array.Empty<byte>();
	Ionic.Zlib.ZlibCodec _compressor;
	Ionic.Zlib.ZlibCodec _decompressor;

	RC4Engine _decryptor;
	RC4Engine _encryptor;

	public FirebirdNetworkHandlingWrapperLegacy(IDataProvider dataProvider)
	{
		_dataProvider = dataProvider;

		_outputBuffer = new Queue<byte>(PreferredBufferSize);
		_inputBuffer = new Queue<byte>(PreferredBufferSize);
		_readBuffer = new byte[PreferredBufferSize];
	}

	public bool IOFailed { get; set; }

	public int Read(byte[] buffer, int offset, int count)
	{
		if (_inputBuffer.Count < count)
		{
			var readBuffer = _readBuffer;
			int read;
			try
			{
				read = _dataProvider.Read(readBuffer, 0, readBuffer.Length);
			}
			catch (IOException)
			{
				IOFailed = true;
				throw;
			}
			if (read != 0)
			{
				if (_decryptor != null)
				{
					_decryptor.ProcessBytes(readBuffer, 0, read, readBuffer, 0);
				}
				if (_decompressor != null)
				{
					read = HandleDecompression(readBuffer, read);
					readBuffer = _compressionBuffer;
				}
				WriteToInputBuffer(readBuffer, read);
			}
		}
		return ReadFromInputBuffer(buffer, offset, count);
	}

	public int Read(Span<byte> buffer, int offset, int count)
	{
		if (_inputBuffer.Count < count)
		{
			var readBuffer = _readBuffer;
			int read;
			try
			{
				read = _dataProvider.Read(readBuffer, 0, readBuffer.Length);
			}
			catch (IOException)
			{
				IOFailed = true;
				throw;
			}
			if (read != 0)
			{
				if (_decryptor != null)
				{
					_decryptor.ProcessBytes(readBuffer, 0, read, readBuffer, 0);
				}
				if (_decompressor != null)
				{
					read = HandleDecompression(readBuffer, read);
					readBuffer = _compressionBuffer;
				}
				WriteToInputBuffer(readBuffer, read);
			}
		}
		return ReadFromInputBuffer(buffer, offset, count);
	}

	public ValueTask<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default)
	{
		return new ValueTask<int>(Read(buffer, offset, count));
	}

	public ValueTask<int> ReadAsync(Memory<byte> buffer, int offset, int count, CancellationToken cancellationToken = default)
	{
		return new ValueTask<int>(Read(buffer.Span, offset, count));
	}

	public void Write(ReadOnlySpan<byte> buffer)
	{
		foreach (var b in buffer)
			_outputBuffer.Enqueue(b);
	}

	public void Write(byte[] buffer, int offset, int count)
	{
		if (buffer == null || count <= 0)
			return;
		for (var i = 0; i < count; i++)
			_outputBuffer.Enqueue(buffer[offset + i]);
	}

	public ValueTask WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default)
	{
		Write(buffer, offset, count);
		return ValueTask.CompletedTask;
	}

	public ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, int offset, int count, CancellationToken cancellationToken = default)
	{
		Write(buffer.Span.Slice(offset, count));
		return ValueTask.CompletedTask;
	}

	public void Flush()
	{
		var buffer = _outputBuffer.ToArray();
		_outputBuffer.Clear();
		var count = buffer.Length;
		if (_compressor != null)
		{
			count = HandleCompression(buffer, count);
			buffer = _compressionBuffer;
		}
		if (_encryptor != null)
		{
			_encryptor.ProcessBytes(buffer, 0, count, buffer, 0);
		}
		try
		{
			_dataProvider.Write(buffer, 0, count);
			_dataProvider.Flush();
		}
		catch (IOException)
		{
			IOFailed = true;
			throw;
		}
	}

	public ValueTask FlushAsync(CancellationToken cancellationToken = default)
	{
		Flush();
		return ValueTask.CompletedTask;
	}

	public void StartCompression()
	{
		_compressionBuffer = new byte[PreferredBufferSize];
		_compressor = new Ionic.Zlib.ZlibCodec(Ionic.Zlib.CompressionMode.Compress);
		_decompressor = new Ionic.Zlib.ZlibCodec(Ionic.Zlib.CompressionMode.Decompress);
	}

	public void StartEncryption(byte[] key)
	{
		_encryptor = CreateCipher(key);
		_decryptor = CreateCipher(key);
	}

	int ReadFromInputBuffer(byte[] buffer, int offset, int count)
	{
		var read = Math.Min(count, _inputBuffer.Count);
		for (var i = 0; i < read; i++)
		{
			buffer[offset + i] = _inputBuffer.Dequeue();
		}
		return read;
	}

	int ReadFromInputBuffer(Span<byte> buffer, int offset, int count)
	{
		var read = Math.Min(count, _inputBuffer.Count);
		for (var i = 0; i < read; i++)
		{
			buffer[offset + i] = _inputBuffer.Dequeue();
		}
		return read;
	}

	void WriteToInputBuffer(byte[] data, int count)
	{
		for (var i = 0; i < count; i++)
		{
			_inputBuffer.Enqueue(data[i]);
		}
	}

	int HandleDecompression(byte[] buffer, int count)
	{
		_decompressor.InputBuffer = buffer;
		_decompressor.NextOut = 0;
		_decompressor.NextIn = 0;
		_decompressor.AvailableBytesIn = count;
		while (true)
		{
			_decompressor.OutputBuffer = _compressionBuffer;
			_decompressor.AvailableBytesOut = _compressionBuffer.Length - _decompressor.NextOut;
			var rc = _decompressor.Inflate(Ionic.Zlib.FlushType.None);
			if (rc != Ionic.Zlib.ZlibConstants.Z_OK)
				throw new IOException($"Error '{rc}' while decompressing the data.");
			if (_decompressor.AvailableBytesIn > 0 || _decompressor.AvailableBytesOut == 0)
			{
				ResizeBuffer(ref _compressionBuffer);
				continue;
			}
			break;
		}
		return _decompressor.NextOut;
	}

	int HandleCompression(byte[] buffer, int count)
	{
		_compressor.InputBuffer = buffer;
		_compressor.NextOut = 0;
		_compressor.NextIn = 0;
		_compressor.AvailableBytesIn = count;
		while (true)
		{
			_compressor.OutputBuffer = _compressionBuffer;
			_compressor.AvailableBytesOut = _compressionBuffer.Length - _compressor.NextOut;
			var rc = _compressor.Deflate(Ionic.Zlib.FlushType.None);
			if (rc != Ionic.Zlib.ZlibConstants.Z_OK)
				throw new IOException($"Error '{rc}' while compressing the data.");
			if (_compressor.AvailableBytesIn > 0 || _compressor.AvailableBytesOut == 0)
			{
				ResizeBuffer(ref _compressionBuffer);
				continue;
			}
			break;
		}
		while (true)
		{
			_compressor.OutputBuffer = _compressionBuffer;
			_compressor.AvailableBytesOut = _compressionBuffer.Length - _compressor.NextOut;
			var rc = _compressor.Deflate(Ionic.Zlib.FlushType.Sync);
			if (rc != Ionic.Zlib.ZlibConstants.Z_OK)
				throw new IOException($"Error '{rc}' while compressing the data.");
			if (_compressor.AvailableBytesIn > 0 || _compressor.AvailableBytesOut == 0)
			{
				ResizeBuffer(ref _compressionBuffer);
				continue;
			}
			break;
		}
		return _compressor.NextOut;
	}

	static void ResizeBuffer(ref byte[] buffer)
	{
		Array.Resize(ref buffer, buffer.Length * 2);
	}

	static RC4Engine CreateCipher(byte[] key)
	{
		var cipher = new RC4Engine();
		cipher.Init(forEncryption: false, new KeyParameter(key));
		return cipher;
	}
}
