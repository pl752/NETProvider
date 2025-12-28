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
using System.Threading;
using System.Threading.Tasks;

namespace FirebirdSql.Data.Client.Managed;

sealed class PooledWriteBuffer : IDataProvider, IDisposable
{
	private const int DefaultInitialCapacity = 256;

	private byte[] _buffer;
	private int _position;
	private bool _disposed;

	public int WrittenCount => _position;
	public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _position);

	public PooledWriteBuffer(int initialCapacity = DefaultInitialCapacity)
	{
		if (initialCapacity <= 0)
			throw new ArgumentOutOfRangeException(nameof(initialCapacity));
		_buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
		_position = 0;
		_disposed = false;
	}

	public void Reset()
	{
		_position = 0;
	}

	public void Dispose()
	{
		if (_disposed)
			return;
		_disposed = true;
		ArrayPool<byte>.Shared.Return(_buffer);
		_buffer = Array.Empty<byte>();
		_position = 0;
	}

	void EnsureNotDisposed()
	{
		if (_disposed)
			throw new ObjectDisposedException(nameof(PooledWriteBuffer));
	}

	void EnsureCapacity(int additionalBytes)
	{
		if (additionalBytes <= 0)
			return;

		var required = checked(_position + additionalBytes);
		if (required <= _buffer.Length)
			return;

		var newCapacity = _buffer.Length;
		while (newCapacity < required)
		{
			newCapacity *= 2;
		}

		var newBuffer = ArrayPool<byte>.Shared.Rent(newCapacity);
		_buffer.AsSpan(0, _position).CopyTo(newBuffer);
		ArrayPool<byte>.Shared.Return(_buffer);
		_buffer = newBuffer;
	}

	public void Write(ReadOnlySpan<byte> buffer)
	{
		EnsureNotDisposed();
		if (buffer.IsEmpty)
			return;

		EnsureCapacity(buffer.Length);
		buffer.CopyTo(_buffer.AsSpan(_position));
		_position += buffer.Length;
	}

	public void Write(byte[] buffer, int offset, int count)
	{
		if (buffer == null)
			throw new ArgumentNullException(nameof(buffer));
		Write(buffer.AsSpan(offset, count));
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
		EnsureNotDisposed();
	}

	public ValueTask FlushAsync(CancellationToken cancellationToken = default)
	{
		EnsureNotDisposed();
		return ValueTask.CompletedTask;
	}

	public int Read(byte[] buffer, int offset, int count)
	{
		throw new NotSupportedException();
	}

	public int Read(Span<byte> buffer, int offset, int count)
	{
		throw new NotSupportedException();
	}

	public ValueTask<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default)
	{
		throw new NotSupportedException();
	}

	public ValueTask<int> ReadAsync(Memory<byte> buffer, int offset, int count, CancellationToken cancellationToken = default)
	{
		throw new NotSupportedException();
	}
}

