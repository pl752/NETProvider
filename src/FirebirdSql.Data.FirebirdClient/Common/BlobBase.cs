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

//$Authors = Carlos Guzman Alvarez, Jiri Cincura (jiri@cincura.net)

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace FirebirdSql.Data.Common;

internal abstract class BlobBase
{
	private int _rblFlags;
	private Charset _charset;
	private int _segmentSize;

	protected long _blobId;
	protected bool _isOpen;
	protected int _position;
	protected TransactionBase _transaction;

	public abstract int Handle { get; }
	public long Id => _blobId;
	public bool EOF => (_rblFlags & IscCodes.RBL_eof_pending) != 0;
	public bool IsOpen => _isOpen;

	public int SegmentSize => _segmentSize;
	public int Position => _position;

	public abstract DatabaseBase Database { get; }

	protected BlobBase(DatabaseBase db)
	{
		_segmentSize = db.PacketSize;
		_charset = db.Charset;
	}

	public string ReadString()
	{
		var buffer = Read();
		return _charset.GetString(buffer, 0, buffer.Length);
	}
	public async ValueTask<string> ReadStringAsync(CancellationToken cancellationToken = default)
	{
		var buffer = await ReadAsync(cancellationToken).ConfigureAwait(false);
		return _charset.GetString(buffer, 0, buffer.Length);
	}

	public byte[] Read()
	{
		try
		{
			Open();

			var length = GetLength();
			if (length == 0)
			{
				Close();
				return Array.Empty<byte>();
			}
			if (length < 0)
				throw new IOException($"Invalid blob length: {length}.");

			var result = GC.AllocateUninitializedArray<byte>(length);
			var output = new FixedLengthStream(result);

			while (!EOF)
			{
				GetSegment(output);
			}

			Close();

			if (output.Written == length)
				return result;

			var trimmed = new byte[output.Written];
			Buffer.BlockCopy(result, 0, trimmed, 0, output.Written);
			return trimmed;
		}
		catch
		{
			// Cancel the blob and rethrow the exception
			Cancel();
			throw;
		}
	}
	public async ValueTask<byte[]> ReadAsync(CancellationToken cancellationToken = default)
	{
		try
		{
			await OpenAsync(cancellationToken).ConfigureAwait(false);

			var length = await GetLengthAsync(cancellationToken).ConfigureAwait(false);
			if (length == 0)
			{
				await CloseAsync(cancellationToken).ConfigureAwait(false);
				return Array.Empty<byte>();
			}
			if (length < 0)
				throw new IOException($"Invalid blob length: {length}.");

			var result = GC.AllocateUninitializedArray<byte>(length);
			var output = new FixedLengthStream(result);

			while (!EOF)
			{
				await GetSegmentAsync(output, cancellationToken).ConfigureAwait(false);
			}

			await CloseAsync(cancellationToken).ConfigureAwait(false);

			if (output.Written == length)
				return result;

			var trimmed = new byte[output.Written];
			Buffer.BlockCopy(result, 0, trimmed, 0, output.Written);
			return trimmed;
		}
		catch
		{
			// Cancel the blob and rethrow the exception
			await CancelAsync(cancellationToken).ConfigureAwait(false);
			throw;
		}
	}

	public void Write(string data)
	{
		Write(_charset.GetBytes(data));
	}
	public ValueTask WriteAsync(string data, CancellationToken cancellationToken = default)
	{
		return WriteAsync(_charset.GetBytes(data), cancellationToken);
	}

	public void Write(byte[] buffer)
	{
		Write(buffer, 0, buffer.Length);
	}
	public ValueTask WriteAsync(byte[] buffer, CancellationToken cancellationToken = default)
	{
		return WriteAsync(buffer, 0, buffer.Length, cancellationToken);
	}

	public void Write(byte[] buffer, int index, int count)
	{
		try
		{
			Create();

			var length = count;
			var offset = index;
			while (length > 0)
			{
				var chunk = length >= _segmentSize ? _segmentSize : length;
				PutSegment(buffer, offset, chunk);

				offset += chunk;
				length -= chunk;
			}

			Close();
		}
		catch
		{
			// Cancel the blob and rethrow the exception
			Cancel();

			throw;
		}
	}
	public async ValueTask WriteAsync(byte[] buffer, int index, int count, CancellationToken cancellationToken = default)
	{
		try
		{
			await CreateAsync(cancellationToken).ConfigureAwait(false);

			var length = count;
			var offset = index;
			while (length > 0)
			{
				var chunk = length >= _segmentSize ? _segmentSize : length;
				await PutSegmentAsync(buffer, offset, chunk, cancellationToken).ConfigureAwait(false);

				offset += chunk;
				length -= chunk;
			}

			await CloseAsync(cancellationToken).ConfigureAwait(false);
		}
		catch
		{
			// Cancel the blob and rethrow the exception
			await CancelAsync(cancellationToken).ConfigureAwait(false);

			throw;
		}
	}

	public abstract void Create();
	public abstract ValueTask CreateAsync(CancellationToken cancellationToken = default);

	public abstract void Open();
	public abstract ValueTask OpenAsync(CancellationToken cancellationToken = default);

	public abstract int GetLength();
	public abstract ValueTask<int> GetLengthAsync(CancellationToken cancellationToken = default);

	public abstract byte[] GetSegment();
	public abstract ValueTask<byte[]> GetSegmentAsync(CancellationToken cancellationToken = default);

	public abstract void GetSegment(Stream stream);
	public abstract ValueTask GetSegmentAsync(Stream stream, CancellationToken cancellationToken = default);

	public abstract void PutSegment(byte[] buffer);
	public abstract ValueTask PutSegmentAsync(byte[] buffer, CancellationToken cancellationToken = default);

	public abstract void PutSegment(byte[] buffer, int offset, int count);
	public abstract ValueTask PutSegmentAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default);

	public abstract void Seek(int offset, int seekMode);
	public abstract ValueTask SeekAsync(int offset, int seekMode, CancellationToken cancellationToken = default);

	public abstract void Close();
	public abstract ValueTask CloseAsync(CancellationToken cancellationToken = default);

	public abstract void Cancel();
	public abstract ValueTask CancelAsync(CancellationToken cancellationToken = default);

	private sealed class FixedLengthStream : Stream
	{
		private readonly byte[] _buffer;
		private int _position;

		public int Written => _position;

		public FixedLengthStream(byte[] buffer)
		{
			_buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
			_position = 0;
		}

		public override void Write(ReadOnlySpan<byte> buffer)
		{
			if (buffer.IsEmpty)
				return;

			if (_position > _buffer.Length - buffer.Length)
				throw new IOException("Blob length mismatch.");

			buffer.CopyTo(_buffer.AsSpan(_position));
			_position += buffer.Length;
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			Write(buffer.AsSpan(offset, count));
		}

		public override void Flush()
		{ }

		public override bool CanRead => false;
		public override bool CanSeek => false;
		public override bool CanWrite => true;
		public override long Length => _buffer.Length;
		public override long Position { get => _position; set => throw new NotSupportedException(); }
		public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
		public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
		public override void SetLength(long value) => throw new NotSupportedException();
	}

	protected void RblAddValue(int rblValue)
	{
		_rblFlags |= rblValue;
	}

	protected void RblRemoveValue(int rblValue)
	{
		_rblFlags &= ~rblValue;
	}
}
