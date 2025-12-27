using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace FirebirdSql.Data.Common;

public sealed class BlobStream : Stream
{
	private readonly BlobBase _blobHandle;
	private int _position;

	private byte[] _segmentBuffer;
	private int _segmentLength;
	private int _segmentPosition;
	private readonly SegmentWriteStream _segmentWriteStream;

	private int Available => _segmentLength - _segmentPosition;

	internal BlobStream(BlobBase blob)
	{
		_blobHandle = blob;
		_position = 0;
		_segmentWriteStream = new SegmentWriteStream(this);
	}

	public override long Position
	{
		get => _position;
		set => Seek(value, SeekOrigin.Begin);
	}

	public override long Length
	{
		get
		{
			if (!_blobHandle.IsOpen)
				_blobHandle.Open();

			return _blobHandle.GetLength();
		}
	}

	public override void Flush()
	{
	}

	public override int Read(byte[] buffer, int offset, int count)
	{
		ValidateBufferSize(buffer, offset, count);
		if (count == 0)
			return 0;

		if (!_blobHandle.IsOpen)
			_blobHandle.Open();

		var copied = 0;
		while (copied < count)
		{
			if (Available > 0)
			{
				var toCopy = Math.Min(Available, count - copied);
				Array.Copy(_segmentBuffer, _segmentPosition, buffer, offset + copied, toCopy);
				copied += toCopy;
				_segmentPosition += toCopy;
				_position += toCopy;
				continue;
			}

			if (_blobHandle.EOF)
				break;

			FillSegmentBuffer();
		}

		return copied;
	}
	public override async Task<int> ReadAsync(byte[] buffer, int offset, int count,
		CancellationToken cancellationToken)
	{
		ValidateBufferSize(buffer, offset, count);
		if (count == 0)
			return 0;

		if (!_blobHandle.IsOpen)
			await _blobHandle.OpenAsync(cancellationToken).ConfigureAwait(false);

		var copied = 0;
		while (copied < count)
		{
			if (Available > 0)
			{
				var toCopy = Math.Min(Available, count - copied);
				Array.Copy(_segmentBuffer, _segmentPosition, buffer, offset + copied, toCopy);
				copied += toCopy;
				_segmentPosition += toCopy;
				_position += toCopy;
				continue;
			}

			if (_blobHandle.EOF)
				break;

			await FillSegmentBufferAsync(cancellationToken).ConfigureAwait(false);
		}

		return copied;
	}

	public override long Seek(long offset, SeekOrigin origin)
	{
		if (!_blobHandle.IsOpen)
			_blobHandle.Open();

		var seekMode = origin switch
		{
			SeekOrigin.Begin => IscCodes.isc_blb_seek_from_head,
			SeekOrigin.Current => IscCodes.isc_blb_seek_relative,
			SeekOrigin.End => IscCodes.isc_blb_seek_from_tail,
			_ => throw new ArgumentOutOfRangeException(nameof(origin))
		};

		_blobHandle.Seek((int)offset, seekMode);
		_segmentLength = 0;
		_segmentPosition = 0;
		return _position = _blobHandle.Position;
	}

	public override void SetLength(long value)
	{
		throw new NotSupportedException();
	}

	public override void Write(byte[] buffer, int offset, int count)
	{
		ValidateBufferSize(buffer, offset, count);
		if (count == 0)
			return;

		try
		{
			if (!_blobHandle.IsOpen)
				_blobHandle.Create();

			while (count > 0)
			{
				var chunk = count >= _blobHandle.SegmentSize ? _blobHandle.SegmentSize : count;
				_blobHandle.PutSegment(buffer, offset, chunk);

				offset += chunk;
				count -= chunk;
				_position += chunk;
			}
		}
		catch
		{
			_blobHandle.Cancel();
			throw;
		}
	}
	public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
	{
		ValidateBufferSize(buffer, offset, count);
		if (count == 0)
			return;

		try
		{
			if (!_blobHandle.IsOpen)
				await _blobHandle.CreateAsync(cancellationToken).ConfigureAwait(false);

			while (count > 0)
			{
				var chunk = count >= _blobHandle.SegmentSize ? _blobHandle.SegmentSize : count;
				await _blobHandle.PutSegmentAsync(buffer, offset, chunk, cancellationToken).ConfigureAwait(false);

				offset += chunk;
				count -= chunk;
				_position += chunk;
			}
		}
		catch
		{
			await _blobHandle.CancelAsync(cancellationToken).ConfigureAwait(false);
			throw;
		}
	}

	public override bool CanRead => true;
	public override bool CanSeek => true;
	public override bool CanWrite => true;

	protected override void Dispose(bool disposing)
	{
		try
		{
			_blobHandle.Close();
		}
		finally
		{
			ReturnSegmentBuffer();
		}
	}

	public override ValueTask DisposeAsync()
	{
		return DisposeAsyncCore();
	}

	private async ValueTask DisposeAsyncCore()
	{
		try
		{
			await _blobHandle.CloseAsync().ConfigureAwait(false);
		}
		finally
		{
			ReturnSegmentBuffer();
		}
	}

	private void EnsureSegmentBufferCapacity(int requiredCapacity, int bytesWritten)
	{
		if (_segmentBuffer == null)
		{
			var initialCapacity = Math.Max(requiredCapacity, _blobHandle.SegmentSize * 2);
			_segmentBuffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
			return;
		}

		if (_segmentBuffer.Length >= requiredCapacity)
			return;

		var newBuffer = ArrayPool<byte>.Shared.Rent(requiredCapacity);
		Array.Copy(_segmentBuffer, 0, newBuffer, 0, bytesWritten);
		ArrayPool<byte>.Shared.Return(_segmentBuffer, clearArray: true);
		_segmentBuffer = newBuffer;
	}

	private void ReturnSegmentBuffer()
	{
		if (_segmentBuffer == null)
			return;
		ArrayPool<byte>.Shared.Return(_segmentBuffer, clearArray: true);
		_segmentBuffer = null;
		_segmentLength = 0;
		_segmentPosition = 0;
	}

	private void FillSegmentBuffer()
	{
		_segmentWriteStream.Reset();
		_blobHandle.GetSegment(_segmentWriteStream);
		_segmentLength = _segmentWriteStream.Written;
		_segmentPosition = 0;
	}

	private async ValueTask FillSegmentBufferAsync(CancellationToken cancellationToken)
	{
		_segmentWriteStream.Reset();
		await _blobHandle.GetSegmentAsync(_segmentWriteStream, cancellationToken).ConfigureAwait(false);
		_segmentLength = _segmentWriteStream.Written;
		_segmentPosition = 0;
	}

	private sealed class SegmentWriteStream : Stream
	{
		private readonly BlobStream _owner;
		private int _written;

		public int Written => _written;

		public SegmentWriteStream(BlobStream owner)
		{
			_owner = owner;
		}

		public void Reset()
		{
			_written = 0;
			_owner.EnsureSegmentBufferCapacity(_owner._blobHandle.SegmentSize * 2, bytesWritten: 0);
		}

		public override void Write(ReadOnlySpan<byte> buffer)
		{
			_owner.EnsureSegmentBufferCapacity(_written + buffer.Length, _written);
			buffer.CopyTo(_owner._segmentBuffer.AsSpan(_written));
			_written += buffer.Length;
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			Write(buffer.AsSpan(offset, count));
		}

		public override void Flush()
		{
		}

		public override bool CanRead => false;
		public override bool CanSeek => false;
		public override bool CanWrite => true;
		public override long Length => throw new NotSupportedException();
		public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
		public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
		public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
		public override void SetLength(long value) => throw new NotSupportedException();
	}

	private static void ValidateBufferSize(byte[] buffer, int offset, int count)
	{
		if (buffer is null)
			throw new ArgumentNullException(nameof(buffer));

		if (offset < 0)
			throw new ArgumentOutOfRangeException(nameof(offset));

		if (count < 0)
			throw new ArgumentOutOfRangeException(nameof(count));

		if (buffer.Length - offset < count)
			throw new ArgumentException(null, nameof(buffer));
	}
}
