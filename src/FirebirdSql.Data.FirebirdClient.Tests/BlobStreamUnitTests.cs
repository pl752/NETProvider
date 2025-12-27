using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FirebirdSql.Data.Common;
using NUnit.Framework;

namespace FirebirdSql.Data.FirebirdClient.Tests;

[TestFixture]
public sealed class BlobStreamUnitTests
{
	[Test]
	public void Read_HonorsCountAndDoesNotOverwrite()
	{
		var data = CreateTestData(32);
		var db = new FakeDatabase(packetSize: 8);
		var blob = new FakeBlob(db, data);

		using var stream = new BlobStream(blob);

		var buffer = new byte[64];
		Array.Fill(buffer, (byte)0xCC);

		var read = stream.Read(buffer, offset: 10, count: 5);

		Assert.That(read, Is.EqualTo(5));
		Assert.That(buffer[10], Is.EqualTo(0));
		Assert.That(buffer[14], Is.EqualTo(4));
		Assert.That(buffer[15], Is.EqualTo(0xCC));
	}

	[Test]
	public void Seek_ClearsBufferedSegment()
	{
		var data = CreateTestData(32);
		var db = new FakeDatabase(packetSize: 8);
		var blob = new FakeBlob(db, data);

		using var stream = new BlobStream(blob);

		var first = new byte[3];
		Assert.That(stream.Read(first, 0, first.Length), Is.EqualTo(first.Length));

		stream.Seek(0, SeekOrigin.Begin);

		var second = new byte[3];
		Assert.That(stream.Read(second, 0, second.Length), Is.EqualTo(second.Length));

		CollectionAssert.AreEqual(first, second);
	}

	private static byte[] CreateTestData(int length)
	{
		var data = new byte[length];
		for (var i = 0; i < data.Length; i++)
			data[i] = (byte)i;
		return data;
	}

	private sealed class FakeDatabase : DatabaseBase
	{
		public FakeDatabase(int packetSize)
			: base(Charset.DefaultCharset, packetSize, dialect: 3)
		{
		}

		public override bool UseUtf8ParameterBuffer => false;
		public override int Handle => 0;
		public override bool HasRemoteEventSupport => false;
		public override bool ConnectionBroken => false;

		public override void Attach(DatabaseParameterBufferBase dpb, string database, byte[] cryptKey) => throw new NotSupportedException();
		public override ValueTask AttachAsync(DatabaseParameterBufferBase dpb, string database, byte[] cryptKey, CancellationToken cancellationToken = default) => throw new NotSupportedException();
		public override void AttachWithTrustedAuth(DatabaseParameterBufferBase dpb, string database, byte[] cryptKey) => throw new NotSupportedException();
		public override ValueTask AttachWithTrustedAuthAsync(DatabaseParameterBufferBase dpb, string database, byte[] cryptKey, CancellationToken cancellationToken = default) => throw new NotSupportedException();
		public override void Detach() => throw new NotSupportedException();
		public override ValueTask DetachAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
		public override void CreateDatabase(DatabaseParameterBufferBase dpb, string database, byte[] cryptKey) => throw new NotSupportedException();
		public override ValueTask CreateDatabaseAsync(DatabaseParameterBufferBase dpb, string database, byte[] cryptKey, CancellationToken cancellationToken = default) => throw new NotSupportedException();
		public override void CreateDatabaseWithTrustedAuth(DatabaseParameterBufferBase dpb, string database, byte[] cryptKey) => throw new NotSupportedException();
		public override ValueTask CreateDatabaseWithTrustedAuthAsync(DatabaseParameterBufferBase dpb, string database, byte[] cryptKey, CancellationToken cancellationToken = default) => throw new NotSupportedException();
		public override void DropDatabase() => throw new NotSupportedException();
		public override ValueTask DropDatabaseAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
		public override TransactionBase BeginTransaction(TransactionParameterBuffer tpb) => throw new NotSupportedException();
		public override ValueTask<TransactionBase> BeginTransactionAsync(TransactionParameterBuffer tpb, CancellationToken cancellationToken = default) => throw new NotSupportedException();
		public override StatementBase CreateStatement() => throw new NotSupportedException();
		public override StatementBase CreateStatement(TransactionBase transaction) => throw new NotSupportedException();
		public override DatabaseParameterBufferBase CreateDatabaseParameterBuffer() => throw new NotSupportedException();
		public override EventParameterBuffer CreateEventParameterBuffer() => throw new NotSupportedException();
		public override TransactionParameterBuffer CreateTransactionParameterBuffer() => throw new NotSupportedException();
		public override List<object> GetDatabaseInfo(byte[] items) => throw new NotSupportedException();
		public override ValueTask<List<object>> GetDatabaseInfoAsync(byte[] items, CancellationToken cancellationToken = default) => throw new NotSupportedException();
		public override List<object> GetDatabaseInfo(byte[] items, int bufferLength) => throw new NotSupportedException();
		public override ValueTask<List<object>> GetDatabaseInfoAsync(byte[] items, int bufferLength, CancellationToken cancellationToken = default) => throw new NotSupportedException();
		public override void CloseEventManager() => throw new NotSupportedException();
		public override ValueTask CloseEventManagerAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
		public override void QueueEvents(RemoteEvent events) => throw new NotSupportedException();
		public override ValueTask QueueEventsAsync(RemoteEvent events, CancellationToken cancellationToken = default) => throw new NotSupportedException();
		public override void CancelEvents(RemoteEvent events) => throw new NotSupportedException();
		public override ValueTask CancelEventsAsync(RemoteEvent events, CancellationToken cancellationToken = default) => throw new NotSupportedException();
		public override void CancelOperation(short kind) => throw new NotSupportedException();
		public override ValueTask CancelOperationAsync(short kind, CancellationToken cancellationToken = default) => throw new NotSupportedException();
	}

	private sealed class FakeBlob : BlobBase
	{
		private readonly DatabaseBase _database;
		private readonly byte[] _data;
		private int _readPosition;

		public FakeBlob(DatabaseBase database, byte[] data)
			: base(database)
		{
			_database = database;
			_data = data ?? throw new ArgumentNullException(nameof(data));
			_readPosition = 0;
		}

		public override int Handle => 0;
		public override DatabaseBase Database => _database;

		public override void Create() => throw new NotSupportedException();
		public override ValueTask CreateAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

		public override void Open()
		{
			_isOpen = true;
			RblRemoveValue(IscCodes.RBL_eof_pending);
			RblRemoveValue(IscCodes.RBL_segment);
			_readPosition = _position;
		}
		public override ValueTask OpenAsync(CancellationToken cancellationToken = default)
		{
			Open();
			return ValueTask.CompletedTask;
		}

		public override int GetLength() => _data.Length;
		public override ValueTask<int> GetLengthAsync(CancellationToken cancellationToken = default) => ValueTask.FromResult(_data.Length);

		public override byte[] GetSegment()
		{
			using var ms = new MemoryStream();
			GetSegment(ms);
			return ms.ToArray();
		}
		public override ValueTask<byte[]> GetSegmentAsync(CancellationToken cancellationToken = default) => ValueTask.FromResult(GetSegment());

		public override void GetSegment(Stream stream)
		{
			RblRemoveValue(IscCodes.RBL_segment);
			if (_readPosition >= _data.Length)
			{
				RblAddValue(IscCodes.RBL_eof_pending);
				return;
			}

			var toCopy = Math.Min(SegmentSize, _data.Length - _readPosition);
			stream.Write(_data.AsSpan(_readPosition, toCopy));

			_readPosition += toCopy;
			_position = _readPosition;
			RblAddValue(IscCodes.RBL_segment);

			if (_readPosition >= _data.Length)
			{
				RblAddValue(IscCodes.RBL_eof_pending);
			}
		}
		public override ValueTask GetSegmentAsync(Stream stream, CancellationToken cancellationToken = default)
		{
			GetSegment(stream);
			return ValueTask.CompletedTask;
		}

		public override void PutSegment(byte[] buffer) => throw new NotSupportedException();
		public override ValueTask PutSegmentAsync(byte[] buffer, CancellationToken cancellationToken = default) => throw new NotSupportedException();
		public override void PutSegment(byte[] buffer, int offset, int count) => throw new NotSupportedException();
		public override ValueTask PutSegmentAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default) => throw new NotSupportedException();

		public override void Seek(int offset, int seekMode)
		{
			var newPosition = seekMode switch
			{
				IscCodes.isc_blb_seek_from_head => offset,
				IscCodes.isc_blb_seek_relative => _readPosition + offset,
				IscCodes.isc_blb_seek_from_tail => _data.Length + offset,
				_ => throw new ArgumentOutOfRangeException(nameof(seekMode))
			};

			newPosition = Math.Clamp(newPosition, 0, _data.Length);
			_readPosition = newPosition;
			_position = newPosition;
			RblRemoveValue(IscCodes.RBL_eof_pending);
			RblRemoveValue(IscCodes.RBL_segment);
		}
		public override ValueTask SeekAsync(int offset, int seekMode, CancellationToken cancellationToken = default)
		{
			Seek(offset, seekMode);
			return ValueTask.CompletedTask;
		}

		public override void Close() => _isOpen = false;
		public override ValueTask CloseAsync(CancellationToken cancellationToken = default)
		{
			Close();
			return ValueTask.CompletedTask;
		}

		public override void Cancel() => Close();
		public override ValueTask CancelAsync(CancellationToken cancellationToken = default)
		{
			Cancel();
			return ValueTask.CompletedTask;
		}
	}
}

