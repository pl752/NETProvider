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
using System.Collections;
using System.IO;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using FirebirdSql.Data.Common;

namespace FirebirdSql.Data.Client.Managed.Version13;

internal class GdsStatement : Version12.GdsStatement
{
	const int STACKALLOC_LIMIT = 1024;
	byte[] _nullBitmapBuffer;

	#region Constructors

	public GdsStatement(GdsDatabase database)
		: base(database)
	{ }

	public GdsStatement(GdsDatabase database, Version10.GdsTransaction transaction)
		: base(database, transaction)
	{ }

	#endregion

	#region Overriden Methods

	byte[] EnsureNullBitmapBuffer(int length)
	{
		if (_nullBitmapBuffer == null || _nullBitmapBuffer.Length < length)
		{
			_nullBitmapBuffer = new byte[length];
		}
		return _nullBitmapBuffer;
	}

	protected override void WriteParametersTo(IXdrWriter xdr)
	{
		if (_parameters == null)
			return;

		try
		{
			var count = _parameters.Count;
			var bytesLen = (count + 7) / 8;
			byte[] rented = null;
			Span<byte> buffer = bytesLen > STACKALLOC_LIMIT
				? (rented = ArrayPool<byte>.Shared.Rent(bytesLen)).AsSpan(0, bytesLen)
				: stackalloc byte[bytesLen];
			buffer.Clear();
			for (var i = 0; i < count; i++)
			{
				if (_parameters[i].DbValue.IsDBNull())
				{
					buffer[i / 8] |= (byte)(1 << (i % 8));
				}
			}
			xdr.WriteOpaque(buffer);
			if (rented != null)
			{
				ArrayPool<byte>.Shared.Return(rented);
			}

			for (var i = 0; i < _parameters.Count; i++)
			{
				var field = _parameters[i];
				if (field.DbValue.IsDBNull())
				{
					continue;
				}
				WriteRawParameter(xdr, field);
			}
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}

	protected override DbValue[] ReadRow()
	{
		var row = _fields.Count > 0 ? new DbValue[_fields.Count] : Array.Empty<DbValue>();
		try
		{
			if (_fields.Count > 0)
			{
				var len = (_fields.Count + 7) / 8;
				Span<byte> nullBitmap = len > STACKALLOC_LIMIT
					? EnsureNullBitmapBuffer(len).AsSpan(0, len)
					: stackalloc byte[len];

				_database.Xdr.ReadOpaque(nullBitmap, len);
				for (var i = 0; i < _fields.Count; i++)
				{
					var isNull = (nullBitmap[i / 8] & (1 << (i % 8))) != 0;
					if (isNull)
					{
						row[i] = new DbValue(this, _fields[i], null);
					}
					else
					{
						var value = ReadRawValue(_database.Xdr, _fields[i]);
						row[i] = new DbValue(this, _fields[i], value);
					}
				}
			}
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
		return row;
	}
	protected override async ValueTask<DbValue[]> ReadRowAsync(CancellationToken cancellationToken = default)
	{
		var row = _fields.Count > 0 ? new DbValue[_fields.Count] : Array.Empty<DbValue>();
		try
		{
			if (_fields.Count > 0)
			{
				var len = (_fields.Count + 7) / 8;
				var nullBitmap = EnsureNullBitmapBuffer(len);

				await _database.Xdr.ReadOpaqueAsync(nullBitmap.AsMemory(0, len), len, cancellationToken).ConfigureAwait(false);
				for (var i = 0; i < _fields.Count; i++)
				{
					var isNull = (nullBitmap[i / 8] & (1 << (i % 8))) != 0;
					if (isNull)
					{
						row[i] = new DbValue(this, _fields[i], null);
					}
					else
					{
						var value = await ReadRawValueAsync(_database.Xdr, _fields[i], cancellationToken).ConfigureAwait(false);
						row[i] = new DbValue(this, _fields[i], value);
					}
				}
			}
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
		return row;
	}

	protected override DbValueStorage[] ReadRowStorage()
	{
		var row = RentRowStorage();
		try
		{
			if (_fields.Count > 0)
			{
				var len = (_fields.Count + 7) / 8;
				Span<byte> nullBitmap = len > STACKALLOC_LIMIT
					? EnsureNullBitmapBuffer(len).AsSpan(0, len)
					: stackalloc byte[len];

				_database.Xdr.ReadOpaque(nullBitmap, len);
				for (var i = 0; i < _fields.Count; i++)
				{
					var isNull = (nullBitmap[i / 8] & (1 << (i % 8))) != 0;
					if (isNull)
					{
						row[i] = default;
					}
					else
					{
						row[i] = ReadRawValueStorage(_database.Xdr, _fields[i]);
					}
				}
			}
		}
		catch (IOException ex)
		{
			ReturnRowStorage(row);
			throw IscException.ForIOException(ex);
		}
		return row;
	}

	protected override async ValueTask<DbValueStorage[]> ReadRowStorageAsync(CancellationToken cancellationToken = default)
	{
		var row = RentRowStorage();
		try
		{
			if (_fields.Count > 0)
			{
				var len = (_fields.Count + 7) / 8;
				var nullBitmap = EnsureNullBitmapBuffer(len);

				await _database.Xdr.ReadOpaqueAsync(nullBitmap.AsMemory(0, len), len, cancellationToken).ConfigureAwait(false);
				for (var i = 0; i < _fields.Count; i++)
				{
					var isNull = (nullBitmap[i / 8] & (1 << (i % 8))) != 0;
					if (isNull)
					{
						row[i] = default;
					}
					else
					{
						row[i] = await ReadRawValueStorageAsync(_database.Xdr, _fields[i], cancellationToken).ConfigureAwait(false);
					}
				}
			}
		}
		catch (IOException ex)
		{
			ReturnRowStorage(row);
			throw IscException.ForIOException(ex);
		}
		return row;
	}

	#endregion
}
