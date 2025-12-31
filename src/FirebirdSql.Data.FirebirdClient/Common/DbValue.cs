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
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.Types;

namespace FirebirdSql.Data.Common;

internal sealed class DbValue
{
	private StatementBase _statement;
	private DbField _field;
	private DbValueKind _kind;
	private DbValueBuffer16 _data;
	private object _object;

	public DbField Field
	{
		get { return _field; }
	}

	public DbValue(DbField field, object value)
	{
		_field = field;
		SetValue(value);
	}

	public DbValue(StatementBase statement, DbField field, object value)
	{
		_statement = statement;
		_field = field;
		SetValue(value);
	}

	public bool IsDBNull()
	{
		return _kind == DbValueKind.DbNull;
	}

	public object GetValue()
	{
		if (IsDBNull())
		{
			return DBNull.Value;
		}

		switch (_field.DbDataType)
		{
			case DbDataType.Text:
				if (_statement == null)
				{
					return GetInt64();
				}
				else
				{
					return GetString();
				}

			case DbDataType.Binary:
				if (_statement == null)
				{
					return GetInt64();
				}
				else
				{
					return GetBinary();
				}

			case DbDataType.Array:
				if (_statement == null)
				{
					return GetInt64();
				}
				else
				{
					return GetArray();
				}

			default:
				return GetValueCore();
		}
	}
	public async ValueTask<object> GetValueAsync(CancellationToken cancellationToken = default)
	{
		if (IsDBNull())
		{
			return DBNull.Value;
		}

		switch (_field.DbDataType)
		{
			case DbDataType.Text:
				if (_statement == null)
				{
					return GetInt64();
				}
				else
				{
					return await GetStringAsync(cancellationToken).ConfigureAwait(false);
				}

			case DbDataType.Binary:
				if (_statement == null)
				{
					return GetInt64();
				}
				else
				{
					return await GetBinaryAsync(cancellationToken).ConfigureAwait(false);
				}

			case DbDataType.Array:
				if (_statement == null)
				{
					return GetInt64();
				}
				else
				{
					return await GetArrayAsync(cancellationToken).ConfigureAwait(false);
				}

			default:
				return GetValueCore();
		}
	}

	public void SetValue(object value)
	{
		if (value == null || value is DBNull)
		{
			SetDBNull();
			return;
		}

		switch (value)
		{
			case bool b:
				SetValue(b);
				break;
			case byte b:
				SetValue(b);
				break;
			case short s:
				SetValue(s);
				break;
			case int i:
				SetValue(i);
				break;
			case long l:
				SetValue(l);
				break;
			case float f:
				SetValue(f);
				break;
			case double d:
				SetValue(d);
				break;
			case decimal dec:
				SetValue(dec);
				break;
			case Guid guid:
				SetValue(guid);
				break;
			case DateTime dt:
				SetValue(dt);
				break;
			case TimeSpan ts:
				SetValue(ts);
				break;
			case string s:
				SetValue(s);
				break;
			case byte[] bytes:
				SetValue(bytes);
				break;
			case FbZonedDateTime zdt:
				SetValue(zdt);
				break;
			case FbZonedTime zt:
				SetValue(zt);
				break;
			default:
				_kind = DbValueKind.Object;
				_data = default;
				_object = value;
				break;
		}
	}

	public void SetValue(bool value)
	{
		_kind = DbValueKind.Boolean;
		_data = new DbValueBuffer16 { Lo = value ? 1UL : 0UL };
		_object = null;
	}

	public void SetValue(byte value)
	{
		_kind = DbValueKind.Byte;
		_data = new DbValueBuffer16 { Lo = value };
		_object = null;
	}

	public void SetValue(short value)
	{
		_kind = DbValueKind.Int16;
		_data = new DbValueBuffer16 { Lo = unchecked((ulong)value) };
		_object = null;
	}

	public void SetValue(int value)
	{
		_kind = DbValueKind.Int32;
		_data = new DbValueBuffer16 { Lo = unchecked((ulong)value) };
		_object = null;
	}

	public void SetValue(long value)
	{
		_kind = DbValueKind.Int64;
		_data = new DbValueBuffer16 { Lo = unchecked((ulong)value) };
		_object = null;
	}

	public void SetValue(float value)
	{
		_kind = DbValueKind.Single;
		_data = new DbValueBuffer16 { Lo = unchecked((ulong)(uint)BitConverter.SingleToInt32Bits(value)) };
		_object = null;
	}

	public void SetValue(double value)
	{
		_kind = DbValueKind.Double;
		_data = new DbValueBuffer16 { Lo = unchecked((ulong)BitConverter.DoubleToInt64Bits(value)) };
		_object = null;
	}

	public void SetValue(decimal value)
	{
		_kind = DbValueKind.Decimal;
		_object = null;
		MemoryMarshal.Write(_data.AsBytes(), in value);
	}

	public void SetValue(Guid value)
	{
		_kind = DbValueKind.Guid;
		_object = null;
		MemoryMarshal.Write(_data.AsBytes(), in value);
	}

	public void SetValue(DateTime value)
	{
		_kind = DbValueKind.DateTime;
		_data = new DbValueBuffer16 { Lo = unchecked((ulong)value.ToBinary()) };
		_object = null;
	}

	public void SetValue(TimeSpan value)
	{
		_kind = DbValueKind.TimeSpan;
		_data = new DbValueBuffer16 { Lo = unchecked((ulong)value.Ticks) };
		_object = null;
	}

	public void SetValue(string value)
	{
		if (value == null)
		{
			SetDBNull();
			return;
		}
		_kind = DbValueKind.String;
		_data = default;
		_object = value;
	}

	public void SetValue(byte[] value)
	{
		if (value == null)
		{
			SetDBNull();
			return;
		}
		_kind = DbValueKind.Bytes;
		_data = default;
		_object = value;
	}

	public void SetValue(FbZonedDateTime value)
	{
		_kind = value.Offset.HasValue ? DbValueKind.ZonedDateTimeEx : DbValueKind.ZonedDateTime;
		_data = new DbValueBuffer16
		{
			Lo = unchecked((ulong)value.DateTime.ToBinary()),
			Hi = unchecked((ulong)(value.Offset?.Ticks ?? 0)),
		};
		_object = value.TimeZone;
	}

	public void SetValue(FbZonedTime value)
	{
		_kind = value.Offset.HasValue ? DbValueKind.ZonedTimeEx : DbValueKind.ZonedTime;
		_data = new DbValueBuffer16
		{
			Lo = unchecked((ulong)value.Time.Ticks),
			Hi = unchecked((ulong)(value.Offset?.Ticks ?? 0)),
		};
		_object = value.TimeZone;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void SetInt128BigEndian(ReadOnlySpan<byte> bytes)
	{
		if (bytes.Length != 16)
			throw new ArgumentOutOfRangeException(nameof(bytes));
		_kind = DbValueKind.Int128;
		_object = null;
		_data = default;
		bytes.CopyTo(_data.AsBytes());
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void SetInt128LittleEndian(ReadOnlySpan<byte> bytes)
	{
		if (bytes.Length != 16)
			throw new ArgumentOutOfRangeException(nameof(bytes));
		_kind = DbValueKind.Int128;
		_object = null;
		_data = default;
		var dst = _data.AsBytes();
		for (var i = 0; i < 16; i++)
		{
			dst[i] = bytes[15 - i];
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void SetDec16BigEndian(ReadOnlySpan<byte> bytes)
	{
		if (bytes.Length != 8)
			throw new ArgumentOutOfRangeException(nameof(bytes));
		_kind = DbValueKind.Dec16;
		_object = null;
		_data = default;
		bytes.CopyTo(_data.AsBytes());
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void SetDec16LittleEndian(ReadOnlySpan<byte> bytes)
	{
		if (bytes.Length != 8)
			throw new ArgumentOutOfRangeException(nameof(bytes));
		_kind = DbValueKind.Dec16;
		_object = null;
		_data = default;
		var dst = _data.AsBytes();
		for (var i = 0; i < 8; i++)
		{
			dst[i] = bytes[7 - i];
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void SetDec34BigEndian(ReadOnlySpan<byte> bytes)
	{
		if (bytes.Length != 16)
			throw new ArgumentOutOfRangeException(nameof(bytes));
		_kind = DbValueKind.Dec34;
		_object = null;
		_data = default;
		bytes.CopyTo(_data.AsBytes());
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void SetDec34LittleEndian(ReadOnlySpan<byte> bytes)
	{
		if (bytes.Length != 16)
			throw new ArgumentOutOfRangeException(nameof(bytes));
		_kind = DbValueKind.Dec34;
		_object = null;
		_data = default;
		var dst = _data.AsBytes();
		for (var i = 0; i < 16; i++)
		{
			dst[i] = bytes[15 - i];
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal DbValueStorage ExportStorage()
	{
		return new DbValueStorage { Kind = _kind, Data = _data, Object = _object };
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void ImportStorage(in DbValueStorage storage)
	{
		_kind = storage.Kind;
		_data = storage.Data;
		_object = storage.Object;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void Reset(StatementBase statement, DbField field)
	{
		_statement = statement;
		_field = field;
		SetDBNull();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void SetDBNull()
	{
		_kind = DbValueKind.DbNull;
		_data = default;
		_object = null;
	}

	object GetValueCore()
	{
		if (_kind == DbValueKind.Int128)
		{
			GetInt128();
			return _object;
		}
		if (_kind == DbValueKind.Dec16 || _kind == DbValueKind.Dec34)
		{
			GetDecFloat();
			return _object;
		}

		return _kind switch
		{
			DbValueKind.DbNull => DBNull.Value,
			DbValueKind.Boolean => GetBoolean(),
			DbValueKind.Byte => GetByte(),
			DbValueKind.Int16 => GetInt16(),
			DbValueKind.Int32 => GetInt32(),
			DbValueKind.Int64 => GetInt64(),
			DbValueKind.Single => GetFloat(),
			DbValueKind.Double => GetDouble(),
			DbValueKind.Decimal => GetDecimal(),
			DbValueKind.Guid => GetGuid(),
			DbValueKind.DateTime => GetDateTime(),
			DbValueKind.TimeSpan => GetTimeSpan(),
			DbValueKind.String => _object,
			DbValueKind.Bytes => _object,
			DbValueKind.ZonedDateTime => GetZonedDateTime(),
			DbValueKind.ZonedDateTimeEx => GetZonedDateTime(),
			DbValueKind.ZonedTime => GetZonedTime(),
			DbValueKind.ZonedTimeEx => GetZonedTime(),
			_ => _object,
		};
	}

	public string GetString()
	{
		if (Field.DbDataType == DbDataType.Text && _kind == DbValueKind.Int64)
		{
			var l = GetInt64();
			_object = GetClobData(l);
			_kind = DbValueKind.String;
			_data = default;
		}

		if (_kind == DbValueKind.Bytes && _object is byte[] bytes)
		{
			return Field.Charset.GetString(bytes);
		}
		if (_kind == DbValueKind.String && _object is string s)
		{
			return s;
		}
		return GetValueCore().ToString();
	}
	public async ValueTask<string> GetStringAsync(CancellationToken cancellationToken = default)
	{
		if (Field.DbDataType == DbDataType.Text && _kind == DbValueKind.Int64)
		{
			var l = GetInt64();
			_object = await GetClobDataAsync(l, cancellationToken).ConfigureAwait(false);
			_kind = DbValueKind.String;
			_data = default;
		}

		if (_kind == DbValueKind.Bytes && _object is byte[] bytes)
		{
			return Field.Charset.GetString(bytes);
		}
		if (_kind == DbValueKind.String && _object is string s)
		{
			return s;
		}
		return GetValueCore().ToString();
	}

	public char GetChar()
	{
		return Convert.ToChar(GetValueCore(), CultureInfo.CurrentCulture);
	}

	public bool GetBoolean()
	{
		return _kind switch
		{
			DbValueKind.Boolean => _data.Lo != 0,
			DbValueKind.Byte => Convert.ToBoolean((byte)_data.Lo),
			DbValueKind.Int16 => Convert.ToBoolean(unchecked((short)_data.Lo)),
			DbValueKind.Int32 => Convert.ToBoolean(unchecked((int)_data.Lo)),
			DbValueKind.Int64 => Convert.ToBoolean(unchecked((long)_data.Lo)),
			DbValueKind.Decimal => Convert.ToBoolean(GetDecimal()),
			DbValueKind.Single => Convert.ToBoolean(BitConverter.Int32BitsToSingle(unchecked((int)_data.Lo))),
			DbValueKind.Double => Convert.ToBoolean(BitConverter.Int64BitsToDouble(unchecked((long)_data.Lo))),
			DbValueKind.String => Convert.ToBoolean((string)_object, CultureInfo.InvariantCulture),
			DbValueKind.Object => Convert.ToBoolean(_object, CultureInfo.InvariantCulture),
			_ => Convert.ToBoolean(DBNull.Value, CultureInfo.InvariantCulture),
		};
	}

	public byte GetByte()
	{
		return _kind switch
		{
			DbValueKind.Byte => (byte)_data.Lo,
			DbValueKind.Int16 => Convert.ToByte(unchecked((short)_data.Lo)),
			DbValueKind.Int32 => Convert.ToByte(unchecked((int)_data.Lo)),
			DbValueKind.Int64 => Convert.ToByte(unchecked((long)_data.Lo)),
			DbValueKind.Decimal => Convert.ToByte(GetDecimal()),
			DbValueKind.Single => Convert.ToByte(BitConverter.Int32BitsToSingle(unchecked((int)_data.Lo))),
			DbValueKind.Double => Convert.ToByte(BitConverter.Int64BitsToDouble(unchecked((long)_data.Lo))),
			DbValueKind.Int128 => (byte)GetInt128(),
			DbValueKind.String => Convert.ToByte((string)_object, CultureInfo.InvariantCulture),
			DbValueKind.Object => _object switch
			{
				BigInteger bi => (byte)bi,
				_ => Convert.ToByte(_object, CultureInfo.InvariantCulture),
			},
			_ => Convert.ToByte(DBNull.Value, CultureInfo.InvariantCulture),
		};
	}

	public short GetInt16()
	{
		return _kind switch
		{
			DbValueKind.Int16 => unchecked((short)_data.Lo),
			DbValueKind.Byte => Convert.ToInt16((byte)_data.Lo),
			DbValueKind.Int32 => Convert.ToInt16(unchecked((int)_data.Lo)),
			DbValueKind.Int64 => Convert.ToInt16(unchecked((long)_data.Lo)),
			DbValueKind.Decimal => Convert.ToInt16(GetDecimal()),
			DbValueKind.Single => Convert.ToInt16(BitConverter.Int32BitsToSingle(unchecked((int)_data.Lo))),
			DbValueKind.Double => Convert.ToInt16(BitConverter.Int64BitsToDouble(unchecked((long)_data.Lo))),
			DbValueKind.Int128 => (short)GetInt128(),
			DbValueKind.String => Convert.ToInt16((string)_object, CultureInfo.InvariantCulture),
			DbValueKind.Object => _object switch
			{
				BigInteger bi => (short)bi,
				_ => Convert.ToInt16(_object, CultureInfo.InvariantCulture),
			},
			_ => Convert.ToInt16(DBNull.Value, CultureInfo.InvariantCulture),
		};
	}

	public int GetInt32()
	{
		return _kind switch
		{
			DbValueKind.Int32 => unchecked((int)_data.Lo),
			DbValueKind.Int16 => Convert.ToInt32(unchecked((short)_data.Lo)),
			DbValueKind.Byte => Convert.ToInt32((byte)_data.Lo),
			DbValueKind.Int64 => Convert.ToInt32(unchecked((long)_data.Lo)),
			DbValueKind.Decimal => Convert.ToInt32(GetDecimal()),
			DbValueKind.Single => Convert.ToInt32(BitConverter.Int32BitsToSingle(unchecked((int)_data.Lo))),
			DbValueKind.Double => Convert.ToInt32(BitConverter.Int64BitsToDouble(unchecked((long)_data.Lo))),
			DbValueKind.Int128 => (int)GetInt128(),
			DbValueKind.String => Convert.ToInt32((string)_object, CultureInfo.InvariantCulture),
			DbValueKind.Object => _object switch
			{
				BigInteger bi => (int)bi,
				_ => Convert.ToInt32(_object, CultureInfo.InvariantCulture),
			},
			_ => Convert.ToInt32(DBNull.Value, CultureInfo.InvariantCulture),
		};
	}

	public long GetInt64()
	{
		return _kind switch
		{
			DbValueKind.Int64 => unchecked((long)_data.Lo),
			DbValueKind.Int32 => Convert.ToInt64(unchecked((int)_data.Lo)),
			DbValueKind.Int16 => Convert.ToInt64(unchecked((short)_data.Lo)),
			DbValueKind.Byte => Convert.ToInt64((byte)_data.Lo),
			DbValueKind.Decimal => Convert.ToInt64(GetDecimal()),
			DbValueKind.Single => Convert.ToInt64(BitConverter.Int32BitsToSingle(unchecked((int)_data.Lo))),
			DbValueKind.Double => Convert.ToInt64(BitConverter.Int64BitsToDouble(unchecked((long)_data.Lo))),
			DbValueKind.Int128 => (long)GetInt128(),
			DbValueKind.String => Convert.ToInt64((string)_object, CultureInfo.InvariantCulture),
			DbValueKind.Object => _object switch
			{
				BigInteger bi => (long)bi,
				_ => Convert.ToInt64(_object, CultureInfo.InvariantCulture),
			},
			_ => Convert.ToInt64(DBNull.Value, CultureInfo.InvariantCulture),
		};
	}

	public decimal GetDecimal()
	{
		return _kind switch
		{
			DbValueKind.Decimal => MemoryMarshal.Read<decimal>(_data.AsBytes()),
			DbValueKind.Int16 => Convert.ToDecimal(unchecked((short)_data.Lo)),
			DbValueKind.Int32 => Convert.ToDecimal(unchecked((int)_data.Lo)),
			DbValueKind.Int64 => Convert.ToDecimal(unchecked((long)_data.Lo)),
			DbValueKind.Byte => Convert.ToDecimal((byte)_data.Lo),
			DbValueKind.Single => Convert.ToDecimal(BitConverter.Int32BitsToSingle(unchecked((int)_data.Lo))),
			DbValueKind.Double => Convert.ToDecimal(BitConverter.Int64BitsToDouble(unchecked((long)_data.Lo))),
			DbValueKind.String => Convert.ToDecimal((string)_object, CultureInfo.InvariantCulture),
			DbValueKind.Object => Convert.ToDecimal(_object, CultureInfo.InvariantCulture),
			_ => Convert.ToDecimal(DBNull.Value, CultureInfo.InvariantCulture),
		};
	}

	public float GetFloat()
	{
		return _kind switch
		{
			DbValueKind.Single => BitConverter.Int32BitsToSingle(unchecked((int)_data.Lo)),
			DbValueKind.Double => Convert.ToSingle(BitConverter.Int64BitsToDouble(unchecked((long)_data.Lo))),
			DbValueKind.Decimal => Convert.ToSingle(GetDecimal()),
			DbValueKind.Int16 => Convert.ToSingle(unchecked((short)_data.Lo)),
			DbValueKind.Int32 => Convert.ToSingle(unchecked((int)_data.Lo)),
			DbValueKind.Int64 => Convert.ToSingle(unchecked((long)_data.Lo)),
			DbValueKind.Byte => Convert.ToSingle((byte)_data.Lo),
			DbValueKind.String => Convert.ToSingle((string)_object, CultureInfo.InvariantCulture),
			DbValueKind.Object => Convert.ToSingle(_object, CultureInfo.InvariantCulture),
			_ => Convert.ToSingle(DBNull.Value, CultureInfo.InvariantCulture),
		};
	}

	public Guid GetGuid()
	{
		return _kind switch
		{
			DbValueKind.Guid => MemoryMarshal.Read<Guid>(_data.AsBytes()),
			DbValueKind.Bytes when _object is byte[] bytes => TypeDecoder.DecodeGuid(bytes),
			DbValueKind.Object when _object is Guid guid => guid,
			DbValueKind.Object when _object is byte[] bytes => TypeDecoder.DecodeGuid(bytes),
			_ => throw new InvalidOperationException($"Incorrect {nameof(Guid)} value."),
		};
	}

	public double GetDouble()
	{
		return _kind switch
		{
			DbValueKind.Double => BitConverter.Int64BitsToDouble(unchecked((long)_data.Lo)),
			DbValueKind.Single => Convert.ToDouble(BitConverter.Int32BitsToSingle(unchecked((int)_data.Lo))),
			DbValueKind.Decimal => Convert.ToDouble(GetDecimal()),
			DbValueKind.Int16 => Convert.ToDouble(unchecked((short)_data.Lo)),
			DbValueKind.Int32 => Convert.ToDouble(unchecked((int)_data.Lo)),
			DbValueKind.Int64 => Convert.ToDouble(unchecked((long)_data.Lo)),
			DbValueKind.Byte => Convert.ToDouble((byte)_data.Lo),
			DbValueKind.String => Convert.ToDouble((string)_object, CultureInfo.InvariantCulture),
			DbValueKind.Int128 => (double)GetInt128(),
			DbValueKind.Object => Convert.ToDouble(_object, CultureInfo.InvariantCulture),
			_ => Convert.ToDouble(DBNull.Value, CultureInfo.InvariantCulture),
		};
	}

	public DateTime GetDateTime()
	{
		return _kind switch
		{
			DbValueKind.DateTime => DateTime.FromBinary(unchecked((long)_data.Lo)),
			DbValueKind.ZonedDateTime => DateTime.FromBinary(unchecked((long)_data.Lo)),
			DbValueKind.ZonedDateTimeEx => DateTime.FromBinary(unchecked((long)_data.Lo)),
			DbValueKind.String => Convert.ToDateTime((string)_object, CultureInfo.CurrentCulture.DateTimeFormat),
			DbValueKind.Object => _object switch
			{
				DateTimeOffset dto => dto.DateTime,
				FbZonedDateTime zdt => zdt.DateTime,
				DateTime dt => dt,
				_ => Convert.ToDateTime(_object, CultureInfo.CurrentCulture.DateTimeFormat),
			},
			_ => Convert.ToDateTime(DBNull.Value, CultureInfo.CurrentCulture.DateTimeFormat),
		};
	}

	public TimeSpan GetTimeSpan()
	{
		return _kind switch
		{
			DbValueKind.TimeSpan => new TimeSpan(unchecked((long)_data.Lo)),
			DbValueKind.ZonedTime => new TimeSpan(unchecked((long)_data.Lo)),
			DbValueKind.ZonedTimeEx => new TimeSpan(unchecked((long)_data.Lo)),
			DbValueKind.Object => _object switch
			{
				TimeSpan ts => ts,
				FbZonedTime zt => zt.Time,
				_ => (TimeSpan)_object,
			},
			_ => (TimeSpan)GetValueCore(),
		};
	}

	public FbDecFloat GetDecFloat()
	{
		if (_kind == DbValueKind.Dec16)
		{
			var tmp = new byte[8];
			_data.AsBytes().Slice(0, 8).CopyTo(tmp);
			var value = TypeDecoder.DecodeDec16(tmp);
			_kind = DbValueKind.Object;
			_data = default;
			_object = value;
			return value;
		}
		if (_kind == DbValueKind.Dec34)
		{
			var tmp = new byte[16];
			_data.AsBytes().CopyTo(tmp);
			var value = TypeDecoder.DecodeDec34(tmp);
			_kind = DbValueKind.Object;
			_data = default;
			_object = value;
			return value;
		}
		if (_kind == DbValueKind.Object && _object is FbDecFloat fbDecFloat)
		{
			return fbDecFloat;
		}
		return (FbDecFloat)GetValueCore();
	}

	public BigInteger GetInt128()
	{
		if (_kind == DbValueKind.Int128)
		{
			var bytes = _data.AsBytes();
			var value = new BigInteger(bytes, isUnsigned: false, isBigEndian: true);
			_kind = DbValueKind.Object;
			_data = default;
			_object = value;
			return value;
		}
		return _kind switch
		{
			DbValueKind.Byte => (byte)_data.Lo,
			DbValueKind.Int16 => unchecked((short)_data.Lo),
			DbValueKind.Int32 => unchecked((int)_data.Lo),
			DbValueKind.Int64 => unchecked((long)_data.Lo),
			DbValueKind.Object when _object is BigInteger bi => bi,
			DbValueKind.Object => (BigInteger)_object,
			_ => (BigInteger)GetValueCore(),
		};
	}

	public FbZonedDateTime GetZonedDateTime()
	{
		return _kind switch
		{
			DbValueKind.ZonedDateTime => new FbZonedDateTime(DateTime.FromBinary(unchecked((long)_data.Lo)), (string)_object, null),
			DbValueKind.ZonedDateTimeEx => new FbZonedDateTime(DateTime.FromBinary(unchecked((long)_data.Lo)), (string)_object, new TimeSpan(unchecked((long)_data.Hi))),
			DbValueKind.Object when _object is FbZonedDateTime zdt => zdt,
			_ => (FbZonedDateTime)GetValueCore(),
		};
	}

	public FbZonedTime GetZonedTime()
	{
		return _kind switch
		{
			DbValueKind.ZonedTime => new FbZonedTime(new TimeSpan(unchecked((long)_data.Lo)), (string)_object, null),
			DbValueKind.ZonedTimeEx => new FbZonedTime(new TimeSpan(unchecked((long)_data.Lo)), (string)_object, new TimeSpan(unchecked((long)_data.Hi))),
			DbValueKind.Object when _object is FbZonedTime zt => zt,
			_ => (FbZonedTime)GetValueCore(),
		};
	}

	public Array GetArray()
	{
		if (_kind == DbValueKind.Int64)
		{
			var l = GetInt64();
			_object = GetArrayData(l);
			_kind = DbValueKind.Object;
			_data = default;
		}

		return (Array)_object;
	}
	public async ValueTask<Array> GetArrayAsync(CancellationToken cancellationToken = default)
	{
		if (_kind == DbValueKind.Int64)
		{
			var l = GetInt64();
			_object = await GetArrayDataAsync(l, cancellationToken).ConfigureAwait(false);
			_kind = DbValueKind.Object;
			_data = default;
		}

		return (Array)_object;
	}

	public byte[] GetBinary()
	{
		if (_kind == DbValueKind.Int64)
		{
			var l = GetInt64();
			_object = GetBlobData(l);
			_kind = DbValueKind.Bytes;
			_data = default;
		}
		if (_kind == DbValueKind.Guid)
		{
			return TypeEncoder.EncodeGuid(GetGuid());
		}

		return (byte[])_object;
	}
	public async ValueTask<byte[]> GetBinaryAsync(CancellationToken cancellationToken = default)
	{
		if (_kind == DbValueKind.Int64)
		{
			var l = GetInt64();
			_object = await GetBlobDataAsync(l, cancellationToken).ConfigureAwait(false);
			_kind = DbValueKind.Bytes;
			_data = default;
		}
		if (_kind == DbValueKind.Guid)
		{
			return TypeEncoder.EncodeGuid(GetGuid());
		}

		return (byte[])_object;
	}

	public BlobStream GetBinaryStream()
	{
		if (_kind != DbValueKind.Int64)
			throw new NotSupportedException();

		return GetBlobStream(GetInt64());
	}
	public ValueTask<BlobStream> GetBinaryStreamAsync(CancellationToken cancellationToken = default)
	{
		if (_kind != DbValueKind.Int64)
			throw new NotSupportedException();

		return GetBlobStreamAsync(GetInt64(), cancellationToken);
	}

	public int GetDate()
	{
		if (_kind == DbValueKind.Object && _object is DateOnly @do)
		{
			return TypeEncoder.EncodeDate(@do);
		}
		return TypeEncoder.EncodeDate(GetDateTime());
	}

	public int GetTime()
	{
		return _kind switch
		{
			DbValueKind.TimeSpan => TypeEncoder.EncodeTime(new TimeSpan(unchecked((long)_data.Lo))),
			DbValueKind.ZonedTime => TypeEncoder.EncodeTime(new TimeSpan(unchecked((long)_data.Lo))),
			DbValueKind.ZonedTimeEx => TypeEncoder.EncodeTime(new TimeSpan(unchecked((long)_data.Lo))),
			DbValueKind.Object when _object is TimeSpan ts => TypeEncoder.EncodeTime(ts),
			DbValueKind.Object when _object is FbZonedTime zt => TypeEncoder.EncodeTime(zt.Time),
			DbValueKind.Object when _object is TimeOnly to => TypeEncoder.EncodeTime(to),
			_ => TypeEncoder.EncodeTime(TypeHelper.DateTimeTimeToTimeSpan(GetDateTime())),
		};
	}

	public ushort GetTimeZoneId()
	{
		if ((_kind == DbValueKind.ZonedDateTime || _kind == DbValueKind.ZonedDateTimeEx
				|| _kind == DbValueKind.ZonedTime || _kind == DbValueKind.ZonedTimeEx)
			&& _object is string tz && TimeZoneMapping.TryGetByName(tz, out var id))
		{
			return id;
		}

		if (_kind == DbValueKind.Object)
		{
			if (_object is FbZonedDateTime zdt && TimeZoneMapping.TryGetByName(zdt.TimeZone, out var id2))
			{
				return id2;
			}
			if (_object is FbZonedTime zt && TimeZoneMapping.TryGetByName(zt.TimeZone, out var id3))
			{
				return id3;
			}
		}
		throw new InvalidOperationException($"Incorrect time zone value.");
	}

	public byte[] GetBytes()
	{
		if (IsDBNull())
		{
			int length = _field.Length;

			if (Field.SqlType == IscCodes.SQL_VARYING)
			{
				// Add two bytes more for store	value length
				length += 2;
			}

			return new byte[length];
		}


		switch (Field.DbDataType)
		{
			case DbDataType.Char:
				{
					var buffer = new byte[Field.Length];
					byte[] bytes;

					if (Field.Charset.IsOctetsCharset)
					{
						bytes = GetBinary();
					}
					else if (Field.Charset.IsNoneCharset)
					{
						var bvalue = Field.Charset.GetBytes(GetString());
						if (bvalue.Length > Field.Length)
						{
							throw IscException.ForErrorCodes(new[] { IscCodes.isc_arith_except, IscCodes.isc_string_truncation });
						}
						bytes = bvalue;
					}
					else
					{
						var svalue = GetString();
						if ((Field.Length % Field.Charset.BytesPerCharacter) == 0 && svalue.CountRunes() > Field.CharCount)
						{
							throw IscException.ForErrorCodes(new[] { IscCodes.isc_arith_except, IscCodes.isc_string_truncation });
						}
						bytes = Field.Charset.GetBytes(svalue);
					}

					for (var i = 0; i < buffer.Length; i++)
					{
						buffer[i] = (byte)' ';
					}
					Buffer.BlockCopy(bytes, 0, buffer, 0, bytes.Length);
					return buffer;
				}

			case DbDataType.VarChar:
				{
					var buffer = new byte[Field.Length + 2];
					byte[] bytes;

					if (Field.Charset.IsOctetsCharset)
					{
						bytes = GetBinary();
					}
					else if (Field.Charset.IsNoneCharset)
					{
						var bvalue = Field.Charset.GetBytes(GetString());
						if (bvalue.Length > Field.Length)
						{
							throw IscException.ForErrorCodes(new[] { IscCodes.isc_arith_except, IscCodes.isc_string_truncation });
						}
						bytes = bvalue;
					}
					else
					{
						var svalue = GetString();
						if ((Field.Length % Field.Charset.BytesPerCharacter) == 0 && svalue.CountRunes() > Field.CharCount)
						{
							throw IscException.ForErrorCodes(new[] { IscCodes.isc_arith_except, IscCodes.isc_string_truncation });
						}
						bytes = Field.Charset.GetBytes(svalue);
					}

					Buffer.BlockCopy(BitConverter.GetBytes((short)bytes.Length), 0, buffer, 0, 2);
					Buffer.BlockCopy(bytes, 0, buffer, 2, bytes.Length);
					return buffer;
				}

			case DbDataType.Numeric:
			case DbDataType.Decimal:
				return GetNumericBytes();

			case DbDataType.SmallInt:
				return BitConverter.GetBytes(GetInt16());

			case DbDataType.Integer:
				return BitConverter.GetBytes(GetInt32());

			case DbDataType.Array:
			case DbDataType.Binary:
			case DbDataType.Text:
			case DbDataType.BigInt:
				return BitConverter.GetBytes(GetInt64());

			case DbDataType.Float:
				return BitConverter.GetBytes(GetFloat());

			case DbDataType.Double:
				return BitConverter.GetBytes(GetDouble());

			case DbDataType.Date:
				return BitConverter.GetBytes(GetDate());

			case DbDataType.Time:
				return BitConverter.GetBytes(GetTime());

			case DbDataType.TimeStamp:
				{
					var dt = GetDateTime();
					var date = BitConverter.GetBytes(TypeEncoder.EncodeDate(dt));
					var time = BitConverter.GetBytes(TypeEncoder.EncodeTime(TypeHelper.DateTimeTimeToTimeSpan(dt)));

					var result = new byte[8];
					Buffer.BlockCopy(date, 0, result, 0, date.Length);
					Buffer.BlockCopy(time, 0, result, 4, time.Length);
					return result;
				}

			case DbDataType.Guid:
				{
					var bytes = TypeEncoder.EncodeGuid(GetGuid());
					byte[] buffer;
					if (Field.SqlType == IscCodes.SQL_VARYING)
					{
						buffer = new byte[bytes.Length + 2];
						Buffer.BlockCopy(BitConverter.GetBytes((short)bytes.Length), 0, buffer, 0, 2);
						Buffer.BlockCopy(bytes, 0, buffer, 2, bytes.Length);
					}
					else
					{
						buffer = new byte[bytes.Length];
						Buffer.BlockCopy(bytes, 0, buffer, 0, bytes.Length);
					}
					return buffer;
				}

			case DbDataType.Boolean:
				return BitConverter.GetBytes(GetBoolean());

			case DbDataType.TimeStampTZ:
				{
					var dt = GetDateTime();
					var date = BitConverter.GetBytes(TypeEncoder.EncodeDate(dt));
					var time = BitConverter.GetBytes(TypeEncoder.EncodeTime(TypeHelper.DateTimeTimeToTimeSpan(dt)));
					var tzId = BitConverter.GetBytes(GetTimeZoneId());

					var result = new byte[10];
					Buffer.BlockCopy(date, 0, result, 0, date.Length);
					Buffer.BlockCopy(time, 0, result, 4, time.Length);
					Buffer.BlockCopy(tzId, 0, result, 8, tzId.Length);
					return result;
				}

			case DbDataType.TimeStampTZEx:
				{
					var dt = GetDateTime();
					var date = BitConverter.GetBytes(TypeEncoder.EncodeDate(dt));
					var time = BitConverter.GetBytes(TypeEncoder.EncodeTime(TypeHelper.DateTimeTimeToTimeSpan(dt)));
					var tzId = BitConverter.GetBytes(GetTimeZoneId());
					var offset = new byte[] { 0, 0 };

					var result = new byte[12];
					Buffer.BlockCopy(date, 0, result, 0, date.Length);
					Buffer.BlockCopy(time, 0, result, 4, time.Length);
					Buffer.BlockCopy(tzId, 0, result, 8, tzId.Length);
					Buffer.BlockCopy(offset, 0, result, 10, offset.Length);
					return result;
				}

			case DbDataType.TimeTZ:
				{
					var time = BitConverter.GetBytes(GetTime());
					var tzId = BitConverter.GetBytes(GetTimeZoneId());

					var result = new byte[6];
					Buffer.BlockCopy(time, 0, result, 0, time.Length);
					Buffer.BlockCopy(tzId, 0, result, 4, tzId.Length);
					return result;
				}

			case DbDataType.TimeTZEx:
				{
					var time = BitConverter.GetBytes(GetTime());
					var tzId = BitConverter.GetBytes(GetTimeZoneId());
					var offset = new byte[] { 0, 0 };

					var result = new byte[8];
					Buffer.BlockCopy(time, 0, result, 0, time.Length);
					Buffer.BlockCopy(tzId, 0, result, 4, tzId.Length);
					Buffer.BlockCopy(offset, 0, result, 6, offset.Length);
					return result;
				}

			case DbDataType.Dec16:
				return DecimalCodec.DecFloat16.EncodeDecimal(GetDecFloat());

			case DbDataType.Dec34:
				return DecimalCodec.DecFloat34.EncodeDecimal(GetDecFloat());

			case DbDataType.Int128:
				return Int128Helper.GetBytes(GetInt128());

			default:
				throw TypeHelper.InvalidDataType((int)Field.DbDataType);
		}
	}
	public async ValueTask<byte[]> GetBytesAsync(CancellationToken cancellationToken = default)
	{
		if (IsDBNull())
		{
			int length = _field.Length;

			if (Field.SqlType == IscCodes.SQL_VARYING)
			{
				// Add two bytes more for store	value length
				length += 2;
			}

			return new byte[length];
		}


		switch (Field.DbDataType)
		{
			case DbDataType.Char:
				{
					var buffer = new byte[Field.Length];
					byte[] bytes;

					if (Field.Charset.IsOctetsCharset)
					{
						bytes = await GetBinaryAsync(cancellationToken).ConfigureAwait(false);
					}
					else if (Field.Charset.IsNoneCharset)
					{
						var bvalue = Field.Charset.GetBytes(await GetStringAsync(cancellationToken).ConfigureAwait(false));
						if (bvalue.Length > Field.Length)
						{
							throw IscException.ForErrorCodes(new[] { IscCodes.isc_arith_except, IscCodes.isc_string_truncation });
						}
						bytes = bvalue;
					}
					else
					{
						var svalue = await GetStringAsync(cancellationToken).ConfigureAwait(false);
						if ((Field.Length % Field.Charset.BytesPerCharacter) == 0 && svalue.CountRunes() > Field.CharCount)
						{
							throw IscException.ForErrorCodes(new[] { IscCodes.isc_arith_except, IscCodes.isc_string_truncation });
						}
						bytes = Field.Charset.GetBytes(svalue);
					}

					for (var i = 0; i < buffer.Length; i++)
					{
						buffer[i] = (byte)' ';
					}
					Buffer.BlockCopy(bytes, 0, buffer, 0, bytes.Length);
					return buffer;
				}

			case DbDataType.VarChar:
				{
					var buffer = new byte[Field.Length + 2];
					byte[] bytes;

					if (Field.Charset.IsOctetsCharset)
					{
						bytes = await GetBinaryAsync(cancellationToken).ConfigureAwait(false);
					}
					else if (Field.Charset.IsNoneCharset)
					{
						var bvalue = Field.Charset.GetBytes(await GetStringAsync(cancellationToken).ConfigureAwait(false));
						if (bvalue.Length > Field.Length)
						{
							throw IscException.ForErrorCodes(new[] { IscCodes.isc_arith_except, IscCodes.isc_string_truncation });
						}
						bytes = bvalue;
					}
					else
					{
						var svalue = await GetStringAsync(cancellationToken).ConfigureAwait(false);
						if ((Field.Length % Field.Charset.BytesPerCharacter) == 0 && svalue.CountRunes() > Field.CharCount)
						{
							throw IscException.ForErrorCodes(new[] { IscCodes.isc_arith_except, IscCodes.isc_string_truncation });
						}
						bytes = Field.Charset.GetBytes(svalue);
					}

					Buffer.BlockCopy(BitConverter.GetBytes((short)bytes.Length), 0, buffer, 0, 2);
					Buffer.BlockCopy(bytes, 0, buffer, 2, bytes.Length);
					return buffer;
				}

			case DbDataType.Numeric:
			case DbDataType.Decimal:
				return GetNumericBytes();

			case DbDataType.SmallInt:
				return BitConverter.GetBytes(GetInt16());

			case DbDataType.Integer:
				return BitConverter.GetBytes(GetInt32());

			case DbDataType.Array:
			case DbDataType.Binary:
			case DbDataType.Text:
			case DbDataType.BigInt:
				return BitConverter.GetBytes(GetInt64());

			case DbDataType.Float:
				return BitConverter.GetBytes(GetFloat());

			case DbDataType.Double:
				return BitConverter.GetBytes(GetDouble());

			case DbDataType.Date:
				return BitConverter.GetBytes(GetDate());

			case DbDataType.Time:
				return BitConverter.GetBytes(GetTime());

			case DbDataType.TimeStamp:
				{
					var dt = GetDateTime();
					var date = BitConverter.GetBytes(TypeEncoder.EncodeDate(dt));
					var time = BitConverter.GetBytes(TypeEncoder.EncodeTime(TypeHelper.DateTimeTimeToTimeSpan(dt)));

					var result = new byte[8];
					Buffer.BlockCopy(date, 0, result, 0, date.Length);
					Buffer.BlockCopy(time, 0, result, 4, time.Length);
					return result;
				}

			case DbDataType.Guid:
				{
					var bytes = TypeEncoder.EncodeGuid(GetGuid());
					byte[] buffer;
					if (Field.SqlType == IscCodes.SQL_VARYING)
					{
						buffer = new byte[bytes.Length + 2];
						Buffer.BlockCopy(BitConverter.GetBytes((short)bytes.Length), 0, buffer, 0, 2);
						Buffer.BlockCopy(bytes, 0, buffer, 2, bytes.Length);
					}
					else
					{
						buffer = new byte[bytes.Length];
						Buffer.BlockCopy(bytes, 0, buffer, 0, bytes.Length);
					}
					return buffer;
				}

			case DbDataType.Boolean:
				return BitConverter.GetBytes(GetBoolean());

			case DbDataType.TimeStampTZ:
				{
					var dt = GetDateTime();
					var date = BitConverter.GetBytes(TypeEncoder.EncodeDate(dt));
					var time = BitConverter.GetBytes(TypeEncoder.EncodeTime(TypeHelper.DateTimeTimeToTimeSpan(dt)));
					var tzId = BitConverter.GetBytes(GetTimeZoneId());

					var result = new byte[10];
					Buffer.BlockCopy(date, 0, result, 0, date.Length);
					Buffer.BlockCopy(time, 0, result, 4, time.Length);
					Buffer.BlockCopy(tzId, 0, result, 8, tzId.Length);
					return result;
				}

			case DbDataType.TimeStampTZEx:
				{
					var dt = GetDateTime();
					var date = BitConverter.GetBytes(TypeEncoder.EncodeDate(dt));
					var time = BitConverter.GetBytes(TypeEncoder.EncodeTime(TypeHelper.DateTimeTimeToTimeSpan(dt)));
					var tzId = BitConverter.GetBytes(GetTimeZoneId());
					var offset = new byte[] { 0, 0 };

					var result = new byte[12];
					Buffer.BlockCopy(date, 0, result, 0, date.Length);
					Buffer.BlockCopy(time, 0, result, 4, time.Length);
					Buffer.BlockCopy(tzId, 0, result, 8, tzId.Length);
					Buffer.BlockCopy(offset, 0, result, 10, offset.Length);
					return result;
				}

			case DbDataType.TimeTZ:
				{
					var time = BitConverter.GetBytes(GetTime());
					var tzId = BitConverter.GetBytes(GetTimeZoneId());

					var result = new byte[6];
					Buffer.BlockCopy(time, 0, result, 0, time.Length);
					Buffer.BlockCopy(tzId, 0, result, 4, tzId.Length);
					return result;
				}

			case DbDataType.TimeTZEx:
				{
					var time = BitConverter.GetBytes(GetTime());
					var tzId = BitConverter.GetBytes(GetTimeZoneId());
					var offset = new byte[] { 0, 0 };

					var result = new byte[8];
					Buffer.BlockCopy(time, 0, result, 0, time.Length);
					Buffer.BlockCopy(tzId, 0, result, 4, tzId.Length);
					Buffer.BlockCopy(offset, 0, result, 6, offset.Length);
					return result;
				}

			case DbDataType.Dec16:
				return DecimalCodec.DecFloat16.EncodeDecimal(GetDecFloat());

			case DbDataType.Dec34:
				return DecimalCodec.DecFloat34.EncodeDecimal(GetDecFloat());

			case DbDataType.Int128:
				return Int128Helper.GetBytes(GetInt128());

			default:
				throw TypeHelper.InvalidDataType((int)Field.DbDataType);
		}
	}

	private byte[] GetNumericBytes()
	{
		var value = GetDecimal();
		var numeric = TypeEncoder.EncodeDecimal(value, Field.NumericScale, Field.DataType);

		switch (_field.SqlType)
		{
			case IscCodes.SQL_SHORT:
				return BitConverter.GetBytes((short)numeric);

			case IscCodes.SQL_LONG:
				return BitConverter.GetBytes((int)numeric);

			case IscCodes.SQL_QUAD:
			case IscCodes.SQL_INT64:
				return BitConverter.GetBytes((long)numeric);

			case IscCodes.SQL_DOUBLE:
			case IscCodes.SQL_D_FLOAT:
				return BitConverter.GetBytes((double)numeric);

			case IscCodes.SQL_INT128:
				return Int128Helper.GetBytes((BigInteger)numeric);

			default:
				return null;
		}
	}

	private string GetClobData(long blobId)
	{
		var clob = _statement.CreateBlob(blobId);
		return clob.ReadString();
	}
	private ValueTask<string> GetClobDataAsync(long blobId, CancellationToken cancellationToken = default)
	{
		var clob = _statement.CreateBlob(blobId);
		return clob.ReadStringAsync(cancellationToken);
	}

	private byte[] GetBlobData(long blobId)
	{
		var blob = _statement.CreateBlob(blobId);
		return blob.Read();
	}
	private ValueTask<byte[]> GetBlobDataAsync(long blobId, CancellationToken cancellationToken = default)
	{
		var blob = _statement.CreateBlob(blobId);
		return blob.ReadAsync(cancellationToken);
	}

	private BlobStream GetBlobStream(long blobId)
	{
		var blob = _statement.CreateBlob(blobId);
		return new BlobStream(blob);
	}
	private ValueTask<BlobStream> GetBlobStreamAsync(long blobId, CancellationToken cancellationToken = default)
	{
		var blob = _statement.CreateBlob(blobId);
		return ValueTask.FromResult(new BlobStream(blob));
	}

	private Array GetArrayData(long handle)
	{
		if (_field.ArrayHandle == null)
		{
			_field.ArrayHandle = _statement.CreateArray(handle, Field.Relation, Field.Name);
		}

		var gdsArray = _statement.CreateArray(_field.ArrayHandle.Descriptor);
		gdsArray.Handle = handle;
		gdsArray.Database = _statement.Database;
		gdsArray.Transaction = _statement.Transaction;
		return gdsArray.Read();
	}
	private async ValueTask<Array> GetArrayDataAsync(long handle, CancellationToken cancellationToken = default)
	{
		if (_field.ArrayHandle == null)
		{
			_field.ArrayHandle = await _statement.CreateArrayAsync(handle, Field.Relation, Field.Name, cancellationToken).ConfigureAwait(false);
		}

		var gdsArray = await _statement.CreateArrayAsync(_field.ArrayHandle.Descriptor, cancellationToken).ConfigureAwait(false);
		gdsArray.Handle = handle;
		gdsArray.Database = _statement.Database;
		gdsArray.Transaction = _statement.Transaction;
		return await gdsArray.ReadAsync(cancellationToken).ConfigureAwait(false);
	}
}
