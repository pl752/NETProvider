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

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FirebirdSql.Data.Types;

namespace FirebirdSql.Data.Common;

[StructLayout(LayoutKind.Sequential)]
internal struct DbValueBuffer16
{
	public ulong Lo;
	public ulong Hi;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public Span<byte> AsBytes() => MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref this, 1));
}

internal enum DbValueKind : byte
{
	DbNull = 0,

	Boolean,
	Byte,
	Int16,
	Int32,
	Int64,
	Single,
	Double,
	Decimal,
	Guid,
	DateTime,
	TimeSpan,

	String,
	Bytes,

	Int128,
	Dec16,
	Dec34,

	ZonedDateTime,
	ZonedDateTimeEx,
	ZonedTime,
	ZonedTimeEx,

	Object,
}

[StructLayout(LayoutKind.Sequential)]
internal struct DbValueStorage
{
	public DbValueKind Kind;
	public DbValueBuffer16 Data;
	public object Object;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage DbNull() => default;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage FromBoolean(bool value)
		=> new() { Kind = DbValueKind.Boolean, Data = new DbValueBuffer16 { Lo = value ? 1UL : 0UL }, Object = null };

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage FromByte(byte value)
		=> new() { Kind = DbValueKind.Byte, Data = new DbValueBuffer16 { Lo = value }, Object = null };

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage FromInt16(short value)
		=> new() { Kind = DbValueKind.Int16, Data = new DbValueBuffer16 { Lo = unchecked((ulong)value) }, Object = null };

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage FromInt32(int value)
		=> new() { Kind = DbValueKind.Int32, Data = new DbValueBuffer16 { Lo = unchecked((ulong)value) }, Object = null };

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage FromInt64(long value)
		=> new() { Kind = DbValueKind.Int64, Data = new DbValueBuffer16 { Lo = unchecked((ulong)value) }, Object = null };

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage FromSingle(float value)
		=> new() { Kind = DbValueKind.Single, Data = new DbValueBuffer16 { Lo = unchecked((ulong)(uint)BitConverter.SingleToInt32Bits(value)) }, Object = null };

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage FromDouble(double value)
		=> new() { Kind = DbValueKind.Double, Data = new DbValueBuffer16 { Lo = unchecked((ulong)BitConverter.DoubleToInt64Bits(value)) }, Object = null };

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage FromDecimal(decimal value)
	{
		var storage = new DbValueStorage { Kind = DbValueKind.Decimal, Object = null };
		MemoryMarshal.Write(storage.Data.AsBytes(), in value);
		return storage;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage FromGuid(Guid value)
	{
		var storage = new DbValueStorage { Kind = DbValueKind.Guid, Object = null };
		MemoryMarshal.Write(storage.Data.AsBytes(), in value);
		return storage;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage FromDateTime(DateTime value)
		=> new() { Kind = DbValueKind.DateTime, Data = new DbValueBuffer16 { Lo = unchecked((ulong)value.ToBinary()) }, Object = null };

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage FromTimeSpan(TimeSpan value)
		=> new() { Kind = DbValueKind.TimeSpan, Data = new DbValueBuffer16 { Lo = unchecked((ulong)value.Ticks) }, Object = null };

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage FromString(string value)
		=> value == null ? default : new() { Kind = DbValueKind.String, Object = value };

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage FromBytes(byte[] value)
		=> value == null ? default : new() { Kind = DbValueKind.Bytes, Object = value };

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage FromObject(object value)
		=> value == null ? default : new() { Kind = DbValueKind.Object, Object = value };

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage FromZonedDateTime(FbZonedDateTime value)
		=> new()
		{
			Kind = value.Offset.HasValue ? DbValueKind.ZonedDateTimeEx : DbValueKind.ZonedDateTime,
			Data = new DbValueBuffer16
			{
				Lo = unchecked((ulong)value.DateTime.ToBinary()),
				Hi = unchecked((ulong)(value.Offset?.Ticks ?? 0)),
			},
			Object = value.TimeZone,
		};

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static DbValueStorage FromZonedTime(FbZonedTime value)
		=> new()
		{
			Kind = value.Offset.HasValue ? DbValueKind.ZonedTimeEx : DbValueKind.ZonedTime,
			Data = new DbValueBuffer16
			{
				Lo = unchecked((ulong)value.Time.Ticks),
				Hi = unchecked((ulong)(value.Offset?.Ticks ?? 0)),
			},
			Object = value.TimeZone,
		};
}
