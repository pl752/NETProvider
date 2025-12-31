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
using System.Buffers.Binary;
using System.Net;
using System.Numerics;
using FirebirdSql.Data.Types;

namespace FirebirdSql.Data.Common;

internal static class TypeDecoder
{
	public static decimal DecodeDecimal(short value, int scale, int type)
	{
		var shift = scale < 0 ? -scale : scale;
		return DecimalShiftHelper.ShiftDecimalLeft(value, shift);
	}

	public static decimal DecodeDecimal(int value, int scale, int type)
	{
		var shift = scale < 0 ? -scale : scale;
		return DecimalShiftHelper.ShiftDecimalLeft(value, shift);
	}

	public static decimal DecodeDecimal(long value, int scale, int type)
	{
		var shift = scale < 0 ? -scale : scale;
		return DecimalShiftHelper.ShiftDecimalLeft(value, shift);
	}

	public static decimal DecodeDecimal(double value, int scale, int type)
	{
		return (decimal)value;
	}

	public static decimal DecodeDecimal(BigInteger value, int scale, int type)
	{
		var shift = scale < 0 ? -scale : scale;
		return DecimalShiftHelper.ShiftDecimalLeft((decimal)value, shift);
	}

	public static decimal DecodeDecimal(object value, int scale, int type)
	{
		switch (type & ~1)
		{
			case IscCodes.SQL_SHORT:
				return DecodeDecimal((short)value, scale, type);

			case IscCodes.SQL_LONG:
				return DecodeDecimal((int)value, scale, type);

			case IscCodes.SQL_QUAD:
			case IscCodes.SQL_INT64:
				return DecodeDecimal((long)value, scale, type);

			case IscCodes.SQL_DOUBLE:
			case IscCodes.SQL_D_FLOAT:
				return DecodeDecimal((double)value, scale, type);

			case IscCodes.SQL_INT128:
				return DecodeDecimal((BigInteger)value, scale, type);

			default:
				throw new ArgumentOutOfRangeException(nameof(type), $"{nameof(type)}={type}");
		}
	}

	public static TimeSpan DecodeTime(int sqlTime)
	{
		return TimeSpan.FromTicks(sqlTime * 1000L);
	}

	public static DateTime DecodeDate(int sqlDate)
	{
		var (year, month, day) = DecodeDateImpl(sqlDate);
		var date = new DateTime(year, month, day);
		return date.Date;
	}
	static (int year, int month, int day) DecodeDateImpl(int sqlDate)
	{
		sqlDate -= 1721119 - 2400001;
		var century = (4 * sqlDate - 1) / 146097;
		sqlDate = 4 * sqlDate - 1 - 146097 * century;
		var day = sqlDate / 4;

		sqlDate = (4 * day + 3) / 1461;
		day = 4 * day + 3 - 1461 * sqlDate;
		day = (day + 4) / 4;

		var month = (5 * day - 3) / 153;
		day = 5 * day - 3 - 153 * month;
		day = (day + 5) / 5;

		var year = 100 * century + sqlDate;

		if (month < 10)
		{
			month += 3;
		}
		else
		{
			month -= 9;
			year += 1;
		}

		return (year, month, day);
	}

	public static bool DecodeBoolean(byte[] value)
	{
		return value[0] != 0;
	}

	public static bool DecodeBoolean(ReadOnlySpan<byte> value)
	{
		return value[0] != 0;
	}

	public static Guid DecodeGuid(byte[] value)
	{
		var a = IPAddress.HostToNetworkOrder(BitConverter.ToInt32(value, 0));
		var b = IPAddress.HostToNetworkOrder(BitConverter.ToInt16(value, 4));
		var c = IPAddress.HostToNetworkOrder(BitConverter.ToInt16(value, 6));
		return new Guid(a, b, c, value[8], value[9], value[10], value[11], value[12], value[13], value[14], value[15]);
	}

	public static Guid DecodeGuidSpan(Span<byte> value)
	{
		var a = IPAddress.HostToNetworkOrder(BitConverter.ToInt32(value[..4]));
		var b = IPAddress.HostToNetworkOrder(BitConverter.ToInt16(value[4..6]));
		var c = IPAddress.HostToNetworkOrder(BitConverter.ToInt16(value[6..8]));
		return new Guid(a, b, c, value[8], value[9], value[10], value[11], value[12], value[13], value[14], value[15]);
	}

	public static int DecodeInt32(byte[] value)
	{
		return BinaryPrimitives.ReadInt32BigEndian(value);
	}

	public static int DecodeInt32(Span<byte> value) {
		return BinaryPrimitives.ReadInt32BigEndian(value);
	}

	public static long DecodeInt64(byte[] value)
	{
		return BinaryPrimitives.ReadInt64BigEndian(value);
	}

	public static long DecodeInt64(Span<byte> value) {
		return BinaryPrimitives.ReadInt64BigEndian(value);
	}

	public static FbDecFloat DecodeDec16(byte[] value)
	{
		if (BitConverter.IsLittleEndian)
		{
			Array.Reverse(value);
		}
		return DecimalCodec.DecFloat16.ParseBytes(value);
	}

	public static FbDecFloat DecodeDec34(byte[] value)
	{
		if (BitConverter.IsLittleEndian)
		{
			Array.Reverse(value);
		}
		return DecimalCodec.DecFloat34.ParseBytes(value);
	}

	public static BigInteger DecodeInt128(byte[] value)
	{
		if (BitConverter.IsLittleEndian)
		{
			Array.Reverse(value);
		}
		return Int128Helper.GetInt128(value);
	}
}
