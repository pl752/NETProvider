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
using System.Data;
using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.Types;

namespace FirebirdSql.Data.Common;

internal static class TypeHelper
{
	public static bool IsDBNull(object value) => value == null || value == DBNull.Value;

	public static short? GetSize(DbDataType type) => type switch
	{
		DbDataType.Array or DbDataType.Binary or DbDataType.Text => (short?) 8,
		DbDataType.SmallInt => (short?) 2,
		DbDataType.Integer or DbDataType.Float or DbDataType.Date or DbDataType.Time => (short?) 4,
		DbDataType.BigInt or DbDataType.Double or DbDataType.TimeStamp or DbDataType.Dec16 or DbDataType.TimeTZ => (short?) 8,
		DbDataType.Guid or DbDataType.TimeStampTZEx or DbDataType.Dec34 or DbDataType.Int128 => (short?) 16,
		DbDataType.Boolean => (short?) 1,
		DbDataType.TimeStampTZ or DbDataType.TimeTZEx => (short?) 12,
		_ => null,
	};

	public static int GetSqlTypeFromDbDataType(DbDataType type, bool isNullable)
	{
		int sqltype = type switch
		{
			DbDataType.Array => IscCodes.SQL_ARRAY,
			DbDataType.Binary or DbDataType.Text => IscCodes.SQL_BLOB,
			DbDataType.Char => IscCodes.SQL_TEXT,
			DbDataType.VarChar => IscCodes.SQL_VARYING,
			DbDataType.SmallInt => IscCodes.SQL_SHORT,
			DbDataType.Integer => IscCodes.SQL_LONG,
			DbDataType.BigInt => IscCodes.SQL_INT64,
			DbDataType.Float => IscCodes.SQL_FLOAT,
			DbDataType.Guid => IscCodes.SQL_TEXT,
			DbDataType.Double => IscCodes.SQL_DOUBLE,
			DbDataType.Date => IscCodes.SQL_TYPE_DATE,
			DbDataType.Time => IscCodes.SQL_TYPE_TIME,
			DbDataType.TimeStamp => IscCodes.SQL_TIMESTAMP,
			DbDataType.Boolean => IscCodes.SQL_BOOLEAN,
			DbDataType.TimeStampTZ => IscCodes.SQL_TIMESTAMP_TZ,
			DbDataType.TimeStampTZEx => IscCodes.SQL_TIMESTAMP_TZ_EX,
			DbDataType.TimeTZ => IscCodes.SQL_TIME_TZ,
			DbDataType.TimeTZEx => IscCodes.SQL_TIME_TZ_EX,
			DbDataType.Dec16 => IscCodes.SQL_DEC16,
			DbDataType.Dec34 => IscCodes.SQL_DEC34,
			DbDataType.Int128 => IscCodes.SQL_INT128,
			_ => throw InvalidDataType((int) type),
		};
		if (isNullable)
		{
			sqltype++;
		}

		return sqltype;
	}

	public static int GetSqlTypeFromBlrType(int type) => type switch
	{
		IscCodes.blr_varying or IscCodes.blr_varying2 => IscCodes.SQL_VARYING,
		IscCodes.blr_text or IscCodes.blr_text2 or IscCodes.blr_cstring or IscCodes.blr_cstring2 => IscCodes.SQL_TEXT,
		IscCodes.blr_short => IscCodes.SQL_SHORT,
		IscCodes.blr_long => IscCodes.SQL_LONG,
		IscCodes.blr_quad => IscCodes.SQL_QUAD,
		IscCodes.blr_int64 or IscCodes.blr_blob_id => IscCodes.SQL_INT64,
		IscCodes.blr_double => IscCodes.SQL_DOUBLE,
		IscCodes.blr_d_float => IscCodes.SQL_D_FLOAT,
		IscCodes.blr_float => IscCodes.SQL_FLOAT,
		IscCodes.blr_sql_date => IscCodes.SQL_TYPE_DATE,
		IscCodes.blr_sql_time => IscCodes.SQL_TYPE_TIME,
		IscCodes.blr_timestamp => IscCodes.SQL_TIMESTAMP,
		IscCodes.blr_blob => IscCodes.SQL_BLOB,
		IscCodes.blr_bool => IscCodes.SQL_BOOLEAN,
		IscCodes.blr_ex_timestamp_tz => IscCodes.SQL_TIMESTAMP_TZ_EX,
		IscCodes.blr_timestamp_tz => IscCodes.SQL_TIMESTAMP_TZ,
		IscCodes.blr_sql_time_tz => IscCodes.SQL_TIME_TZ,
		IscCodes.blr_ex_time_tz => IscCodes.SQL_TIME_TZ_EX,
		IscCodes.blr_dec64 => IscCodes.SQL_DEC16,
		IscCodes.blr_dec128 => IscCodes.SQL_DEC34,
		IscCodes.blr_int128 => IscCodes.SQL_INT128,
		_ => throw InvalidDataType(type),
	};

	public static string GetDataTypeName(DbDataType type) => type switch
	{
		DbDataType.Array => "ARRAY",
		DbDataType.Binary => "BLOB",
		DbDataType.Text => "BLOB SUB_TYPE 1",
		DbDataType.Char or DbDataType.Guid => "CHAR",
		DbDataType.VarChar => "VARCHAR",
		DbDataType.SmallInt => "SMALLINT",
		DbDataType.Integer => "INTEGER",
		DbDataType.Float => "FLOAT",
		DbDataType.Double => "DOUBLE PRECISION",
		DbDataType.BigInt => "BIGINT",
		DbDataType.Numeric => "NUMERIC",
		DbDataType.Decimal => "DECIMAL",
		DbDataType.Date => "DATE",
		DbDataType.Time => "TIME",
		DbDataType.TimeStamp => "TIMESTAMP",
		DbDataType.Boolean => "BOOLEAN",
		DbDataType.TimeStampTZ or DbDataType.TimeStampTZEx => "TIMESTAMP WITH TIME ZONE",
		DbDataType.TimeTZ or DbDataType.TimeTZEx => "TIME WITH TIME ZONE",
		DbDataType.Dec16 or DbDataType.Dec34 => "DECFLOAT",
		DbDataType.Int128 => "INT128",
		_ => throw InvalidDataType((int) type),
	};

	public static Type GetTypeFromDbDataType(DbDataType type) => type switch
	{
		DbDataType.Array => typeof(System.Array),
		DbDataType.Binary => typeof(System.Byte[]),
		DbDataType.Text or DbDataType.Char or DbDataType.VarChar => typeof(System.String),
		DbDataType.Guid => typeof(System.Guid),
		DbDataType.SmallInt => typeof(System.Int16),
		DbDataType.Integer => typeof(System.Int32),
		DbDataType.BigInt => typeof(System.Int64),
		DbDataType.Float => typeof(System.Single),
		DbDataType.Double => typeof(System.Double),
		DbDataType.Numeric or DbDataType.Decimal => typeof(System.Decimal),
		DbDataType.Date or DbDataType.TimeStamp => typeof(System.DateTime),
		DbDataType.Time => typeof(System.TimeSpan),
		DbDataType.Boolean => typeof(System.Boolean),
		DbDataType.TimeStampTZ or DbDataType.TimeStampTZEx => typeof(FbZonedDateTime),
		DbDataType.TimeTZ or DbDataType.TimeTZEx => typeof(FbZonedTime),
		DbDataType.Dec16 or DbDataType.Dec34 => typeof(FbDecFloat),
		DbDataType.Int128 => typeof(System.Numerics.BigInteger),
		_ => throw InvalidDataType((int) type),
	};

	public static FbDbType GetFbDataTypeFromType(Type type)
	{
		if (type.IsEnum)
		{
			return GetFbDataTypeFromType(Enum.GetUnderlyingType(type));
		}

		if (type == typeof(System.DBNull))
		{
			return FbDbType.VarChar;
		}

		if (type == typeof(System.String))
		{
			return FbDbType.VarChar;
		}
		else if (type == typeof(System.Char))
		{
			return FbDbType.Char;
		}
		else if (type == typeof(System.Boolean))
		{
			return FbDbType.Boolean;
		}
		else if (type == typeof(System.Byte) || type == typeof(System.SByte) || type == typeof(System.Int16) || type == typeof(System.UInt16))
		{
			return FbDbType.SmallInt;
		}
		else if (type == typeof(System.Int32) || type == typeof(System.UInt32))
		{
			return FbDbType.Integer;
		}
		else if (type == typeof(System.Int64) || type == typeof(System.UInt64))
		{
			return FbDbType.BigInt;
		}
		else if (type == typeof(System.Single))
		{
			return FbDbType.Float;
		}
		else if (type == typeof(System.Double))
		{
			return FbDbType.Double;
		}
		else if (type == typeof(System.Decimal))
		{
			return FbDbType.Decimal;
		}
		else if (type == typeof(System.DateTime))
		{
			return FbDbType.TimeStamp;
		}
		else if (type == typeof(System.TimeSpan))
		{
			return FbDbType.Time;
		}
		else if (type == typeof(System.Guid))
		{
			return FbDbType.Guid;
		}
		else if (type == typeof(FbZonedDateTime))
		{
			return FbDbType.TimeStampTZ;
		}
		else if (type == typeof(FbZonedTime))
		{
			return FbDbType.TimeTZ;
		}
		else if (type == typeof(FbDecFloat))
		{
			return FbDbType.Dec34;
		}
		else if (type == typeof(System.Numerics.BigInteger))
		{
			return FbDbType.Int128;
		}
		else if (type == typeof(System.Byte[]))
		{
			return FbDbType.Binary;
		}
#if NET6_0_OR_GREATER
		else if (type == typeof(System.DateOnly))
		{
			return FbDbType.Date;
		}
#endif
#if NET6_0_OR_GREATER
		else if (type == typeof(System.TimeOnly))
		{
			return FbDbType.Time;
		}
#endif
		else
		{
			throw new ArgumentException($"Unknown type: {type}.");
		}
	}

	public static Type GetTypeFromBlrType(int type, int subType, int scale) => GetTypeFromDbDataType(GetDbDataTypeFromBlrType(type, subType, scale));

	public static DbType GetDbTypeFromDbDataType(DbDataType type) => type switch
	{
		DbDataType.Array or DbDataType.Binary => DbType.Binary,
		DbDataType.Text or DbDataType.VarChar or DbDataType.Char => DbType.String,
		DbDataType.SmallInt => DbType.Int16,
		DbDataType.Integer => DbType.Int32,
		DbDataType.BigInt => DbType.Int64,
		DbDataType.Date => DbType.Date,
		DbDataType.Time => DbType.Time,
		DbDataType.TimeStamp => DbType.DateTime,
		DbDataType.Numeric or DbDataType.Decimal => DbType.Decimal,
		DbDataType.Float => DbType.Single,
		DbDataType.Double => DbType.Double,
		DbDataType.Guid => DbType.Guid,
		DbDataType.Boolean => DbType.Boolean,
		DbDataType.TimeStampTZ or DbDataType.TimeStampTZEx or DbDataType.TimeTZ or DbDataType.TimeTZEx or DbDataType.Dec16 or DbDataType.Dec34 or DbDataType.Int128 => DbType.Object,// nothing better at the moment
		_ => throw InvalidDataType((int) type),
	};

	public static DbDataType GetDbDataTypeFromDbType(DbType type) => type switch
	{
		DbType.String or DbType.AnsiString => DbDataType.VarChar,
		DbType.StringFixedLength or DbType.AnsiStringFixedLength => DbDataType.Char,
		DbType.Byte or DbType.SByte or DbType.Int16 or DbType.UInt16 => DbDataType.SmallInt,
		DbType.Int32 or DbType.UInt32 => DbDataType.Integer,
		DbType.Int64 or DbType.UInt64 => DbDataType.BigInt,
		DbType.Date => DbDataType.Date,
		DbType.Time => DbDataType.Time,
		DbType.DateTime => DbDataType.TimeStamp,
		DbType.Object or DbType.Binary => DbDataType.Binary,
		DbType.Decimal => DbDataType.Decimal,
		DbType.Double => DbDataType.Double,
		DbType.Single => DbDataType.Float,
		DbType.Guid => DbDataType.Guid,
		DbType.Boolean => DbDataType.Boolean,
		_ => throw InvalidDataType((int) type),
	};

	public static DbDataType GetDbDataTypeFromBlrType(int type, int subType, int scale) => GetDbDataTypeFromSqlType(GetSqlTypeFromBlrType(type), subType, scale);

	public static DbDataType GetDbDataTypeFromSqlType(int type, int subType, int scale, int? length = null, Charset charset = null)
	{
		// Special case for Guid handling
		if ((type == IscCodes.SQL_TEXT || type == IscCodes.SQL_VARYING) && length == 16 && (charset?.IsOctetsCharset ?? false))
		{
			return DbDataType.Guid;
		}

		switch (type)
		{
			case IscCodes.SQL_TEXT:
				return DbDataType.Char;

			case IscCodes.SQL_VARYING:
				return DbDataType.VarChar;

			case IscCodes.SQL_SHORT:
				if (subType == 2)
				{
					return DbDataType.Decimal;
				}
				else
				{
					return subType == 1 ? DbDataType.Numeric : scale < 0 ? DbDataType.Decimal : DbDataType.SmallInt;
				}

			case IscCodes.SQL_LONG:
				if (subType == 2)
				{
					return DbDataType.Decimal;
				}
				else
				{
					return subType == 1 ? DbDataType.Numeric : scale < 0 ? DbDataType.Decimal : DbDataType.Integer;
				}

			case IscCodes.SQL_QUAD:
			case IscCodes.SQL_INT64:
				if (subType == 2)
				{
					return DbDataType.Decimal;
				}
				else
				{
					return subType == 1 ? DbDataType.Numeric : scale < 0 ? DbDataType.Decimal : DbDataType.BigInt;
				}

			case IscCodes.SQL_FLOAT:
				return DbDataType.Float;

			case IscCodes.SQL_DOUBLE:
			case IscCodes.SQL_D_FLOAT:
				if (subType == 2)
				{
					return DbDataType.Decimal;
				}
				else
				{
					return subType == 1 ? DbDataType.Numeric : scale < 0 ? DbDataType.Decimal : DbDataType.Double;
				}

			case IscCodes.SQL_BLOB:
				return subType == 1 ? DbDataType.Text : DbDataType.Binary;

			case IscCodes.SQL_TIMESTAMP:
				return DbDataType.TimeStamp;

			case IscCodes.SQL_TYPE_TIME:
				return DbDataType.Time;

			case IscCodes.SQL_TYPE_DATE:
				return DbDataType.Date;

			case IscCodes.SQL_ARRAY:
				return DbDataType.Array;

			case IscCodes.SQL_NULL:
				return DbDataType.Null;

			case IscCodes.SQL_BOOLEAN:
				return DbDataType.Boolean;

			case IscCodes.SQL_TIMESTAMP_TZ:
				return DbDataType.TimeStampTZ;

			case IscCodes.SQL_TIMESTAMP_TZ_EX:
				return DbDataType.TimeStampTZEx;

			case IscCodes.SQL_TIME_TZ:
				return DbDataType.TimeTZ;

			case IscCodes.SQL_TIME_TZ_EX:
				return DbDataType.TimeTZEx;

			case IscCodes.SQL_DEC16:
				return DbDataType.Dec16;

			case IscCodes.SQL_DEC34:
				return DbDataType.Dec34;

			case IscCodes.SQL_INT128:
				if (subType == 2)
				{
					return DbDataType.Decimal;
				}
				else
				{
					return subType == 1 ? DbDataType.Numeric : scale < 0 ? DbDataType.Decimal : DbDataType.Int128;
				}

			default:
				throw InvalidDataType(type);
		}
	}

	public static DbDataType GetDbDataTypeFromFbDbType(FbDbType type) =>
			// these are aligned for this conversion
			(DbDataType) type;

	public static TimeSpan DateTimeTimeToTimeSpan(DateTime d) => TimeSpan.FromTicks(d.Subtract(d.Date).Ticks);

	public static FbZonedDateTime CreateZonedDateTime(DateTime dateTime, ushort tzId, short? offset) => !TimeZoneMapping.TryGetById(tzId, out string tz)
					? throw new ArgumentException("Unknown time zone ID.")
					: new FbZonedDateTime(dateTime, tz, offset != null ? TimeSpan.FromMinutes((short) offset) : (TimeSpan?) null);

	public static FbZonedTime CreateZonedTime(TimeSpan time, ushort tzId, short? offset) => !TimeZoneMapping.TryGetById(tzId, out string tz)
					? throw new ArgumentException("Unknown time zone ID.")
					: new FbZonedTime(time, tz, offset != null ? TimeSpan.FromMinutes((short) offset) : (TimeSpan?) null);

	public static Exception InvalidDataType(int type) => new ArgumentException($"Invalid data type: {type}.");

	public static int BlrAlign(int current, int alignment) => (current + alignment - 1) & ~(alignment - 1);
}
