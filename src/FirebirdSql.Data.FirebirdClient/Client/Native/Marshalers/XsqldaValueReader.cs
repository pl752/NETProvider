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
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FirebirdSql.Data.Common;

namespace FirebirdSql.Data.Client.Native.Marshalers;

internal static unsafe class XsqldaValueReader
{
	private const int SqldataOffset = sizeof(short) * 4;
	private static readonly int SqlindOffset = SqldataOffset + IntPtr.Size;

	public static void FillValuePointers(IntPtr sqlda, int sqln, IntPtr[] sqldataPointers, IntPtr[] sqlindPointers)
	{
		if (sqlda == IntPtr.Zero)
			throw new ArgumentOutOfRangeException(nameof(sqlda));
		if (sqln < 0)
			throw new ArgumentOutOfRangeException(nameof(sqln));
		if (sqldataPointers == null)
			throw new ArgumentNullException(nameof(sqldataPointers));
		if (sqlindPointers == null)
			throw new ArgumentNullException(nameof(sqlindPointers));
		if (sqldataPointers.Length < sqln)
			throw new ArgumentOutOfRangeException(nameof(sqldataPointers));
		if (sqlindPointers.Length < sqln)
			throw new ArgumentOutOfRangeException(nameof(sqlindPointers));

		for (var i = 0; i < sqln; i++)
		{
			var xsqlvarPtr = IntPtr.Add(sqlda, XsqldaMarshaler.ComputeLength(i));
			sqldataPointers[i] = Marshal.ReadIntPtr(xsqlvarPtr, SqldataOffset);
			sqlindPointers[i] = Marshal.ReadIntPtr(xsqlvarPtr, SqlindOffset);
		}
	}

	public static void ReadRowValues(StatementBase statement, Descriptor fields, IntPtr[] sqldataPointers, IntPtr[] sqlindPointers, DbValue[] rowValues)
	{
		if (fields == null)
			throw new ArgumentNullException(nameof(fields));
		if (sqldataPointers == null)
			throw new ArgumentNullException(nameof(sqldataPointers));
		if (sqlindPointers == null)
			throw new ArgumentNullException(nameof(sqlindPointers));
		if (rowValues == null)
			throw new ArgumentNullException(nameof(rowValues));

		var count = fields.ActualCount;
		for (var i = 0; i < count; i++)
		{
			var field = fields[i];
			var dbValue = rowValues[i];

			dbValue.Reset(statement, field);

			var sqlindPtr = sqlindPointers[i];
			var nullFlag = sqlindPtr == IntPtr.Zero
				? (short)0
				: Unsafe.ReadUnaligned<short>((void*)sqlindPtr);
			field.NullFlag = nullFlag;

			if (nullFlag == -1)
			{
				dbValue.SetDBNull();
				continue;
			}

			var sqldataPtr = sqldataPointers[i];
			if (sqldataPtr == IntPtr.Zero)
			{
				dbValue.SetDBNull();
				continue;
			}

			switch (field.SqlType)
			{
				case IscCodes.SQL_TEXT:
					{
						var length = field.Length;
						if (length == 0)
						{
							dbValue.SetDBNull();
							break;
						}
						ReadTextOrGuidOrOctets(field, dbValue, (byte*)sqldataPtr, length);
						break;
					}

				case IscCodes.SQL_VARYING:
					{
						var length = Unsafe.ReadUnaligned<short>((void*)sqldataPtr);
						var data = (byte*)sqldataPtr + 2;
						ReadTextOrGuidOrOctets(field, dbValue, data, length);
						break;
					}

				case IscCodes.SQL_SHORT:
					{
						var v = Unsafe.ReadUnaligned<short>((void*)sqldataPtr);
						if (field.NumericScale < 0)
						{
							dbValue.SetValue(TypeDecoder.DecodeDecimal(v, field.NumericScale, field.DataType));
						}
						else
						{
							dbValue.SetValue(v);
						}
						break;
					}

				case IscCodes.SQL_LONG:
					{
						var v = Unsafe.ReadUnaligned<int>((void*)sqldataPtr);
						if (field.NumericScale < 0)
						{
							dbValue.SetValue(TypeDecoder.DecodeDecimal(v, field.NumericScale, field.DataType));
						}
						else
						{
							dbValue.SetValue(v);
						}
						break;
					}

				case IscCodes.SQL_FLOAT:
					dbValue.SetValue(Unsafe.ReadUnaligned<float>((void*)sqldataPtr));
					break;

				case IscCodes.SQL_DOUBLE:
				case IscCodes.SQL_D_FLOAT:
					dbValue.SetValue(Unsafe.ReadUnaligned<double>((void*)sqldataPtr));
					break;

				case IscCodes.SQL_QUAD:
				case IscCodes.SQL_INT64:
				case IscCodes.SQL_BLOB:
				case IscCodes.SQL_ARRAY:
					{
						var v = Unsafe.ReadUnaligned<long>((void*)sqldataPtr);
						if (field.NumericScale < 0)
						{
							dbValue.SetValue(TypeDecoder.DecodeDecimal(v, field.NumericScale, field.DataType));
						}
						else
						{
							dbValue.SetValue(v);
						}
						break;
					}

				case IscCodes.SQL_TIMESTAMP:
					{
						var date = Unsafe.ReadUnaligned<int>((void*)sqldataPtr);
						var time = Unsafe.ReadUnaligned<int>((void*)((byte*)sqldataPtr + 4));
						var dt = TypeDecoder.DecodeDate(date);
						dbValue.SetValue(dt.Add(TypeDecoder.DecodeTime(time)));
						break;
					}

				case IscCodes.SQL_TYPE_TIME:
					{
						var time = Unsafe.ReadUnaligned<int>((void*)sqldataPtr);
						dbValue.SetValue(TypeDecoder.DecodeTime(time));
						break;
					}

				case IscCodes.SQL_TYPE_DATE:
					{
						var date = Unsafe.ReadUnaligned<int>((void*)sqldataPtr);
						dbValue.SetValue(TypeDecoder.DecodeDate(date));
						break;
					}

				case IscCodes.SQL_BOOLEAN:
					dbValue.SetValue(Unsafe.ReadUnaligned<byte>((void*)sqldataPtr) != 0);
					break;

				case IscCodes.SQL_TIMESTAMP_TZ:
					{
						var date = Unsafe.ReadUnaligned<int>((void*)sqldataPtr);
						var time = Unsafe.ReadUnaligned<int>((void*)((byte*)sqldataPtr + 4));
						var tzId = Unsafe.ReadUnaligned<ushort>((void*)((byte*)sqldataPtr + 8));
						var dt = DateTime.SpecifyKind(TypeDecoder.DecodeDate(date).Add(TypeDecoder.DecodeTime(time)), DateTimeKind.Utc);
						dbValue.SetValue(TypeHelper.CreateZonedDateTime(dt, tzId, null));
						break;
					}

				case IscCodes.SQL_TIMESTAMP_TZ_EX:
					{
						var date = Unsafe.ReadUnaligned<int>((void*)sqldataPtr);
						var time = Unsafe.ReadUnaligned<int>((void*)((byte*)sqldataPtr + 4));
						var tzId = Unsafe.ReadUnaligned<ushort>((void*)((byte*)sqldataPtr + 8));
						var offset = Unsafe.ReadUnaligned<short>((void*)((byte*)sqldataPtr + 10));
						var dt = DateTime.SpecifyKind(TypeDecoder.DecodeDate(date).Add(TypeDecoder.DecodeTime(time)), DateTimeKind.Utc);
						dbValue.SetValue(TypeHelper.CreateZonedDateTime(dt, tzId, offset));
						break;
					}

				case IscCodes.SQL_TIME_TZ:
					{
						var time = Unsafe.ReadUnaligned<int>((void*)sqldataPtr);
						var tzId = Unsafe.ReadUnaligned<ushort>((void*)((byte*)sqldataPtr + 4));
						dbValue.SetValue(TypeHelper.CreateZonedTime(TypeDecoder.DecodeTime(time), tzId, null));
						break;
					}

				case IscCodes.SQL_TIME_TZ_EX:
					{
						var time = Unsafe.ReadUnaligned<int>((void*)sqldataPtr);
						var tzId = Unsafe.ReadUnaligned<ushort>((void*)((byte*)sqldataPtr + 4));
						var offset = Unsafe.ReadUnaligned<short>((void*)((byte*)sqldataPtr + 6));
						dbValue.SetValue(TypeHelper.CreateZonedTime(TypeDecoder.DecodeTime(time), tzId, offset));
						break;
					}

				case IscCodes.SQL_DEC16:
					dbValue.SetDec16LittleEndian(new ReadOnlySpan<byte>((void*)sqldataPtr, 8));
					break;

				case IscCodes.SQL_DEC34:
					dbValue.SetDec34LittleEndian(new ReadOnlySpan<byte>((void*)sqldataPtr, 16));
					break;

				case IscCodes.SQL_INT128:
					{
						var bytes = new ReadOnlySpan<byte>((void*)sqldataPtr, 16);
						if (field.NumericScale < 0)
						{
							var int128 = new BigInteger(bytes, isUnsigned: false, isBigEndian: !BitConverter.IsLittleEndian);
							dbValue.SetValue(TypeDecoder.DecodeDecimal(int128, field.NumericScale, field.DataType));
						}
						else
						{
							dbValue.SetInt128LittleEndian(bytes);
						}
						break;
					}

				default:
					throw TypeHelper.InvalidDataType(field.SqlType);
			}
		}
	}

	private static void ReadTextOrGuidOrOctets(DbField field, DbValue dbValue, byte* data, int length)
	{
		if (field.DbDataType == DbDataType.Guid)
		{
			if (length == 16)
			{
				dbValue.SetValue(TypeDecoder.DecodeGuidSpan(new Span<byte>(data, 16)));
			}
			else
			{
				var tmp = new byte[length];
				new ReadOnlySpan<byte>(data, length).CopyTo(tmp);
				dbValue.SetValue(TypeDecoder.DecodeGuid(tmp));
			}
			return;
		}

		if (field.Charset.IsOctetsCharset)
		{
			var tmp = new byte[length];
			new ReadOnlySpan<byte>(data, length).CopyTo(tmp);
			dbValue.SetValue(tmp);
			return;
		}

		var s = field.Charset.GetString(new ReadOnlySpan<byte>(data, length));
		if ((field.Length % field.Charset.BytesPerCharacter) == 0)
		{
			var runes = s.CountRunes();
			if (runes > field.CharCount)
			{
				s = new string(s.TruncateStringToRuneCount(field.CharCount));
			}
		}
		dbValue.SetValue(s);
	}
}

