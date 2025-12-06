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
using System.Threading;
using System.Threading.Tasks;
using FirebirdSql.Data.Types;

namespace FirebirdSql.Data.Common;

internal struct DbValue
{
		private readonly StatementBase _statement;
		private readonly DbField _field;
		private object _value;

		public readonly DbField Field => _field;

		public DbValue(DbField field, object value)
		{
				_field = field;
				_value = value ?? DBNull.Value;
		}

		public DbValue(StatementBase statement, DbField field, object value)
		{
				_statement = statement;
				_field = field;
				_value = value ?? DBNull.Value;
		}

		public readonly bool IsDBNull() => TypeHelper.IsDBNull(_value);

		public object GetValue()
		{
				if (IsDBNull())
				{
						return DBNull.Value;
				}

				switch (_field.DbDataType)
				{
						case DbDataType.Text:
								return _statement == null ? GetInt64() : GetString();

						case DbDataType.Binary:
								return _statement == null ? GetInt64() : GetBinary();

						case DbDataType.Array:
								return _statement == null ? GetInt64() : GetArray();

						default:
								return _value;
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
								return _statement == null ? GetInt64() : await GetStringAsync(cancellationToken).ConfigureAwait(false);

						case DbDataType.Binary:
								return _statement == null ? GetInt64() : await GetBinaryAsync(cancellationToken).ConfigureAwait(false);

						case DbDataType.Array:
								return _statement == null ? GetInt64() : await GetArrayAsync(cancellationToken).ConfigureAwait(false);

						default:
								return _value;
				}
		}

		public void SetValue(object value) => _value = value;

		public string GetString()
		{
				if (Field.DbDataType == DbDataType.Text && _value is long l)
				{
						_value = GetClobData(l);
				}

				return _value is byte[] bytes ? Field.Charset.GetString(bytes) : _value.ToString();
		}
		public async ValueTask<string> GetStringAsync(CancellationToken cancellationToken = default)
		{
				if (Field.DbDataType == DbDataType.Text && _value is long l)
				{
						_value = await GetClobDataAsync(l, cancellationToken).ConfigureAwait(false);
				}

				return _value is byte[] bytes ? Field.Charset.GetString(bytes) : _value.ToString();
		}

		public readonly char GetChar() => Convert.ToChar(_value, CultureInfo.CurrentCulture);

		public readonly bool GetBoolean() => Convert.ToBoolean(_value, CultureInfo.InvariantCulture);

		public readonly byte GetByte() => _value switch
		{
				BigInteger bi => (byte) bi,
				_ => Convert.ToByte(_value, CultureInfo.InvariantCulture),
		};

		public readonly short GetInt16() => _value switch
		{
				BigInteger bi => (short) bi,
				_ => Convert.ToInt16(_value, CultureInfo.InvariantCulture),
		};

		public readonly int GetInt32() => _value switch
		{
				BigInteger bi => (int) bi,
				_ => Convert.ToInt32(_value, CultureInfo.InvariantCulture),
		};

		public readonly long GetInt64() => _value switch
		{
				BigInteger bi => (long) bi,
				_ => Convert.ToInt64(_value, CultureInfo.InvariantCulture),
		};

		public readonly decimal GetDecimal() => Convert.ToDecimal(_value, CultureInfo.InvariantCulture);

		public readonly float GetFloat() => Convert.ToSingle(_value, CultureInfo.InvariantCulture);

		public readonly Guid GetGuid() => _value switch
		{
				Guid guid => guid,
				byte[] bytes => TypeDecoder.DecodeGuid(bytes),
				_ => throw new InvalidOperationException($"Incorrect {nameof(Guid)} value."),
		};

		public readonly double GetDouble() => Convert.ToDouble(_value, CultureInfo.InvariantCulture);

		public readonly DateTime GetDateTime() => _value switch
		{
				DateTimeOffset dto => dto.DateTime,
				FbZonedDateTime zdt => zdt.DateTime,
				_ => Convert.ToDateTime(_value, CultureInfo.CurrentCulture.DateTimeFormat),
		};

		public readonly TimeSpan GetTimeSpan() => (TimeSpan) _value;

		public readonly FbDecFloat GetDecFloat() => (FbDecFloat) _value;

		public readonly BigInteger GetInt128() => _value switch
		{
				byte b => b,
				short s => s,
				int i => i,
				long l => l,
				_ => (BigInteger) _value,
		};

		public readonly FbZonedDateTime GetZonedDateTime() => (FbZonedDateTime) _value;

		public readonly FbZonedTime GetZonedTime() => (FbZonedTime) _value;

		public Array GetArray()
		{
				if (_value is long l)
				{
						_value = GetArrayData(l);
				}

				return (Array) _value;
		}
		public async ValueTask<Array> GetArrayAsync(CancellationToken cancellationToken = default)
		{
				if (_value is long l)
				{
						_value = await GetArrayDataAsync(l, cancellationToken).ConfigureAwait(false);
				}

				return (Array) _value;
		}

		public byte[] GetBinary()
		{
				if (_value is long l)
				{
						_value = GetBlobData(l);
				}
				return _value is Guid guid ? TypeEncoder.EncodeGuid(guid) : (byte[]) _value;
		}
		public async ValueTask<byte[]> GetBinaryAsync(CancellationToken cancellationToken = default)
		{
				if (_value is long l)
				{
						_value = await GetBlobDataAsync(l, cancellationToken).ConfigureAwait(false);
				}
				return _value is Guid guid ? TypeEncoder.EncodeGuid(guid) : (byte[]) _value;
		}

		public readonly BlobStream GetBinaryStream() => _value is not long l ? throw new NotSupportedException() : GetBlobStream(l);
		public readonly ValueTask<BlobStream> GetBinaryStreamAsync(CancellationToken cancellationToken = default) => _value is not long l ? throw new NotSupportedException() : GetBlobStreamAsync(l, cancellationToken);

		public readonly int GetDate() => _value switch
		{
#if NET6_0_OR_GREATER
				DateOnly @do => TypeEncoder.EncodeDate(@do),
#endif
				_ => TypeEncoder.EncodeDate(GetDateTime()),
		};

		public readonly int GetTime() => _value switch
		{
				TimeSpan ts => TypeEncoder.EncodeTime(ts),
				FbZonedTime zt => TypeEncoder.EncodeTime(zt.Time),
#if NET6_0_OR_GREATER
				TimeOnly to => TypeEncoder.EncodeTime(to),
#endif
				_ => TypeEncoder.EncodeTime(TypeHelper.DateTimeTimeToTimeSpan(GetDateTime())),
		};

		public readonly ushort GetTimeZoneId()
		{
				{
						if (_value is FbZonedDateTime zdt && TimeZoneMapping.TryGetByName(zdt.TimeZone, out ushort id))
						{
								return id;
						}
				}
				{
						if (_value is FbZonedTime zt && TimeZoneMapping.TryGetByName(zt.TimeZone, out ushort id))
						{
								return id;
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
										byte[] buffer = new byte[Field.Length];
										byte[] bytes;

										if (Field.Charset.IsOctetsCharset)
										{
												bytes = GetBinary();
										}
										else if (Field.Charset.IsNoneCharset)
										{
												byte[] bvalue = Field.Charset.GetBytes(GetString());
												if (bvalue.Length > Field.Length)
												{
														throw IscException.ForErrorCodes([IscCodes.isc_arith_except, IscCodes.isc_string_truncation]);
												}
												bytes = bvalue;
										}
										else
										{
												string svalue = GetString();
												if ((Field.Length % Field.Charset.BytesPerCharacter) == 0 && svalue.EnumerateRunes().Count() > Field.CharCount)
												{
														throw IscException.ForErrorCodes([IscCodes.isc_arith_except, IscCodes.isc_string_truncation]);
												}
												bytes = Field.Charset.GetBytes(svalue);
										}

										for (int i = 0; i < buffer.Length; i++)
										{
												buffer[i] = (byte) ' ';
										}
										Buffer.BlockCopy(bytes, 0, buffer, 0, bytes.Length);
										return buffer;
								}

						case DbDataType.VarChar:
								{
										byte[] buffer = new byte[Field.Length + 2];
										byte[] bytes;

										if (Field.Charset.IsOctetsCharset)
										{
												bytes = GetBinary();
										}
										else if (Field.Charset.IsNoneCharset)
										{
												byte[] bvalue = Field.Charset.GetBytes(GetString());
												if (bvalue.Length > Field.Length)
												{
														throw IscException.ForErrorCodes([IscCodes.isc_arith_except, IscCodes.isc_string_truncation]);
												}
												bytes = bvalue;
										}
										else
										{
												string svalue = GetString();
												if ((Field.Length % Field.Charset.BytesPerCharacter) == 0 && svalue.EnumerateRunes().Count() > Field.CharCount)
												{
														throw IscException.ForErrorCodes([IscCodes.isc_arith_except, IscCodes.isc_string_truncation]);
												}
												bytes = Field.Charset.GetBytes(svalue);
										}

										Buffer.BlockCopy(BitConverter.GetBytes((short) bytes.Length), 0, buffer, 0, 2);
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
										byte[] date = BitConverter.GetBytes(TypeEncoder.EncodeDate(dt));
										byte[] time = BitConverter.GetBytes(TypeEncoder.EncodeTime(TypeHelper.DateTimeTimeToTimeSpan(dt)));

										byte[] result = new byte[8];
										Buffer.BlockCopy(date, 0, result, 0, date.Length);
										Buffer.BlockCopy(time, 0, result, 4, time.Length);
										return result;
								}

						case DbDataType.Guid:
								{
										byte[] bytes = TypeEncoder.EncodeGuid(GetGuid());
										byte[] buffer;
										if (Field.SqlType == IscCodes.SQL_VARYING)
										{
												buffer = new byte[bytes.Length + 2];
												Buffer.BlockCopy(BitConverter.GetBytes((short) bytes.Length), 0, buffer, 0, 2);
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
										byte[] date = BitConverter.GetBytes(TypeEncoder.EncodeDate(dt));
										byte[] time = BitConverter.GetBytes(TypeEncoder.EncodeTime(TypeHelper.DateTimeTimeToTimeSpan(dt)));
										byte[] tzId = BitConverter.GetBytes(GetTimeZoneId());

										byte[] result = new byte[10];
										Buffer.BlockCopy(date, 0, result, 0, date.Length);
										Buffer.BlockCopy(time, 0, result, 4, time.Length);
										Buffer.BlockCopy(tzId, 0, result, 8, tzId.Length);
										return result;
								}

						case DbDataType.TimeStampTZEx:
								{
										var dt = GetDateTime();
										byte[] date = BitConverter.GetBytes(TypeEncoder.EncodeDate(dt));
										byte[] time = BitConverter.GetBytes(TypeEncoder.EncodeTime(TypeHelper.DateTimeTimeToTimeSpan(dt)));
										byte[] tzId = BitConverter.GetBytes(GetTimeZoneId());
										byte[] offset = [0, 0];

										byte[] result = new byte[12];
										Buffer.BlockCopy(date, 0, result, 0, date.Length);
										Buffer.BlockCopy(time, 0, result, 4, time.Length);
										Buffer.BlockCopy(tzId, 0, result, 8, tzId.Length);
										Buffer.BlockCopy(offset, 0, result, 10, offset.Length);
										return result;
								}

						case DbDataType.TimeTZ:
								{
										byte[] time = BitConverter.GetBytes(GetTime());
										byte[] tzId = BitConverter.GetBytes(GetTimeZoneId());

										byte[] result = new byte[6];
										Buffer.BlockCopy(time, 0, result, 0, time.Length);
										Buffer.BlockCopy(tzId, 0, result, 4, tzId.Length);
										return result;
								}

						case DbDataType.TimeTZEx:
								{
										byte[] time = BitConverter.GetBytes(GetTime());
										byte[] tzId = BitConverter.GetBytes(GetTimeZoneId());
										byte[] offset = [0, 0];

										byte[] result = new byte[8];
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
								throw TypeHelper.InvalidDataType((int) Field.DbDataType);
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
										byte[] buffer = new byte[Field.Length];
										byte[] bytes;

										if (Field.Charset.IsOctetsCharset)
										{
												bytes = await GetBinaryAsync(cancellationToken).ConfigureAwait(false);
										}
										else if (Field.Charset.IsNoneCharset)
										{
												byte[] bvalue = Field.Charset.GetBytes(await GetStringAsync(cancellationToken).ConfigureAwait(false));
												if (bvalue.Length > Field.Length)
												{
														throw IscException.ForErrorCodes([IscCodes.isc_arith_except, IscCodes.isc_string_truncation]);
												}
												bytes = bvalue;
										}
										else
										{
												string svalue = await GetStringAsync(cancellationToken).ConfigureAwait(false);
												if ((Field.Length % Field.Charset.BytesPerCharacter) == 0 && svalue.EnumerateRunes().Count() > Field.CharCount)
												{
														throw IscException.ForErrorCodes([IscCodes.isc_arith_except, IscCodes.isc_string_truncation]);
												}
												bytes = Field.Charset.GetBytes(svalue);
										}

										for (int i = 0; i < buffer.Length; i++)
										{
												buffer[i] = (byte) ' ';
										}
										Buffer.BlockCopy(bytes, 0, buffer, 0, bytes.Length);
										return buffer;
								}

						case DbDataType.VarChar:
								{
										byte[] buffer = new byte[Field.Length + 2];
										byte[] bytes;

										if (Field.Charset.IsOctetsCharset)
										{
												bytes = await GetBinaryAsync(cancellationToken).ConfigureAwait(false);
										}
										else if (Field.Charset.IsNoneCharset)
										{
												byte[] bvalue = Field.Charset.GetBytes(await GetStringAsync(cancellationToken).ConfigureAwait(false));
												if (bvalue.Length > Field.Length)
												{
														throw IscException.ForErrorCodes([IscCodes.isc_arith_except, IscCodes.isc_string_truncation]);
												}
												bytes = bvalue;
										}
										else
										{
												string svalue = await GetStringAsync(cancellationToken).ConfigureAwait(false);
												if ((Field.Length % Field.Charset.BytesPerCharacter) == 0 && svalue.EnumerateRunes().Count() > Field.CharCount)
												{
														throw IscException.ForErrorCodes([IscCodes.isc_arith_except, IscCodes.isc_string_truncation]);
												}
												bytes = Field.Charset.GetBytes(svalue);
										}

										Buffer.BlockCopy(BitConverter.GetBytes((short) bytes.Length), 0, buffer, 0, 2);
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
										byte[] date = BitConverter.GetBytes(TypeEncoder.EncodeDate(dt));
										byte[] time = BitConverter.GetBytes(TypeEncoder.EncodeTime(TypeHelper.DateTimeTimeToTimeSpan(dt)));

										byte[] result = new byte[8];
										Buffer.BlockCopy(date, 0, result, 0, date.Length);
										Buffer.BlockCopy(time, 0, result, 4, time.Length);
										return result;
								}

						case DbDataType.Guid:
								{
										byte[] bytes = TypeEncoder.EncodeGuid(GetGuid());
										byte[] buffer;
										if (Field.SqlType == IscCodes.SQL_VARYING)
										{
												buffer = new byte[bytes.Length + 2];
												Buffer.BlockCopy(BitConverter.GetBytes((short) bytes.Length), 0, buffer, 0, 2);
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
										byte[] date = BitConverter.GetBytes(TypeEncoder.EncodeDate(dt));
										byte[] time = BitConverter.GetBytes(TypeEncoder.EncodeTime(TypeHelper.DateTimeTimeToTimeSpan(dt)));
										byte[] tzId = BitConverter.GetBytes(GetTimeZoneId());

										byte[] result = new byte[10];
										Buffer.BlockCopy(date, 0, result, 0, date.Length);
										Buffer.BlockCopy(time, 0, result, 4, time.Length);
										Buffer.BlockCopy(tzId, 0, result, 8, tzId.Length);
										return result;
								}

						case DbDataType.TimeStampTZEx:
								{
										var dt = GetDateTime();
										byte[] date = BitConverter.GetBytes(TypeEncoder.EncodeDate(dt));
										byte[] time = BitConverter.GetBytes(TypeEncoder.EncodeTime(TypeHelper.DateTimeTimeToTimeSpan(dt)));
										byte[] tzId = BitConverter.GetBytes(GetTimeZoneId());
										byte[] offset = [0, 0];

										byte[] result = new byte[12];
										Buffer.BlockCopy(date, 0, result, 0, date.Length);
										Buffer.BlockCopy(time, 0, result, 4, time.Length);
										Buffer.BlockCopy(tzId, 0, result, 8, tzId.Length);
										Buffer.BlockCopy(offset, 0, result, 10, offset.Length);
										return result;
								}

						case DbDataType.TimeTZ:
								{
										byte[] time = BitConverter.GetBytes(GetTime());
										byte[] tzId = BitConverter.GetBytes(GetTimeZoneId());

										byte[] result = new byte[6];
										Buffer.BlockCopy(time, 0, result, 0, time.Length);
										Buffer.BlockCopy(tzId, 0, result, 4, tzId.Length);
										return result;
								}

						case DbDataType.TimeTZEx:
								{
										byte[] time = BitConverter.GetBytes(GetTime());
										byte[] tzId = BitConverter.GetBytes(GetTimeZoneId());
										byte[] offset = [0, 0];

										byte[] result = new byte[8];
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
								throw TypeHelper.InvalidDataType((int) Field.DbDataType);
				}
		}

		private readonly byte[] GetNumericBytes()
		{
				decimal value = GetDecimal();
				object numeric = TypeEncoder.EncodeDecimal(value, Field.NumericScale, Field.DataType);

				return _field.SqlType switch
				{
						IscCodes.SQL_SHORT => BitConverter.GetBytes((short) numeric),
						IscCodes.SQL_LONG => BitConverter.GetBytes((int) numeric),
						IscCodes.SQL_QUAD or IscCodes.SQL_INT64 => BitConverter.GetBytes((long) numeric),
						IscCodes.SQL_DOUBLE or IscCodes.SQL_D_FLOAT => BitConverter.GetBytes((double) numeric),
						IscCodes.SQL_INT128 => Int128Helper.GetBytes((BigInteger) numeric),
						_ => null,
				};
		}

		private readonly string GetClobData(long blobId)
		{
				var clob = _statement.CreateBlob(blobId);
				return clob.ReadString();
		}
		private readonly ValueTask<string> GetClobDataAsync(long blobId, CancellationToken cancellationToken = default)
		{
				var clob = _statement.CreateBlob(blobId);
				return clob.ReadStringAsync(cancellationToken);
		}

		private readonly byte[] GetBlobData(long blobId)
		{
				var blob = _statement.CreateBlob(blobId);
				return blob.Read();
		}
		private readonly ValueTask<byte[]> GetBlobDataAsync(long blobId, CancellationToken cancellationToken = default)
		{
				var blob = _statement.CreateBlob(blobId);
				return blob.ReadAsync(cancellationToken);
		}

		private readonly BlobStream GetBlobStream(long blobId)
		{
				var blob = _statement.CreateBlob(blobId);
				return new BlobStream(blob);
		}
		private readonly ValueTask<BlobStream> GetBlobStreamAsync(long blobId, CancellationToken cancellationToken = default)
		{
				var blob = _statement.CreateBlob(blobId);
				return ValueTask2.FromResult(new BlobStream(blob));
		}

		private readonly Array GetArrayData(long handle)
		{
				_field.ArrayHandle ??= _statement.CreateArray(handle, Field.Relation, Field.Name);

				var gdsArray = _statement.CreateArray(_field.ArrayHandle.Descriptor);
				gdsArray.Handle = handle;
				gdsArray.Database = _statement.Database;
				gdsArray.Transaction = _statement.Transaction;
				return gdsArray.Read();
		}
		private async readonly ValueTask<Array> GetArrayDataAsync(long handle, CancellationToken cancellationToken = default)
		{
				_field.ArrayHandle ??= await _statement.CreateArrayAsync(handle, Field.Relation, Field.Name, cancellationToken).ConfigureAwait(false);

				var gdsArray = await _statement.CreateArrayAsync(_field.ArrayHandle.Descriptor, cancellationToken).ConfigureAwait(false);
				gdsArray.Handle = handle;
				gdsArray.Database = _statement.Database;
				gdsArray.Transaction = _statement.Transaction;
				return await gdsArray.ReadAsync(cancellationToken).ConfigureAwait(false);
		}
}
