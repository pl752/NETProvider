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
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Text;
using FirebirdSql.Data.Common;

namespace FirebirdSql.Data.FirebirdClient;

[ParenthesizePropertyName(true)]
public sealed class FbParameter : DbParameter, ICloneable
{
		#region Fields

		private FbParameterCollection _parent;
		private FbDbType _fbDbType;
		private ParameterDirection _direction;
		private DataRowVersion _sourceVersion;
		private FbCharset _charset;
		private bool _isNullable;
		private bool _sourceColumnNullMapping;
		private byte _precision;
		private byte _scale;
		private int _size;
		private object _value;
		private string _parameterName;
		private string _sourceColumn;
		private string _internalParameterName;
		private bool _isUnicodeParameterName;

		#endregion

		#region DbParameter properties

		[DefaultValue("")]
		public override string ParameterName
		{
				get => _parameterName;
				set
				{
						_parameterName = value;
						_internalParameterName = NormalizeParameterName(_parameterName);
						_isUnicodeParameterName = IsNonAsciiParameterName(_parameterName);
						_parent?.ParameterNameChanged();
				}
		}

		[Category("Data")]
		[DefaultValue(0)]
		public override int Size
		{
				get => HasSize ? _size : RealValueSize ?? 0;
				set
				{
						ArgumentOutOfRangeException.ThrowIfNegative(value);

						_size = value;

						// Hack for Clob parameters
						if (value == 2147483647 &&
							(FbDbType == FbDbType.VarChar || FbDbType == FbDbType.Char))
						{
								FbDbType = FbDbType.Text;
						}
				}
		}

		[Category("Data")]
		[DefaultValue(ParameterDirection.Input)]
		public override ParameterDirection Direction
		{
				get => _direction; set => _direction = value;
		}

		[Browsable(false)]
		[DesignOnly(true)]
		[DefaultValue(false)]
		[EditorBrowsable(EditorBrowsableState.Advanced)]
		public override bool IsNullable
		{
				get => _isNullable; set => _isNullable = value;
		}

		[Category("Data")]
		[DefaultValue("")]
		public override string SourceColumn
		{
				get => _sourceColumn; set => _sourceColumn = value;
		}

		[Category("Data")]
		[DefaultValue(DataRowVersion.Current)]
		public override DataRowVersion SourceVersion
		{
				get => _sourceVersion; set => _sourceVersion = value;
		}

		[Browsable(false)]
		[Category("Data")]
		[RefreshProperties(RefreshProperties.All)]
		[DesignerSerializationVisibility(DesignerSerializationVisibility.Hidden)]
		public override DbType DbType
		{
				get => TypeHelper.GetDbTypeFromDbDataType((DbDataType) _fbDbType); set => FbDbType = (FbDbType) TypeHelper.GetDbDataTypeFromDbType(value);
		}

		[RefreshProperties(RefreshProperties.All)]
		[Category("Data")]
		[DefaultValue(FbDbType.VarChar)]
		public FbDbType FbDbType
		{
				get => _fbDbType;
				set
				{
						_fbDbType = value;
						IsTypeSet = true;
				}
		}

		[Category("Data")]
		[TypeConverter(typeof(StringConverter)), DefaultValue(null)]
		public override object Value
		{
				get => _value;
				set
				{
						value ??= DBNull.Value;

						if (FbDbType == FbDbType.Guid && value != null &&
							value != DBNull.Value && value is not Guid && value is not byte[])
						{
								throw new InvalidOperationException("Incorrect Guid value.");
						}

						_value = value;

						if (!IsTypeSet)
						{
								SetFbDbType(value);
						}
				}
		}

		[Category("Data")]
		[DefaultValue(FbCharset.Default)]
		public FbCharset Charset
		{
				get => _charset; set => _charset = value;
		}

		public override bool SourceColumnNullMapping
		{
				get => _sourceColumnNullMapping; set => _sourceColumnNullMapping = value;
		}

		#endregion

		#region Properties

		[Category("Data")]
		[DefaultValue((byte) 0)]
		public override byte Precision
		{
				get => _precision; set => _precision = value;
		}

		[Category("Data")]
		[DefaultValue((byte) 0)]
		public override byte Scale
		{
				get => _scale; set => _scale = value;
		}

		#endregion

		#region Internal Properties

		internal FbParameterCollection Parent
		{
				get => _parent;
				set
				{
						_parent?.ParameterNameChanged();
						_parent = value;
						_parent?.ParameterNameChanged();
				}
		}

		internal string InternalParameterName => _internalParameterName;

		internal bool IsTypeSet { get; private set; }

		internal object InternalValue
		{
				get
				{
						switch (_value)
						{
								case string svalue:
										return svalue[..Math.Min(Size, svalue.Length)];
								case byte[] bvalue:
										byte[] result = new byte[Math.Min(Size, bvalue.Length)];
										Array.Copy(bvalue, result, result.Length);
										return result;
								default:
										return _value;
						}
				}
		}

		internal bool HasSize => _size != default;

		#endregion

		#region Constructors

		public FbParameter()
		{
				_fbDbType = FbDbType.VarChar;
				_direction = ParameterDirection.Input;
				_sourceVersion = DataRowVersion.Current;
				_sourceColumn = string.Empty;
				_parameterName = string.Empty;
				_charset = FbCharset.Default;
				_internalParameterName = string.Empty;
		}

		public FbParameter(string parameterName, object value)
			: this()
		{
				ParameterName = parameterName;
				Value = value;
		}

		public FbParameter(string parameterName, FbDbType fbType)
			: this()
		{
				ParameterName = parameterName;
				FbDbType = fbType;
		}

		public FbParameter(string parameterName, FbDbType fbType, int size)
			: this()
		{
				ParameterName = parameterName;
				FbDbType = fbType;
				Size = size;
		}

		public FbParameter(string parameterName, FbDbType fbType, int size, string sourceColumn)
			: this()
		{
				ParameterName = parameterName;
				FbDbType = fbType;
				Size = size;
				_sourceColumn = sourceColumn;
		}

		[EditorBrowsable(EditorBrowsableState.Advanced)]
		public FbParameter(
			string parameterName,
			FbDbType dbType,
			int size,
			ParameterDirection direction,
			bool isNullable,
			byte precision,
			byte scale,
			string sourceColumn,
			DataRowVersion sourceVersion,
			object value)
		{
				ParameterName = parameterName;
				FbDbType = dbType;
				Size = size;
				_direction = direction;
				_isNullable = isNullable;
				_precision = precision;
				_scale = scale;
				_sourceColumn = sourceColumn;
				_sourceVersion = sourceVersion;
				Value = value;
				_charset = FbCharset.Default;
		}

		#endregion

		#region ICloneable Methods
		object ICloneable.Clone() => new FbParameter(
					_parameterName,
					_fbDbType,
					_size,
					_direction,
					_isNullable,
					_precision,
					_scale,
					_sourceColumn,
					_sourceVersion,
					_value)
		{
				Charset = _charset
		};

		#endregion

		#region DbParameter methods

		public override string ToString() => _parameterName;

		public override void ResetDbType() => throw new NotImplementedException();

		#endregion

		#region Private Methods

		private void SetFbDbType(object value)
		{
				value ??= DBNull.Value;
				_fbDbType = TypeHelper.GetFbDataTypeFromType(value.GetType());
		}

		#endregion

		#region Private Properties

		private int? RealValueSize
		{
				get
				{
						string svalue = _value as string;
						if (svalue != null)
						{
								return svalue.Length;
						}
						byte[] bvalue = _value as byte[];
						return bvalue?.Length;
				}
		}

		internal bool IsUnicodeParameterName => _isUnicodeParameterName;

		#endregion

		#region Static Methods

		internal static string NormalizeParameterName(string parameterName) => string.IsNullOrEmpty(parameterName) || parameterName[0] == '@'
					? parameterName
					: "@" + parameterName;

		internal static bool IsNonAsciiParameterName(string parameterName)
		{
				bool isAscii = string.IsNullOrWhiteSpace(parameterName)
					|| Encoding.UTF8.GetByteCount(parameterName) == parameterName.Length;
				return !isAscii;
		}

		#endregion
}
