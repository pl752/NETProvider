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
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;

namespace FirebirdSql.Data.FirebirdClient;

[ListBindable(false)]
public sealed class FbParameterCollection : DbParameterCollection
{
		#region Fields

		private readonly List<FbParameter> _parameters;
		private bool? _hasParameterWithNonAsciiName;

		#endregion

		#region Indexers

		[Browsable(false)]
		[DesignerSerializationVisibility(DesignerSerializationVisibility.Hidden)]
		public new FbParameter this[string parameterName]
		{
				get => this[IndexOf(parameterName)]; set => this[IndexOf(parameterName)] = value;
		}

		[Browsable(false)]
		[DesignerSerializationVisibility(DesignerSerializationVisibility.Hidden)]
		public new FbParameter this[int index]
		{
				get => _parameters[index]; set => _parameters[index] = value;
		}

		#endregion

		#region DbParameterCollection overriden properties

		[Browsable(false)]
		[DesignerSerializationVisibility(DesignerSerializationVisibility.Hidden)]
		public override int Count => _parameters.Count;

		public override bool IsFixedSize => ((IList) _parameters).IsFixedSize;

		public override bool IsReadOnly => ((IList) _parameters).IsReadOnly;

		public override bool IsSynchronized => ((ICollection) _parameters).IsSynchronized;

		public override object SyncRoot => ((ICollection) _parameters).SyncRoot;

		#endregion

		#region Internal properties

		internal bool HasParameterWithNonAsciiName => _hasParameterWithNonAsciiName ?? (bool) (_hasParameterWithNonAsciiName = _parameters.Any(x => x.IsUnicodeParameterName));

		#endregion

		#region Constructors

		internal FbParameterCollection()
		{
				_parameters = [];
				_hasParameterWithNonAsciiName = null;
		}

		#endregion

		#region DbParameterCollection overriden methods

		public void AddRange(IEnumerable<FbParameter> values)
		{
				foreach (var p in values)
				{
						_ = Add(p);
				}
		}

		public override void AddRange(Array values) => AddRange(values.Cast<object>().Select(x => { EnsureFbParameterType(x); return (FbParameter) x; }));

		public FbParameter AddWithValue(string parameterName, object value) => Add(new FbParameter(parameterName, value));

		public FbParameter Add(string parameterName, object value) => Add(new FbParameter(parameterName, value));

		public FbParameter Add(string parameterName, FbDbType type) => Add(new FbParameter(parameterName, type));

		public FbParameter Add(string parameterName, FbDbType fbType, int size) => Add(new FbParameter(parameterName, fbType, size));

		public FbParameter Add(string parameterName, FbDbType fbType, int size, string sourceColumn) => Add(new FbParameter(parameterName, fbType, size, sourceColumn));

		public FbParameter Add(FbParameter value)
		{
				EnsureFbParameterAddOrInsert(value);

				AttachParameter(value);
				_parameters.Add(value);
				return value;
		}

		public override int Add(object value)
		{
				EnsureFbParameterType(value);

				return IndexOf(Add((FbParameter) value));
		}

		public bool Contains(FbParameter value) => _parameters.Contains(value);

		public override bool Contains(object value)
		{
				EnsureFbParameterType(value);

				return Contains((FbParameter) value);
		}

		public override bool Contains(string parameterName) => IndexOf(parameterName) != -1;

		public int IndexOf(FbParameter value) => _parameters.IndexOf(value);

		public override int IndexOf(object value)
		{
				EnsureFbParameterType(value);

				return IndexOf((FbParameter) value);
		}

		public override int IndexOf(string parameterName) => IndexOf(parameterName, -1);

		internal int IndexOf(string parameterName, int luckyIndex)
		{
				bool isNonAsciiParameterName = FbParameter.IsNonAsciiParameterName(parameterName);
				var usedComparison = isNonAsciiParameterName || HasParameterWithNonAsciiName
					? StringComparison.CurrentCultureIgnoreCase
					: StringComparison.OrdinalIgnoreCase;
				string normalizedParameterName = FbParameter.NormalizeParameterName(parameterName);
				if (luckyIndex != -1 && luckyIndex < _parameters.Count)
				{
						if (_parameters[luckyIndex].InternalParameterName.Equals(normalizedParameterName, usedComparison))
						{
								return luckyIndex;
						}
				}

				return _parameters.FindIndex(x => x.InternalParameterName.Equals(normalizedParameterName, usedComparison));
		}

		public void Insert(int index, FbParameter value)
		{
				EnsureFbParameterAddOrInsert(value);

				AttachParameter(value);
				_parameters.Insert(index, value);
		}

		public override void Insert(int index, object value)
		{
				EnsureFbParameterType(value);

				Insert(index, (FbParameter) value);
		}

		public void Remove(FbParameter value)
		{
				if (!_parameters.Remove(value))
				{
						throw new ArgumentException("The parameter does not exist in the collection.");
				}

				ReleaseParameter(value);
		}

		public override void Remove(object value)
		{
				EnsureFbParameterType(value);

				Remove((FbParameter) value);
		}

		public override void RemoveAt(int index)
		{
				if (index < 0 || index > Count)
				{
						throw new IndexOutOfRangeException("The specified index does not exist.");
				}

				var parameter = this[index];
				_parameters.RemoveAt(index);
				ReleaseParameter(parameter);
		}

		public override void RemoveAt(string parameterName) => RemoveAt(IndexOf(parameterName));

		public void CopyTo(FbParameter[] array, int index) => _parameters.CopyTo(array, index);

		public override void CopyTo(Array array, int index) => ((IList) _parameters).CopyTo(array, index);

		public override void Clear()
		{
				var parameters = _parameters.ToArray();
				_parameters.Clear();
				foreach (var parameter in parameters)
				{
						ReleaseParameter(parameter);
				}
		}

		public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();

		#endregion

		#region DbParameterCollection overriden protected methods

		protected override DbParameter GetParameter(string parameterName) => this[parameterName];

		protected override DbParameter GetParameter(int index) => this[index];

		protected override void SetParameter(int index, DbParameter value) => this[index] = (FbParameter) value;

		protected override void SetParameter(string parameterName, DbParameter value) => this[parameterName] = (FbParameter) value;

		#endregion

		#region Internal Methods

		internal void ParameterNameChanged() => _hasParameterWithNonAsciiName = null;

		#endregion

		#region Private Methods

		private string GenerateParameterName()
		{
				int index = Count + 1;
				while (true)
				{
						string name = "Parameter" + index.ToString(CultureInfo.InvariantCulture);
						if (!Contains(name))
						{
								return name;
						}
						index++;
				}
		}

		private void EnsureFbParameterType(object value)
		{
				if (value is not FbParameter)
				{
						throw new InvalidCastException($"The parameter passed was not a {nameof(FbParameter)}.");
				}
		}

		private void EnsureFbParameterAddOrInsert(FbParameter value)
		{
				ArgumentNullException.ThrowIfNull(value);
				if (value.Parent != null)
				{
						throw new ArgumentException($"The {nameof(FbParameter)} specified in the value parameter is already added to this or another {nameof(FbParameterCollection)}.");
				}
				if (value.ParameterName == null || value.ParameterName.Length == 0)
				{
						value.ParameterName = GenerateParameterName();
				}
				else
				{
						if (Contains(value.ParameterName))
						{
								throw new ArgumentException($"{nameof(FbParameterCollection)} already contains {nameof(FbParameter)} with {nameof(FbParameter.ParameterName)} '{value.ParameterName}'.");
						}
				}
		}

		private void AttachParameter(FbParameter parameter) => parameter.Parent = this;

		private static void ReleaseParameter(FbParameter parameter) => parameter.Parent = null;

		#endregion
}
