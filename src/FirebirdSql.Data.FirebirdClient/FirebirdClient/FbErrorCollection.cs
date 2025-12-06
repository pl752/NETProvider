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

namespace FirebirdSql.Data.FirebirdClient;

[Serializable]
[ListBindable(false)]
public sealed class FbErrorCollection : ICollection<FbError>
{
	#region Fields

	private readonly List<FbError> _errors;

	#endregion

	#region Constructors

	internal FbErrorCollection()
	{
		_errors = [];
	}

	#endregion

	#region Properties

	public int Count => _errors.Count;

	public bool IsReadOnly => true;

	#endregion

	#region Methods

	internal int IndexOf(string errorMessage) => _errors.FindIndex(x => string.Equals(x.Message, errorMessage, StringComparison.CurrentCultureIgnoreCase));

	internal FbError Add(FbError error)
	{
		_errors.Add(error);

		return error;
	}

	internal FbError Add(string errorMessage, int number) => Add(new FbError(errorMessage, number));

	void ICollection<FbError>.Add(FbError item) => throw new NotSupportedException();

	void ICollection<FbError>.Clear() => throw new NotSupportedException();

	public bool Contains(FbError item) => _errors.Contains(item);

	public void CopyTo(FbError[] array, int arrayIndex) => _errors.CopyTo(array, arrayIndex);

	bool ICollection<FbError>.Remove(FbError item) => throw new NotSupportedException();

	public IEnumerator<FbError> GetEnumerator() => _errors.GetEnumerator();

	IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

	#endregion
}
