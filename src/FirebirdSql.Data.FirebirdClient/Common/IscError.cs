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

namespace FirebirdSql.Data.Common;

[Serializable]
internal sealed class IscError
{
	private readonly string _strParam;

	public string Message { get; set; }
	public int ErrorCode { get; }
	public int Type { get; }

	public string StrParam => Type switch
	{
		IscCodes.isc_arg_interpreted or IscCodes.isc_arg_string or IscCodes.isc_arg_cstring or IscCodes.isc_arg_sql_state => _strParam,
		IscCodes.isc_arg_number => ErrorCode.ToString(CultureInfo.InvariantCulture),
		_ => string.Empty,
	};

	public bool IsArgument => Type switch
	{
		IscCodes.isc_arg_interpreted or IscCodes.isc_arg_string or IscCodes.isc_arg_cstring or IscCodes.isc_arg_number => true,
		_ => false,
	};

	public bool IsWarning => Type == IscCodes.isc_arg_warning;

	internal IscError(int errorCode)
	{
		ErrorCode = errorCode;
	}

	internal IscError(int type, string strParam)
	{
		Type = type;
		_strParam = strParam;
	}

	internal IscError(int type, int errorCode)
	{
		Type = type;
		ErrorCode = errorCode;
	}
}
