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

//$Authors = Jiri Cincura (jiri@cincura.net)

using System;

namespace FirebirdSql.Data.Common;

internal static class SizeHelper
{
	public static bool IsValidPageSize(int value) => value is 1024
				or 2048
				or 4096
				or 8192
				or 16384
				or 32768;

	public static bool IsValidBatchBufferSize(int value) => value is >= 0 and <= (256 * 1024 * 1024);

	public static Exception InvalidSizeException(string what) => new InvalidOperationException($"Invalid {what} size.");
}
