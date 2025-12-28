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
using System.Runtime.CompilerServices;
using FirebirdSql.Data.Common;

namespace FirebirdSql.Data.Client.Native.Marshalers;

internal static unsafe class XsqldaValueWriter
{
	public static void WriteValues(Descriptor descriptor, IntPtr[] sqldataPointers, IntPtr[] sqlindPointers)
	{
		if (descriptor == null)
			throw new ArgumentNullException(nameof(descriptor));
		if (sqldataPointers == null)
			throw new ArgumentNullException(nameof(sqldataPointers));
		if (sqlindPointers == null)
			throw new ArgumentNullException(nameof(sqlindPointers));

		var count = descriptor.ActualCount;
		for (var i = 0; i < count; i++)
		{
			var field = descriptor[i];

			var sqlindPtr = sqlindPointers[i];
			if (sqlindPtr != IntPtr.Zero)
			{
				Unsafe.WriteUnaligned((void*)sqlindPtr, field.NullFlag);
			}

			var sqldataPtr = sqldataPointers[i];
			if (sqldataPtr == IntPtr.Zero)
			{
				continue;
			}

			var nativeLength = (int)field.Length + (field.SqlType == IscCodes.SQL_VARYING ? 2 : 0);

			if (field.NullFlag == -1 || field.DbValue.IsDBNull())
			{
				new Span<byte>((void*)sqldataPtr, nativeLength).Clear();
				continue;
			}

			var buffer = field.DbValue.GetBytes();
			buffer.CopyTo(new Span<byte>((void*)sqldataPtr, buffer.Length));
		}
	}
}

