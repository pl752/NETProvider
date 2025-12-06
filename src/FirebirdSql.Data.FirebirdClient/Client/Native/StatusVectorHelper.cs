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
using System.Collections.Generic;
using System.Runtime.InteropServices;
using FirebirdSql.Data.Common;

namespace FirebirdSql.Data.Client.Native
{
	static class StatusVectorHelper
	{
		public static void ProcessStatusVector(IntPtr[] statusVector, Charset charset, Action<IscException> warningMessage)
		{
			var ex = ParseStatusVector(statusVector, charset);

			if (ex != null)
			{
				if (ex.IsWarning)
				{
					warningMessage?.Invoke(ex);
				}
				else
				{
					throw ex;
				}
			}
		}

		public static void ClearStatusVector(IntPtr[] statusVector) => Array.Clear(statusVector, 0, statusVector.Length);

		public static IscException ParseStatusVector(IntPtr[] statusVector, Charset charset)
		{
			IscException exception = null;
			bool eof = false;

			for (int i = 0; i < statusVector.Length;)
			{
				nint arg = statusVector[i++];

				switch (arg.AsInt())
				{
					case IscCodes.isc_arg_gds:
					default:
						nint er = statusVector[i++];
						if (er != IntPtr.Zero)
						{
							exception ??= IscException.ForBuilding();
							exception.Errors.Add(new IscError(arg.AsInt(), er.AsInt()));
						}
						break;

					case IscCodes.isc_arg_end:
						exception?.BuildExceptionData();
						eof = true;
						break;

					case IscCodes.isc_arg_interpreted:
					case IscCodes.isc_arg_string:
						{
							nint ptr = statusVector[i++];
							byte[] buffer = ReadStringData(ptr);
							string value = charset.GetString(buffer);
							exception.Errors.Add(new IscError(arg.AsInt(), value));
						}
						break;

					case IscCodes.isc_arg_cstring:
						{
							i++;

							nint ptr = statusVector[i++];
							byte[] buffer = ReadStringData(ptr);
							string value = charset.GetString(buffer);
							exception.Errors.Add(new IscError(arg.AsInt(), value));
						}
						break;

					case IscCodes.isc_arg_win32:
					case IscCodes.isc_arg_number:
						exception.Errors.Add(new IscError(arg.AsInt(), statusVector[i++].AsInt()));
						break;
					case IscCodes.isc_arg_sql_state:
						{
							nint ptr = statusVector[i++];
							byte[] buffer = ReadStringData(ptr);
							string value = charset.GetString(buffer);
							exception.Errors.Add(new IscError(arg.AsInt(), value));
						}
						break;
				}

				if (eof)
				{
					break;
				}
			}

			return exception;
		}

		private static byte[] ReadStringData(IntPtr ptr)
		{
			var buffer = new List<byte>();
			int offset = 0;
			while (true)
			{
				byte b = Marshal.ReadByte(ptr, offset);
				if (b == 0)
					break;
				buffer.Add(b);
				offset++;
			}
			return [.. buffer];
		}
	}
}
