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

namespace FirebirdSql.Data.FirebirdClient;

[Serializable]
public sealed class FbError
{
		#region Fields

		private readonly byte _classError;
		private readonly int _lineNumber;
		private readonly string _message;
		private readonly int _number;

		#endregion

		#region Properties

		public byte Class => _classError;

		public int LineNumber => _lineNumber;

		public string Message => _message;

		public int Number => _number;

		#endregion

		#region Constructors

		internal FbError(string message, int number)
			: this(0, 0, message, number)
		{
		}

		internal FbError(byte classError, string message, int number)
			: this(classError, 0, message, number)
		{
		}

		internal FbError(byte classError, int line, string message, int number)
		{
				_classError = classError;
				_lineNumber = line;
				_number = number;
				_message = message;
		}

		#endregion
}
