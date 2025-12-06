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
using System.Text;

namespace FirebirdSql.Data.Logging;

[Obsolete("Use ConsoleLoggingProvider instead.")]
public class ConsoleLoggerProvider(FbLogLevel minimumLevel = FbLogLevel.Info) : ConsoleLoggingProvider(minimumLevel)
{
}

public class ConsoleLoggingProvider(FbLogLevel minimumLevel = FbLogLevel.Info) : IFbLoggingProvider
{
		readonly FbLogLevel _minimumLevel = minimumLevel;

		public IFbLogger CreateLogger(string name) => new ConsoleLogger(_minimumLevel);

		sealed class ConsoleLogger(FbLogLevel minimumLevel) : IFbLogger
		{
				readonly FbLogLevel _minimumLevel = minimumLevel;

				public bool IsEnabled(FbLogLevel level) => level >= _minimumLevel;

				public void Log(FbLogLevel level, string msg, Exception exception = null)
				{
						if (!IsEnabled(level))
								return;

						var sb = new StringBuilder();
						_ = sb.Append('[');
						_ = sb.Append(level.ToString().ToUpperInvariant());
						_ = sb.Append("] ");

						_ = sb.AppendLine(msg);

						if (exception != null)
								_ = sb.AppendLine(exception.ToString());

						Console.Error.Write(sb.ToString());
				}
		}
}
