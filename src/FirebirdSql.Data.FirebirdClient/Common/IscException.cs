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
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace FirebirdSql.Data.Common;

internal sealed class IscException : Exception
{
		private string _message;

		public List<IscError> Errors { get; private set; }
		public int ErrorCode { get; private set; }
		public string SQLSTATE { get; private set; }
		public override string Message => _message;
		public bool IsWarning => Errors.FirstOrDefault()?.IsWarning ?? false;

		private IscException(Exception innerException = null)
			: base(innerException?.Message, innerException)
		{
				Errors = [];
		}

		public static IscException ForBuilding() => new IscException();

		public static IscException ForErrorCode(int errorCode, Exception innerException = null)
		{
				var result = new IscException(innerException);
				result.Errors.Add(new IscError(IscCodes.isc_arg_gds, errorCode));
				result.BuildExceptionData();
				return result;
		}

		public static IscException ForErrorCodes(IEnumerable<int> errorCodes, Exception innerException = null)
		{
				var result = new IscException(innerException);
				foreach (int errorCode in errorCodes)
				{
						result.Errors.Add(new IscError(IscCodes.isc_arg_gds, errorCode));
				}
				result.BuildExceptionData();
				return result;
		}

		public static IscException ForSQLSTATE(string sqlState, Exception innerException = null)
		{
				var result = new IscException(innerException);
				result.Errors.Add(new IscError(IscCodes.isc_arg_sql_state, sqlState));
				result.BuildExceptionData();
				return result;
		}

		public static IscException ForStrParam(string strParam, Exception innerException = null)
		{
				var result = new IscException(innerException);
				result.Errors.Add(new IscError(IscCodes.isc_arg_string, strParam));
				result.BuildExceptionData();
				return result;
		}

		public static IscException ForErrorCodeIntParam(int errorCode, int intParam, Exception innerException = null)
		{
				var result = new IscException(innerException);
				result.Errors.Add(new IscError(IscCodes.isc_arg_gds, errorCode));
				result.Errors.Add(new IscError(IscCodes.isc_arg_number, intParam));
				result.BuildExceptionData();
				return result;
		}

		public static IscException ForTypeErrorCodeStrParam(int type, int errorCode, string strParam, Exception innerException = null)
		{
				var result = new IscException(innerException);
				result.Errors.Add(new IscError(type, errorCode));
				result.Errors.Add(new IscError(IscCodes.isc_arg_string, strParam));
				result.BuildExceptionData();
				return result;
		}

		public static IscException ForTypeErrorCodeIntParamStrParam(int type, int errorCode, int intParam, string strParam, Exception innerException = null)
		{
				var result = new IscException(innerException);
				result.Errors.Add(new IscError(type, errorCode));
				result.Errors.Add(new IscError(IscCodes.isc_arg_number, intParam));
				result.Errors.Add(new IscError(IscCodes.isc_arg_string, strParam));
				result.BuildExceptionData();
				return result;
		}

		public static IscException ForIOException(IOException exception) => ForErrorCodes([IscCodes.isc_net_write_err, IscCodes.isc_net_read_err], exception);

		public void BuildExceptionData()
		{
				BuildErrorCode();
				BuildSqlState();
				BuildExceptionMessage();
		}

		private void BuildErrorCode() => ErrorCode = Errors.Count != 0 ? Errors[0].ErrorCode : 0;

		private void BuildSqlState()
		{
				var error = Errors.Find(e => e.Type == IscCodes.isc_arg_sql_state);
				// step #1, maybe we already have a SQLSTATE stuffed in the status vector
				SQLSTATE = error != null
						? error.StrParam
						// step #2, see if we can find a mapping.
						: SqlStateMapping.TryGet(ErrorCode, out string value)
							? value
							: string.Empty;
		}

		private void BuildExceptionMessage()
		{
				var builder = new StringBuilder();

				for (int i = 0; i < Errors.Count; i++)
				{
						if (Errors[i].Type is IscCodes.isc_arg_gds or IscCodes.isc_arg_warning)
						{
								int code = Errors[i].ErrorCode;
								string message = IscErrorMessages.TryGet(code, out string value)
									? value
									: BuildDefaultErrorMessage(code);

								var args = new List<string>();
								int index = i + 1;
								while (index < Errors.Count && Errors[index].IsArgument)
								{
										args.Add(Errors[index++].StrParam);
										i++;
								}

								try
								{
										switch (code)
										{
												case IscCodes.isc_except:
														// Custom exception	add	the	first argument as error	code
														ErrorCode = Convert.ToInt32(args[0], CultureInfo.InvariantCulture);
														// ignoring the message - historical reason
														break;
												case IscCodes.isc_except2:
														// Custom exception. Next Error should be the exception name.
														// And the next one the Exception message
														break;
												case IscCodes.isc_stack_trace:
														// The next error contains the PSQL Stack Trace
														AppendMessage(builder, message, args);
														break;
												default:
														AppendMessage(builder, message, args);
														break;
										}
								}
								catch
								{
										message = BuildDefaultErrorMessage(code);
										AppendMessage(builder, message, args);
								}
						}
				}

				// Update error	collection only	with the main error
				var mainError = new IscError(ErrorCode)
				{
						Message = builder.ToString()
				};

				Errors.Add(mainError);

				// Update exception	message
				_message = builder.ToString();
		}

		private static string BuildDefaultErrorMessage(int code) => string.Format(CultureInfo.CurrentCulture, "No message for error code {0} found.", code);

		private static void AppendMessage(StringBuilder builder, string message, List<string> args)
		{
				if (builder.Length > 0)
				{
						_ = builder.Append(Environment.NewLine);
				}
				_ = builder.AppendFormat(CultureInfo.CurrentCulture, message, args.ToArray());
		}
}
