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

//$Authors = Abel Eduardo Pereira, Jiri Cincura (jiri@cincura.net)

using System;
using System.Collections.Generic;
using System.Text;

namespace FirebirdSql.Data.Isql;

class SqlStringParser(string targetString) {
		readonly string _source = targetString;
		readonly int _sourceLength = targetString.Length;
		string[] _tokens = [" "];

		public string[] Tokens {
				get => _tokens;
				set {
						ArgumentNullException.ThrowIfNull(value);
						foreach(string item in value) {
								ArgumentNullException.ThrowIfNull(value);
								if(string.IsNullOrEmpty(item))
										throw new ArgumentException();
						}
						_tokens = value;
				}
		}

		public IEnumerable<FbStatement> Parse() {
				int lastYield = 0;
				int index = 0;
				var rawResult = new StringBuilder();
				while(true) {
				Continue:
						{ }
						if(index >= _sourceLength) {
								break;
						}
						if(GetChar(index) == '\'') {
								_ = rawResult.Append(GetChar(index));
								index++;
								_ = rawResult.Append(ProcessLiteral(ref index));
								_ = rawResult.Append(GetChar(index));
								index++;
						}
						else if(GetChar(index) == '-' && GetNextChar(index) == '-') {
								index++;
								ProcessSinglelineComment(ref index);
								index++;
						}
						else if(GetChar(index) == '/' && GetNextChar(index) == '*') {
								index++;
								ProcessMultilineComment(ref index);
								index++;
						}
						else {
								foreach(string token in Tokens) {
										if(string.Compare(_source, index, token, 0, token.Length, StringComparison.Ordinal) == 0) {
												index += token.Length;
												yield return new FbStatement(_source.Substring(lastYield, index - lastYield - token.Length), rawResult.ToString());
												lastYield = index;
												_ = rawResult.Clear();
												goto Continue;
										}
								}
								if(!(rawResult.Length == 0 && char.IsWhiteSpace(GetChar(index)))) {
										_ = rawResult.Append(GetChar(index));
								}
								index++;
						}
				}

				if(index >= _sourceLength) {
						string parsed = _source[lastYield..];
						if(parsed.Trim() == string.Empty) {
								yield break;
						}
						yield return new FbStatement(parsed, rawResult.ToString());
						_ = rawResult.Clear();
				}
				else {
						yield return new FbStatement(_source[lastYield..index], rawResult.ToString());
						_ = rawResult.Clear();
				}
		}

		string ProcessLiteral(ref int index) {
				var sb = new StringBuilder();
				while(index < _sourceLength) {
						if(GetChar(index) == '\'') {
								if(GetNextChar(index) == '\'') {
										_ = sb.Append(GetChar(index));
										index++;
								}
								else {
										break;
								}
						}
						_ = sb.Append(GetChar(index));
						index++;
				}
				return sb.ToString();
		}

		void ProcessMultilineComment(ref int index) {
				while(index < _sourceLength) {
						if(GetChar(index) == '*' && GetNextChar(index) == '/') {
								index++;
								break;
						}
						index++;
				}
		}

		void ProcessSinglelineComment(ref int index) {
				while(index < _sourceLength) {
						if(GetChar(index) == '\n') {
								break;
						}
						if(GetChar(index) == '\r') {
								if(GetNextChar(index) == '\n') {
										index++;
								}
								break;
						}
						index++;
				}
		}

		char GetChar(int index) => _source[index];

		char? GetNextChar(int index) => index + 1 < _sourceLength
					? _source[index + 1]
					: (char?)null;
}
