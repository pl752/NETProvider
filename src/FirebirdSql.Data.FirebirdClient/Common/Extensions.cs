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
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;

namespace FirebirdSql.Data.Common;

internal static class Extensions
{
	public static int AsInt(this IntPtr ptr)
	{
		return (int)ptr.ToInt64();
	}

	public static IntPtr ReadIntPtr(this BinaryReader self)
	{
		if (IntPtr.Size == sizeof(int))
		{
			return new IntPtr(self.ReadInt32());
		}
		else if (IntPtr.Size == sizeof(long))
		{
			return new IntPtr(self.ReadInt64());
		}
		else
		{
			throw new NotSupportedException();
		}
	}

	public static string ToHexString(this byte[] b)
	{
#if NET5_0_OR_GREATER
		return Convert.ToHexString(b);
#else
		return BitConverter.ToString(b).Replace("-", string.Empty);
#endif
	}

	public static IEnumerable<IEnumerable<T>> Split<T>(this T[] array, int size)
	{
		for (var i = 0; i < (float)array.Length / size; i++)
		{
			yield return array.Skip(i * size).Take(size);
		}
	}

#if NETSTANDARD2_0
	public static HashSet<T> ToHashSet<T>(this IEnumerable<T> source) => new HashSet<T>(source);
#endif

	public static IEnumerable<char> RunesToChars(this IEnumerable<Rune> s)
	{
		char[] chars = new char[2];
		foreach (var rune in s)
		{
			int n = rune.EncodeToUtf16(chars);
			yield return chars[0];
			if(n > 1) yield return chars[1];
		}
	}

	public static int CountRunes(this ReadOnlySpan<char> text) {
		var count = 0;
		var i = 0;
		while(i < text.Length) {
			if(char.IsHighSurrogate(text[i]) && i + 1 < text.Length && char.IsLowSurrogate(text[i + 1])) {
				i += 2;
			}
			else {
				i++;
			}
			count++;
		}
		return count;
	}

	public static ReadOnlySpan<char> TruncateStringToRuneCount(this ReadOnlySpan<char> text, int maxRuneCount) {
		var count = 0;
		var i = 0;
		while(i < text.Length && count < maxRuneCount) {
			var nextI = i;
			if(char.IsHighSurrogate(text[i]) && i + 1 < text.Length && char.IsLowSurrogate(text[i + 1])) {
				nextI += 2;
			}
			else {
				nextI++;
			}
			count++;
			if(count <= maxRuneCount) {
				i = nextI;
			}
		}
		return text[..i];
	}
}
