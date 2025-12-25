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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using BenchmarkDotNet.Attributes;
using FirebirdSql.Data.Common;

namespace Perf;

[Config(typeof(InProcessMemoryConfig))]
public class RuneExtensionsTruncationBenchmark
{
	public enum TextKind
	{
		Ascii,
		MixedBmpAndSurrogates,
		MixedBmpAndSurrogates25,
		MostlySurrogates,
	}

	[Params(TextKind.Ascii, TextKind.MixedBmpAndSurrogates, TextKind.MixedBmpAndSurrogates25, TextKind.MostlySurrogates)]
	public TextKind Kind { get; set; }

	[Params(128, 1024)]
	public int RuneLength { get; set; }

	[Params(512)]
	public int MaxRuneCount { get; set; }

	string _text = string.Empty;

	[GlobalSetup]
	public void GlobalSetup()
	{
		_text = CreateText(Kind, RuneLength);
	}

	// Old truncation pattern: enumerate runes -> allocate char[] per rune -> LINQ SelectMany -> concat to string.
	[Benchmark(Description = "old truncate via EnumerateRunesToChars")]
	public string Old_EnumerateRunesToChars()
	{
		if (MaxRuneCount <= 0 || _text.Length == 0)
			return string.Empty;

		return string.Concat(_text.EnumerateRunesToChars().Take(MaxRuneCount).SelectMany(x => x));
	}

	// New approach: slice by rune count without allocating intermediates, then materialize only final string.
	[Benchmark(Description = "new TruncateStringToRuneCount().ToString()")]
	public string New_TruncateStringToRuneCount()
	{
		return _text.AsSpan().TruncateStringToRuneCount(MaxRuneCount).ToString();
	}

	static string CreateText(TextKind kind, int runeLength)
	{
		if (runeLength <= 0)
			return string.Empty;

		var sb = new StringBuilder(runeLength * 2);
		for (var i = 0; i < runeLength; i++)
		{
			switch (kind)
			{
				case TextKind.Ascii:
					sb.Append((char)('a' + (i % 26)));
					break;
				case TextKind.MixedBmpAndSurrogates:
					switch (i & 15)
					{
						case 0: sb.Append("\U0001F600"); break; // 😀 (~6.25% surrogate pairs)
						case 1: sb.Append('Ω'); break;
						case 2: sb.Append('界'); break;
						default: sb.Append('a'); break;
					}
					break;
				case TextKind.MixedBmpAndSurrogates25:
					switch (i & 3)
					{
						case 0: sb.Append('a'); break;
						case 1: sb.Append('Ω'); break;
						case 2: sb.Append("\U0001F600"); break; // 😀 (25% surrogate pairs)
						default: sb.Append('界'); break;
					}
					break;
				case TextKind.MostlySurrogates:
					if ((i & 7) == 0)
						sb.Append('a');
					else
						sb.Append("\U0001F642"); // 🙂
					break;
				default:
					throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
			}
		}
		return sb.ToString();
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class RuneExtensionsCountBenchmark
{
	public enum TextKind
	{
		Ascii,
		MixedBmpAndSurrogates,
		MixedBmpAndSurrogates25,
		MostlySurrogates,
	}

	[Params(TextKind.Ascii, TextKind.MixedBmpAndSurrogates, TextKind.MixedBmpAndSurrogates25, TextKind.MostlySurrogates)]
	public TextKind Kind { get; set; }

	[Params(128, 8192)]
	public int RuneLength { get; set; }

	string _text = string.Empty;

	[GlobalSetup]
	public void GlobalSetup()
	{
		_text = CreateText(Kind, RuneLength);
	}

	[Benchmark(Description = "old Count() over EnumerateRunesToChars")]
	public int Old_EnumerateRunesToChars_Count()
	{
		// Horrible old pattern: allocates char[] per rune, then LINQ Count enumerates them all.
		return _text.EnumerateRunesToChars().Count();
	}

	[Benchmark(Description = "new CountRunes(span)")]
	public int New_CountRunes()
	{
		return _text.AsSpan().CountRunes();
	}

	static string CreateText(TextKind kind, int runeLength)
	{
		if (runeLength <= 0)
			return string.Empty;

		var sb = new StringBuilder(runeLength * 2);
		for (var i = 0; i < runeLength; i++)
		{
			switch (kind)
			{
				case TextKind.Ascii:
					sb.Append((char)('a' + (i % 26)));
					break;
				case TextKind.MixedBmpAndSurrogates:
					switch (i & 15)
					{
						case 0: sb.Append("\U0001F600"); break; // 😀 (~6.25% surrogate pairs)
						case 1: sb.Append('Ω'); break;
						case 2: sb.Append('界'); break;
						default: sb.Append('a'); break;
					}
					break;
				case TextKind.MixedBmpAndSurrogates25:
					switch (i & 3)
					{
						case 0: sb.Append('a'); break;
						case 1: sb.Append('Ω'); break;
						case 2: sb.Append("\U0001F600"); break; // 😀 (25% surrogate pairs)
						default: sb.Append('界'); break;
					}
					break;
				case TextKind.MostlySurrogates:
					if ((i & 7) == 0)
						sb.Append('a');
					else
						sb.Append("\U0001F642"); // 🙂
					break;
				default:
					throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
			}
		}
		return sb.ToString();
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class RuneExtensionsCountImplementationBenchmark
{
	public enum TextKind
	{
		Ascii,
		MixedBmpAndSurrogates,
		MixedBmpAndSurrogates25,
		MostlySurrogates,
	}

	[Params(TextKind.Ascii, TextKind.MixedBmpAndSurrogates, TextKind.MixedBmpAndSurrogates25, TextKind.MostlySurrogates)]
	public TextKind Kind { get; set; }

	[Params(128, 8192)]
	public int RuneLength { get; set; }

	string _text = string.Empty;

	[GlobalSetup]
	public void GlobalSetup()
	{
		_text = CreateText(Kind, RuneLength);

		var span = _text.AsSpan();
		var expected = span.CountRunes();
		if (PrevCountRunes(span) != expected)
			throw new InvalidOperationException("PrevCountRunes does not match CountRunes.");
		if (CandidateCountRunes(span) != expected)
			throw new InvalidOperationException("CandidateCountRunes does not match CountRunes.");
		if (Candidate2CountRunes(span) != expected)
			throw new InvalidOperationException("Candidate2CountRunes does not match CountRunes.");
		if (Candidate4CountRunes(span) != expected)
			throw new InvalidOperationException("Candidate4CountRunes does not match CountRunes.");
	}

	[Benchmark(Baseline = true, Description = "prev CountRunes(span)")]
	public int Prev_CountRunes()
	{
		return PrevCountRunes(_text.AsSpan());
	}

	[Benchmark(Description = "candidate CountRunes(slice IndexOfAnyInRange)")]
	public int Candidate_CountRunes()
	{
		return CandidateCountRunes(_text.AsSpan());
	}

	[Benchmark(Description = "candidate2 CountRunes(hybrid HighSurrogate loop)")]
	public int Candidate2_CountRunes()
	{
		return Candidate2CountRunes(_text.AsSpan());
	}

	[Benchmark(Description = "candidate4 CountRunes(IndexOfAnyInRange + Rune.DecodeFromUtf16)")]
	public int Candidate4_CountRunes()
	{
		return Candidate4CountRunes(_text.AsSpan());
	}

	[Benchmark(Description = "new CountRunes(span)")]
	public int New_CountRunes()
	{
		return _text.AsSpan().CountRunes();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static int PrevCountRunes(ReadOnlySpan<char> text)
	{
		var count = 0;
		var i = 0;
		while (i < text.Length)
		{
			if (char.IsHighSurrogate(text[i]) && i + 1 < text.Length && char.IsLowSurrogate(text[i + 1]))
			{
				i += 2;
			}
			else
			{
				i++;
			}
			count++;
		}
		return count;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static int CandidateCountRunes(ReadOnlySpan<char> text)
	{
		var length = text.Length;
		if (length == 0)
			return 0;

		var i = 0;
		var pairCount = 0;

		while (true)
		{
			var rel = text.Slice(i).IndexOfAnyInRange('\uD800', '\uDBFF'); // high surrogates
			if (rel < 0)
				return length - pairCount;

			i += rel;
			if ((uint)i >= (uint)length)
				return length - pairCount;

			// If next is low surrogate, it's a valid surrogate pair => one rune, two chars.
			if (i + 1 < length && char.IsLowSurrogate(text[i + 1]))
			{
				pairCount++;
				i += 2;
			}
			else
			{
				i += 1; // unpaired high surrogate counts as one rune
			}
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static int Candidate2CountRunes(ReadOnlySpan<char> text)
	{
		var length = text.Length;
		if (length == 0)
			return 0;

		var i = 0;
		var pairCount = 0;

		while (true)
		{
			while (i < length && char.IsHighSurrogate(text[i]))
			{
				if (i + 1 < length && char.IsLowSurrogate(text[i + 1]))
				{
					pairCount++;
					i += 2;
				}
				else
				{
					i++;
				}
			}

			if ((uint)i >= (uint)length)
				return length - pairCount;

			var rel = text.Slice(i).IndexOfAnyInRange('\uD800', '\uDBFF');
			if (rel < 0)
				return length - pairCount;

			i += rel;
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static int Candidate4CountRunes(ReadOnlySpan<char> text)
	{
		var length = text.Length;
		if (length == 0)
			return 0;

		var i = text.IndexOfAnyInRange('\uD800', '\uDBFF');
		if (i < 0)
			return length;

		var count = i;
		while (i < length)
		{
			_ = Rune.DecodeFromUtf16(text.Slice(i), out _, out var charsConsumed);
			if (charsConsumed == 0)
				charsConsumed = 1;
			i += charsConsumed;
			count++;
		}
		return count;
	}

	static string CreateText(TextKind kind, int runeLength)
	{
		if (runeLength <= 0)
			return string.Empty;

		var sb = new StringBuilder(runeLength * 2);
		for (var i = 0; i < runeLength; i++)
		{
			switch (kind)
			{
				case TextKind.Ascii:
					sb.Append((char)('a' + (i % 26)));
					break;
				case TextKind.MixedBmpAndSurrogates:
					switch (i & 15)
					{
						case 0: sb.Append("\U0001F600"); break; // 😀 (~6.25% surrogate pairs)
						case 1: sb.Append('Ω'); break;
						case 2: sb.Append('界'); break;
						default: sb.Append('a'); break;
					}
					break;
				case TextKind.MixedBmpAndSurrogates25:
					switch (i & 3)
					{
						case 0: sb.Append('a'); break;
						case 1: sb.Append('Ω'); break;
						case 2: sb.Append("\U0001F600"); break; // 😀 (25% surrogate pairs)
						default: sb.Append('界'); break;
					}
					break;
				case TextKind.MostlySurrogates:
					if ((i & 7) == 0)
						sb.Append('a');
					else
						sb.Append("\U0001F642"); // 🙂
					break;
				default:
					throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
			}
		}
		return sb.ToString();
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class RuneExtensionsTruncationImplementationBenchmark
{
	public enum TextKind
	{
		Ascii,
		MixedBmpAndSurrogates,
		MixedBmpAndSurrogates25,
		MostlySurrogates,
	}

	[Params(TextKind.Ascii, TextKind.MixedBmpAndSurrogates, TextKind.MixedBmpAndSurrogates25, TextKind.MostlySurrogates)]
	public TextKind Kind { get; set; }

	[Params(128, 1024)]
	public int RuneLength { get; set; }

	[Params(512)]
	public int MaxRuneCount { get; set; }

	string _text = string.Empty;

	[GlobalSetup]
	public void GlobalSetup()
	{
		_text = CreateText(Kind, RuneLength);

		var span = _text.AsSpan();
		var expected = span.TruncateStringToRuneCount(MaxRuneCount);
		if (!PrevTruncateStringToRuneCount(span, MaxRuneCount).SequenceEqual(expected))
			throw new InvalidOperationException("PrevTruncateStringToRuneCount does not match TruncateStringToRuneCount.");
		if (!CandidateTruncateToRuneCount(span, MaxRuneCount).SequenceEqual(expected))
			throw new InvalidOperationException("CandidateTruncateToRuneCount does not match TruncateStringToRuneCount.");
		if (!Candidate2TruncateToRuneCount(span, MaxRuneCount).SequenceEqual(expected))
			throw new InvalidOperationException("Candidate2TruncateToRuneCount does not match TruncateStringToRuneCount.");
		if (!Candidate4TruncateToRuneCount(span, MaxRuneCount).SequenceEqual(expected))
			throw new InvalidOperationException("Candidate4TruncateToRuneCount does not match TruncateStringToRuneCount.");
	}

	[Benchmark(Baseline = true, Description = "prev TruncateStringToRuneCount().ToString()")]
	public string Prev_TruncateStringToRuneCount()
	{
		return PrevTruncateStringToRuneCount(_text.AsSpan(), MaxRuneCount).ToString();
	}

	[Benchmark(Description = "candidate TruncateToRuneCount(window IndexOfAnyInRange).ToString()")]
	public string Candidate_TruncateToRuneCount()
	{
		return CandidateTruncateToRuneCount(_text.AsSpan(), MaxRuneCount).ToString();
	}

	[Benchmark(Description = "candidate2 TruncateToRuneCount(hybrid HighSurrogate loop).ToString()")]
	public string Candidate2_TruncateToRuneCount()
	{
		return Candidate2TruncateToRuneCount(_text.AsSpan(), MaxRuneCount).ToString();
	}

	[Benchmark(Description = "candidate4 TruncateToRuneCount(IndexOfAnyInRange + Rune.DecodeFromUtf16).ToString()")]
	public string Candidate4_TruncateToRuneCount()
	{
		return Candidate4TruncateToRuneCount(_text.AsSpan(), MaxRuneCount).ToString();
	}

	[Benchmark(Description = "new TruncateStringToRuneCount().ToString()")]
	public string New_TruncateStringToRuneCount()
	{
		return _text.AsSpan().TruncateStringToRuneCount(MaxRuneCount).ToString();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static ReadOnlySpan<char> PrevTruncateStringToRuneCount(ReadOnlySpan<char> text, int maxRuneCount)
	{
		var count = 0;
		var i = 0;
		while (i < text.Length && count < maxRuneCount)
		{
			var nextI = i;
			if (char.IsHighSurrogate(text[i]) && i + 1 < text.Length && char.IsLowSurrogate(text[i + 1]))
			{
				nextI += 2;
			}
			else
			{
				nextI++;
			}
			count++;
			if (count <= maxRuneCount)
			{
				i = nextI;
			}
		}
		return text[..i];
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static ReadOnlySpan<char> CandidateTruncateToRuneCount(ReadOnlySpan<char> text, int maxRunes)
	{
		if (maxRunes <= 0 || text.IsEmpty)
			return ReadOnlySpan<char>.Empty;

		var length = text.Length;
		if (maxRunes >= length) // runeCount <= length always
			return text;

		var i = 0;
		var remaining = maxRunes;

		while (remaining > 0 && i < length)
		{
			// In the best case (no surrogates), we can take 'remaining' chars directly.
			var take = Math.Min(remaining, length - i);
			var window = text.Slice(i, take);

			var rel = window.IndexOfAnyInRange('\uD800', '\uDBFF');
			if (rel < 0)
			{
				i += take;
				remaining -= take;
				continue;
			}

			// Consume BMP chars up to the next high surrogate.
			i += rel;
			remaining -= rel;

			if (remaining == 0 || i >= length)
				break;

			// Consume 1 rune at this position (either a surrogate pair or a single char).
			if (i + 1 < length && char.IsLowSurrogate(text[i + 1]))
				i += 2;
			else
				i += 1;

			remaining -= 1;
		}

		return text[..i];
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static ReadOnlySpan<char> Candidate2TruncateToRuneCount(ReadOnlySpan<char> text, int maxRunes)
	{
		if (maxRunes <= 0 || text.IsEmpty)
			return ReadOnlySpan<char>.Empty;

		var length = text.Length;
		if (maxRunes >= length)
			return text;

		var i = 0;
		var remaining = maxRunes;

		while (remaining > 0 && i < length)
		{
			while (remaining > 0 && i < length && char.IsHighSurrogate(text[i]))
			{
				if (i + 1 < length && char.IsLowSurrogate(text[i + 1]))
					i += 2;
				else
					i += 1;
				remaining--;
			}

			if (remaining == 0 || i >= length)
				break;

			// In the best case (no surrogates), we can take 'remaining' chars directly.
			var take = Math.Min(remaining, length - i);
			var window = text.Slice(i, take);

			var rel = window.IndexOfAnyInRange('\uD800', '\uDBFF');
			if (rel < 0)
			{
				i += take;
				remaining -= take;
				continue;
			}

			// Consume BMP chars up to the next high surrogate.
			i += rel;
			remaining -= rel;
		}

		return text[..i];
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static ReadOnlySpan<char> Candidate4TruncateToRuneCount(ReadOnlySpan<char> text, int maxRuneCount)
	{
		if (maxRuneCount <= 0 || text.IsEmpty)
			return ReadOnlySpan<char>.Empty;

		var length = text.Length;
		if (maxRuneCount >= length)
			return text;

		var prefix = text[..maxRuneCount];
		var i = prefix.IndexOfAnyInRange('\uD800', '\uDBFF');
		if (i < 0)
			return prefix;

		var remaining = maxRuneCount - i;
		while (i < length && remaining > 0)
		{
			_ = Rune.DecodeFromUtf16(text.Slice(i), out _, out var charsConsumed);
			if (charsConsumed == 0)
				charsConsumed = 1;
			i += charsConsumed;
			remaining--;
		}

		return text[..i];
	}

	static string CreateText(TextKind kind, int runeLength)
	{
		if (runeLength <= 0)
			return string.Empty;

		var sb = new StringBuilder(runeLength * 2);
		for (var i = 0; i < runeLength; i++)
		{
			switch (kind)
			{
				case TextKind.Ascii:
					sb.Append((char)('a' + (i % 26)));
					break;
				case TextKind.MixedBmpAndSurrogates:
					switch (i & 15)
					{
						case 0: sb.Append("\U0001F600"); break; // 😀 (~6.25% surrogate pairs)
						case 1: sb.Append('Ω'); break;
						case 2: sb.Append('界'); break;
						default: sb.Append('a'); break;
					}
					break;
				case TextKind.MixedBmpAndSurrogates25:
					switch (i & 3)
					{
						case 0: sb.Append('a'); break;
						case 1: sb.Append('Ω'); break;
						case 2: sb.Append("\U0001F600"); break; // 😀 (25% surrogate pairs)
						default: sb.Append('界'); break;
					}
					break;
				case TextKind.MostlySurrogates:
					if ((i & 7) == 0)
						sb.Append('a');
					else
						sb.Append("\U0001F642"); // 🙂
					break;
				default:
					throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
			}
		}
		return sb.ToString();
	}
}

public enum RuneFixedLengthCaseKind
{
	BmpOnly,
	OnePairMiddle,
	OnePairEnd,
	SurrogatePercent,
}

public readonly record struct RuneFixedLengthCase(RuneFixedLengthCaseKind Kind, int SurrogatePercent)
{
	public override string ToString()
	{
		return Kind switch
		{
			RuneFixedLengthCaseKind.BmpOnly => "BMP_only",
			RuneFixedLengthCaseKind.OnePairMiddle => "1_pair_middle",
			RuneFixedLengthCaseKind.OnePairEnd => "1_pair_end",
			RuneFixedLengthCaseKind.SurrogatePercent => $"{SurrogatePercent:D3}%_surrogates",
			_ => Kind.ToString(),
		};
	}
}

static class RuneFixedLengthText
{
	const char BmpChar = 'a';

	// 😀 U+1F600 (high + low surrogate)
	const char HighSurrogate = '\uD83D';
	const char LowSurrogate = '\uDE00';

	public static IEnumerable<RuneFixedLengthCase> GetCases()
	{
		yield return new RuneFixedLengthCase(RuneFixedLengthCaseKind.BmpOnly, 0);
		yield return new RuneFixedLengthCase(RuneFixedLengthCaseKind.OnePairMiddle, 0);
		yield return new RuneFixedLengthCase(RuneFixedLengthCaseKind.OnePairEnd, 0);

		for (var p = 5; p <= 100; p += 5)
		{
			yield return new RuneFixedLengthCase(RuneFixedLengthCaseKind.SurrogatePercent, p);
		}
	}

	public static string Create(int length, RuneFixedLengthCase @case)
	{
		if (length <= 0)
			return string.Empty;

		var chars = new char[length];
		chars.AsSpan().Fill(BmpChar);

		switch (@case.Kind)
		{
			case RuneFixedLengthCaseKind.BmpOnly:
				break;

			case RuneFixedLengthCaseKind.OnePairMiddle:
				InsertPair(chars, Math.Min(length - 2, length / 2));
				break;

			case RuneFixedLengthCaseKind.OnePairEnd:
				InsertPair(chars, length - 2);
				break;

			case RuneFixedLengthCaseKind.SurrogatePercent:
			{
				// Percentage is in UTF-16 code units, rounded to an even number (valid pairs).
				// Pairs are distributed deterministically across the span (not clustered).
				var surrogateCodeUnits = (length * @case.SurrogatePercent + 50) / 100;
				surrogateCodeUnits &= ~1;
				if (surrogateCodeUnits > length)
					surrogateCodeUnits = length & ~1;

				var pairCount = surrogateCodeUnits / 2;
				if (pairCount == 0)
					break;

				var candidateCount = length / 2;
				var candidatePositions = new int[candidateCount];
				for (var i = 0; i < candidateCount; i++)
				{
					candidatePositions[i] = i * 2;
				}

				var seed = unchecked((uint)(length * 397) ^ (uint)@case.SurrogatePercent * 0x9E3779B9u);
				if (seed == 0)
					seed = 0x1234567u;
				Shuffle(candidatePositions, seed);

				for (var i = 0; i < pairCount; i++)
				{
					var pos = candidatePositions[i];
					chars[pos] = HighSurrogate;
					chars[pos + 1] = LowSurrogate;
				}
				break;
			}

			default:
				throw new ArgumentOutOfRangeException(nameof(@case), @case, null);
		}

		return new string(chars);
	}

	static void InsertPair(char[] chars, int index)
	{
		if ((uint)index >= (uint)(chars.Length - 1))
			throw new ArgumentOutOfRangeException(nameof(index));

		chars[index] = HighSurrogate;
		chars[index + 1] = LowSurrogate;
	}

	static void Shuffle(Span<int> items, uint seed)
	{
		var state = seed;
		for (var i = items.Length - 1; i > 0; i--)
		{
			state = XorShift32(state);
			var j = (int)(state % (uint)(i + 1));
			(items[i], items[j]) = (items[j], items[i]);
		}
	}

	static uint XorShift32(uint x)
	{
		x ^= x << 13;
		x ^= x >> 17;
		x ^= x << 5;
		return x;
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class RuneExtensionsCandidate2CountFixedLengthBenchmark
{
	[Params(32768)] //[Params(256, 32768)]
	public int Length { get; set; }

	[ParamsSource(nameof(Cases))]
	public RuneFixedLengthCase Case { get; set; }

	string _text = string.Empty;

	public IEnumerable<RuneFixedLengthCase> Cases() => RuneFixedLengthText.GetCases();

	[GlobalSetup]
	public void GlobalSetup()
	{
		_text = RuneFixedLengthText.Create(Length, Case);

		var span = _text.AsSpan();
		var expected = span.CountRunes();
		if (Candidate2CountRunes(span) != expected)
			throw new InvalidOperationException("Candidate2CountRunes does not match CountRunes.");
	}

	[Benchmark(Baseline = true, Description = "new CountRunes(span)")]
	public int New_CountRunes()
	{
		return _text.AsSpan().CountRunes();
	}

	[Benchmark(Description = "candidate2 CountRunes(hybrid)")]
	public int Candidate2_CountRunes()
	{
		return Candidate2CountRunes(_text.AsSpan());
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static int Candidate2CountRunes(ReadOnlySpan<char> text)
	{
		var length = text.Length;
		if (length == 0)
			return 0;

		var i = 0;
		var pairCount = 0;

		while (true)
		{
			while (i < length && char.IsHighSurrogate(text[i]))
			{
				if (i + 1 < length && char.IsLowSurrogate(text[i + 1]))
				{
					pairCount++;
					i += 2;
				}
				else
				{
					i++;
				}
			}

			if ((uint)i >= (uint)length)
				return length - pairCount;

			var rel = text.Slice(i).IndexOfAnyInRange('\uD800', '\uDBFF');
			if (rel < 0)
				return length - pairCount;

			i += rel;
		}
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class RuneExtensionsCandidate4CountFixedLengthBenchmark
{
	[Params(256, 32768)]
	public int Length { get; set; }

	[ParamsSource(nameof(Cases))]
	public RuneFixedLengthCase Case { get; set; }

	string _text = string.Empty;

	public IEnumerable<RuneFixedLengthCase> Cases() => RuneFixedLengthText.GetCases();

	[GlobalSetup]
	public void GlobalSetup()
	{
		_text = RuneFixedLengthText.Create(Length, Case);

		var span = _text.AsSpan();
		var expected = span.CountRunes();
		if (Candidate4CountRunes(span) != expected)
			throw new InvalidOperationException("Candidate4CountRunes does not match CountRunes.");
	}

	[Benchmark(Baseline = true, Description = "new CountRunes(span)")]
	public int New_CountRunes()
	{
		return _text.AsSpan().CountRunes();
	}

	[Benchmark(Description = "candidate4 CountRunes(IndexOfAnyInRange + Rune.DecodeFromUtf16)")]
	public int Candidate4_CountRunes()
	{
		return Candidate4CountRunes(_text.AsSpan());
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static int Candidate4CountRunes(ReadOnlySpan<char> text)
	{
		var length = text.Length;
		if (length == 0)
			return 0;

		var i = text.IndexOfAnyInRange('\uD800', '\uDBFF');
		if (i < 0)
			return length;

		var count = i;
		while (i < length)
		{
			_ = Rune.DecodeFromUtf16(text.Slice(i), out _, out var charsConsumed);
			if (charsConsumed == 0)
				charsConsumed = 1;
			i += charsConsumed;
			count++;
		}
		return count;
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class RuneExtensionsCandidate3CountClusteredWindowBenchmark
{
	public enum TextKind
	{
		Ascii,
		MixedBmpAndSurrogates,
		MixedBmpAndSurrogates25,
		MostlySurrogates,
	}

	[Params(TextKind.Ascii, TextKind.MixedBmpAndSurrogates, TextKind.MixedBmpAndSurrogates25, TextKind.MostlySurrogates)]
	public TextKind Kind { get; set; }

	[Params(128, 8192)]
	public int RuneLength { get; set; }

	[Params(128, 256, 512)]
	public int Window { get; set; }

	string _text = string.Empty;

	[GlobalSetup]
	public void GlobalSetup()
	{
		_text = CreateText(Kind, RuneLength);

		var span = _text.AsSpan();
		var expected = span.CountRunes();
		if (Candidate3CountRunes(span, Window) != expected)
			throw new InvalidOperationException("Candidate3CountRunes does not match CountRunes.");
	}

	[Benchmark(Baseline = true, Description = "new CountRunes(span)")]
	public int New_CountRunes()
	{
		return _text.AsSpan().CountRunes();
	}

	[Benchmark(Description = "candidate3 CountRunes(clustered window)")]
	public int Candidate3_CountRunes()
	{
		return Candidate3CountRunes(_text.AsSpan(), Window);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static bool IsHigh(char c) => (uint)(c - '\uD800') <= 0x3FF;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static bool IsLow(char c) => (uint)(c - '\uDC00') <= 0x3FF;

	[MethodImpl(MethodImplOptions.NoInlining)]
	static int Candidate3CountRunes(ReadOnlySpan<char> text, int window)
	{
		var length = text.Length;
		if (length == 0)
			return 0;

		var i = 0;
		var pairCount = 0;

		while (true)
		{
			var rel = text.Slice(i).IndexOfAnyInRange('\uD800', '\uDBFF');
			if (rel < 0)
				return length - pairCount;

			i += rel;

			var end = Math.Min(length, i + window);
			while (i < end)
			{
				var c = text[i];
				if (IsHigh(c) && i + 1 < length && IsLow(text[i + 1]))
				{
					pairCount++;
					i += 2;
				}
				else
				{
					i++;
				}
			}

			if ((uint)i >= (uint)length)
				return length - pairCount;
		}
	}

	static string CreateText(TextKind kind, int runeLength)
	{
		if (runeLength <= 0)
			return string.Empty;

		var sb = new StringBuilder(runeLength * 2);
		for (var i = 0; i < runeLength; i++)
		{
			switch (kind)
			{
				case TextKind.Ascii:
					sb.Append((char)('a' + (i % 26)));
					break;
				case TextKind.MixedBmpAndSurrogates:
					switch (i & 15)
					{
						case 0: sb.Append("\U0001F600"); break; // 😀 (~6.25% surrogate pairs)
						case 1: sb.Append('Ω'); break;
						case 2: sb.Append('界'); break;
						default: sb.Append('a'); break;
					}
					break;
				case TextKind.MixedBmpAndSurrogates25:
					switch (i & 3)
					{
						case 0: sb.Append('a'); break;
						case 1: sb.Append('Ω'); break;
						case 2: sb.Append("\U0001F600"); break; // 😀 (25% surrogate pairs)
						default: sb.Append('界'); break;
					}
					break;
				case TextKind.MostlySurrogates:
					if ((i & 7) == 0)
						sb.Append('a');
					else
						sb.Append("\U0001F642"); // 🙂
					break;
				default:
					throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
			}
		}
		return sb.ToString();
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class RuneExtensionsCandidate3CountFixedLengthBenchmark
{
	[Params(256, 32768)]
	public int Length { get; set; }

	[ParamsSource(nameof(Cases))]
	public RuneFixedLengthCase Case { get; set; }

	[Params(128, 256, 512)]
	public int Window { get; set; }

	string _text = string.Empty;

	public IEnumerable<RuneFixedLengthCase> Cases() => RuneFixedLengthText.GetCases();

	[GlobalSetup]
	public void GlobalSetup()
	{
		_text = RuneFixedLengthText.Create(Length, Case);

		var span = _text.AsSpan();
		var expected = span.CountRunes();
		if (Candidate3CountRunes(span, Window) != expected)
			throw new InvalidOperationException("Candidate3CountRunes does not match CountRunes.");
	}

	[Benchmark(Baseline = true, Description = "new CountRunes(span)")]
	public int New_CountRunes()
	{
		return _text.AsSpan().CountRunes();
	}

	[Benchmark(Description = "candidate3 CountRunes(clustered window)")]
	public int Candidate3_CountRunes()
	{
		return Candidate3CountRunes(_text.AsSpan(), Window);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static bool IsHigh(char c) => (uint)(c - '\uD800') <= 0x3FF;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	static bool IsLow(char c) => (uint)(c - '\uDC00') <= 0x3FF;

	[MethodImpl(MethodImplOptions.NoInlining)]
	static int Candidate3CountRunes(ReadOnlySpan<char> text, int window)
	{
		var length = text.Length;
		if (length == 0)
			return 0;

		var i = 0;
		var pairCount = 0;

		while (true)
		{
			var rel = text.Slice(i).IndexOfAnyInRange('\uD800', '\uDBFF');
			if (rel < 0)
				return length - pairCount;

			i += rel;

			var end = Math.Min(length, i + window);
			while (i < end)
			{
				var c = text[i];
				if (IsHigh(c) && i + 1 < length && IsLow(text[i + 1]))
				{
					pairCount++;
					i += 2;
				}
				else
				{
					i++;
				}
			}

			if ((uint)i >= (uint)length)
				return length - pairCount;
		}
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class RuneExtensionsCandidate2TruncationFixedLengthBenchmark
{
	[Params(256, 32768)]
	public int Length { get; set; }

	[ParamsSource(nameof(Cases))]
	public RuneFixedLengthCase Case { get; set; }

	string _text = string.Empty;
	int _maxRunes;

	public IEnumerable<RuneFixedLengthCase> Cases() => RuneFixedLengthText.GetCases();

	[GlobalSetup]
	public void GlobalSetup()
	{
		_text = RuneFixedLengthText.Create(Length, Case);
		_maxRunes = Math.Max(0, Length - 1);

		var span = _text.AsSpan();
		var expected = span.TruncateStringToRuneCount(_maxRunes);
		if (!Candidate2TruncateToRuneCount(span, _maxRunes).SequenceEqual(expected))
			throw new InvalidOperationException("Candidate2TruncateToRuneCount does not match TruncateStringToRuneCount.");
	}

	[Benchmark(Baseline = true, Description = "new TruncateStringToRuneCount(span).Length")]
	public int New_TruncateStringToRuneCount_Length()
	{
		return _text.AsSpan().TruncateStringToRuneCount(_maxRunes).Length;
	}

	[Benchmark(Description = "candidate2 TruncateToRuneCount(span).Length")]
	public int Candidate2_TruncateToRuneCount_Length()
	{
		return Candidate2TruncateToRuneCount(_text.AsSpan(), _maxRunes).Length;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static ReadOnlySpan<char> Candidate2TruncateToRuneCount(ReadOnlySpan<char> text, int maxRunes)
	{
		if (maxRunes <= 0 || text.IsEmpty)
			return ReadOnlySpan<char>.Empty;

		var length = text.Length;
		if (maxRunes >= length)
			return text;

		var i = 0;
		var remaining = maxRunes;

		while (remaining > 0 && i < length)
		{
			while (remaining > 0 && i < length && char.IsHighSurrogate(text[i]))
			{
				if (i + 1 < length && char.IsLowSurrogate(text[i + 1]))
					i += 2;
				else
					i += 1;
				remaining--;
			}

			if (remaining == 0 || i >= length)
				break;

			// In the best case (no surrogates), we can take 'remaining' chars directly.
			var take = Math.Min(remaining, length - i);
			var window = text.Slice(i, take);

			var rel = window.IndexOfAnyInRange('\uD800', '\uDBFF');
			if (rel < 0)
			{
				i += take;
				remaining -= take;
				continue;
			}

			// Consume BMP chars up to the next high surrogate.
			i += rel;
			remaining -= rel;
		}

		return text[..i];
	}
}

[Config(typeof(InProcessMemoryConfig))]
public class RuneExtensionsCandidate4TruncationFixedLengthBenchmark
{
	[Params(256, 32768)]
	public int Length { get; set; }

	[ParamsSource(nameof(Cases))]
	public RuneFixedLengthCase Case { get; set; }

	string _text = string.Empty;
	int _maxRunes;

	public IEnumerable<RuneFixedLengthCase> Cases() => RuneFixedLengthText.GetCases();

	[GlobalSetup]
	public void GlobalSetup()
	{
		_text = RuneFixedLengthText.Create(Length, Case);
		_maxRunes = Math.Max(0, Length - 1);

		var span = _text.AsSpan();
		var expected = span.TruncateStringToRuneCount(_maxRunes);
		if (!Candidate4TruncateToRuneCount(span, _maxRunes).SequenceEqual(expected))
			throw new InvalidOperationException("Candidate4TruncateToRuneCount does not match TruncateStringToRuneCount.");
	}

	[Benchmark(Baseline = true, Description = "new TruncateStringToRuneCount(span).Length")]
	public int New_TruncateStringToRuneCount_Length()
	{
		return _text.AsSpan().TruncateStringToRuneCount(_maxRunes).Length;
	}

	[Benchmark(Description = "candidate4 TruncateToRuneCount(span).Length")]
	public int Candidate4_TruncateToRuneCount_Length()
	{
		return Candidate4TruncateToRuneCount(_text.AsSpan(), _maxRunes).Length;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	static ReadOnlySpan<char> Candidate4TruncateToRuneCount(ReadOnlySpan<char> text, int maxRuneCount)
	{
		if (maxRuneCount <= 0 || text.IsEmpty)
			return ReadOnlySpan<char>.Empty;

		var length = text.Length;
		if (maxRuneCount >= length)
			return text;

		var prefix = text[..maxRuneCount];
		var i = prefix.IndexOfAnyInRange('\uD800', '\uDBFF');
		if (i < 0)
			return prefix;

		var remaining = maxRuneCount - i;
		while (i < length && remaining > 0)
		{
			_ = Rune.DecodeFromUtf16(text.Slice(i), out _, out var charsConsumed);
			if (charsConsumed == 0)
				charsConsumed = 1;
			i += charsConsumed;
			remaining--;
		}

		return text[..i];
	}
}
