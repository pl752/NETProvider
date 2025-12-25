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
using BenchmarkDotNet.Attributes;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Engines;
using Org.BouncyCastle.Crypto.Parameters;

namespace Perf;

[Config(typeof(InProcessMemoryConfig))]
public class RC4EngineProcessBytesBenchmark
{
	[Params(1024, 32 * 1024)]
	public int Length { get; set; }

	[Params(false, true)]
	public bool InPlace { get; set; }

	byte[] _key = Array.Empty<byte>();
	byte[] _inputSeed = Array.Empty<byte>();
	byte[] _input = Array.Empty<byte>();
	byte[] _output = Array.Empty<byte>();

	RC4Engine _new = null!;
	RC4Engine_Old _old = null!;

	[GlobalSetup]
	public void GlobalSetup()
	{
		_key = new byte[16];
		for (var i = 0; i < _key.Length; i++)
			_key[i] = (byte)(i + 1);

		_inputSeed = new byte[Length];
		for (var i = 0; i < _inputSeed.Length; i++)
			_inputSeed[i] = (byte)i;

		_input = new byte[Length];
		_output = new byte[Length];
		Buffer.BlockCopy(_inputSeed, 0, _input, 0, _inputSeed.Length);

		_new = new RC4Engine();
		_new.Init(forEncryption: false, new KeyParameter(_key));

		_old = new RC4Engine_Old();
		_old.Init(forEncryption: false, new KeyParameter(_key));

		VerifySameOutput();
	}

	[Benchmark(Baseline = true, Description = "old RC4Engine.ProcessBytes()")]
	public byte Old()
	{
		var output = InPlace ? _input : _output;
		_old.ProcessBytes(_input, 0, Length, output, 0);
		return output[0];
	}

	[Benchmark(Description = "new RC4Engine.ProcessBytes()")]
	public byte New()
	{
		var output = InPlace ? _input : _output;
		_new.ProcessBytes(_input, 0, Length, output, 0);
		return output[0];
	}

	void VerifySameOutput()
	{
		var input = new byte[_inputSeed.Length];
		Buffer.BlockCopy(_inputSeed, 0, input, 0, _inputSeed.Length);
		var outputOld = new byte[_inputSeed.Length];
		var outputNew = new byte[_inputSeed.Length];

		var oldEngine = new RC4Engine_Old();
		oldEngine.Init(forEncryption: false, new KeyParameter(_key));
		oldEngine.ProcessBytes(input, 0, input.Length, outputOld, 0);

		Buffer.BlockCopy(_inputSeed, 0, input, 0, _inputSeed.Length);
		var newEngine = new RC4Engine();
		newEngine.Init(forEncryption: false, new KeyParameter(_key));
		newEngine.ProcessBytes(input, 0, input.Length, outputNew, 0);

		if (!outputOld.AsSpan().SequenceEqual(outputNew))
			throw new InvalidOperationException("RC4 old/new ProcessBytes output mismatch.");
	}

	sealed class RC4Engine_Old
	{
		private const int StateLength = 256;

		private byte[] engineState;
		private int x;
		private int y;
		private byte[] workingKey;

		public void Init(bool forEncryption, ICipherParameters parameters)
		{
			if (parameters is KeyParameter)
			{
				workingKey = ((KeyParameter)parameters).GetKey();
				SetKey(workingKey);
				return;
			}

			throw new ArgumentException("invalid parameter passed to RC4 init");
		}

		public void ProcessBytes(byte[] input, int inOff, int length, byte[] output, int outOff)
		{
			for (int i = 0; i < length; i++)
			{
				x = (x + 1) & 0xff;
				y = (engineState[x] + y) & 0xff;

				// swap
				byte tmp = engineState[x];
				engineState[x] = engineState[y];
				engineState[y] = tmp;

				// xor
				output[i + outOff] = (byte)(input[i + inOff]
						^ engineState[(engineState[x] + engineState[y]) & 0xff]);
			}
		}

		private void SetKey(byte[] keyBytes)
		{
			workingKey = keyBytes;

			x = 0;
			y = 0;

			if (engineState == null)
			{
				engineState = new byte[StateLength];
			}

			for (int i = 0; i < StateLength; i++)
			{
				engineState[i] = (byte)i;
			}

			int i1 = 0;
			int i2 = 0;

			for (int i = 0; i < StateLength; i++)
			{
				i2 = ((keyBytes[i1] & 0xff) + engineState[i] + i2) & 0xff;
				byte tmp = engineState[i];
				engineState[i] = engineState[i2];
				engineState[i2] = tmp;
				i1 = (i1 + 1) % keyBytes.Length;
			}
		}
	}
}
