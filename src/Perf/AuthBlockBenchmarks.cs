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
using System.IO;
using System.Net;
using System.Text;
using BenchmarkDotNet.Attributes;
using FirebirdSql.Data.Common;

namespace Perf;

[Config(typeof(InProcessMemoryConfig))]
public class AuthBlockUserIdentificationDataBenchmark
{
	public enum Scenario
	{
		Srp,
		Sspi,
	}

	public enum WireCryptOptionBench
	{
		Disabled,
		Enabled,
		Required,
	}

	[Params(Scenario.Srp, Scenario.Sspi)]
	public Scenario Case { get; set; }

	[Params(WireCryptOptionBench.Disabled, WireCryptOptionBench.Enabled)]
	public WireCryptOptionBench WireCrypt { get; set; }

	readonly string _envUser = "user";
	readonly string _host = "host";
	readonly string _login = "SYSDBA";
	readonly string _srp256Name = "Srp256";
	readonly string _srpName = "Srp";
	readonly string _sspiName = "Sspi";

	string _publicKeyHex = string.Empty;
	byte[] _sspiSpecificData = Array.Empty<byte>();

	[GlobalSetup]
	public void GlobalSetup()
	{
		// Must fit in the new code's stackalloc scratchpad (258 bytes total).
		_publicKeyHex = new string('a', 64);

		// Representative "token" size; content doesn't matter for the surrounding code.
		_sspiSpecificData = new byte[160];
		for (var i = 0; i < _sspiSpecificData.Length; i++)
			_sspiSpecificData[i] = (byte)(i + 1);
	}

	[Benchmark(Description = "master AuthBlock.UserIdentificationData()")]
	public byte[] Master()
	{
		return Case == Scenario.Srp
			? Master_Srp()
			: Master_Sspi();
	}

	[Benchmark(Description = "local_opt2 AuthBlock.UserIdentificationData()")]
	public byte[] LocalOpt2()
	{
		return Case == Scenario.Srp
			? LocalOpt2_Srp()
			: LocalOpt2_Sspi();
	}

	byte[] Master_Srp()
	{
		using var result = new MemoryStream(256);

		var user = Encoding.UTF8.GetBytes(_envUser);
		result.WriteByte(IscCodes.CNCT_user);
		result.WriteByte((byte)user.Length);
		result.Write(user, 0, user.Length);

		var host = Encoding.UTF8.GetBytes(_host);
		result.WriteByte(IscCodes.CNCT_host);
		result.WriteByte((byte)host.Length);
		result.Write(host, 0, host.Length);

		result.WriteByte(IscCodes.CNCT_user_verification);
		result.WriteByte(0);

		var login = Encoding.UTF8.GetBytes(_login);
		result.WriteByte(IscCodes.CNCT_login);
		result.WriteByte((byte)login.Length);
		result.Write(login, 0, login.Length);

		var pluginNameBytes = Encoding.UTF8.GetBytes(_srp256Name);
		result.WriteByte(IscCodes.CNCT_plugin_name);
		result.WriteByte((byte)pluginNameBytes.Length);
		result.Write(pluginNameBytes, 0, pluginNameBytes.Length);

		var specificData = Encoding.UTF8.GetBytes(_publicKeyHex);
		WriteMultiPartHelper_Master(result, IscCodes.CNCT_specific_data, specificData);

		var plugins = string.Join(",", new[] { _srp256Name, _srpName });
		var pluginsBytes = Encoding.UTF8.GetBytes(plugins);
		result.WriteByte(IscCodes.CNCT_plugin_list);
		result.WriteByte((byte)pluginsBytes.Length);
		result.Write(pluginsBytes, 0, pluginsBytes.Length);

		result.WriteByte(IscCodes.CNCT_client_crypt);
		result.WriteByte(4);
		result.Write(TypeEncoder.EncodeInt32(WireCryptOptionValue(WireCrypt)), 0, 4);

		return result.ToArray();
	}

	byte[] Master_Sspi()
	{
		using var result = new MemoryStream(256);

		var user = Encoding.UTF8.GetBytes(_envUser);
		result.WriteByte(IscCodes.CNCT_user);
		result.WriteByte((byte)user.Length);
		result.Write(user, 0, user.Length);

		var host = Encoding.UTF8.GetBytes(_host);
		result.WriteByte(IscCodes.CNCT_host);
		result.WriteByte((byte)host.Length);
		result.Write(host, 0, host.Length);

		result.WriteByte(IscCodes.CNCT_user_verification);
		result.WriteByte(0);

		var pluginNameBytes = Encoding.UTF8.GetBytes(_sspiName);
		result.WriteByte(IscCodes.CNCT_plugin_name);
		result.WriteByte((byte)pluginNameBytes.Length);
		result.Write(pluginNameBytes, 0, pluginNameBytes.Length);

		WriteMultiPartHelper_Master(result, IscCodes.CNCT_specific_data, _sspiSpecificData);

		result.WriteByte(IscCodes.CNCT_plugin_list);
		result.WriteByte((byte)pluginNameBytes.Length);
		result.Write(pluginNameBytes, 0, pluginNameBytes.Length);

		result.WriteByte(IscCodes.CNCT_client_crypt);
		result.WriteByte(4);
		result.Write(TypeEncoder.EncodeInt32(IscCodes.WIRE_CRYPT_DISABLED), 0, 4);

		return result.ToArray();
	}

	byte[] LocalOpt2_Srp()
	{
		using var result = new MemoryStream(256);

		Span<byte> scratchpad = stackalloc byte[258];
		WriteUserIdentificationParams_LocalOpt2(result, scratchpad, IscCodes.CNCT_user, _envUser);
		WriteUserIdentificationParams_LocalOpt2(result, scratchpad, IscCodes.CNCT_host, _host);

		result.WriteByte(IscCodes.CNCT_user_verification);
		result.WriteByte(0);

		WriteUserIdentificationParams_LocalOpt2(result, scratchpad, IscCodes.CNCT_login, _login);
		WriteUserIdentificationParams_LocalOpt2(result, scratchpad, IscCodes.CNCT_plugin_name, _srp256Name);

		var len = Encoding.UTF8.GetBytes(_publicKeyHex, scratchpad);
		WriteMultiPartHelper_LocalOpt2(result, IscCodes.CNCT_specific_data, scratchpad[..len]);

		WriteUserIdentificationParams_LocalOpt2(result, scratchpad, IscCodes.CNCT_plugin_list, _srp256Name, _srpName);

		result.WriteByte(IscCodes.CNCT_client_crypt);
		result.WriteByte(4);
		if (!BitConverter.TryWriteBytes(scratchpad, IPAddress.NetworkToHostOrder(WireCryptOptionValue(WireCrypt))))
		{
			throw new InvalidOperationException("Failed to write wire crypt option bytes.");
		}
		result.Write(scratchpad[..4]);

		scratchpad.Clear();
		return result.ToArray();
	}

	byte[] LocalOpt2_Sspi()
	{
		using var result = new MemoryStream(256);

		Span<byte> scratchpad = stackalloc byte[258];
		WriteUserIdentificationParams_LocalOpt2(result, scratchpad, IscCodes.CNCT_user, _envUser);
		WriteUserIdentificationParams_LocalOpt2(result, scratchpad, IscCodes.CNCT_host, _host);

		result.WriteByte(IscCodes.CNCT_user_verification);
		result.WriteByte(0);

		WriteUserIdentificationParams_LocalOpt2(result, scratchpad, IscCodes.CNCT_plugin_name, _sspiName);
		WriteMultiPartHelper_Master(result, IscCodes.CNCT_specific_data, _sspiSpecificData);

		WriteUserIdentificationParams_LocalOpt2(result, scratchpad, IscCodes.CNCT_plugin_list, _sspiName);

		result.WriteByte(IscCodes.CNCT_client_crypt);
		result.WriteByte(4);
		if (!BitConverter.TryWriteBytes(scratchpad, IPAddress.NetworkToHostOrder(IscCodes.WIRE_CRYPT_DISABLED)))
		{
			throw new InvalidOperationException("Failed to write wire crypt disabled bytes.");
		}
		result.Write(scratchpad[..4]);

		scratchpad.Clear();
		return result.ToArray();
	}

	static void WriteMultiPartHelper_Master(MemoryStream stream, byte code, byte[] data)
	{
		const int MaxLength = 255 - 1;
		var part = 0;
		for (var i = 0; i < data.Length; i += MaxLength)
		{
			stream.WriteByte(code);
			var length = Math.Min(data.Length - i, MaxLength);
			stream.WriteByte((byte)(length + 1));
			stream.WriteByte((byte)part);
			stream.Write(data, i, length);
			part++;
		}
	}

	static void WriteMultiPartHelper_LocalOpt2(MemoryStream stream, byte code, ReadOnlySpan<byte> data)
	{
		const int MaxLength = 255 - 1;
		var part = 0;
		for (var i = 0; i < data.Length; i += MaxLength)
		{
			stream.WriteByte(code);
			var length = Math.Min(data.Length - i, MaxLength);
			stream.WriteByte((byte)(length + 1));
			stream.WriteByte((byte)part);
			stream.Write(data[i..(i + length)]);
			part++;
		}
	}

	private const byte SEPARATOR_BYTE = (byte)',';

	static void WriteUserIdentificationParams_LocalOpt2(MemoryStream result, Span<byte> scratchpad, byte type, params ReadOnlySpan<string> strings)
	{
		scratchpad[0] = type;
		int len = 2;
		if (strings.Length > 0)
		{
			len += Encoding.UTF8.GetBytes(strings[0], scratchpad[len..]);
			for (int i = 1; i < strings.Length; i++)
			{
				scratchpad[len++] = SEPARATOR_BYTE;
				len += Encoding.UTF8.GetBytes(strings[i], scratchpad[len..]);
			}
		}
		scratchpad[1] = (byte)(len - 2);
		result.Write(scratchpad[..len]);
	}

	static int WireCryptOptionValue(WireCryptOptionBench wireCrypt)
	{
		return wireCrypt switch
		{
			WireCryptOptionBench.Disabled => IscCodes.WIRE_CRYPT_DISABLED,
			WireCryptOptionBench.Enabled => IscCodes.WIRE_CRYPT_ENABLED,
			WireCryptOptionBench.Required => IscCodes.WIRE_CRYPT_REQUIRED,
			_ => throw new ArgumentOutOfRangeException(nameof(wireCrypt), $"{nameof(wireCrypt)}={wireCrypt}"),
		};
	}
}
