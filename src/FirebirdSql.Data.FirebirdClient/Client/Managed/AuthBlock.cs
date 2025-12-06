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
using System.Threading;
using System.Threading.Tasks;
using FirebirdSql.Data.Client.Managed.Srp;
using FirebirdSql.Data.Client.Managed.Sspi;
using FirebirdSql.Data.Common;
using WireCryptOption = FirebirdSql.Data.Client.Managed.Version13.WireCryptOption;

namespace FirebirdSql.Data.Client.Managed;

sealed class AuthBlock(GdsConnection connection, string user, string password, WireCryptOption wireCrypt)
{
	Srp256Client _srp256 = new Srp256Client();
	SrpClient _srp = new SrpClient();
	SspiHelper _sspi = new SspiHelper();

	public GdsConnection Connection { get; } = connection;
	public string User { get; } = user;
	public string Password { get; } = password;
	public WireCryptOption WireCrypt { get; } = wireCrypt;

	public ReadOnlyMemory<byte> ServerData { get; private set; }
	public string AcceptPluginName { get; private set; }
	public bool IsAuthenticated { get; private set; }
	public ReadOnlyMemory<byte> ServerKeys { get; private set; }

	public byte[] PublicClientData { get; private set; }
	public bool HasClientData => ClientData != null;
	public byte[] ClientData { get; private set; }
	public byte[] SessionKey { get; private set; }
	public string SessionKeyName { get; private set; }

	public bool WireCryptInitialized { get; private set; }

	public byte[] UserIdentificationData()
	{
		using (var result = new MemoryStream(256))
		{
			string userString = Environment.GetEnvironmentVariable("USERNAME") ?? Environment.GetEnvironmentVariable("USER") ?? string.Empty;
			Span<byte> user = stackalloc byte[Encoding.UTF8.GetByteCount(userString)];
			_ = Encoding.UTF8.TryGetBytes(userString, user, out int userLen);
			result.WriteByte(IscCodes.CNCT_user);
			result.WriteByte((byte)userLen);
			result.Write(user);

			string hostName = Dns.GetHostName();
			Span<byte> host = stackalloc byte[Encoding.UTF8.GetByteCount(hostName)];
			_ = Encoding.UTF8.TryGetBytes(hostName, host, out int hostLen);
			result.WriteByte(IscCodes.CNCT_host);
			result.WriteByte((byte)hostLen);
			result.Write(host);

			result.WriteByte(IscCodes.CNCT_user_verification);
			result.WriteByte(0);

			if (!string.IsNullOrEmpty(User))
			{
				{
					Span<byte> bytes = stackalloc byte[Encoding.UTF8.GetByteCount(User)];
					_ = Encoding.UTF8.TryGetBytes(User, bytes, out int len);
					result.WriteByte(IscCodes.CNCT_login);
					result.WriteByte((byte)len);
					result.Write(bytes);
				}
				{
					Span<byte> bytes = stackalloc byte[Encoding.UTF8.GetByteCount(_srp256.Name)];
					_ = Encoding.UTF8.TryGetBytes(_srp256.Name, bytes, out int len);
					result.WriteByte(IscCodes.CNCT_plugin_name);
					result.WriteByte((byte)len);
					result.Write(bytes[..len]);
				}
				{
					Span<byte> specificData = stackalloc byte[Encoding.UTF8.GetByteCount(_srp256.PublicKeyHex)];
					_ = Encoding.UTF8.TryGetBytes(_srp256.PublicKeyHex, specificData, out _);
					WriteMultiPartHelper(result, IscCodes.CNCT_specific_data, specificData);
				}
				{
					Span<byte> bytes1 = stackalloc byte[Encoding.UTF8.GetByteCount(_srp256.Name)];
					Span<byte> bytes2 = stackalloc byte[1];
					Span<byte> bytes3 = stackalloc byte[Encoding.UTF8.GetByteCount(_srp.Name)];
					_ = Encoding.UTF8.TryGetBytes(_srp256.Name, bytes1, out int l1);
					_ = Encoding.UTF8.TryGetBytes(",", bytes2, out int l2);
					_ = Encoding.UTF8.TryGetBytes(_srp.Name, bytes3, out int l3);
					result.WriteByte(IscCodes.CNCT_plugin_list);
					result.WriteByte((byte)(l1 + l2 + l3));
					result.Write(bytes1);
					result.Write(bytes2);
					result.Write(bytes3);
				}

				{
					result.WriteByte(IscCodes.CNCT_client_crypt);
					result.WriteByte(4);
					Span<byte> bytes = stackalloc byte[4];
					_ = BitConverter.TryWriteBytes(bytes, IPAddress.NetworkToHostOrder(WireCryptOptionValue(WireCrypt)));
					result.Write(bytes);
				}
			}
			else
			{
				Span<byte> pluginNameBytes = stackalloc byte[Encoding.UTF8.GetByteCount(_sspi.Name)];
				_ = Encoding.UTF8.TryGetBytes(_sspi.Name, pluginNameBytes, out int pluginNameLen);
				result.WriteByte(IscCodes.CNCT_plugin_name);
				result.WriteByte((byte)pluginNameLen);
				result.Write(pluginNameBytes);

				byte[] specificData = _sspi.InitializeClientSecurity();
				WriteMultiPartHelper(result, IscCodes.CNCT_specific_data, specificData);

				result.WriteByte(IscCodes.CNCT_plugin_list);
				result.WriteByte((byte)pluginNameLen);
				result.Write(pluginNameBytes);

				result.WriteByte(IscCodes.CNCT_client_crypt);
				result.WriteByte(4);
				Span<byte> wireCryptBytes = stackalloc byte[4];
				_ = BitConverter.TryWriteBytes(wireCryptBytes, IPAddress.NetworkToHostOrder(IscCodes.WIRE_CRYPT_DISABLED));
				result.Write(wireCryptBytes);
			}

			return result.ToArray();
		}
	}

	public void SendContAuthToBuffer()
	{
		Connection.Xdr.Write(IscCodes.op_cont_auth);
		Connection.Xdr.WriteBuffer(HasClientData ? ClientData : PublicClientData); // p_data
		Connection.Xdr.Write(AcceptPluginName); // p_name
		Connection.Xdr.Write(AcceptPluginName); // p_list
		Connection.Xdr.WriteBuffer(ServerKeys.Span); // p_keys
	}
	public async ValueTask SendContAuthToBufferAsync(CancellationToken cancellationToken = default)
	{
		await Connection.Xdr.WriteAsync(IscCodes.op_cont_auth, cancellationToken).ConfigureAwait(false);
		await Connection.Xdr.WriteBufferAsync(HasClientData ? ClientData : PublicClientData, cancellationToken).ConfigureAwait(false); // p_data
		await Connection.Xdr.WriteAsync(AcceptPluginName, cancellationToken).ConfigureAwait(false); // p_name
		await Connection.Xdr.WriteAsync(AcceptPluginName, cancellationToken).ConfigureAwait(false); // p_list
		await Connection.Xdr.WriteBufferAsync(ServerKeys, cancellationToken).ConfigureAwait(false); // p_keys
	}

	// TODO: maybe more logic can be pulled up here
	public IResponse ProcessContAuthResponse()
	{
		int operation = Connection.Xdr.ReadOperation();
		var response = Connection.ProcessOperation(operation);
		response.HandleResponseException();
		if (response is Version13.ContAuthResponse)
		{
			return response;
		}
		else if (response is Version13.CryptKeyCallbackResponse or Version15.CryptKeyCallbackResponse)
		{
			return response;
		}
		else if (response is GenericResponse genericResponse)
		{
			ServerKeys = genericResponse.Data;
			Complete();
		}
		else
		{
			throw new InvalidOperationException($"Unexpected response ({operation}).");
		}
		return response;
	}
	public async ValueTask<IResponse> ProcessContAuthResponseAsync(CancellationToken cancellationToken = default)
	{
		int operation = await Connection.Xdr.ReadOperationAsync(cancellationToken).ConfigureAwait(false);
		var response = await Connection.ProcessOperationAsync(operation, cancellationToken).ConfigureAwait(false);
		response.HandleResponseException();
		if (response is Version13.ContAuthResponse)
		{
			return response;
		}
		else if (response is Version13.CryptKeyCallbackResponse or Version15.CryptKeyCallbackResponse)
		{
			return response;
		}
		else if (response is GenericResponse genericResponse)
		{
			ServerKeys = genericResponse.Data;
			Complete();
		}
		else
		{
			throw new InvalidOperationException($"Unexpected response ({operation}).");
		}
		return response;
	}

	public void SendWireCryptToBuffer()
	{
		if (WireCrypt == WireCryptOption.Disabled)
			return;

		Connection.Xdr.Write(IscCodes.op_crypt);
		Connection.Xdr.Write(FirebirdNetworkHandlingWrapper.EncryptionName);
		Connection.Xdr.Write(SessionKeyName);
	}
	public async ValueTask SendWireCryptToBufferAsync(CancellationToken cancellationToken = default)
	{
		if (WireCrypt == WireCryptOption.Disabled)
			return;

		await Connection.Xdr.WriteAsync(IscCodes.op_crypt, cancellationToken).ConfigureAwait(false);
		await Connection.Xdr.WriteAsync(FirebirdNetworkHandlingWrapper.EncryptionName, cancellationToken).ConfigureAwait(false);
		await Connection.Xdr.WriteAsync(SessionKeyName, cancellationToken).ConfigureAwait(false);
	}

	public void ProcessWireCryptResponse()
	{
		if (WireCrypt == WireCryptOption.Disabled)
			return;

		// after writing before reading
		Connection.StartEncryption();

		int operation = Connection.Xdr.ReadOperation();
		var response = Connection.ProcessOperation(operation);
		response.HandleResponseException();

		WireCryptInitialized = true;
	}
	public async ValueTask ProcessWireCryptResponseAsync(CancellationToken cancellationToken = default)
	{
		if (WireCrypt == WireCryptOption.Disabled)
			return;

		// after writing before reading
		Connection.StartEncryption();

		int operation = await Connection.Xdr.ReadOperationAsync(cancellationToken).ConfigureAwait(false);
		var response = await Connection.ProcessOperationAsync(operation, cancellationToken).ConfigureAwait(false);
		response.HandleResponseException();

		WireCryptInitialized = true;
	}

	public void WireCryptValidate(int protocolVersion)
	{
		bool validProtocolVersion = protocolVersion is IscCodes.PROTOCOL_VERSION13
			or IscCodes.PROTOCOL_VERSION15
			or IscCodes.PROTOCOL_VERSION16;
		if (validProtocolVersion && WireCrypt == WireCryptOption.Required && IsAuthenticated && !WireCryptInitialized)
		{
			throw IscException.ForErrorCode(IscCodes.isc_wirecrypt_incompatible);
		}
	}

	public void Start(ReadOnlyMemory<byte> serverData, string acceptPluginName, bool isAuthenticated, ReadOnlyMemory<byte> serverKeys)
	{
		ServerData = serverData;
		AcceptPluginName = acceptPluginName;
		IsAuthenticated = isAuthenticated;
		ServerKeys = serverKeys;

		bool hasServerData = ServerData.Length != 0;
		if (AcceptPluginName.Equals(_srp256.Name, StringComparison.Ordinal))
		{
			PublicClientData = Encoding.UTF8.GetBytes(_srp256.PublicKeyHex);
			if (hasServerData)
			{
				ClientData = Encoding.UTF8.GetBytes(_srp256.ClientProof(NormalizeLogin(User), Password, ServerData.ToArray()).ToHexString());
			}
			SessionKey = _srp256.SessionKey;
			SessionKeyName = _srp256.SessionKeyName;
		}
		else if (AcceptPluginName.Equals(_srp.Name, StringComparison.Ordinal))
		{
			PublicClientData = Encoding.UTF8.GetBytes(_srp.PublicKeyHex);
			if (hasServerData)
			{
				ClientData = Encoding.UTF8.GetBytes(_srp.ClientProof(NormalizeLogin(User), Password, ServerData.ToArray()).ToHexString());
			}
			SessionKey = _srp.SessionKey;
			SessionKeyName = _srp.SessionKeyName;
		}
		else if (AcceptPluginName.Equals(_sspi.Name, StringComparison.Ordinal))
		{
			if (hasServerData)
			{
				ClientData = _sspi.GetClientSecurity(ServerData.Span);
			}
		}
		else
		{
			throw new NotSupportedException($"Not supported plugin '{AcceptPluginName}'.");
		}
	}

	public void Complete()
	{
		IsAuthenticated = true;
		ReleaseAuth();
	}

	void ReleaseAuth()
	{
		_srp256 = null;
		_srp = null;
		_sspi?.Dispose();
		_sspi = null;
	}

	static void WriteMultiPartHelper(MemoryStream stream, byte code, byte[] data)
	{
		const int MaxLength = 255 - 1;
		int part = 0;
		for (int i = 0; i < data.Length; i += MaxLength)
		{
			stream.WriteByte(code);
			int length = Math.Min(data.Length - i, MaxLength);
			stream.WriteByte((byte)(length + 1));
			stream.WriteByte((byte)part);
			stream.Write(data, i, length);
			part++;
		}
	}

	static void WriteMultiPartHelper(MemoryStream stream, byte code, ReadOnlySpan<byte> data)
	{
		const int MaxLength = 255 - 1;
		int part = 0;
		for (int i = 0; i < data.Length; i += MaxLength)
		{
			stream.WriteByte(code);
			int length = Math.Min(data.Length - i, MaxLength);
			stream.WriteByte((byte)(length + 1));
			stream.WriteByte((byte)part);
			stream.Write(data[i..(i + length)]);
			part++;
		}
	}

	static int WireCryptOptionValue(WireCryptOption wireCrypt) => wireCrypt switch
	{
		WireCryptOption.Disabled => IscCodes.WIRE_CRYPT_DISABLED,
		WireCryptOption.Enabled => IscCodes.WIRE_CRYPT_ENABLED,
		WireCryptOption.Required => IscCodes.WIRE_CRYPT_REQUIRED,
		_ => throw new ArgumentOutOfRangeException(nameof(wireCrypt), $"{nameof(wireCrypt)}={wireCrypt}"),
	};

	internal static string NormalizeLogin(string login)
	{
		if (string.IsNullOrEmpty(login))
		{
			return login;
		}
		if (login.Length > 2 && login[0] == '"' && login[login.Length - 1] == '"')
		{
			var sb = new StringBuilder(login, 1, login.Length - 2, login.Length - 2);
			for (int idx = 0; idx < sb.Length; idx++)
			{
				// Double double quotes ("") escape a double quote in a quoted string
				if (sb[idx] == '"')
				{
					// Strip double quote escape
					_ = sb.Remove(idx, 1);
					if (idx < sb.Length && sb[idx] == '"')
					{
						// Retain escaped double quote
						idx += 1;
					}
					else
					{
						// The character after escape is not a double quote, we terminate the conversion and truncate.
						// Firebird does this as well (see common/utils.cpp#dpbItemUpper)
						sb.Length = idx;
						return sb.ToString();
					}
				}
			}
			return sb.ToString();
		}
		return login.ToUpperInvariant();
	}
}
