using System;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FirebirdSql.Data.Client.Managed;
using FirebirdSql.Data.Common;
using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.TestsBase;
using FirebirdSql.Data.Types;
using NUnit.Framework;

namespace FirebirdSql.Data.FirebirdClient.Tests;

[NoServerCategory]
public class OptimizationRegressionTests
{
	[TestCase(false)]
	[TestCase(true)]
	public void ReplacementParameterRenames_RebuildNamedMappings(bool batch)
	{
		object command = batch ? new FbBatchCommand() : new FbCommand();
		var parameters = batch ? ((FbBatchCommand)command).AddBatchParameters() : ((FbCommand)command).Parameters;
		var originalA = parameters.Add(new FbParameter("@a", 1));
		var originalB = parameters.Add(new FbParameter("@b", 2));
		parameters[0] = new FbParameter("@a", 11);
		parameters[1] = new FbParameter("@b", 22);
		Assert.IsNull(originalA.Parent);
		Assert.IsNull(originalB.Parent);
		Assert.AreSame(parameters, parameters[0].Parent);
		Assert.AreSame(parameters, parameters[1].Parent);
		command.GetType().GetField("_namedParameters", BindingFlags.Instance | BindingFlags.NonPublic)
			.SetValue(command, new[] { "@a", "@b" });
		var descriptor = new Descriptor(2);
		for (var i = 0; i < 2; i++)
		{
			descriptor[i].DataType = IscCodes.SQL_LONG;
			descriptor[i].Length = 4;
		}
		var filler = (IDescriptorFiller)command;
		filler.Fill(descriptor, 0);
		Assert.AreEqual(11, descriptor[0].DbValue.GetInt32());
		Assert.AreEqual(22, descriptor[1].DbValue.GetInt32());
		parameters[0].ParameterName = "@temporary";
		parameters[1].ParameterName = "@a";
		parameters[0].ParameterName = "@b";
		filler.Fill(descriptor, 0);
		Assert.AreEqual(22, descriptor[0].DbValue.GetInt32());
		Assert.AreEqual(11, descriptor[1].DbValue.GetInt32());
	}

	[Test]
	public void ReplacementParameter_RejectsOtherOwnersWithoutChangingCollection()
	{
		using var first = new FbCommand();
		using var second = new FbCommand();
		var original = first.Parameters.Add(new FbParameter("@p", 1));
		var owned = second.Parameters.Add(new FbParameter("@p", 2));
		Assert.Throws<ArgumentException>(() => first.Parameters[0] = owned);
		Assert.AreSame(original, first.Parameters[0]);
		Assert.AreSame(first.Parameters, original.Parent);
		Assert.Throws<ArgumentNullException>(() => first.Parameters[0] = null);
		first.Parameters[0] = original;
		Assert.AreSame(first.Parameters, original.Parent);
	}

	[TestCase(false)]
	[TestCase(true)]
	public async Task DecFloat16_ReadsExactWidthAndPreservesFollowingValues(bool asynchronous)
	{
		var first = new FbDecFloat(123, 0);
		var second = new FbDecFloat(-456, -2);
		using var stream = new MemoryStream();
		stream.Write(TypeEncoder.EncodeDec16(first));
		stream.Write(TypeEncoder.EncodeInt32(123456));
		stream.Write(TypeEncoder.EncodeDec16(second));
		stream.Position = 0;
		var xdr = new XdrReaderWriter(new DataProviderStreamWrapper(stream));
		if (asynchronous)
		{
			Assert.AreEqual(first, await xdr.ReadDec16Async());
			Assert.AreEqual(123456, await xdr.ReadInt32Async());
			Assert.AreEqual(second, await xdr.ReadDec16Async());
		}
		else
		{
			Assert.AreEqual(first, xdr.ReadDec16());
			Assert.AreEqual(123456, xdr.ReadInt32());
			Assert.AreEqual(second, xdr.ReadDec16());
		}
		Assert.AreEqual(stream.Length, stream.Position);
	}

	[TestCase(false)]
	[TestCase(true)]
	public void BooleanNumericGetters_PreserveConvertSemantics(bool value)
	{
		var dbValue = new DbValue(null, value);
		Assert.AreEqual(Convert.ToByte(value), dbValue.GetByte());
		Assert.AreEqual(Convert.ToInt16(value), dbValue.GetInt16());
		Assert.AreEqual(Convert.ToInt32(value), dbValue.GetInt32());
		Assert.AreEqual(Convert.ToInt64(value), dbValue.GetInt64());
		Assert.AreEqual(Convert.ToDecimal(value), dbValue.GetDecimal());
		Assert.AreEqual(Convert.ToSingle(value), dbValue.GetFloat());
		Assert.AreEqual(Convert.ToDouble(value), dbValue.GetDouble());
	}

	[TestCase(0)] // EOF
	[TestCase(1)] // row
	[TestCase(2)] // fetch exception
	public async Task ReadAsync_UnregistersCancellationOnEveryExit(int outcome)
	{
		using var cancellation = new CancellationTokenSource();
		using var connection = new FbConnection();
		var command = new FbCommand { Connection = connection };
		typeof(FbCommand).GetField("_statement", BindingFlags.Instance | BindingFlags.NonPublic)
			.SetValue(command, new ReaderStatement(outcome));
		var reader = new FbDataReader();
		typeof(FbDataReader).GetField("_command", BindingFlags.Instance | BindingFlags.NonPublic)
			.SetValue(reader, command);
		if (outcome == 2)
		{
			Assert.ThrowsAsync<InvalidOperationException>(async () => await reader.ReadAsync(cancellation.Token));
		}
		else
		{
			Assert.AreEqual(outcome == 1, await reader.ReadAsync(cancellation.Token));
		}
		// A leaked callback invokes CancelCommand on the closed connection and
		// throws; no server or timing-dependent cancellation race is needed.
		Assert.DoesNotThrow(() => cancellation.Cancel());
	}

	private sealed class ReaderStatement : FirebirdSql.Data.Client.Managed.Version10.GdsStatement
	{
		private readonly int _outcome;
		public ReaderStatement(int outcome)
			: base((FirebirdSql.Data.Client.Managed.Version10.GdsDatabase)null)
		{
			_outcome = outcome;
		}
		public override async ValueTask<DbValue[]> FetchAsync(CancellationToken cancellationToken = default)
		{
			await Task.Yield();
			if (_outcome == 2)
				throw new InvalidOperationException("Controlled fetch failure.");
			return _outcome == 0 ? null : new[] { new DbValue(null, 1) };
		}
	}
}
