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

using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using FirebirdSql.Data.Client.Managed;
using FirebirdSql.Data.Common;
using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.TestsBase;
using NUnit.Framework;

namespace FirebirdSql.Data.FirebirdClient.Tests;

[NoServerCategory]
public class OptimizationCachingTests
{
	private static void SetNamedParameters(FbCommand command, IReadOnlyList<string> namedParameters)
	{
		var field = typeof(FbCommand).GetField("_namedParameters", BindingFlags.Instance | BindingFlags.NonPublic);
		Assert.NotNull(field);
		field.SetValue(command, namedParameters);
	}

	private static void SetNamedParameters(FbBatchCommand command, IReadOnlyList<string> namedParameters)
	{
		var field = typeof(FbBatchCommand).GetField("_namedParameters", BindingFlags.Instance | BindingFlags.NonPublic);
		Assert.NotNull(field);
		field.SetValue(command, namedParameters);
	}

	private static Descriptor CreateIntegerDescriptor(int count)
	{
		var descriptor = new Descriptor((short)count);
		for (var i = 0; i < count; i++)
		{
			descriptor[i].DataType = IscCodes.SQL_LONG;
			descriptor[i].NumericScale = 0;
			descriptor[i].SubType = 0;
			descriptor[i].Length = 4;
		}
		return descriptor;
	}

	private sealed class TestGdsStatement : FirebirdSql.Data.Client.Managed.Version10.GdsStatement
	{
		public TestGdsStatement()
			: base((FirebirdSql.Data.Client.Managed.Version10.GdsDatabase)null)
		{ }

		public static byte[] Process(GenericResponse response) => ProcessInfoSqlResponse(response);
		public static ValueTask<byte[]> ProcessAsync(GenericResponse response) => ProcessInfoSqlResponseAsync(response);
	}

	private sealed class TestStatement : StatementBase
	{
		public override DatabaseBase Database => null;
		public override TransactionBase Transaction { get; set; }
		public override Descriptor Parameters { get; set; }
		public override Descriptor Fields => null;
		public override int FetchSize { get; set; }

		public void SetStatementType(DbStatementType statementType)
		{
			StatementType = statementType;
		}

		public override void Prepare(string commandText) => throw new System.NotSupportedException();
		public override ValueTask PrepareAsync(string commandText, System.Threading.CancellationToken cancellationToken = default) => throw new System.NotSupportedException();
		public override void Execute(int timeout, IDescriptorFiller descriptorFiller) => throw new System.NotSupportedException();
		public override ValueTask ExecuteAsync(int timeout, IDescriptorFiller descriptorFiller, System.Threading.CancellationToken cancellationToken = default) => throw new System.NotSupportedException();
		public override DbValue[] Fetch() => throw new System.NotSupportedException();
		public override ValueTask<DbValue[]> FetchAsync(System.Threading.CancellationToken cancellationToken = default) => throw new System.NotSupportedException();
		public override BlobBase CreateBlob() => throw new System.NotSupportedException();
		public override BlobBase CreateBlob(long handle) => throw new System.NotSupportedException();
		public override ArrayBase CreateArray(ArrayDesc descriptor) => throw new System.NotSupportedException();
		public override ValueTask<ArrayBase> CreateArrayAsync(ArrayDesc descriptor, System.Threading.CancellationToken cancellationToken = default) => throw new System.NotSupportedException();
		public override ArrayBase CreateArray(string tableName, string fieldName) => throw new System.NotSupportedException();
		public override ValueTask<ArrayBase> CreateArrayAsync(string tableName, string fieldName, System.Threading.CancellationToken cancellationToken = default) => throw new System.NotSupportedException();
		public override ArrayBase CreateArray(long handle, string tableName, string fieldName) => throw new System.NotSupportedException();
		public override ValueTask<ArrayBase> CreateArrayAsync(long handle, string tableName, string fieldName, System.Threading.CancellationToken cancellationToken = default) => throw new System.NotSupportedException();
		public override BatchBase CreateBatch() => throw new System.NotSupportedException();
		public override BatchParameterBuffer CreateBatchParameterBuffer() => throw new System.NotSupportedException();

		protected override void TransactionUpdated(object sender, System.EventArgs e) => throw new System.NotSupportedException();
		protected override byte[] GetSqlInfo(byte[] items, int bufferLength) => throw new System.NotSupportedException();
		protected override ValueTask<byte[]> GetSqlInfoAsync(byte[] items, int bufferLength, System.Threading.CancellationToken cancellationToken = default) => throw new System.NotSupportedException();
		protected override void Free(int option) => throw new System.NotSupportedException();
		protected override ValueTask FreeAsync(int option, System.Threading.CancellationToken cancellationToken = default) => throw new System.NotSupportedException();
	}

	[Test]
	public void FbParameterCollection_VersionIncrements_OnAddRemoveAndNameChange()
	{
		var command = new FbCommand();
		var parameters = command.Parameters;

		var version0 = parameters.Version;
		parameters.Add(new FbParameter("@p", 1));
		Assert.Greater(parameters.Version, version0);

		var version1 = parameters.Version;
		parameters[0].ParameterName = "@p2";
		Assert.Greater(parameters.Version, version1);

		var version2 = parameters.Version;
		parameters.RemoveAt(0);
		Assert.Greater(parameters.Version, version2);
	}

	[Test]
	public void FbCommand_NamedParameterMapping_Rebuilds_OnParameterNameChange()
	{
		var command = new FbCommand();
		command.Parameters.Add(new FbParameter("@b", 22));
		command.Parameters.Add(new FbParameter("@a", 11));
		SetNamedParameters(command, new[] { "@a", "@b" });

		var descriptor = CreateIntegerDescriptor(2);

		((IDescriptorFiller)command).Fill(descriptor, 0);
		Assert.AreEqual(11, descriptor[0].DbValue.GetValue());
		Assert.AreEqual(22, descriptor[1].DbValue.GetValue());

		command.Parameters[1].ParameterName = "@c";
		var ex = Assert.Throws<FbException>(() => ((IDescriptorFiller)command).Fill(descriptor, 0));
		StringAssert.Contains("Must declare the variable '@a'", ex.Message);
	}

	[Test]
	public void FbCommand_NamedParameterMapping_HandlesDuplicateNames()
	{
		var command = new FbCommand();
		command.Parameters.Add(new FbParameter("@p", 123));
		SetNamedParameters(command, new[] { "@p", "@p" });

		var descriptor = CreateIntegerDescriptor(2);
		((IDescriptorFiller)command).Fill(descriptor, 0);
		Assert.AreEqual(123, descriptor[0].DbValue.GetValue());
		Assert.AreEqual(123, descriptor[1].DbValue.GetValue());
	}

	[Test]
	public void FbBatchCommand_NamedParameterMapping_IsPerBatchParameterCollection()
	{
		var batchCommand = new FbBatchCommand();
		var p0 = batchCommand.AddBatchParameters();
		p0.Add(new FbParameter("@b", 22));
		p0.Add(new FbParameter("@a", 11));

		var p1 = batchCommand.AddBatchParameters();
		p1.Add(new FbParameter("@b", 220));
		p1.Add(new FbParameter("@a", 110));

		SetNamedParameters(batchCommand, new[] { "@a", "@b" });

		var descriptor = CreateIntegerDescriptor(2);

		((IDescriptorFiller)batchCommand).Fill(descriptor, 0);
		Assert.AreEqual(11, descriptor[0].DbValue.GetValue());
		Assert.AreEqual(22, descriptor[1].DbValue.GetValue());

		((IDescriptorFiller)batchCommand).Fill(descriptor, 1);
		Assert.AreEqual(110, descriptor[0].DbValue.GetValue());
		Assert.AreEqual(220, descriptor[1].DbValue.GetValue());

		p1[1].ParameterName = "@c";
		var ex = Assert.Throws<FbException>(() => ((IDescriptorFiller)batchCommand).Fill(descriptor, 1));
		StringAssert.Contains("Must declare the variable '@a'", ex.Message);

		((IDescriptorFiller)batchCommand).Fill(descriptor, 0);
		Assert.AreEqual(11, descriptor[0].DbValue.GetValue());
		Assert.AreEqual(22, descriptor[1].DbValue.GetValue());
	}

	[Test]
	public void DbField_DbDataTypeCacheInvalidates_WhenPropertiesChange()
	{
		var field = new DbField();
		field.DataType = IscCodes.SQL_LONG;
		field.NumericScale = 0;
		field.SubType = 0;
		field.Length = 4;

		Assert.AreEqual(DbDataType.Integer, field.DbDataType);

		field.NumericScale = -1;
		Assert.AreEqual(DbDataType.Decimal, field.DbDataType);

		field.SubType = 1;
		Assert.AreEqual(DbDataType.Numeric, field.DbDataType);
	}

	[Test]
	public void ProcessInfoSqlResponse_DoesNotCopyBuffer()
	{
		var data = new byte[] { 1, 2, 3 };
		var response = new GenericResponse(0, 0, data, null);

		var syncResult = TestGdsStatement.Process(response);
		Assert.AreSame(data, syncResult);

		var asyncResult = TestGdsStatement.ProcessAsync(response).AsTask().GetAwaiter().GetResult();
		Assert.AreSame(data, asyncResult);
	}
}
