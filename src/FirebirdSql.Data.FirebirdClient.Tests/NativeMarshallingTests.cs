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

using System;
using System.Numerics;
using System.Runtime.InteropServices;
using FirebirdSql.Data.Client.Native.Marshalers;
using FirebirdSql.Data.Common;
using FirebirdSql.Data.TestsBase;
using NUnit.Framework;

namespace FirebirdSql.Data.FirebirdClient.Tests;

[NoServerCategory]
public class NativeMarshallingTests
{
	private static Descriptor CreateDescriptor(short count)
	{
		return new Descriptor(count);
	}

	private static IntPtr AllocateSqlda(Descriptor descriptor, out IntPtr[] sqldataPointers, out IntPtr[] sqlindPointers)
	{
		var sqlda = XsqldaMarshaler.MarshalManagedToNative(Charset.DefaultCharset, descriptor);
		sqldataPointers = new IntPtr[descriptor.Count];
		sqlindPointers = new IntPtr[descriptor.Count];
		XsqldaValueReader.FillValuePointers(sqlda, descriptor.Count, sqldataPointers, sqlindPointers);
		return sqlda;
	}

	[Test]
	public void XsqldaValueReader_ReadsInt32AndNull()
	{
		var descriptor = CreateDescriptor(2);
		for (var i = 0; i < descriptor.Count; i++)
		{
			descriptor[i].DataType = IscCodes.SQL_LONG;
			descriptor[i].NumericScale = 0;
			descriptor[i].SubType = 0;
			descriptor[i].Length = 4;
		}

		var sqlda = IntPtr.Zero;
		try
		{
			sqlda = AllocateSqlda(descriptor, out var data, out var ind);

			Marshal.WriteInt32(data[0], 42);
			Marshal.WriteInt16(ind[0], 0);
			Marshal.WriteInt16(ind[1], -1);

			var row = new[]
			{
				new DbValue(descriptor[0], null),
				new DbValue(descriptor[1], null),
			};

			XsqldaValueReader.ReadRowValues(null, descriptor, data, ind, row);

			Assert.AreEqual(42, row[0].GetInt32());
			Assert.True(row[1].IsDBNull());
		}
		finally
		{
			XsqldaMarshaler.CleanUpNativeData(ref sqlda);
		}
	}

	[Test]
	public void XsqldaValueReader_ReadsUtf8Varying_TruncatesByRuneCount()
	{
		var descriptor = CreateDescriptor(1);
		descriptor[0].DataType = IscCodes.SQL_VARYING;
		descriptor[0].NumericScale = 0;
		descriptor[0].SubType = 4; // UTF8
		descriptor[0].Length = 8; // CharCount = 2 for UTF8 (BytesPerCharacter = 4)

		var sqlda = IntPtr.Zero;
		try
		{
			sqlda = AllocateSqlda(descriptor, out var data, out var ind);

			Marshal.WriteInt16(ind[0], 0);
			Marshal.WriteInt16(data[0], 4);
			Marshal.Copy(new byte[] { (byte)'a', (byte)'b', (byte)'c', (byte)'d' }, 0, IntPtr.Add(data[0], 2), 4);

			var row = new[] { new DbValue(descriptor[0], null) };
			XsqldaValueReader.ReadRowValues(null, descriptor, data, ind, row);

			Assert.AreEqual("ab", row[0].GetString());
		}
		finally
		{
			XsqldaMarshaler.CleanUpNativeData(ref sqlda);
		}
	}

	[Test]
	public void XsqldaValueReader_ReadsOctetsVarying_AsBytes()
	{
		var descriptor = CreateDescriptor(1);
		descriptor[0].DataType = IscCodes.SQL_VARYING;
		descriptor[0].NumericScale = 0;
		descriptor[0].SubType = 1; // OCTETS
		descriptor[0].Length = 4;

		var sqlda = IntPtr.Zero;
		try
		{
			sqlda = AllocateSqlda(descriptor, out var data, out var ind);

			Marshal.WriteInt16(ind[0], 0);
			Marshal.WriteInt16(data[0], 3);
			Marshal.Copy(new byte[] { 1, 2, 3 }, 0, IntPtr.Add(data[0], 2), 3);

			var row = new[] { new DbValue(descriptor[0], null) };
			XsqldaValueReader.ReadRowValues(null, descriptor, data, ind, row);

			CollectionAssert.AreEqual(new byte[] { 1, 2, 3 }, row[0].GetBinary());
		}
		finally
		{
			XsqldaMarshaler.CleanUpNativeData(ref sqlda);
		}
	}

	[Test]
	public void XsqldaValueReader_ReadsGuid_FromOctetsChar()
	{
		var descriptor = CreateDescriptor(1);
		descriptor[0].DataType = IscCodes.SQL_TEXT;
		descriptor[0].NumericScale = 0;
		descriptor[0].SubType = 1; // OCTETS
		descriptor[0].Length = 16;

		var expected = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
		var encoded = TypeEncoder.EncodeGuid(expected);

		var sqlda = IntPtr.Zero;
		try
		{
			sqlda = AllocateSqlda(descriptor, out var data, out var ind);

			Marshal.WriteInt16(ind[0], 0);
			Marshal.Copy(encoded, 0, data[0], encoded.Length);

			var row = new[] { new DbValue(descriptor[0], null) };
			XsqldaValueReader.ReadRowValues(null, descriptor, data, ind, row);

			Assert.AreEqual(expected, row[0].GetGuid());
		}
		finally
		{
			XsqldaMarshaler.CleanUpNativeData(ref sqlda);
		}
	}

	[Test]
	public void XsqldaValueReader_ReadsInt128()
	{
		var descriptor = CreateDescriptor(1);
		descriptor[0].DataType = IscCodes.SQL_INT128;
		descriptor[0].NumericScale = 0;
		descriptor[0].SubType = 0;
		descriptor[0].Length = 16;

		var expected = BigInteger.Parse("123456789012345678901234567890");
		var encoded = Int128Helper.GetBytes(expected);

		var sqlda = IntPtr.Zero;
		try
		{
			sqlda = AllocateSqlda(descriptor, out var data, out var ind);

			Marshal.WriteInt16(ind[0], 0);
			Marshal.Copy(encoded, 0, data[0], encoded.Length);

			var row = new[] { new DbValue(descriptor[0], null) };
			XsqldaValueReader.ReadRowValues(null, descriptor, data, ind, row);

			Assert.AreEqual(expected, row[0].GetInt128());
		}
		finally
		{
			XsqldaMarshaler.CleanUpNativeData(ref sqlda);
		}
	}

	[Test]
	public void XsqldaValueWriter_WritesIndicatorsAndBuffers()
	{
		var descriptor = CreateDescriptor(2);

		descriptor[0].DataType = IscCodes.SQL_LONG;
		descriptor[0].NumericScale = 0;
		descriptor[0].SubType = 0;
		descriptor[0].Length = 4;

		descriptor[1].DataType = IscCodes.SQL_VARYING;
		descriptor[1].NumericScale = 0;
		descriptor[1].SubType = 0; // NONE
		descriptor[1].Length = 10;

		var sqlda = IntPtr.Zero;
		try
		{
			sqlda = AllocateSqlda(descriptor, out var data, out var ind);

			descriptor[0].DbValue.SetValue(42);
			descriptor[0].NullFlag = 0;

			descriptor[1].DbValue.SetValue("abcd");
			descriptor[1].NullFlag = 0;

			XsqldaValueWriter.WriteValues(descriptor, data, ind);

			Assert.AreEqual(0, Marshal.ReadInt16(ind[0]));
			Assert.AreEqual(42, Marshal.ReadInt32(data[0]));

			Assert.AreEqual(0, Marshal.ReadInt16(ind[1]));
			Assert.AreEqual(4, Marshal.ReadInt16(data[1]));
			var tmp = new byte[4];
			Marshal.Copy(IntPtr.Add(data[1], 2), tmp, 0, tmp.Length);
			CollectionAssert.AreEqual(new byte[] { (byte)'a', (byte)'b', (byte)'c', (byte)'d' }, tmp);

			descriptor[0].DbValue.SetValue(DBNull.Value);
			descriptor[0].NullFlag = -1;

			XsqldaValueWriter.WriteValues(descriptor, data, ind);

			Assert.AreEqual(-1, Marshal.ReadInt16(ind[0]));
			Assert.AreEqual(0, Marshal.ReadInt32(data[0]));
		}
		finally
		{
			XsqldaMarshaler.CleanUpNativeData(ref sqlda);
		}
	}
}
