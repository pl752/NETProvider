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

using FirebirdSql.Data.Common;
using FirebirdSql.Data.TestsBase;
using NUnit.Framework;

namespace FirebirdSql.Data.FirebirdClient.Tests;

[NoServerCategory]
public class DescriptorBlrCachingTests
{
	[Test]
	public void ToBlr_CachesResultForSameShape()
	{
		var descriptor = new Descriptor(2);
		descriptor[0].DataType = (short)IscCodes.SQL_LONG;
		descriptor[0].NumericScale = 0;

		descriptor[1].DataType = (short)IscCodes.SQL_VARYING;
		descriptor[1].SubType = 0;
		descriptor[1].Length = 10;

		var blr1 = descriptor.ToBlr();
		var blr2 = descriptor.ToBlr();

		Assert.AreSame(blr1, blr2);
		Assert.AreSame(blr1.Data, blr2.Data);
		Assert.AreEqual(blr1.Length, blr2.Length);
	}

	[Test]
	public void ToBlr_DoesNotInvalidateOnNullableFlagChange()
	{
		var descriptor = new Descriptor(1);
		descriptor[0].DataType = (short)IscCodes.SQL_LONG;
		descriptor[0].NumericScale = 0;

		var blr1 = descriptor.ToBlr();
		descriptor[0].DataType++; // toggles the nullable flag bit
		var blr2 = descriptor.ToBlr();

		Assert.AreSame(blr1, blr2);
	}

	[Test]
	public void ToBlr_InvalidatesOnNumericScaleChange()
	{
		var descriptor = new Descriptor(1);
		descriptor[0].DataType = (short)IscCodes.SQL_LONG;
		descriptor[0].NumericScale = 0;

		var blr1 = descriptor.ToBlr();
		descriptor[0].NumericScale = -2;
		var blr2 = descriptor.ToBlr();

		Assert.AreNotSame(blr1, blr2);
	}

	[Test]
	public void ToBlr_InvalidatesOnLengthChange()
	{
		var descriptor = new Descriptor(1);
		descriptor[0].DataType = (short)IscCodes.SQL_TEXT;
		descriptor[0].SubType = 0;
		descriptor[0].Length = 10;

		var blr1 = descriptor.ToBlr();
		descriptor[0].Length = 12;
		var blr2 = descriptor.ToBlr();

		Assert.AreNotSame(blr1, blr2);
	}
}

