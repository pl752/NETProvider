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

using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.CsProj;

namespace Perf;

sealed class InProcessMemoryConfig : ManualConfig
{
	public InProcessMemoryConfig()
	{
		AddDiagnoser(MemoryDiagnoser.Default);
		AddJob(Job.Default
			.WithToolchain(CsProjCoreToolchain.NetCoreApp80));
		AddJob(Job.Default
			.WithToolchain(CsProjCoreToolchain.NetCoreApp10_0));
		/*AddJob(Job.Default
			.WithToolchain(CsProjCoreToolchain.NetCoreApp10_0)
			.WithId("AVX2 off")
			.WithEnvironmentVariables(
				new EnvironmentVariable("DOTNET_EnableAVX2", "0"),
				new EnvironmentVariable("COMPlus_EnableAVX2", "0")));
		AddJob(Job.Default
			.WithToolchain(CsProjCoreToolchain.NetCoreApp10_0)
			.WithId("No HW intrinsics")
			.WithEnvironmentVariables(
				new EnvironmentVariable("DOTNET_EnableHWIntrinsic", "0"),
				new EnvironmentVariable("COMPlus_EnableHWIntrinsic", "0")));*/
	}
}

