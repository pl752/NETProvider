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
using System.Text;

namespace FirebirdSql.Data.Services;

public class FbServiceTraceConfiguration : FbTraceConfiguration {
		public FbServiceTraceConfiguration() {
				Enabled = false;
		}

		public bool Enabled { get; set; }

		public FbServiceTraceEvents Events { get; set; }

		public string IncludeFilter { get; set; }
		public string ExcludeFilter { get; set; }

		public string IncludeGdsCodes { get; set; }
		public string ExcludeGdsCodes { get; set; }

		public string BuildConfiguration(FbTraceVersion version) => version switch {
				FbTraceVersion.Version1 => BuildConfiguration1(),
				FbTraceVersion.Version2 => BuildConfiguration2(),
				_ => throw new ArgumentOutOfRangeException(nameof(version)),
		};
		string BuildConfiguration1() {
				var sb = new StringBuilder();
				_ = sb.AppendLine("<services>");
				_ = sb.AppendFormat("enabled {0}", WriteBoolValue(Enabled));
				_ = sb.AppendLine();
				if(!string.IsNullOrEmpty(IncludeFilter)) {
						_ = sb.AppendFormat("include_filter {0}", WriteRegEx(IncludeFilter));
						_ = sb.AppendLine();
				}
				if(!string.IsNullOrEmpty(ExcludeFilter)) {
						_ = sb.AppendFormat("exclude_filter {0}", WriteRegEx(ExcludeFilter));
						_ = sb.AppendLine();
				}
				_ = sb.AppendFormat("log_services {0}", WriteBoolValue(Events.HasFlag(FbServiceTraceEvents.Services)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_service_query {0}", WriteBoolValue(Events.HasFlag(FbServiceTraceEvents.ServiceQuery)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_errors {0}", WriteBoolValue(Events.HasFlag(FbServiceTraceEvents.Errors)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_warnings {0}", WriteBoolValue(Events.HasFlag(FbServiceTraceEvents.Warnings)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_initfini {0}", WriteBoolValue(Events.HasFlag(FbServiceTraceEvents.InitFini)));
				_ = sb.AppendLine();
				_ = sb.AppendLine("</services>");
				return sb.ToString();
		}
		string BuildConfiguration2() {
				var sb = new StringBuilder();
				_ = sb.AppendLine("services");
				_ = sb.AppendLine("{");
				_ = sb.AppendFormat("enabled = {0}", WriteBoolValue(Enabled));
				_ = sb.AppendLine();
				if(!string.IsNullOrEmpty(IncludeFilter)) {
						_ = sb.AppendFormat("include_filter = {0}", WriteRegEx(IncludeFilter));
						_ = sb.AppendLine();
				}
				if(!string.IsNullOrEmpty(ExcludeFilter)) {
						_ = sb.AppendFormat("exclude_filter = {0}", WriteRegEx(ExcludeFilter));
						_ = sb.AppendLine();
				}
				if(!string.IsNullOrEmpty(IncludeGdsCodes)) {
						_ = sb.AppendFormat("include_gds_codes = {0}", WriteString(IncludeGdsCodes));
						_ = sb.AppendLine();
				}
				if(!string.IsNullOrEmpty(ExcludeGdsCodes)) {
						_ = sb.AppendFormat("exclude_gds_codes = {0}", WriteString(ExcludeGdsCodes));
						_ = sb.AppendLine();
				}
				_ = sb.AppendFormat("log_services = {0}", WriteBoolValue(Events.HasFlag(FbServiceTraceEvents.Services)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_service_query = {0}", WriteBoolValue(Events.HasFlag(FbServiceTraceEvents.ServiceQuery)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_errors = {0}", WriteBoolValue(Events.HasFlag(FbServiceTraceEvents.Errors)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_warnings = {0}", WriteBoolValue(Events.HasFlag(FbServiceTraceEvents.Warnings)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_initfini = {0}", WriteBoolValue(Events.HasFlag(FbServiceTraceEvents.InitFini)));
				_ = sb.AppendLine();
				_ = sb.AppendLine("}");
				return sb.ToString();
		}
}
