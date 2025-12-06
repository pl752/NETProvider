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

public class FbDatabaseTraceConfiguration : FbTraceConfiguration {
		public FbDatabaseTraceConfiguration() {
				Enabled = false;
				ConnectionID = 0;
				TimeThreshold = TimeSpan.FromMilliseconds(100);
				MaxSQLLength = 300;
				MaxBLRLength = 500;
				MaxDYNLength = 500;
				MaxArgumentLength = 80;
				MaxArgumentsCount = 30;
		}

		public string DatabaseName { get; set; }

		public bool Enabled { get; set; }

		public FbDatabaseTraceEvents Events { get; set; }

		public int ConnectionID { get; set; }

		public TimeSpan TimeThreshold { get; set; }
		public int MaxSQLLength { get; set; }
		public int MaxBLRLength { get; set; }
		public int MaxDYNLength { get; set; }
		public int MaxArgumentLength { get; set; }
		public int MaxArgumentsCount { get; set; }

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
				_ = sb.Append("<database");
				_ = sb.Append(!string.IsNullOrEmpty(DatabaseName) ? $" {WriteRegEx(DatabaseName)}" : string.Empty);
				_ = sb.AppendLine(">");
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
				_ = sb.AppendFormat("log_connections {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.Connections)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("connection_id {0}", WriteNumber(ConnectionID));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_transactions {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.Transactions)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_statement_prepare {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.StatementPrepare)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_statement_free {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.StatementFree)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_statement_start {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.StatementStart)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_statement_finish {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.StatementFinish)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_procedure_start {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.ProcedureStart)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_procedure_finish {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.ProcedureFinish)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_trigger_start {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.TriggerStart)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_trigger_finish {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.TriggerFinish)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_context {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.Context)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_errors {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.Errors)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_warnings {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.Warnings)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_initfini {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.InitFini)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_sweep {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.Sweep)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("print_plan {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.PrintPlan)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("print_perf {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.PrintPerf)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_blr_requests {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.BLRRequests)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("print_blr {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.PrintBLR)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_dyn_requests {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.DYNRequests)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("print_dyn {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.PrintDYN)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("time_threshold {0}", WriteNumber((int)TimeThreshold.TotalMilliseconds));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("max_sql_length {0}", WriteNumber(MaxSQLLength));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("max_blr_length {0}", WriteNumber(MaxBLRLength));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("max_dyn_length {0}", WriteNumber(MaxDYNLength));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("max_arg_length {0}", WriteNumber(MaxArgumentLength));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("max_arg_count {0}", WriteNumber(MaxArgumentsCount));
				_ = sb.AppendLine();
				_ = sb.AppendLine("</database>");
				return sb.ToString();
		}
		string BuildConfiguration2() {
				var sb = new StringBuilder();
				_ = sb.Append("database");
				_ = sb.Append(!string.IsNullOrEmpty(DatabaseName) ? $" = {WriteRegEx(DatabaseName)}" : string.Empty);
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
				_ = sb.AppendFormat("log_connections = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.Connections)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("connection_id = {0}", WriteNumber(ConnectionID));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_transactions = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.Transactions)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_statement_prepare = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.StatementPrepare)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_statement_free = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.StatementFree)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_statement_start = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.StatementStart)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_statement_finish = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.StatementFinish)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_procedure_start = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.ProcedureStart)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_procedure_finish = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.ProcedureFinish)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_function_start = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.FunctionStart)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_function_finish = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.FunctionFinish)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_trigger_start = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.TriggerStart)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_trigger_finish = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.TriggerFinish)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_context = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.Context)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_errors = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.Errors)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_warnings = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.Warnings)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_initfini = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.InitFini)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_sweep = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.Sweep)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("print_plan = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.PrintPlan)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("explain_plan = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.ExplainPlan)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("print_perf = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.PrintPerf)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_blr_requests = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.BLRRequests)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("print_blr = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.PrintBLR)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("log_dyn_requests = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.DYNRequests)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("print_dyn = {0}", WriteBoolValue(Events.HasFlag(FbDatabaseTraceEvents.PrintDYN)));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("time_threshold = {0}", WriteNumber((int)TimeThreshold.TotalMilliseconds));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("max_sql_length = {0}", WriteNumber(MaxSQLLength));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("max_blr_length = {0}", WriteNumber(MaxBLRLength));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("max_dyn_length = {0}", WriteNumber(MaxDYNLength));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("max_arg_length = {0}", WriteNumber(MaxArgumentLength));
				_ = sb.AppendLine();
				_ = sb.AppendFormat("max_arg_count = {0}", WriteNumber(MaxArgumentsCount));
				_ = sb.AppendLine();
				_ = sb.AppendLine("}");
				return sb.ToString();
		}
}
