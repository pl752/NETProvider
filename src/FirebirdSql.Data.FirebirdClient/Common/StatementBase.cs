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

//$Authors = Carlos Guzman Alvarez, Jiri Cincura (jiri@cincura.net)

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace FirebirdSql.Data.Common;

internal abstract class StatementBase
{
	#region Protected Static Fields

	protected static readonly byte[] DescribePlanInfoItems =
	[
			IscCodes.isc_info_sql_get_plan,
	];

	protected static readonly byte[] DescribeExplaindPlanInfoItems =
	[
			IscCodes.isc_info_sql_explain_plan,
	];

	protected static readonly byte[] RowsAffectedInfoItems =
	[
			IscCodes.isc_info_sql_records,
	];

	protected static readonly byte[] DescribeInfoAndBindInfoItems =
	[
			IscCodes.isc_info_sql_select,
			IscCodes.isc_info_sql_describe_vars,
			IscCodes.isc_info_sql_sqlda_seq,
			IscCodes.isc_info_sql_type,
			IscCodes.isc_info_sql_sub_type,
			IscCodes.isc_info_sql_length,
			IscCodes.isc_info_sql_scale,
			IscCodes.isc_info_sql_field,
			IscCodes.isc_info_sql_relation,
			// IscCodes.isc_info_sql_owner,
			IscCodes.isc_info_sql_alias,
			IscCodes.isc_info_sql_describe_end,

			IscCodes.isc_info_sql_bind,
			IscCodes.isc_info_sql_describe_vars,
			IscCodes.isc_info_sql_sqlda_seq,
			IscCodes.isc_info_sql_type,
			IscCodes.isc_info_sql_sub_type,
			IscCodes.isc_info_sql_length,
			IscCodes.isc_info_sql_scale,
			IscCodes.isc_info_sql_field,
			IscCodes.isc_info_sql_relation,
			// IscCodes.isc_info_sql_owner,
			IscCodes.isc_info_sql_alias,
			IscCodes.isc_info_sql_describe_end,
	];

	protected static readonly byte[] StatementTypeInfoItems =
	[
			IscCodes.isc_info_sql_stmt_type,
	];

	#endregion

	#region Protected Fields

	protected EventHandler TransactionUpdate;

	#endregion

	#region Properties

	public abstract DatabaseBase Database { get; }
	public abstract TransactionBase Transaction { get; set; }
	public abstract Descriptor Parameters { get; set; }
	public abstract Descriptor Fields { get; }
	public abstract int FetchSize { get; set; }

	protected Queue<DbValue[]> OutputParameters { get; set; }

	public DbStatementType StatementType { get; protected set; } = DbStatementType.None;
	public StatementState State { get; protected set; } = StatementState.Deallocated;
	public int RecordsAffected { get; protected set; } = -1;

	public bool ReturnRecordsAffected { get; set; }

	public bool IsPrepared => State is not (StatementState.Deallocated or StatementState.Error);
	public bool DoRecordsAffected => ReturnRecordsAffected
		&& (StatementType == DbStatementType.Insert
			|| StatementType == DbStatementType.Delete
			|| StatementType == DbStatementType.Update
			|| StatementType == DbStatementType.StoredProcedure
			|| StatementType == DbStatementType.Select);

	#endregion

	#region Dispose2

	public virtual void Dispose2() { }
	public virtual ValueTask Dispose2Async(CancellationToken cancellationToken = default) => ValueTask2.CompletedTask;

	#endregion

	#region Methods

	public string GetExecutionPlan() => GetPlanInfo(DescribePlanInfoItems);
	public ValueTask<string> GetExecutionPlanAsync(CancellationToken cancellationToken) => GetPlanInfoAsync(DescribePlanInfoItems, cancellationToken);

	public string GetExecutionExplainedPlan() => GetPlanInfo(DescribeExplaindPlanInfoItems);
	public ValueTask<string> GetExecutionExplainedPlanAsync(CancellationToken cancellationToken = default) => GetPlanInfoAsync(DescribeExplaindPlanInfoItems, cancellationToken);

	public virtual void Close()
	{
		if (State is StatementState.Executed or
			StatementState.Error)
		{
			if (StatementType is DbStatementType.Select or
				DbStatementType.SelectForUpdate or
				DbStatementType.StoredProcedure)
			{
				if (State is StatementState.Allocated or
					StatementState.Prepared or
					StatementState.Executed)
				{
					try
					{
						Free(IscCodes.DSQL_close);
					}
					catch { }
				}
				ClearArrayHandles();
				State = StatementState.Closed;
			}
		}
	}
	public virtual async ValueTask CloseAsync(CancellationToken cancellationToken = default)
	{
		if (State is StatementState.Executed or
			StatementState.Error)
		{
			if (StatementType is DbStatementType.Select or
				DbStatementType.SelectForUpdate or
				DbStatementType.StoredProcedure)
			{
				if (State is StatementState.Allocated or
					StatementState.Prepared or
					StatementState.Executed)
				{
					try
					{
						await FreeAsync(IscCodes.DSQL_close, cancellationToken).ConfigureAwait(false);
					}
					catch { }
				}
				ClearArrayHandles();
				State = StatementState.Closed;
			}
		}
	}

	public virtual void Release()
	{
		if (Transaction != null && TransactionUpdate != null)
		{
			Transaction.Update -= TransactionUpdate;
			TransactionUpdate = null;
		}

		Free(IscCodes.DSQL_drop);

		ClearArrayHandles();
		State = StatementState.Deallocated;
		StatementType = DbStatementType.None;
	}
	public virtual async ValueTask ReleaseAsync(CancellationToken cancellationToken = default)
	{
		if (Transaction != null && TransactionUpdate != null)
		{
			Transaction.Update -= TransactionUpdate;
			TransactionUpdate = null;
		}

		await FreeAsync(IscCodes.DSQL_drop, cancellationToken).ConfigureAwait(false);

		ClearArrayHandles();
		State = StatementState.Deallocated;
		StatementType = DbStatementType.None;
	}

	#endregion

	#region Abstract Methods

	public abstract void Prepare(string commandText);
	public abstract ValueTask PrepareAsync(string commandText, CancellationToken cancellationToken = default);

	public abstract void Execute(int timeout, IDescriptorFiller descriptorFiller);
	public abstract ValueTask ExecuteAsync(int timeout, IDescriptorFiller descriptorFiller, CancellationToken cancellationToken = default);

	public abstract DbValue[] Fetch();
	public abstract ValueTask<DbValue[]> FetchAsync(CancellationToken cancellationToken = default);

	public abstract BlobBase CreateBlob();
	public abstract BlobBase CreateBlob(long handle);

	public abstract ArrayBase CreateArray(ArrayDesc descriptor);
	public abstract ValueTask<ArrayBase> CreateArrayAsync(ArrayDesc descriptor, CancellationToken cancellationToken = default);

	public abstract ArrayBase CreateArray(string tableName, string fieldName);
	public abstract ValueTask<ArrayBase> CreateArrayAsync(string tableName, string fieldName, CancellationToken cancellationToken = default);

	public abstract ArrayBase CreateArray(long handle, string tableName, string fieldName);
	public abstract ValueTask<ArrayBase> CreateArrayAsync(long handle, string tableName, string fieldName, CancellationToken cancellationToken = default);

	public abstract BatchBase CreateBatch();
	public abstract BatchParameterBuffer CreateBatchParameterBuffer();

	#endregion

	#region Protected Abstract Methods

	protected abstract void TransactionUpdated(object sender, EventArgs e);

	protected abstract byte[] GetSqlInfo(byte[] items, int bufferLength);
	protected abstract ValueTask<byte[]> GetSqlInfoAsync(byte[] items, int bufferLength, CancellationToken cancellationToken = default);

	protected abstract void Free(int option);
	protected abstract ValueTask FreeAsync(int option, CancellationToken cancellationToken = default);

	#endregion

	#region Protected Methods

	public DbValue[] GetOutputParameters() => OutputParameters != null && OutputParameters.Count > 0 ? OutputParameters.Dequeue() : null;

	protected byte[] GetSqlInfo(byte[] items) => GetSqlInfo(items, IscCodes.DEFAULT_MAX_BUFFER_SIZE);
	protected ValueTask<byte[]> GetSqlInfoAsync(byte[] items, CancellationToken cancellationToken = default) => GetSqlInfoAsync(items, IscCodes.DEFAULT_MAX_BUFFER_SIZE, cancellationToken);

	protected int GetRecordsAffected()
	{
		byte[] buffer = GetSqlInfo(RowsAffectedInfoItems, IscCodes.ROWS_AFFECTED_BUFFER_SIZE);
		return ProcessRecordsAffectedBuffer(buffer);
	}
	protected async ValueTask<int> GetRecordsAffectedAsync(CancellationToken cancellationToken = default)
	{
		byte[] buffer = await GetSqlInfoAsync(RowsAffectedInfoItems, IscCodes.ROWS_AFFECTED_BUFFER_SIZE, cancellationToken).ConfigureAwait(false);
		return ProcessRecordsAffectedBuffer(buffer);
	}

	protected static int ProcessRecordsAffectedBuffer(byte[] buffer)
	{
		int insertCount = 0;
		int updateCount = 0;
		int deleteCount = 0;
		int pos = 0;

		int type;
		while ((type = buffer[pos++]) != IscCodes.isc_info_end)
		{
			int length = (int) IscHelper.VaxInteger(buffer, pos, 2);
			pos += 2;
			switch (type)
			{
				case IscCodes.isc_info_sql_records:
					int t;
					while ((t = buffer[pos++]) != IscCodes.isc_info_end)
					{
						int l = (int) IscHelper.VaxInteger(buffer, pos, 2);
						pos += 2;
						switch (t)
						{
							case IscCodes.isc_info_req_insert_count:
								insertCount = (int) IscHelper.VaxInteger(buffer, pos, l);
								break;
							case IscCodes.isc_info_req_update_count:
								updateCount = (int) IscHelper.VaxInteger(buffer, pos, l);
								break;
							case IscCodes.isc_info_req_delete_count:
								deleteCount = (int) IscHelper.VaxInteger(buffer, pos, l);
								break;
							case IscCodes.isc_info_req_select_count:
								int selectCount = (int) IscHelper.VaxInteger(buffer, pos, l);
								break;
						}
						pos += l;
					}
					break;
				default:
					pos += length;
					break;
			}
		}

		return insertCount + updateCount + deleteCount;
	}

	protected DbStatementType GetStatementType()
	{
		byte[] buffer = GetSqlInfo(StatementTypeInfoItems, IscCodes.STATEMENT_TYPE_BUFFER_SIZE);
		return ProcessStatementTypeInfoBuffer(buffer);
	}
	protected async ValueTask<DbStatementType> GetStatementTypeAsync(CancellationToken cancellationToken = default)
	{
		byte[] buffer = await GetSqlInfoAsync(StatementTypeInfoItems, IscCodes.STATEMENT_TYPE_BUFFER_SIZE, cancellationToken).ConfigureAwait(false);
		return ProcessStatementTypeInfoBuffer(buffer);
	}

	protected static DbStatementType ProcessStatementTypeInfoBuffer(byte[] buffer)
	{
		var stmtType = DbStatementType.None;
		int pos = 0;
		int type;
		while ((type = buffer[pos++]) != IscCodes.isc_info_end)
		{
			int length = (int) IscHelper.VaxInteger(buffer, pos, 2);
			pos += 2;
			switch (type)
			{
				case IscCodes.isc_info_sql_stmt_type:
					stmtType = (DbStatementType) IscHelper.VaxInteger(buffer, pos, length);
					pos += length;
					break;

				default:
					pos += length;
					break;
			}
		}

		return stmtType;
	}

	protected void ClearArrayHandles()
	{
		if (Fields != null && Fields.Count > 0)
		{
			for (int i = 0; i < Fields.Count; i++)
			{
				if (Fields[i].IsArray())
				{
					Fields[i].ArrayHandle = null;
				}
			}
		}
	}

	protected string GetPlanInfo(byte[] planInfoItems)
	{
		int count = 0;
		int bufferSize = IscCodes.DEFAULT_MAX_BUFFER_SIZE;
		byte[] buffer = GetSqlInfo(planInfoItems, bufferSize);

		if (buffer[0] == IscCodes.isc_info_end)
		{
			return string.Empty;
		}

		while (buffer[0] == IscCodes.isc_info_truncated && count < 4)
		{
			bufferSize *= 2;
			buffer = GetSqlInfo(planInfoItems, bufferSize);
			count++;
		}
		if (count > 3)
		{
			return null;
		}

		int len = buffer[1];
		len += buffer[2] << 8;
		return len > 0 ? Database.Charset.GetString(buffer, 4, --len) : string.Empty;
	}
	protected async ValueTask<string> GetPlanInfoAsync(byte[] planInfoItems, CancellationToken cancellationToken = default)
	{
		int count = 0;
		int bufferSize = IscCodes.DEFAULT_MAX_BUFFER_SIZE;
		byte[] buffer = await GetSqlInfoAsync(planInfoItems, bufferSize, cancellationToken).ConfigureAwait(false);

		if (buffer[0] == IscCodes.isc_info_end)
		{
			return string.Empty;
		}

		while (buffer[0] == IscCodes.isc_info_truncated && count < 4)
		{
			bufferSize *= 2;
			buffer = await GetSqlInfoAsync(planInfoItems, bufferSize, cancellationToken).ConfigureAwait(false);
			count++;
		}
		if (count > 3)
		{
			return null;
		}

		int len = buffer[1];
		len += buffer[2] << 8;
		return len > 0 ? Database.Charset.GetString(buffer, 4, --len) : string.Empty;
	}

	protected void EnsureNotDeallocated()
	{
		if (State == StatementState.Deallocated)
		{
			throw new InvalidOperationException("Statement is not correctly created.");
		}
	}

	#endregion
}
