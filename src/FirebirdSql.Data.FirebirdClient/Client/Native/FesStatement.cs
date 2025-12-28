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
using FirebirdSql.Data.Client.Native.Handles;
using FirebirdSql.Data.Client.Native.Marshalers;
using FirebirdSql.Data.Common;

namespace FirebirdSql.Data.Client.Native;

internal sealed class FesStatement : StatementBase
{
	#region Fields

	private StatementHandle _handle;
	private bool _disposed;
	private FesDatabase _database;
	private FesTransaction _transaction;
	private Descriptor _parameters;
	private Descriptor _fields;
	private bool _allRowsFetched;
	private IntPtr[] _statusVector;
	private IntPtr _fetchSqlDa;
	private IntPtr[] _fetchSqldataPointers;
	private IntPtr[] _fetchSqlindPointers;
	private IntPtr _inSqlDa;
	private IntPtr[] _inSqldataPointers;
	private IntPtr[] _inSqlindPointers;
	private IntPtr _outSqlDa;
	private IntPtr[] _outSqldataPointers;
	private IntPtr[] _outSqlindPointers;
	private DbValue[] _reusableRow;

	#endregion

	#region Properties

	public override DatabaseBase Database
	{
		get { return _database; }
	}

	public override TransactionBase Transaction
	{
		get { return _transaction; }
		set
		{
			if (_transaction != value)
			{
				if (TransactionUpdate != null && _transaction != null)
				{
					_transaction.Update -= TransactionUpdate;
					TransactionUpdate = null;
				}

				if (value == null)
				{
					_transaction = null;
				}
				else
				{
					_transaction = (FesTransaction)value;
					TransactionUpdate = new EventHandler(TransactionUpdated);
					_transaction.Update += TransactionUpdate;
				}
			}
		}
	}

	public override Descriptor Parameters
	{
		get { return _parameters; }
		set { _parameters = value; }
	}

	public override Descriptor Fields
	{
		get { return _fields; }
	}

	public override int FetchSize
	{
		get { return 200; }
		set { }
	}

	#endregion

	#region Constructors

	public FesStatement(FesDatabase database)
		: this(database, null)
	{
	}

	public FesStatement(FesDatabase database, FesTransaction transaction)
	{
		_database = database;
		_handle = new StatementHandle();
		OutputParameters = new Queue<DbValue[]>();
		_statusVector = new IntPtr[IscCodes.ISC_STATUS_LENGTH];
		_fetchSqlDa = IntPtr.Zero;

		if (transaction != null)
		{
			Transaction = transaction;
		}
	}

	#endregion

	#region Dispose2

	public override void Dispose2()
	{
		if (!_disposed)
		{
			_disposed = true;
			Release();
			Clear();
			_database = null;
			_fields = null;
			_parameters = null;
			_transaction = null;
			OutputParameters = null;
			_statusVector = null;
			_allRowsFetched = false;
			_handle.Dispose();
			FetchSize = 0;
			base.Dispose2();
		}
	}
	public override async ValueTask Dispose2Async(CancellationToken cancellationToken = default)
	{
		if (!_disposed)
		{
			_disposed = true;
			await ReleaseAsync(cancellationToken).ConfigureAwait(false);
			Clear();
			_database = null;
			_fields = null;
			_parameters = null;
			_transaction = null;
			OutputParameters = null;
			_statusVector = null;
			_allRowsFetched = false;
			_handle.Dispose();
			FetchSize = 0;
			await base.Dispose2Async(cancellationToken).ConfigureAwait(false);
		}
	}

	#endregion

	#region Blob Creation Metods

	public override BlobBase CreateBlob()
	{
		return new FesBlob(_database, _transaction);
	}

	public override BlobBase CreateBlob(long blobId)
	{
		return new FesBlob(_database, _transaction, blobId);
	}

	#endregion

	#region Array Creation Methods

	public override ArrayBase CreateArray(ArrayDesc descriptor)
	{
		var array = new FesArray(descriptor);
		return array;
	}
	public override ValueTask<ArrayBase> CreateArrayAsync(ArrayDesc descriptor, CancellationToken cancellationToken = default)
	{
		var array = new FesArray(descriptor);
		return ValueTask.FromResult<ArrayBase>(array);
	}

	public override ArrayBase CreateArray(string tableName, string fieldName)
	{
		var array = new FesArray(_database, _transaction, tableName, fieldName);
		array.Initialize();
		return array;
	}
	public override async ValueTask<ArrayBase> CreateArrayAsync(string tableName, string fieldName, CancellationToken cancellationToken = default)
	{
		var array = new FesArray(_database, _transaction, tableName, fieldName);
		await array.InitializeAsync(cancellationToken).ConfigureAwait(false);
		return array;
	}

	public override ArrayBase CreateArray(long handle, string tableName, string fieldName)
	{
		var array = new FesArray(_database, _transaction, handle, tableName, fieldName);
		array.Initialize();
		return array;
	}
	public override async ValueTask<ArrayBase> CreateArrayAsync(long handle, string tableName, string fieldName, CancellationToken cancellationToken = default)
	{
		var array = new FesArray(_database, _transaction, handle, tableName, fieldName);
		await array.InitializeAsync(cancellationToken).ConfigureAwait(false);
		return array;
	}

	public override BatchBase CreateBatch()
	{
		throw new NotSupportedException("Batching isn't, yet, supported on Firebird Embedded.");
	}

	public override BatchParameterBuffer CreateBatchParameterBuffer()
	{
		throw new NotSupportedException("Batching isn't, yet, supported on Firebird Embedded.");
	}

	#endregion

	#region Methods

	public override void Release()
	{
		XsqldaMarshaler.CleanUpNativeData(ref _fetchSqlDa);
		_fetchSqldataPointers = null;
		_fetchSqlindPointers = null;
		XsqldaMarshaler.CleanUpNativeData(ref _inSqlDa);
		_inSqldataPointers = null;
		_inSqlindPointers = null;
		XsqldaMarshaler.CleanUpNativeData(ref _outSqlDa);
		_outSqldataPointers = null;
		_outSqlindPointers = null;

		base.Release();
	}
	public override ValueTask ReleaseAsync(CancellationToken cancellationToken = default)
	{
		XsqldaMarshaler.CleanUpNativeData(ref _fetchSqlDa);
		_fetchSqldataPointers = null;
		_fetchSqlindPointers = null;
		XsqldaMarshaler.CleanUpNativeData(ref _inSqlDa);
		_inSqldataPointers = null;
		_inSqlindPointers = null;
		XsqldaMarshaler.CleanUpNativeData(ref _outSqlDa);
		_outSqldataPointers = null;
		_outSqlindPointers = null;

		return base.ReleaseAsync(cancellationToken);
	}

	public override void Close()
	{
		XsqldaMarshaler.CleanUpNativeData(ref _fetchSqlDa);
		_fetchSqldataPointers = null;
		_fetchSqlindPointers = null;
		XsqldaMarshaler.CleanUpNativeData(ref _inSqlDa);
		_inSqldataPointers = null;
		_inSqlindPointers = null;
		XsqldaMarshaler.CleanUpNativeData(ref _outSqlDa);
		_outSqldataPointers = null;
		_outSqlindPointers = null;

		base.Close();
	}
	public override ValueTask CloseAsync(CancellationToken cancellationToken = default)
	{
		XsqldaMarshaler.CleanUpNativeData(ref _fetchSqlDa);
		_fetchSqldataPointers = null;
		_fetchSqlindPointers = null;
		XsqldaMarshaler.CleanUpNativeData(ref _inSqlDa);
		_inSqldataPointers = null;
		_inSqlindPointers = null;
		XsqldaMarshaler.CleanUpNativeData(ref _outSqlDa);
		_outSqldataPointers = null;
		_outSqlindPointers = null;

		return base.CloseAsync(cancellationToken);
	}

	public override void Prepare(string commandText)
	{
		ClearAll();

		ClearStatusVector();

		if (State == StatementState.Deallocated)
		{
			Allocate();
		}

		_fields = new Descriptor(1);

		var sqlda = XsqldaMarshaler.MarshalManagedToNative(_database.Charset, _fields);
		var trHandle = _transaction.HandlePtr;

		var buffer = _database.Charset.GetBytes(commandText);

		_database.FbClient.isc_dsql_prepare(
			_statusVector,
			ref trHandle,
			ref _handle,
			(short)buffer.Length,
			buffer,
			_database.Dialect,
			sqlda);

		var descriptor = XsqldaMarshaler.MarshalNativeToManaged(_database.Charset, sqlda);

		XsqldaMarshaler.CleanUpNativeData(ref sqlda);

		_database.ProcessStatusVector(_statusVector);

		_fields = descriptor;

		if (_fields.ActualCount > 0 && _fields.ActualCount != _fields.Count)
		{
			Describe();
		}
		else
		{
			if (_fields.ActualCount == 0)
			{
				_fields = new Descriptor(0);
			}
		}

		_fields.ResetValues();

		DescribeParameters();

		StatementType = GetStatementType();

		State = StatementState.Prepared;
	}
	public override async ValueTask PrepareAsync(string commandText, CancellationToken cancellationToken = default)
	{
		ClearAll();

		ClearStatusVector();

		if (State == StatementState.Deallocated)
		{
			Allocate();
		}

		_fields = new Descriptor(1);

		var sqlda = XsqldaMarshaler.MarshalManagedToNative(_database.Charset, _fields);
		var trHandle = _transaction.HandlePtr;

		var buffer = _database.Charset.GetBytes(commandText);

		_database.FbClient.isc_dsql_prepare(
			_statusVector,
			ref trHandle,
			ref _handle,
			(short)buffer.Length,
			buffer,
			_database.Dialect,
			sqlda);

		var descriptor = XsqldaMarshaler.MarshalNativeToManaged(_database.Charset, sqlda);

		XsqldaMarshaler.CleanUpNativeData(ref sqlda);

		_database.ProcessStatusVector(_statusVector);

		_fields = descriptor;

		if (_fields.ActualCount > 0 && _fields.ActualCount != _fields.Count)
		{
			Describe();
		}
		else
		{
			if (_fields.ActualCount == 0)
			{
				_fields = new Descriptor(0);
			}
		}

		_fields.ResetValues();

		DescribeParameters();

		StatementType = await GetStatementTypeAsync(cancellationToken).ConfigureAwait(false);

		State = StatementState.Prepared;
	}

	public override void Execute(int timeout, IDescriptorFiller descriptorFiller)
	{
		EnsureNotDeallocated();

		descriptorFiller.Fill(_parameters, 0);

		ClearStatusVector();
		NativeHelpers.CallIfExists(
			nameof(IFbClient.fb_dsql_set_timeout),
			() =>
			{
				_database.FbClient.fb_dsql_set_timeout(_statusVector, ref _handle, (uint)timeout);
				_database.ProcessStatusVector(_statusVector);
			});

		ClearStatusVector();

		var inSqlda = IntPtr.Zero;
		var outSqlda = IntPtr.Zero;

		if (_parameters != null)
		{
			if (_inSqlDa == IntPtr.Zero)
			{
				_inSqlDa = XsqldaMarshaler.MarshalManagedToNative(_database.Charset, _parameters);
				_inSqldataPointers = new IntPtr[_parameters.Count];
				_inSqlindPointers = new IntPtr[_parameters.Count];
				XsqldaValueReader.FillValuePointers(_inSqlDa, _parameters.Count, _inSqldataPointers, _inSqlindPointers);
			}
			else
			{
				XsqldaValueWriter.WriteValues(_parameters, _inSqldataPointers, _inSqlindPointers);
			}

			inSqlda = _inSqlDa;
		}
		if (StatementType == DbStatementType.StoredProcedure)
		{
			if (_outSqlDa == IntPtr.Zero)
			{
				Fields.ResetValues();
				_outSqlDa = XsqldaMarshaler.MarshalManagedToNative(_database.Charset, _fields);
				_outSqldataPointers = new IntPtr[_fields.Count];
				_outSqlindPointers = new IntPtr[_fields.Count];
				XsqldaValueReader.FillValuePointers(_outSqlDa, _fields.Count, _outSqldataPointers, _outSqlindPointers);
			}

			outSqlda = _outSqlDa;
		}

		var trHandle = _transaction.HandlePtr;

		_database.FbClient.isc_dsql_execute2(
			_statusVector,
			ref trHandle,
			ref _handle,
			IscCodes.SQLDA_VERSION1,
			inSqlda,
			outSqlda);

		if (outSqlda != IntPtr.Zero)
		{
			var count = _fields.ActualCount;
			var values = count > 0 ? new DbValue[count] : Array.Empty<DbValue>();
			for (var i = 0; i < values.Length; i++)
			{
				values[i] = new DbValue(this, _fields[i], null);
			}
			if (values.Length > 0)
			{
				XsqldaValueReader.ReadRowValues(this, _fields, _outSqldataPointers, _outSqlindPointers, values);
			}
			OutputParameters.Enqueue(values);
		}

		_database.ProcessStatusVector(_statusVector);

		if (DoRecordsAffected)
		{
			RecordsAffected = GetRecordsAffected();
		}
		else
		{
			RecordsAffected = -1;
		}

		State = StatementState.Executed;
	}
	public override async ValueTask ExecuteAsync(int timeout, IDescriptorFiller descriptorFiller, CancellationToken cancellationToken = default)
	{
		EnsureNotDeallocated();

		await descriptorFiller.FillAsync(_parameters, 0, cancellationToken).ConfigureAwait(false);

		ClearStatusVector();
		NativeHelpers.CallIfExists(
			nameof(IFbClient.fb_dsql_set_timeout),
			() =>
			{
				_database.FbClient.fb_dsql_set_timeout(_statusVector, ref _handle, (uint)timeout);
				_database.ProcessStatusVector(_statusVector);
			});

		ClearStatusVector();

		var inSqlda = IntPtr.Zero;
		var outSqlda = IntPtr.Zero;

		if (_parameters != null)
		{
			if (_inSqlDa == IntPtr.Zero)
			{
				_inSqlDa = XsqldaMarshaler.MarshalManagedToNative(_database.Charset, _parameters);
				_inSqldataPointers = new IntPtr[_parameters.Count];
				_inSqlindPointers = new IntPtr[_parameters.Count];
				XsqldaValueReader.FillValuePointers(_inSqlDa, _parameters.Count, _inSqldataPointers, _inSqlindPointers);
			}
			else
			{
				XsqldaValueWriter.WriteValues(_parameters, _inSqldataPointers, _inSqlindPointers);
			}

			inSqlda = _inSqlDa;
		}
		if (StatementType == DbStatementType.StoredProcedure)
		{
			if (_outSqlDa == IntPtr.Zero)
			{
				Fields.ResetValues();
				_outSqlDa = XsqldaMarshaler.MarshalManagedToNative(_database.Charset, _fields);
				_outSqldataPointers = new IntPtr[_fields.Count];
				_outSqlindPointers = new IntPtr[_fields.Count];
				XsqldaValueReader.FillValuePointers(_outSqlDa, _fields.Count, _outSqldataPointers, _outSqlindPointers);
			}

			outSqlda = _outSqlDa;
		}

		var trHandle = _transaction.HandlePtr;

		_database.FbClient.isc_dsql_execute2(
			_statusVector,
			ref trHandle,
			ref _handle,
			IscCodes.SQLDA_VERSION1,
			inSqlda,
			outSqlda);

		if (outSqlda != IntPtr.Zero)
		{
			var count = _fields.ActualCount;
			var values = count > 0 ? new DbValue[count] : Array.Empty<DbValue>();
			for (var i = 0; i < values.Length; i++)
			{
				values[i] = new DbValue(this, _fields[i], null);
			}
			if (values.Length > 0)
			{
				XsqldaValueReader.ReadRowValues(this, _fields, _outSqldataPointers, _outSqlindPointers, values);
			}
			OutputParameters.Enqueue(values);
		}

		_database.ProcessStatusVector(_statusVector);

		if (DoRecordsAffected)
		{
			RecordsAffected = await GetRecordsAffectedAsync(cancellationToken).ConfigureAwait(false);
		}
		else
		{
			RecordsAffected = -1;
		}

		State = StatementState.Executed;
	}

	public override DbValue[] Fetch()
	{
		EnsureNotDeallocated();

		if (StatementType == DbStatementType.StoredProcedure && !_allRowsFetched)
		{
			_allRowsFetched = true;
			return GetOutputParameters();
		}
		else if (StatementType == DbStatementType.Insert && _allRowsFetched)
		{
			return null;
		}
		else if (StatementType != DbStatementType.Select && StatementType != DbStatementType.SelectForUpdate)
		{
			return null;
		}

		if (_allRowsFetched)
		{
			return null;
		}

		if (_fetchSqlDa == IntPtr.Zero)
		{
			_fetchSqlDa = XsqldaMarshaler.MarshalManagedToNative(_database.Charset, _fields);
			_fetchSqldataPointers = new IntPtr[_fields.Count];
			_fetchSqlindPointers = new IntPtr[_fields.Count];
			XsqldaValueReader.FillValuePointers(_fetchSqlDa, _fields.Count, _fetchSqldataPointers, _fetchSqlindPointers);
		}

		ClearStatusVector();

		var status = _database.FbClient.isc_dsql_fetch(_statusVector, ref _handle, IscCodes.SQLDA_VERSION1, _fetchSqlDa);
		if (status == new IntPtr(100))
		{
			_allRowsFetched = true;

			XsqldaMarshaler.CleanUpNativeData(ref _fetchSqlDa);
			_fetchSqldataPointers = null;
			_fetchSqlindPointers = null;

			return null;
		}
		else
		{
			_database.ProcessStatusVector(_statusVector);

			var count = _fields.ActualCount;
			if (count <= 0)
			{
				return Array.Empty<DbValue>();
			}

			if (_reusableRow == null || _reusableRow.Length != count)
			{
				_reusableRow = new DbValue[count];
				for (var i = 0; i < count; i++)
				{
					_reusableRow[i] = new DbValue(this, _fields[i], null);
				}
			}

			XsqldaValueReader.ReadRowValues(this, _fields, _fetchSqldataPointers, _fetchSqlindPointers, _reusableRow);

			return _reusableRow;
		}
	}
	public override ValueTask<DbValue[]> FetchAsync(CancellationToken cancellationToken = default)
	{
		return new ValueTask<DbValue[]>(Fetch());
	}

	#endregion

	#region Protected Methods

	protected override void Free(int option)
	{
		// Does	not	seem to	be possible	or necessary to	close
		// an execute procedure	statement.
		if (StatementType == DbStatementType.StoredProcedure && option == IscCodes.DSQL_close)
		{
			return;
		}

		ClearStatusVector();

		_database.FbClient.isc_dsql_free_statement(
			_statusVector,
			ref _handle,
			(short)option);

		if (option == IscCodes.DSQL_drop)
		{
			_parameters = null;
			_fields = null;
		}

		Clear();
		_allRowsFetched = false;

		_database.ProcessStatusVector(_statusVector);
	}
	protected override ValueTask FreeAsync(int option, CancellationToken cancellationToken = default)
	{
		// Does	not	seem to	be possible	or necessary to	close
		// an execute procedure	statement.
		if (StatementType == DbStatementType.StoredProcedure && option == IscCodes.DSQL_close)
		{
			return ValueTask.CompletedTask;
		}

		ClearStatusVector();

		_database.FbClient.isc_dsql_free_statement(
			_statusVector,
			ref _handle,
			(short)option);

		if (option == IscCodes.DSQL_drop)
		{
			_parameters = null;
			_fields = null;
		}

		Clear();
		_allRowsFetched = false;

		_database.ProcessStatusVector(_statusVector);

		return ValueTask.CompletedTask;
	}

	protected override void TransactionUpdated(object sender, EventArgs e)
	{
		if (Transaction != null && TransactionUpdate != null)
		{
			Transaction.Update -= TransactionUpdate;
		}
		XsqldaMarshaler.CleanUpNativeData(ref _fetchSqlDa);
		_fetchSqldataPointers = null;
		_fetchSqlindPointers = null;
		XsqldaMarshaler.CleanUpNativeData(ref _inSqlDa);
		_inSqldataPointers = null;
		_inSqlindPointers = null;
		XsqldaMarshaler.CleanUpNativeData(ref _outSqlDa);
		_outSqldataPointers = null;
		_outSqlindPointers = null;
		Clear();
		State = StatementState.Closed;
		TransactionUpdate = null;
		_allRowsFetched = false;
	}

	protected override byte[] GetSqlInfo(byte[] items, int bufferLength)
	{
		ClearStatusVector();

		var buffer = new byte[bufferLength];

		_database.FbClient.isc_dsql_sql_info(
			_statusVector,
			ref _handle,
			(short)items.Length,
			items,
			(short)bufferLength,
			buffer);

		_database.ProcessStatusVector(_statusVector);

		return buffer;
	}
	protected override ValueTask<byte[]> GetSqlInfoAsync(byte[] items, int bufferLength, CancellationToken cancellationToken = default)
	{
		ClearStatusVector();

		var buffer = new byte[bufferLength];

		_database.FbClient.isc_dsql_sql_info(
			_statusVector,
			ref _handle,
			(short)items.Length,
			items,
			(short)bufferLength,
			buffer);

		_database.ProcessStatusVector(_statusVector);

		return ValueTask.FromResult(buffer);
	}

	#endregion

	#region Private Methods

	private void ClearStatusVector()
	{
		Array.Clear(_statusVector, 0, _statusVector.Length);
	}

	private void Clear()
	{
		OutputParameters?.Clear();
	}

	private void ClearAll()
	{
		Clear();

		XsqldaMarshaler.CleanUpNativeData(ref _fetchSqlDa);
		_fetchSqldataPointers = null;
		_fetchSqlindPointers = null;
		XsqldaMarshaler.CleanUpNativeData(ref _inSqlDa);
		_inSqldataPointers = null;
		_inSqlindPointers = null;
		XsqldaMarshaler.CleanUpNativeData(ref _outSqlDa);
		_outSqldataPointers = null;
		_outSqlindPointers = null;
		_reusableRow = null;
		_parameters = null;
		_fields = null;
	}

	private void Allocate()
	{
		ClearStatusVector();

		var dbHandle = _database.HandlePtr;

		_database.FbClient.isc_dsql_allocate_statement(
			_statusVector,
			ref dbHandle,
			ref _handle);

		_database.ProcessStatusVector(_statusVector);

		_allRowsFetched = false;
		State = StatementState.Allocated;
		StatementType = DbStatementType.None;
	}

	private void Describe()
	{
		ClearStatusVector();

		_fields = new Descriptor(_fields.ActualCount);

		var sqlda = XsqldaMarshaler.MarshalManagedToNative(_database.Charset, _fields);

		_database.FbClient.isc_dsql_describe(
			_statusVector,
			ref _handle,
			IscCodes.SQLDA_VERSION1,
			sqlda);

		var descriptor = XsqldaMarshaler.MarshalNativeToManaged(_database.Charset, sqlda);

		XsqldaMarshaler.CleanUpNativeData(ref sqlda);

		_database.ProcessStatusVector(_statusVector);

		_fields = descriptor;
	}

	private void DescribeParameters()
	{
		ClearStatusVector();

		_parameters = new Descriptor(1);

		var sqlda = XsqldaMarshaler.MarshalManagedToNative(_database.Charset, _parameters);


		_database.FbClient.isc_dsql_describe_bind(
			_statusVector,
			ref _handle,
			IscCodes.SQLDA_VERSION1,
			sqlda);

		var descriptor = XsqldaMarshaler.MarshalNativeToManaged(_database.Charset, sqlda);

		_database.ProcessStatusVector(_statusVector);

		if (descriptor.ActualCount != 0 && descriptor.Count != descriptor.ActualCount)
		{
			var n = descriptor.ActualCount;
			descriptor = new Descriptor(n);

			XsqldaMarshaler.CleanUpNativeData(ref sqlda);

			sqlda = XsqldaMarshaler.MarshalManagedToNative(_database.Charset, descriptor);

			_database.FbClient.isc_dsql_describe_bind(
				_statusVector,
				ref _handle,
				IscCodes.SQLDA_VERSION1,
				sqlda);

			descriptor = XsqldaMarshaler.MarshalNativeToManaged(_database.Charset, sqlda);

			XsqldaMarshaler.CleanUpNativeData(ref sqlda);

			_database.ProcessStatusVector(_statusVector);
		}
		else
		{
			if (descriptor.ActualCount == 0)
			{
				descriptor = new Descriptor(0);
			}
		}

		if (sqlda != IntPtr.Zero)
		{
			XsqldaMarshaler.CleanUpNativeData(ref sqlda);
		}

		_parameters = descriptor;
	}

	#endregion
}
