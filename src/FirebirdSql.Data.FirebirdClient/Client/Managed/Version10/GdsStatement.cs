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
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using FirebirdSql.Data.Common;

namespace FirebirdSql.Data.Client.Managed.Version10;

internal class GdsStatement : StatementBase
{
	#region Fields

	protected int _handle;
	private bool _disposed;
	protected GdsDatabase _database;
	private GdsTransaction _transaction;
	protected Descriptor _parameters;
	protected Descriptor _fields;
	private Descriptor.BlrData _parametersBlr;
	private Descriptor.BlrData _fieldsBlr;
	protected bool _allRowsFetched;
	private Queue<DbValueStorage[]> _rows;
	private Stack<DbValueStorage[]> _rowStoragePool;
	private DbValue[] _reusableRow;
	private readonly byte[] _fixedBytes = new byte[16];
	private PooledWriteBuffer _parametersWriteBuffer;
	private XdrReaderWriter _parametersXdr;
	private int _fetchSize;

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
					_transaction = (GdsTransaction)value;
					TransactionUpdate = new EventHandler(TransactionUpdated);
					_transaction.Update += TransactionUpdate;
				}
			}
		}
	}

	public override Descriptor Parameters
	{
		get { return _parameters; }
		set
		{
			_parameters = value;
			_parametersBlr = _parameters?.ToBlr();
		}
	}

	public override Descriptor Fields
	{
		get { return _fields; }
	}

	public override int FetchSize
	{
		get { return _fetchSize; }
		set { _fetchSize = value; }
	}

	public int Handle
	{
		get { return _handle; }
	}

	internal Descriptor.BlrData ParametersBlr => GetParametersBlr();
	internal Descriptor.BlrData FieldsBlr => GetFieldsBlr();

	#endregion

	#region Constructors

	public GdsStatement(GdsDatabase database)
		: this(database, null)
	{
	}

	public GdsStatement(GdsDatabase database, GdsTransaction transaction)
	{
		_handle = IscCodes.INVALID_OBJECT;
		_fetchSize = 200;
		_rows = new Queue<DbValueStorage[]>();
		_rowStoragePool = new Stack<DbValueStorage[]>();
		OutputParameters = new Queue<DbValue[]>();

		_database = database;

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
			_parametersXdr = null;
			_parametersWriteBuffer?.Dispose();
			_parametersWriteBuffer = null;
			_rows = null;
			_rowStoragePool = null;
			_reusableRow = null;
			OutputParameters = null;
				_database = null;
				_fields = null;
				_parameters = null;
				_fieldsBlr = null;
				_parametersBlr = null;
				_transaction = null;
			_allRowsFetched = false;
			_handle = 0;
			_fetchSize = 0;
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
			_parametersXdr = null;
			_parametersWriteBuffer?.Dispose();
			_parametersWriteBuffer = null;
			_rows = null;
			_rowStoragePool = null;
			_reusableRow = null;
			OutputParameters = null;
				_database = null;
				_fields = null;
				_parameters = null;
				_fieldsBlr = null;
				_parametersBlr = null;
				_transaction = null;
			_allRowsFetched = false;
			_handle = 0;
			_fetchSize = 0;
			await base.Dispose2Async(cancellationToken).ConfigureAwait(false);
		}
	}

	#endregion

	#region Blob Creation Metods

	public override BlobBase CreateBlob()
	{
		return new GdsBlob(_database, _transaction);
	}

	public override BlobBase CreateBlob(long blobId)
	{
		return new GdsBlob(_database, _transaction, blobId);
	}

	#endregion

	#region Array Creation Methods

	public override ArrayBase CreateArray(ArrayDesc descriptor)
	{
		var array = new GdsArray(descriptor);
		return array;
	}
	public override ValueTask<ArrayBase> CreateArrayAsync(ArrayDesc descriptor, CancellationToken cancellationToken = default)
	{
		var array = new GdsArray(descriptor);
		return ValueTask.FromResult<ArrayBase>(array);
	}

	public override ArrayBase CreateArray(string tableName, string fieldName)
	{
		var array = new GdsArray(_database, _transaction, tableName, fieldName);
		array.Initialize();
		return array;
	}
	public override async ValueTask<ArrayBase> CreateArrayAsync(string tableName, string fieldName, CancellationToken cancellationToken = default)
	{
		var array = new GdsArray(_database, _transaction, tableName, fieldName);
		await array.InitializeAsync(cancellationToken).ConfigureAwait(false);
		return array;
	}

	public override ArrayBase CreateArray(long handle, string tableName, string fieldName)
	{
		var array = new GdsArray(_database, _transaction, handle, tableName, fieldName);
		array.Initialize();
		return array;
	}
	public override async ValueTask<ArrayBase> CreateArrayAsync(long handle, string tableName, string fieldName, CancellationToken cancellationToken = default)
	{
		var array = new GdsArray(_database, _transaction, handle, tableName, fieldName);
		await array.InitializeAsync(cancellationToken).ConfigureAwait(false);
		return array;
	}

	#endregion

	#region Batch Creation Methods

	public override BatchBase CreateBatch()
	{
		throw new NotSupportedException("Batching is not supported on this Firebird version.");
	}

	public override BatchParameterBuffer CreateBatchParameterBuffer()
	{
		throw new NotSupportedException("Batching is not supported on this Firebird version.");
	}

	#endregion

	#region Methods

	public override void Prepare(string commandText)
	{
		ClearAll();

		try
		{
			if (State == StatementState.Deallocated)
			{
				SendAllocateToBuffer();
				_database.Xdr.Flush();
				ProcessAllocateResponse((GenericResponse)_database.ReadResponse());
			}

			SendPrepareToBuffer(commandText);
			_database.Xdr.Flush();
			ProcessPrepareResponse((GenericResponse)_database.ReadResponse());

			SendInfoSqlToBuffer(StatementTypeInfoItems, IscCodes.STATEMENT_TYPE_BUFFER_SIZE);
			_database.Xdr.Flush();
			StatementType = ProcessStatementTypeInfoBuffer(ProcessInfoSqlResponse((GenericResponse)_database.ReadResponse()));

			State = StatementState.Prepared;
		}
		catch (IOException ex)
		{
			State = State == StatementState.Allocated ? StatementState.Error : State;
			throw IscException.ForIOException(ex);
		}
	}
	public override async ValueTask PrepareAsync(string commandText, CancellationToken cancellationToken = default)
	{
		ClearAll();

		try
		{
			if (State == StatementState.Deallocated)
			{
				await SendAllocateToBufferAsync(cancellationToken).ConfigureAwait(false);
				await _database.Xdr.FlushAsync(cancellationToken).ConfigureAwait(false);
				await ProcessAllocateResponseAsync((GenericResponse)await _database.ReadResponseAsync(cancellationToken).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
			}

			await SendPrepareToBufferAsync(commandText, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.FlushAsync(cancellationToken).ConfigureAwait(false);
			await ProcessPrepareResponseAsync((GenericResponse)await _database.ReadResponseAsync(cancellationToken).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);

			await SendInfoSqlToBufferAsync(StatementTypeInfoItems, IscCodes.STATEMENT_TYPE_BUFFER_SIZE, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.FlushAsync(cancellationToken).ConfigureAwait(false);
			StatementType = ProcessStatementTypeInfoBuffer(await ProcessInfoSqlResponseAsync((GenericResponse)await _database.ReadResponseAsync(cancellationToken).ConfigureAwait(false), cancellationToken).ConfigureAwait(false));

			State = StatementState.Prepared;
		}
		catch (IOException ex)
		{
			State = State == StatementState.Allocated ? StatementState.Error : State;
			throw IscException.ForIOException(ex);
		}
	}

	public override void Execute(int timeout, IDescriptorFiller descriptorFiller)
	{
		EnsureNotDeallocated();

		Clear();

		try
		{
			SendExecuteToBuffer(timeout, descriptorFiller);

			_database.Xdr.Flush();

			if (StatementType == DbStatementType.StoredProcedure)
			{
				ProcessStoredProcedureExecuteResponse((SqlResponse)_database.ReadResponse());
			}

			var executeResponse = (GenericResponse)_database.ReadResponse();
			ProcessExecuteResponse(executeResponse);

			if (DoRecordsAffected)
			{
				SendInfoSqlToBuffer(RowsAffectedInfoItems, IscCodes.ROWS_AFFECTED_BUFFER_SIZE);
				_database.Xdr.Flush();
				RecordsAffected = ProcessRecordsAffectedBuffer(ProcessInfoSqlResponse((GenericResponse)_database.ReadResponse()));
			}
			else
			{
				RecordsAffected = -1;
			}

			State = StatementState.Executed;
		}
		catch (IOException ex)
		{
			State = StatementState.Error;
			throw IscException.ForIOException(ex);
		}
	}
	public override async ValueTask ExecuteAsync(int timeout, IDescriptorFiller descriptorFiller, CancellationToken cancellationToken = default)
	{
		EnsureNotDeallocated();

		Clear();

		try
		{
			await SendExecuteToBufferAsync(timeout, descriptorFiller, cancellationToken).ConfigureAwait(false);

			await _database.Xdr.FlushAsync(cancellationToken).ConfigureAwait(false);

			if (StatementType == DbStatementType.StoredProcedure)
			{
				await ProcessStoredProcedureExecuteResponseAsync((SqlResponse)await _database.ReadResponseAsync(cancellationToken).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
			}

			var executeResponse = (GenericResponse)await _database.ReadResponseAsync(cancellationToken).ConfigureAwait(false);
			await ProcessExecuteResponseAsync(executeResponse, cancellationToken).ConfigureAwait(false);

			if (DoRecordsAffected)
			{
				await SendInfoSqlToBufferAsync(RowsAffectedInfoItems, IscCodes.ROWS_AFFECTED_BUFFER_SIZE, cancellationToken).ConfigureAwait(false);
				await _database.Xdr.FlushAsync(cancellationToken).ConfigureAwait(false);
				RecordsAffected = ProcessRecordsAffectedBuffer(await ProcessInfoSqlResponseAsync((GenericResponse)await _database.ReadResponseAsync(cancellationToken).ConfigureAwait(false), cancellationToken).ConfigureAwait(false));
			}
			else
			{
				RecordsAffected = -1;
			}

			State = StatementState.Executed;
		}
		catch (IOException ex)
		{
			State = StatementState.Error;
			throw IscException.ForIOException(ex);
		}
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

		if (!_allRowsFetched && _rows.Count == 0)
		{
			try
			{
				_database.Xdr.WriteBytes(bufOpFetch);
				_database.Xdr.Write(_handle);
					_database.Xdr.WriteBuffer(GetFieldsBlr().Data);
				_database.Xdr.WriteBytes(zeroIntBuf); // p_sqldata_message_number
				_database.Xdr.Write(_fetchSize); // p_sqldata_messages
				_database.Xdr.Flush();

				var operation = _database.ReadOperation();
				if (operation == IscCodes.op_fetch_response)
				{
					var hasOperation = true;
					while (!_allRowsFetched)
					{
						var response = hasOperation
							? _database.ReadResponse(operation)
							: _database.ReadResponse();
						hasOperation = false;
						if (response is FetchResponse fetchResponse)
						{
							if (fetchResponse.Count > 0 && fetchResponse.Status == 0)
							{
								_rows.Enqueue(ReadRowStorage());
							}
							else if (fetchResponse.Status == 100)
							{
								_allRowsFetched = true;
							}
							else
							{
								break;
							}
						}
						else
						{
							break;
						}
					}
				}
				else
				{
					_database.ReadResponse(operation);
				}
			}
			catch (IOException ex)
			{
				throw IscException.ForIOException(ex);
			}
		}

		if (_rows != null && _rows.Count > 0)
		{
			return MaterializeRow(_rows.Dequeue());
		}
		return null;
	}
	public override async ValueTask<DbValue[]> FetchAsync(CancellationToken cancellationToken = default)
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

		if (!_allRowsFetched && _rows.Count == 0)
		{
			try
			{
				await _database.Xdr.WriteBytesAsync(bufOpFetch, 4, cancellationToken).ConfigureAwait(false);
				await _database.Xdr.WriteAsync(_handle, cancellationToken).ConfigureAwait(false);
					await _database.Xdr.WriteBufferAsync(GetFieldsBlr().Data, cancellationToken).ConfigureAwait(false);
				await _database.Xdr.WriteBytesAsync(zeroIntBuf, 4, cancellationToken).ConfigureAwait(false); // p_sqldata_message_number
				await _database.Xdr.WriteAsync(_fetchSize, cancellationToken).ConfigureAwait(false); // p_sqldata_messages
				await _database.Xdr.FlushAsync(cancellationToken).ConfigureAwait(false);

				var operation = await _database.ReadOperationAsync(cancellationToken).ConfigureAwait(false);
				if (operation == IscCodes.op_fetch_response)
				{
					var hasOperation = true;
					while (!_allRowsFetched)
					{
						var response = hasOperation
							? await _database.ReadResponseAsync(operation, cancellationToken).ConfigureAwait(false)
							: await _database.ReadResponseAsync(cancellationToken).ConfigureAwait(false);
						hasOperation = false;
						if (response is FetchResponse fetchResponse)
						{
							if (fetchResponse.Count > 0 && fetchResponse.Status == 0)
							{
								_rows.Enqueue(await ReadRowStorageAsync(cancellationToken).ConfigureAwait(false));
							}
							else if (fetchResponse.Status == 100)
							{
								_allRowsFetched = true;
							}
							else
							{
								break;
							}
						}
						else
						{
							break;
						}
					}
				}
				else
				{
					await _database.ReadResponseAsync(operation, cancellationToken).ConfigureAwait(false);
				}
			}
			catch (IOException ex)
			{
				throw IscException.ForIOException(ex);
			}
		}

		if (_rows != null && _rows.Count > 0)
		{
			return MaterializeRow(_rows.Dequeue());
		}
		return null;
	}

	#endregion

	#region Protected Methods

	#region op_prepare methods
	protected void SendPrepareToBuffer(string commandText)
	{
		_database.Xdr.WriteBytes(bufOpPrepare);
		_database.Xdr.Write(_transaction.Handle);
		_database.Xdr.Write(_handle);
		_database.Xdr.Write((int)_database.Dialect);
		_database.Xdr.Write(commandText);
		_database.Xdr.WriteBuffer(DescribeInfoAndBindInfoItems, DescribeInfoAndBindInfoItems.Length);
		_database.Xdr.WriteBytes(bufPrepareInfoSize);
	}
	protected async ValueTask SendPrepareToBufferAsync(string commandText, CancellationToken cancellationToken = default)
	{
		await _database.Xdr.WriteBytesAsync(bufOpPrepare, 4, cancellationToken).ConfigureAwait(false);
		await _database.Xdr.WriteAsync(_transaction.Handle, cancellationToken).ConfigureAwait(false);
		await _database.Xdr.WriteAsync(_handle, cancellationToken).ConfigureAwait(false);
		await _database.Xdr.WriteAsync((int)_database.Dialect, cancellationToken).ConfigureAwait(false);
		await _database.Xdr.WriteAsync(commandText, cancellationToken).ConfigureAwait(false);
		await _database.Xdr.WriteBufferAsync(DescribeInfoAndBindInfoItems, DescribeInfoAndBindInfoItems.Length, cancellationToken).ConfigureAwait(false);
		await _database.Xdr.WriteBytesAsync(bufPrepareInfoSize, 4, cancellationToken).ConfigureAwait(false);
	}

	protected void ProcessPrepareResponse(GenericResponse response)
	{
		var info = response.Data.AsSpan();
		var descriptors = ParseSqlInfoSpan(info, DescribeInfoAndBindInfoItems, new Descriptor[] { null, null });
		_fields = descriptors[0];
		_parameters = descriptors[1];
		_fieldsBlr = _fields?.ToBlr();
		_parametersBlr = _parameters?.ToBlr();
	}

	protected async ValueTask ProcessPrepareResponseAsync(GenericResponse response, CancellationToken cancellationToken = default)
	{
		var info = response.Data;
		var descriptors = await ParseSqlInfoSpanAsync(info, DescribeInfoAndBindInfoItems.AsMemory(), new Descriptor[] { null, null }, cancellationToken).ConfigureAwait(false);
		_fields = descriptors[0];
		_parameters = descriptors[1];
		_fieldsBlr = _fields?.ToBlr();
		_parametersBlr = _parameters?.ToBlr();
	}

	// Span-based parsing to avoid intermediate arrays when possible
	private Descriptor[] ParseSqlInfoSpan(ReadOnlySpan<byte> info, ReadOnlySpan<byte> items, Descriptor[] rowDescs)
	{
		return ParseTruncSqlInfoSpan(info, items, rowDescs);
	}

	private ValueTask<Descriptor[]> ParseSqlInfoSpanAsync(ReadOnlyMemory<byte> info, ReadOnlyMemory<byte> items, Descriptor[] rowDescs, CancellationToken cancellationToken)
	{
		return ParseTruncSqlInfoSpanAsync(info, items, rowDescs, cancellationToken);
	}
	#endregion

	#region op_info_sql methods
	protected override byte[] GetSqlInfo(byte[] items, int bufferLength)
	{
		DoInfoSqlPacket(items, bufferLength);
		_database.Xdr.Flush();
		return ProcessInfoSqlResponse((GenericResponse)_database.ReadResponse());
	}
	protected override async ValueTask<byte[]> GetSqlInfoAsync(byte[] items, int bufferLength, CancellationToken cancellationToken = default)
	{
		await DoInfoSqlPacketAsync(items, bufferLength, cancellationToken).ConfigureAwait(false);
		await _database.Xdr.FlushAsync(cancellationToken).ConfigureAwait(false);
		return await ProcessInfoSqlResponseAsync((GenericResponse)await _database.ReadResponseAsync(cancellationToken).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
	}

	protected void DoInfoSqlPacket(byte[] items, int bufferLength)
	{
		try
		{
			SendInfoSqlToBuffer(items, bufferLength);
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}
	protected async ValueTask DoInfoSqlPacketAsync(byte[] items, int bufferLength, CancellationToken cancellationToken = default)
	{
		try
		{
			await SendInfoSqlToBufferAsync(items, bufferLength, cancellationToken).ConfigureAwait(false);
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}

	protected void SendInfoSqlToBuffer(byte[] items, int bufferLength)
	{
		_database.Xdr.WriteBytes(bufOpInfoSql);
		_database.Xdr.Write(_handle);
		_database.Xdr.WriteBytes(zeroIntBuf);
		_database.Xdr.WriteBuffer(items, items.Length);
		_database.Xdr.Write(bufferLength);
	}
	protected async ValueTask SendInfoSqlToBufferAsync(byte[] items, int bufferLength, CancellationToken cancellationToken = default)
	{
		await _database.Xdr.WriteBytesAsync(bufOpInfoSql, 4, cancellationToken).ConfigureAwait(false);
		await _database.Xdr.WriteAsync(_handle, cancellationToken).ConfigureAwait(false);
		await _database.Xdr.WriteBytesAsync(zeroIntBuf, 4, cancellationToken).ConfigureAwait(false);
		await _database.Xdr.WriteBufferAsync(items, items.Length, cancellationToken).ConfigureAwait(false);
		await _database.Xdr.WriteAsync(bufferLength, cancellationToken).ConfigureAwait(false);
	}

	protected static byte[] ProcessInfoSqlResponse(GenericResponse response)
	{
		Debug.Assert(response.Data.Length > 0);

		return response.Data.ToArray();
	}

	protected static ValueTask<byte[]> ProcessInfoSqlResponseAsync(GenericResponse response, CancellationToken cancellationToken = default)
	{
		Debug.Assert(response.Data.Length > 0);

		return ValueTask.FromResult(response.Data);
	}
	#endregion

	#region op_free_statement methods
	protected override void Free(int option)
	{
		if (FreeNotNeeded(option))
			return;

		DoFreePacket(option);
		ProcessFreeResponse(_database.ReadResponse());
	}
	protected override async ValueTask FreeAsync(int option, CancellationToken cancellationToken = default)
	{
		if (FreeNotNeeded(option))
			return;

		await DoFreePacketAsync(option, cancellationToken).ConfigureAwait(false);
		await ProcessFreeResponseAsync(await _database.ReadResponseAsync(cancellationToken).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
	}

	protected bool FreeNotNeeded(int option)
	{
		// does not seem to be possible or necessary to close an execute procedure statement
		if (StatementType == DbStatementType.StoredProcedure && option == IscCodes.DSQL_close)
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	protected void DoFreePacket(int option)
	{
		try
		{
			_database.Xdr.WriteBytes(bufOpFreeStatement);
			_database.Xdr.Write(_handle);
			_database.Xdr.Write(option);
			_database.Xdr.Flush();

				if (option == IscCodes.DSQL_drop)
				{
					_parameters = null;
					_fields = null;
					_parametersBlr = null;
					_fieldsBlr = null;
				}

			Clear();
		}
		catch (IOException ex)
		{
			State = StatementState.Error;
			throw IscException.ForIOException(ex);
		}
	}
	protected async ValueTask DoFreePacketAsync(int option, CancellationToken cancellationToken = default)
	{
		try
		{
			await _database.Xdr.WriteBytesAsync(bufOpFreeStatement, 4, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteAsync(_handle, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.WriteAsync(option, cancellationToken).ConfigureAwait(false);
			await _database.Xdr.FlushAsync(cancellationToken).ConfigureAwait(false);

				if (option == IscCodes.DSQL_drop)
				{
					_parameters = null;
					_fields = null;
					_parametersBlr = null;
					_fieldsBlr = null;
				}

			Clear();
		}
		catch (IOException ex)
		{
			State = StatementState.Error;
			throw IscException.ForIOException(ex);
		}
	}

	protected static void ProcessFreeResponse(IResponse response)
	{ }
	protected static ValueTask ProcessFreeResponseAsync(IResponse response, CancellationToken cancellationToken = default)
	{
		return ValueTask.CompletedTask;
	}
	#endregion

	#region op_allocate_statement methods
	protected void SendAllocateToBuffer()
	{
		_database.Xdr.WriteBytes(bufOpAllocStatement);
		_database.Xdr.Write(_database.Handle);
	}
	protected async ValueTask SendAllocateToBufferAsync(CancellationToken cancellationToken = default)
	{
		await _database.Xdr.WriteBytesAsync(bufOpAllocStatement, 4, cancellationToken).ConfigureAwait(false);
		await _database.Xdr.WriteAsync(_database.Handle, cancellationToken).ConfigureAwait(false);
	}

	protected void ProcessAllocateResponse(GenericResponse response)
	{
		_handle = response.ObjectHandle;
		_allRowsFetched = false;
		State = StatementState.Allocated;
		StatementType = DbStatementType.None;
	}
	protected ValueTask ProcessAllocateResponseAsync(GenericResponse response, CancellationToken cancellationToken = default)
	{
		_handle = response.ObjectHandle;
		_allRowsFetched = false;
		State = StatementState.Allocated;
		StatementType = DbStatementType.None;
		return ValueTask.CompletedTask;
	}
	#endregion

	#region op_execute/op_execute2 methods

	private static readonly byte[] zeroIntBuf = TypeEncoder.EncodeInt32(0);
	private static readonly byte[] oneIntBuf = TypeEncoder.EncodeInt32(1);
	private static readonly byte[] bufOpEx1 = TypeEncoder.EncodeInt32(IscCodes.op_execute);
	private static readonly byte[] bufOpEx2 = TypeEncoder.EncodeInt32(IscCodes.op_execute2);
	private static readonly byte[] bufOpFetch = TypeEncoder.EncodeInt32(IscCodes.op_fetch);
	private static readonly byte[] bufOpPrepare = TypeEncoder.EncodeInt32(IscCodes.op_prepare_statement);
	private static readonly byte[] bufOpInfoSql = TypeEncoder.EncodeInt32(IscCodes.op_info_sql);
	private static readonly byte[] bufOpFreeStatement = TypeEncoder.EncodeInt32(IscCodes.op_free_statement);
	private static readonly byte[] bufOpAllocStatement = TypeEncoder.EncodeInt32(IscCodes.op_allocate_statement);
	private static readonly byte[] bufPrepareInfoSize = TypeEncoder.EncodeInt32(IscCodes.PREPARE_INFO_BUFFER_SIZE);

	protected virtual void SendExecuteToBuffer(int timeout, IDescriptorFiller descriptorFiller)
	{
		ReadOnlySpan<byte> boe1 = bufOpEx1;
		ReadOnlySpan<byte> boe2 = bufOpEx2;
		ReadOnlySpan<byte> bzero = zeroIntBuf;
		ReadOnlySpan<byte> bone = oneIntBuf;
		// this may throw error, so it needs to be before any writing
		descriptorFiller.Fill(_parameters, 0);
		var parametersData = EncodeCurrentParameters();

		if (StatementType == DbStatementType.StoredProcedure)
		{
			_database.Xdr.WriteBytes(boe2);
		}
		else
		{
			_database.Xdr.WriteBytes(boe1);
		}

		_database.Xdr.Write(_handle);
		_database.Xdr.Write(_transaction.Handle);

		if (_parameters != null)
		{
				_database.Xdr.WriteBuffer(GetParametersBlr().Data);
			_database.Xdr.WriteBytes(bzero); // Message number
			_database.Xdr.WriteBytes(bone); // Number of messages
			_database.Xdr.WriteBytes(parametersData);
		}
		else
		{
			_database.Xdr.WriteBuffer(null);
			_database.Xdr.WriteBytes(bzero);
			_database.Xdr.WriteBytes(bzero);
		}

		if (StatementType == DbStatementType.StoredProcedure)
		{
				_database.Xdr.WriteBuffer(_fields != null ? GetFieldsBlr().Data : null);
			_database.Xdr.WriteBytes(bzero);
		}
	}
	protected virtual async ValueTask SendExecuteToBufferAsync(int timeout, IDescriptorFiller descriptorFiller, CancellationToken cancellationToken = default)
	{
		// this may throw error, so it needs to be before any writing
		await descriptorFiller.FillAsync(_parameters, 0, cancellationToken).ConfigureAwait(false);
		var parametersData = EncodeCurrentParameters();

		if (StatementType == DbStatementType.StoredProcedure)
		{
			_database.Xdr.WriteBytes(bufOpEx2);
		}
		else
		{
			_database.Xdr.WriteBytes(bufOpEx1);
		}

		_database.Xdr.Write(_handle);
		_database.Xdr.Write(_transaction.Handle);

		if (_parameters != null)
		{
				_database.Xdr.WriteBuffer(GetParametersBlr().Data);
			_database.Xdr.WriteBytes(zeroIntBuf); // Message number
			_database.Xdr.WriteBytes(oneIntBuf); // Number of messages
			_database.Xdr.WriteBytes(parametersData);
		}
		else
		{
			_database.Xdr.WriteBuffer(null);
			_database.Xdr.WriteBytes(zeroIntBuf);
			_database.Xdr.WriteBytes(zeroIntBuf);
		}

		if (StatementType == DbStatementType.StoredProcedure)
		{
				_database.Xdr.WriteBuffer(_fields != null ? GetFieldsBlr().Data : null);
			_database.Xdr.WriteBytes(zeroIntBuf); // Output message number
		}
	}

	protected static void ProcessExecuteResponse(GenericResponse response)
	{ }
	protected static ValueTask ProcessExecuteResponseAsync(GenericResponse response, CancellationToken cancellationToken = default)
	{
		return ValueTask.CompletedTask;
	}

	protected void ProcessStoredProcedureExecuteResponse(SqlResponse response)
	{
		try
		{
			if (response.Count > 0)
			{
				OutputParameters.Enqueue(ReadRow());
			}
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}
	protected async ValueTask ProcessStoredProcedureExecuteResponseAsync(SqlResponse response, CancellationToken cancellationToken = default)
	{
		try
		{
			if (response.Count > 0)
			{
				OutputParameters.Enqueue(await ReadRowAsync(cancellationToken).ConfigureAwait(false));
			}
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
	}
	#endregion

	protected override void TransactionUpdated(object sender, EventArgs e)
	{
		if (Transaction != null && TransactionUpdate != null)
		{
			Transaction.Update -= TransactionUpdate;
		}

		State = StatementState.Closed;
		TransactionUpdate = null;
		_allRowsFetched = false;
	}

	protected Descriptor[] ParseSqlInfo(byte[] info, byte[] items, Descriptor[] rowDescs)
	{
		return ParseTruncSqlInfo(info, items, rowDescs);
	}
	protected ValueTask<Descriptor[]> ParseSqlInfoAsync(byte[] info, byte[] items, Descriptor[] rowDescs, CancellationToken cancellationToken = default)
	{
		return ParseTruncSqlInfoAsync(info, items, rowDescs, cancellationToken);
	}

	protected Descriptor[] ParseTruncSqlInfo(byte[] info, byte[] items, Descriptor[] rowDescs)
	{
		var currentPosition = 0;
		var currentDescriptorIndex = -1;
		var currentItemIndex = 0;
		while (info[currentPosition] != IscCodes.isc_info_end)
		{
			byte item;
			while ((item = info[currentPosition++]) != IscCodes.isc_info_sql_describe_end)
			{
				switch (item)
				{
					case IscCodes.isc_info_truncated:
						currentItemIndex--;

						var newItems = new List<byte>(items.Length);
						var part = 0;
						var chock = 0;
						for (var i = 0; i < items.Length; i++)
						{
							if (items[i] == IscCodes.isc_info_sql_describe_end)
							{
								newItems.Insert(chock, IscCodes.isc_info_sql_sqlda_start);
								newItems.Insert(chock + 1, 2);

								var processedItems = (rowDescs[part] != null ? rowDescs[part].Count : (short)0);
								newItems.Insert(chock + 2, (byte)((part == currentDescriptorIndex ? currentItemIndex : processedItems) & 255));
								newItems.Insert(chock + 3, (byte)((part == currentDescriptorIndex ? currentItemIndex : processedItems) >> 8));

								part++;
								chock = i + 4 + 1;
							}
							newItems.Add(items[i]);
						}

						info = GetSqlInfo(newItems.ToArray(), info.Length);

						currentPosition = 0;
						currentDescriptorIndex = -1;
						goto Break;

					case IscCodes.isc_info_sql_select:
					case IscCodes.isc_info_sql_bind:
						currentDescriptorIndex++;

						if (info[currentPosition] == IscCodes.isc_info_truncated)
							break;

						currentPosition++;
						var len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						if (rowDescs[currentDescriptorIndex] == null)
						{
							var n = IscHelper.VaxInteger(info, currentPosition, len);
							rowDescs[currentDescriptorIndex] = new Descriptor((short)n);
							if (n == 0)
							{
								currentPosition += len;
								goto Break;
							}
						}
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_sqlda_seq:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						currentItemIndex = (int)IscHelper.VaxInteger(info, currentPosition, len);
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_type:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						rowDescs[currentDescriptorIndex][currentItemIndex - 1].DataType = (short)IscHelper.VaxInteger(info, currentPosition, len);
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_sub_type:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						rowDescs[currentDescriptorIndex][currentItemIndex - 1].SubType = (short)IscHelper.VaxInteger(info, currentPosition, len);
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_scale:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						rowDescs[currentDescriptorIndex][currentItemIndex - 1].NumericScale = (short)IscHelper.VaxInteger(info, currentPosition, len);
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_length:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						rowDescs[currentDescriptorIndex][currentItemIndex - 1].Length = (short)IscHelper.VaxInteger(info, currentPosition, len);
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_field:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						rowDescs[currentDescriptorIndex][currentItemIndex - 1].Name = _database.Charset.GetString(info, currentPosition, len);
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_relation:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						rowDescs[currentDescriptorIndex][currentItemIndex - 1].Relation = _database.Charset.GetString(info, currentPosition, len);
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_owner:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						rowDescs[currentDescriptorIndex][currentItemIndex - 1].Owner = _database.Charset.GetString(info, currentPosition, len);
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_alias:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						rowDescs[currentDescriptorIndex][currentItemIndex - 1].Alias = _database.Charset.GetString(info, currentPosition, len);
						currentPosition += len;
						break;

					default:
						throw IscException.ForErrorCode(IscCodes.isc_dsql_sqlda_err);
				}
			}
			// just to get out of the loop
			Break:
			{ }
		}
		return rowDescs;
	}
	private Descriptor[] ParseTruncSqlInfoSpan(ReadOnlySpan<byte> info, ReadOnlySpan<byte> items, Descriptor[] rowDescs)
	{
		var currentPosition = 0;
		var currentDescriptorIndex = -1;
		var currentItemIndex = 0;
		while (info[currentPosition] != IscCodes.isc_info_end)
		{
			byte item;
			while ((item = info[currentPosition++]) != IscCodes.isc_info_sql_describe_end)
			{
				switch (item)
				{
					case IscCodes.isc_info_truncated:
						currentItemIndex--;

						var newItems = new List<byte>(items.Length);
						var part = 0;
						var chock = 0;
						for (var i = 0; i < items.Length; i++)
						{
							if (items[i] == IscCodes.isc_info_sql_describe_end)
							{
								newItems.Insert(chock, IscCodes.isc_info_sql_sqlda_start);
								newItems.Insert(chock + 1, 2);

								var processedItems = (rowDescs[part] != null ? rowDescs[part].Count : (short)0);
								newItems.Insert(chock + 2, (byte)((part == currentDescriptorIndex ? currentItemIndex : processedItems) & 255));
								newItems.Insert(chock + 3, (byte)((part == currentDescriptorIndex ? currentItemIndex : processedItems) >> 8));

								part++;
								chock = i + 4 + 1;
							}
							newItems.Add(items[i]);
						}

						var refreshed = GetSqlInfo(newItems.ToArray(), info.Length);
						info = refreshed;

						currentPosition = 0;
						currentDescriptorIndex = -1;
						goto BreakSpan;

					case IscCodes.isc_info_sql_select:
					case IscCodes.isc_info_sql_bind:
						currentDescriptorIndex++;

						if (info[currentPosition] == IscCodes.isc_info_truncated)
							break;

						currentPosition++;
						var len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						if (rowDescs[currentDescriptorIndex] == null)
						{
							var n = IscHelper.VaxInteger(info, currentPosition, len);
							rowDescs[currentDescriptorIndex] = new Descriptor((short)n);
							if (n == 0)
							{
								currentPosition += len;
								goto BreakSpan;
							}
						}
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_sqlda_seq:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						currentItemIndex = (int)IscHelper.VaxInteger(info, currentPosition, len);
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_type:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						rowDescs[currentDescriptorIndex][currentItemIndex - 1].DataType = (short)IscHelper.VaxInteger(info, currentPosition, len);
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_sub_type:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						rowDescs[currentDescriptorIndex][currentItemIndex - 1].SubType = (short)IscHelper.VaxInteger(info, currentPosition, len);
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_scale:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						rowDescs[currentDescriptorIndex][currentItemIndex - 1].NumericScale = (short)IscHelper.VaxInteger(info, currentPosition, len);
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_length:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						rowDescs[currentDescriptorIndex][currentItemIndex - 1].Length = (short)IscHelper.VaxInteger(info, currentPosition, len);
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_field:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						rowDescs[currentDescriptorIndex][currentItemIndex - 1].Name = _database.Charset.GetString(info.Slice(currentPosition, len));
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_relation:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						rowDescs[currentDescriptorIndex][currentItemIndex - 1].Relation = _database.Charset.GetString(info.Slice(currentPosition, len));
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_owner:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						rowDescs[currentDescriptorIndex][currentItemIndex - 1].Owner = _database.Charset.GetString(info.Slice(currentPosition, len));
						currentPosition += len;
						break;

					case IscCodes.isc_info_sql_alias:
						len = (int)IscHelper.VaxInteger(info, currentPosition, 2);
						currentPosition += 2;
						rowDescs[currentDescriptorIndex][currentItemIndex - 1].Alias = _database.Charset.GetString(info.Slice(currentPosition, len));
						currentPosition += len;
						break;

					default:
						throw IscException.ForErrorCode(IscCodes.isc_dsql_sqlda_err);
				}
			}
			// just to get out of the loop
			BreakSpan:
					{ }
		}
		return rowDescs;
	}

	private ValueTask<Descriptor[]> ParseTruncSqlInfoAsync(byte[] info, ReadOnlyMemory<byte> items, Descriptor[] rowDescs, CancellationToken cancellationToken) =>		
		ParseTruncSqlInfoSpanAsync(info.AsMemory(), items, rowDescs, cancellationToken);

	private async ValueTask<Descriptor[]> ParseTruncSqlInfoSpanAsync(ReadOnlyMemory<byte> info, ReadOnlyMemory<byte> items, Descriptor[] rowDescs, CancellationToken cancellationToken)
	{
		var currentPosition = 0;
		var currentDescriptorIndex = -1;
		var currentItemIndex = 0;
		while(info.Span[currentPosition] != IscCodes.isc_info_end) {
			byte item;
			while((item = info.Span[currentPosition++]) != IscCodes.isc_info_sql_describe_end) {
				switch(item) {
				case IscCodes.isc_info_truncated:
					currentItemIndex--;

					var newItems = new List<byte>(items.Length);
					var part = 0;
					var chock = 0;
					for(var i = 0; i < items.Length; i++) {
						if(items.Span[i] == IscCodes.isc_info_sql_describe_end) {
							newItems.Insert(chock, IscCodes.isc_info_sql_sqlda_start);
							newItems.Insert(chock + 1, 2);

							var processedItems = (rowDescs[part] != null ? rowDescs[part].Count : (short)0);
							newItems.Insert(chock + 2, (byte)((part == currentDescriptorIndex ? currentItemIndex : processedItems) & 255));
							newItems.Insert(chock + 3, (byte)((part == currentDescriptorIndex ? currentItemIndex : processedItems) >> 8));

							part++;
							chock = i + 4 + 1;
						}
						newItems.Add(items.Span[i]);
					}

					var refreshed = await GetSqlInfoAsync(newItems.ToArray(), info.Length, cancellationToken).ConfigureAwait(false);
					info = refreshed;

					currentPosition = 0;
					currentDescriptorIndex = -1;
					goto BreakAsync;

				case IscCodes.isc_info_sql_select:
				case IscCodes.isc_info_sql_bind:
					currentDescriptorIndex++;

					if(info.Span[currentPosition] == IscCodes.isc_info_truncated)
						break;

					currentPosition++;
					var len = (int)IscHelper.VaxInteger(info.Span, currentPosition, 2);
					currentPosition += 2;
					if(rowDescs[currentDescriptorIndex] == null) {
						var n = IscHelper.VaxInteger(info.Span, currentPosition, len);
						rowDescs[currentDescriptorIndex] = new Descriptor((short)n);
						if(n == 0) {
							currentPosition += len;
							goto BreakAsync;
						}
					}
					currentPosition += len;
					break;

				case IscCodes.isc_info_sql_sqlda_seq:
					len = (int)IscHelper.VaxInteger(info.Span, currentPosition, 2);
					currentPosition += 2;
					currentItemIndex = (int)IscHelper.VaxInteger(info.Span, currentPosition, len);
					currentPosition += len;
					break;

				case IscCodes.isc_info_sql_type:
					len = (int)IscHelper.VaxInteger(info.Span, currentPosition, 2);
					currentPosition += 2;
					rowDescs[currentDescriptorIndex][currentItemIndex - 1].DataType = (short)IscHelper.VaxInteger(info.Span, currentPosition, len);
					currentPosition += len;
					break;

				case IscCodes.isc_info_sql_sub_type:
					len = (int)IscHelper.VaxInteger(info.Span, currentPosition, 2);
					currentPosition += 2;
					rowDescs[currentDescriptorIndex][currentItemIndex - 1].SubType = (short)IscHelper.VaxInteger(info.Span, currentPosition, len);
					currentPosition += len;
					break;

				case IscCodes.isc_info_sql_scale:
					len = (int)IscHelper.VaxInteger(info.Span, currentPosition, 2);
					currentPosition += 2;
					rowDescs[currentDescriptorIndex][currentItemIndex - 1].NumericScale = (short)IscHelper.VaxInteger(info.Span, currentPosition, len);
					currentPosition += len;
					break;

				case IscCodes.isc_info_sql_length:
					len = (int)IscHelper.VaxInteger(info.Span, currentPosition, 2);
					currentPosition += 2;
					rowDescs[currentDescriptorIndex][currentItemIndex - 1].Length = (short)IscHelper.VaxInteger(info.Span, currentPosition, len);
					currentPosition += len;
					break;

				case IscCodes.isc_info_sql_field:
					len = (int)IscHelper.VaxInteger(info.Span, currentPosition, 2);
					currentPosition += 2;
					rowDescs[currentDescriptorIndex][currentItemIndex - 1].Name = _database.Charset.GetString(info.Span.Slice(currentPosition, len));
					currentPosition += len;
					break;

				case IscCodes.isc_info_sql_relation:
					len = (int)IscHelper.VaxInteger(info.Span, currentPosition, 2);
					currentPosition += 2;
					rowDescs[currentDescriptorIndex][currentItemIndex - 1].Relation = _database.Charset.GetString(info.Span.Slice(currentPosition, len));
					currentPosition += len;
					break;

				case IscCodes.isc_info_sql_owner:
					len = (int)IscHelper.VaxInteger(info.Span, currentPosition, 2);
					currentPosition += 2;
					rowDescs[currentDescriptorIndex][currentItemIndex - 1].Owner = _database.Charset.GetString(info.Span.Slice(currentPosition, len));
					currentPosition += len;
					break;

				case IscCodes.isc_info_sql_alias:
					len = (int)IscHelper.VaxInteger(info.Span, currentPosition, 2);
					currentPosition += 2;
					rowDescs[currentDescriptorIndex][currentItemIndex - 1].Alias = _database.Charset.GetString(info.Span.Slice(currentPosition, len));
					currentPosition += len;
					break;

				default:
					throw IscException.ForErrorCode(IscCodes.isc_dsql_sqlda_err);
				}
			}
			// just to get out of the loop
			BreakAsync:
			{ }
		}
		return rowDescs;
	}

	protected virtual void WriteParametersTo(IXdrWriter xdr)
	{
		if (_parameters == null)
			return;

		for (var i = 0; i < _parameters.Count; i++)
		{
			var field = _parameters[i];
			try
			{
				WriteRawParameter(xdr, field);
				xdr.Write(field.NullFlag);
			}
			catch (IOException ex)
			{
				throw IscException.ForIOException(ex);
			}
		}
	}

	internal ReadOnlySpan<byte> EncodeCurrentParameters()
	{
		if (_parameters == null)
			return ReadOnlySpan<byte>.Empty;

		_parametersWriteBuffer ??= new PooledWriteBuffer(256);
		_parametersXdr ??= new XdrReaderWriter(_parametersWriteBuffer, _database.Charset);
		_parametersWriteBuffer.Reset();
		WriteParametersTo(_parametersXdr);
		_parametersXdr.Flush();
		return _parametersWriteBuffer.WrittenSpan;
	}

	protected static void WriteRawParameter(IXdrWriter xdr, DbField field)
	{
		if (field.DbDataType != DbDataType.Null)
		{
			field.FixNull();

			switch (field.DbDataType)
			{
				case DbDataType.Char:
					if (field.Charset.IsOctetsCharset)
					{
						xdr.WriteOpaque(field.DbValue.GetBinary(), field.Length);
					}
					else
					{
						var svalue = field.DbValue.GetString();
						if ((field.Length % field.Charset.BytesPerCharacter) == 0 && svalue.CountRunes() > field.CharCount)
						{
							throw IscException.ForErrorCodes(new[] { IscCodes.isc_arith_except, IscCodes.isc_string_truncation });
						}
						var encoding = field.Charset.Encoding;
						var byteCount = encoding.GetByteCount(svalue);
						if (byteCount > field.Length)
						{
							throw IscException.ForErrorCodes([IscCodes.isc_arith_except, IscCodes.isc_string_truncation]);
						}
						Span<byte> stack = byteCount <= 512 ? stackalloc byte[byteCount] : Span<byte>.Empty;
						if (!stack.IsEmpty)
						{
							encoding.GetBytes(svalue.AsSpan(), stack);
							xdr.WriteOpaque(stack, field.Length);
						}
						else
						{
							var rented = System.Buffers.ArrayPool<byte>.Shared.Rent(byteCount);
							try
							{
								var written = encoding.GetBytes(svalue, 0, svalue.Length, rented, 0);
								xdr.WriteOpaque(rented.AsSpan(0, written), field.Length);
							}
							finally
							{
								System.Buffers.ArrayPool<byte>.Shared.Return(rented);
							}
						}
					}
					break;

				case DbDataType.VarChar:
					if (field.Charset.IsOctetsCharset)
					{
						xdr.WriteBuffer(field.DbValue.GetBinary());
					}
					else
					{
						var svalue = field.DbValue.GetString();
						if ((field.Length % field.Charset.BytesPerCharacter) == 0 && svalue.CountRunes() > field.CharCount)
						{
							throw IscException.ForErrorCodes([IscCodes.isc_arith_except, IscCodes.isc_string_truncation]);
						}
						var encoding = field.Charset.Encoding;
						var byteCount = encoding.GetByteCount(svalue);
						Span<byte> stack = byteCount <= 512 ? stackalloc byte[byteCount] : Span<byte>.Empty;
						if (!stack.IsEmpty)
						{
							encoding.GetBytes(svalue.AsSpan(), stack);
							xdr.WriteBuffer(stack);
						}
						else
						{
							var rented = System.Buffers.ArrayPool<byte>.Shared.Rent(byteCount);
							try
							{
								var written = encoding.GetBytes(svalue, 0, svalue.Length, rented, 0);
								xdr.WriteBuffer(rented.AsSpan(0, written));
							}
							finally
							{
								System.Buffers.ArrayPool<byte>.Shared.Return(rented);
							}
						}
					}
					break;

				case DbDataType.SmallInt:
					xdr.Write(field.DbValue.GetInt16());
					break;

				case DbDataType.Integer:
					xdr.Write(field.DbValue.GetInt32());
					break;

				case DbDataType.BigInt:
				case DbDataType.Array:
				case DbDataType.Binary:
				case DbDataType.Text:
					xdr.Write(field.DbValue.GetInt64());
					break;

				case DbDataType.Decimal:
				case DbDataType.Numeric:
					xdr.Write(field.DbValue.GetDecimal(), field.DataType, field.NumericScale);
					break;

				case DbDataType.Float:
					xdr.Write(field.DbValue.GetFloat());
					break;

				case DbDataType.Guid:
					xdr.Write(field.DbValue.GetGuid(), field.SqlType);
					break;

				case DbDataType.Double:
					xdr.Write(field.DbValue.GetDouble());
					break;

				case DbDataType.Date:
					xdr.Write(field.DbValue.GetDate());
					break;

				case DbDataType.Time:
					xdr.Write(field.DbValue.GetTime());
					break;

				case DbDataType.TimeStamp:
					xdr.Write(field.DbValue.GetDate());
					xdr.Write(field.DbValue.GetTime());
					break;

				case DbDataType.Boolean:
					xdr.Write(field.DbValue.GetBoolean());
					break;

				case DbDataType.TimeStampTZ:
					xdr.Write(field.DbValue.GetDate());
					xdr.Write(field.DbValue.GetTime());
					xdr.Write(field.DbValue.GetTimeZoneId());
					break;

				case DbDataType.TimeStampTZEx:
					xdr.Write(field.DbValue.GetDate());
					xdr.Write(field.DbValue.GetTime());
					xdr.Write(field.DbValue.GetTimeZoneId());
					xdr.Write((short)0);
					break;

				case DbDataType.TimeTZ:
					xdr.Write(field.DbValue.GetTime());
					xdr.Write(field.DbValue.GetTimeZoneId());
					break;

				case DbDataType.TimeTZEx:
					xdr.Write(field.DbValue.GetTime());
					xdr.Write(field.DbValue.GetTimeZoneId());
					xdr.Write((short)0);
					break;

				case DbDataType.Dec16:
					xdr.Write(field.DbValue.GetDecFloat(), 16);
					break;

				case DbDataType.Dec34:
					xdr.Write(field.DbValue.GetDecFloat(), 34);
					break;

				case DbDataType.Int128:
					xdr.Write(field.DbValue.GetInt128());
					break;

				default:
					throw IscException.ForStrParam($"Unknown SQL data type: {field.DataType}.");
			}
		}
	}
	protected object ReadRawValue(IXdrReader xdr, DbField field)
	{
		var innerCharset = !_database.Charset.IsNoneCharset ? _database.Charset : field.Charset;

		switch (field.DbDataType)
		{
			case DbDataType.Char:
				if (field.Charset.IsOctetsCharset)
				{
					return xdr.ReadOpaque(field.Length);
				}
				else
				{
					var s = xdr.ReadString(innerCharset, field.Length);
					return TruncateStringByRuneCount(s, field);
				}

			case DbDataType.VarChar:
				if (field.Charset.IsOctetsCharset)
				{
					return xdr.ReadBuffer();
				}
				else
				{
					return xdr.ReadString(innerCharset);
				}

			case DbDataType.SmallInt:
				return xdr.ReadInt16();

			case DbDataType.Integer:
				return xdr.ReadInt32();

			case DbDataType.Array:
			case DbDataType.Binary:
			case DbDataType.Text:
			case DbDataType.BigInt:
				return xdr.ReadInt64();

			case DbDataType.Decimal:
			case DbDataType.Numeric:
				return xdr.ReadDecimal(field.DataType, field.NumericScale);

			case DbDataType.Float:
				return xdr.ReadSingle();

			case DbDataType.Guid:
				return xdr.ReadGuid(field.SqlType);

			case DbDataType.Double:
				return xdr.ReadDouble();

			case DbDataType.Date:
				return xdr.ReadDate();

			case DbDataType.Time:
				return xdr.ReadTime();

			case DbDataType.TimeStamp:
				return xdr.ReadDateTime();

			case DbDataType.Boolean:
				return xdr.ReadBoolean();

			case DbDataType.TimeStampTZ:
				return xdr.ReadZonedDateTime(false);

			case DbDataType.TimeStampTZEx:
				return xdr.ReadZonedDateTime(true);

			case DbDataType.TimeTZ:
				return xdr.ReadZonedTime(false);

			case DbDataType.TimeTZEx:
				return xdr.ReadZonedTime(true);

			case DbDataType.Dec16:
				return xdr.ReadDec16();

			case DbDataType.Dec34:
				return xdr.ReadDec34();

			case DbDataType.Int128:
				return xdr.ReadInt128();

			default:
				throw TypeHelper.InvalidDataType((int)field.DbDataType);
		}
	}
	protected async ValueTask<object> ReadRawValueAsync(IXdrReader xdr, DbField field, CancellationToken cancellationToken = default)
	{
		var innerCharset = !_database.Charset.IsNoneCharset ? _database.Charset : field.Charset;

		switch (field.DbDataType)
		{
			case DbDataType.Char:
				if (field.Charset.IsOctetsCharset)
				{
					return await xdr.ReadOpaqueAsync(field.Length, cancellationToken).ConfigureAwait(false);
				}
				else
				{
					var s = await xdr.ReadStringAsync(innerCharset, field.Length, cancellationToken).ConfigureAwait(false);
					return TruncateStringByRuneCount(s, field);
				}

			case DbDataType.VarChar:
				if (field.Charset.IsOctetsCharset)
				{
					return await xdr.ReadBufferAsync(cancellationToken).ConfigureAwait(false);
				}
				else
				{
					return await xdr.ReadStringAsync(innerCharset, cancellationToken).ConfigureAwait(false);
				}

			case DbDataType.SmallInt:
				return await xdr.ReadInt16Async(cancellationToken).ConfigureAwait(false);

			case DbDataType.Integer:
				return await xdr.ReadInt32Async(cancellationToken).ConfigureAwait(false);

			case DbDataType.Array:
			case DbDataType.Binary:
			case DbDataType.Text:
			case DbDataType.BigInt:
				return await xdr.ReadInt64Async(cancellationToken).ConfigureAwait(false);

			case DbDataType.Decimal:
			case DbDataType.Numeric:
				return await xdr.ReadDecimalAsync(field.DataType, field.NumericScale, cancellationToken).ConfigureAwait(false);

			case DbDataType.Float:
				return await xdr.ReadSingleAsync(cancellationToken).ConfigureAwait(false);

			case DbDataType.Guid:
				return await xdr.ReadGuidAsync(field.SqlType, cancellationToken).ConfigureAwait(false);

			case DbDataType.Double:
				return await xdr.ReadDoubleAsync(cancellationToken).ConfigureAwait(false);

			case DbDataType.Date:
				return await xdr.ReadDateAsync(cancellationToken).ConfigureAwait(false);

			case DbDataType.Time:
				return await xdr.ReadTimeAsync(cancellationToken).ConfigureAwait(false);

			case DbDataType.TimeStamp:
				return await xdr.ReadDateTimeAsync(cancellationToken).ConfigureAwait(false);

			case DbDataType.Boolean:
				return await xdr.ReadBooleanAsync(cancellationToken).ConfigureAwait(false);

			case DbDataType.TimeStampTZ:
				return await xdr.ReadZonedDateTimeAsync(false, cancellationToken).ConfigureAwait(false);

			case DbDataType.TimeStampTZEx:
				return await xdr.ReadZonedDateTimeAsync(true, cancellationToken).ConfigureAwait(false);

			case DbDataType.TimeTZ:
				return await xdr.ReadZonedTimeAsync(false, cancellationToken).ConfigureAwait(false);

			case DbDataType.TimeTZEx:
				return await xdr.ReadZonedTimeAsync(true, cancellationToken).ConfigureAwait(false);

			case DbDataType.Dec16:
				return await xdr.ReadDec16Async(cancellationToken).ConfigureAwait(false);

			case DbDataType.Dec34:
				return await xdr.ReadDec34Async(cancellationToken).ConfigureAwait(false);

			case DbDataType.Int128:
				return await xdr.ReadInt128Async(cancellationToken).ConfigureAwait(false);

			default:
				throw TypeHelper.InvalidDataType((int)field.DbDataType);
		}
	}

	protected DbValueStorage ReadRawValueStorage(IXdrReader xdr, DbField field)
	{
		var innerCharset = !_database.Charset.IsNoneCharset ? _database.Charset : field.Charset;

		switch (field.DbDataType)
		{
			case DbDataType.Char:
				if (field.Charset.IsOctetsCharset)
				{
					return DbValueStorage.FromBytes(xdr.ReadOpaque(field.Length));
				}
				else
				{
					var s = xdr.ReadString(innerCharset, field.Length);
					return DbValueStorage.FromString(TruncateStringByRuneCount(s, field));
				}

			case DbDataType.VarChar:
				if (field.Charset.IsOctetsCharset)
				{
					return DbValueStorage.FromBytes(xdr.ReadBuffer());
				}
				else
				{
					return DbValueStorage.FromString(xdr.ReadString(innerCharset));
				}

			case DbDataType.SmallInt:
				return DbValueStorage.FromInt16(xdr.ReadInt16());

			case DbDataType.Integer:
				return DbValueStorage.FromInt32(xdr.ReadInt32());

			case DbDataType.Array:
			case DbDataType.Binary:
			case DbDataType.Text:
			case DbDataType.BigInt:
				return DbValueStorage.FromInt64(xdr.ReadInt64());

			case DbDataType.Decimal:
			case DbDataType.Numeric:
				return DbValueStorage.FromDecimal(xdr.ReadDecimal(field.DataType, field.NumericScale));

			case DbDataType.Float:
				return DbValueStorage.FromSingle(xdr.ReadSingle());

			case DbDataType.Guid:
				return DbValueStorage.FromGuid(xdr.ReadGuid(field.SqlType));

			case DbDataType.Double:
				return DbValueStorage.FromDouble(xdr.ReadDouble());

			case DbDataType.Date:
				return DbValueStorage.FromDateTime(xdr.ReadDate());

			case DbDataType.Time:
				return DbValueStorage.FromTimeSpan(xdr.ReadTime());

			case DbDataType.TimeStamp:
				return DbValueStorage.FromDateTime(xdr.ReadDateTime());

			case DbDataType.Boolean:
				return DbValueStorage.FromBoolean(xdr.ReadBoolean());

			case DbDataType.TimeStampTZ:
				return DbValueStorage.FromZonedDateTime(xdr.ReadZonedDateTime(false));

			case DbDataType.TimeStampTZEx:
				return DbValueStorage.FromZonedDateTime(xdr.ReadZonedDateTime(true));

			case DbDataType.TimeTZ:
				return DbValueStorage.FromZonedTime(xdr.ReadZonedTime(false));

			case DbDataType.TimeTZEx:
				return DbValueStorage.FromZonedTime(xdr.ReadZonedTime(true));

			case DbDataType.Dec16:
				{
					var storage = new DbValueStorage { Kind = DbValueKind.Dec16, Object = null };
					var dst = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref storage.Data, 1));
					xdr.ReadBytes(dst, 8);
					return storage;
				}

			case DbDataType.Dec34:
				{
					var storage = new DbValueStorage { Kind = DbValueKind.Dec34, Object = null };
					var dst = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref storage.Data, 1));
					xdr.ReadBytes(dst, 16);
					return storage;
				}

			case DbDataType.Int128:
				{
					var storage = new DbValueStorage { Kind = DbValueKind.Int128, Object = null };
					var dst = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref storage.Data, 1));
					xdr.ReadBytes(dst, 16);
					return storage;
				}

			default:
				throw TypeHelper.InvalidDataType((int)field.DbDataType);
		}
	}
	protected async ValueTask<DbValueStorage> ReadRawValueStorageAsync(IXdrReader xdr, DbField field, CancellationToken cancellationToken = default)
	{
		var innerCharset = !_database.Charset.IsNoneCharset ? _database.Charset : field.Charset;

		switch (field.DbDataType)
		{
			case DbDataType.Char:
				if (field.Charset.IsOctetsCharset)
				{
					return DbValueStorage.FromBytes(await xdr.ReadOpaqueAsync(field.Length, cancellationToken).ConfigureAwait(false));
				}
				else
				{
					var s = await xdr.ReadStringAsync(innerCharset, field.Length, cancellationToken).ConfigureAwait(false);
					return DbValueStorage.FromString(TruncateStringByRuneCount(s, field));
				}

			case DbDataType.VarChar:
				if (field.Charset.IsOctetsCharset)
				{
					return DbValueStorage.FromBytes(await xdr.ReadBufferAsync(cancellationToken).ConfigureAwait(false));
				}
				else
				{
					return DbValueStorage.FromString(await xdr.ReadStringAsync(innerCharset, cancellationToken).ConfigureAwait(false));
				}

			case DbDataType.SmallInt:
				return DbValueStorage.FromInt16(await xdr.ReadInt16Async(cancellationToken).ConfigureAwait(false));

			case DbDataType.Integer:
				return DbValueStorage.FromInt32(await xdr.ReadInt32Async(cancellationToken).ConfigureAwait(false));

			case DbDataType.Array:
			case DbDataType.Binary:
			case DbDataType.Text:
			case DbDataType.BigInt:
				return DbValueStorage.FromInt64(await xdr.ReadInt64Async(cancellationToken).ConfigureAwait(false));

			case DbDataType.Decimal:
			case DbDataType.Numeric:
				return DbValueStorage.FromDecimal(await xdr.ReadDecimalAsync(field.DataType, field.NumericScale, cancellationToken).ConfigureAwait(false));

			case DbDataType.Float:
				return DbValueStorage.FromSingle(await xdr.ReadSingleAsync(cancellationToken).ConfigureAwait(false));

			case DbDataType.Guid:
				return DbValueStorage.FromGuid(await xdr.ReadGuidAsync(field.SqlType, cancellationToken).ConfigureAwait(false));

			case DbDataType.Double:
				return DbValueStorage.FromDouble(await xdr.ReadDoubleAsync(cancellationToken).ConfigureAwait(false));

			case DbDataType.Date:
				return DbValueStorage.FromDateTime(await xdr.ReadDateAsync(cancellationToken).ConfigureAwait(false));

			case DbDataType.Time:
				return DbValueStorage.FromTimeSpan(await xdr.ReadTimeAsync(cancellationToken).ConfigureAwait(false));

			case DbDataType.TimeStamp:
				return DbValueStorage.FromDateTime(await xdr.ReadDateTimeAsync(cancellationToken).ConfigureAwait(false));

			case DbDataType.Boolean:
				return DbValueStorage.FromBoolean(await xdr.ReadBooleanAsync(cancellationToken).ConfigureAwait(false));

			case DbDataType.TimeStampTZ:
				return DbValueStorage.FromZonedDateTime(await xdr.ReadZonedDateTimeAsync(false, cancellationToken).ConfigureAwait(false));

			case DbDataType.TimeStampTZEx:
				return DbValueStorage.FromZonedDateTime(await xdr.ReadZonedDateTimeAsync(true, cancellationToken).ConfigureAwait(false));

			case DbDataType.TimeTZ:
				return DbValueStorage.FromZonedTime(await xdr.ReadZonedTimeAsync(false, cancellationToken).ConfigureAwait(false));

			case DbDataType.TimeTZEx:
				return DbValueStorage.FromZonedTime(await xdr.ReadZonedTimeAsync(true, cancellationToken).ConfigureAwait(false));

			case DbDataType.Dec16:
				{
					await xdr.ReadBytesAsync(_fixedBytes, 8, cancellationToken).ConfigureAwait(false);
					var storage = new DbValueStorage { Kind = DbValueKind.Dec16, Object = null };
					var dst = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref storage.Data, 1));
					_fixedBytes.AsSpan(0, 8).CopyTo(dst);
					return storage;
				}

			case DbDataType.Dec34:
				{
					await xdr.ReadBytesAsync(_fixedBytes, 16, cancellationToken).ConfigureAwait(false);
					var storage = new DbValueStorage { Kind = DbValueKind.Dec34, Object = null };
					var dst = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref storage.Data, 1));
					_fixedBytes.CopyTo(dst);
					return storage;
				}

			case DbDataType.Int128:
				{
					await xdr.ReadBytesAsync(_fixedBytes, 16, cancellationToken).ConfigureAwait(false);
					var storage = new DbValueStorage { Kind = DbValueKind.Int128, Object = null };
					var dst = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref storage.Data, 1));
					_fixedBytes.CopyTo(dst);
					return storage;
				}

			default:
				throw TypeHelper.InvalidDataType((int)field.DbDataType);
		}
	}

	protected void Clear()
	{
		if (_rows != null && _rows.Count > 0)
		{
			while (_rows.Count > 0)
			{
				ReturnRowStorage(_rows.Dequeue());
			}
		}
		if (OutputParameters != null && OutputParameters.Count > 0)
		{
			OutputParameters.Clear();
		}

		_allRowsFetched = false;
	}

	protected void ClearAll()
	{
		Clear();

		_reusableRow = null;
		_rowStoragePool?.Clear();

		_parameters = null;
		_fields = null;
		_parametersBlr = null;
		_fieldsBlr = null;
	}

	private Descriptor.BlrData GetParametersBlr()
	{
		if (_parameters == null)
		{
			return null;
		}

		if (_parametersBlr == null)
		{
			_parametersBlr = _parameters.ToBlr();
		}

		return _parametersBlr;
	}

	private Descriptor.BlrData GetFieldsBlr()
	{
		if (_fields == null)
		{
			return null;
		}

		if (_fieldsBlr == null)
		{
			_fieldsBlr = _fields.ToBlr();
		}

		return _fieldsBlr;
	}

	void EnsureReusableRow()
	{
		if (_fields == null || _fields.Count == 0)
		{
			_reusableRow = Array.Empty<DbValue>();
			return;
		}

		if (_reusableRow != null && _reusableRow.Length == _fields.Count)
		{
			return;
		}

		_reusableRow = new DbValue[_fields.Count];
		for (var i = 0; i < _fields.Count; i++)
		{
			_reusableRow[i] = new DbValue(this, _fields[i], null);
		}
	}

	protected DbValueStorage[] RentRowStorage()
	{
		if (_fields == null || _fields.Count == 0)
		{
			return Array.Empty<DbValueStorage>();
		}

		return _rowStoragePool.Count != 0 ? _rowStoragePool.Pop() : new DbValueStorage[_fields.Count];
	}

	protected void ReturnRowStorage(DbValueStorage[] row)
	{
		if (row == null || row.Length == 0 || _rowStoragePool == null)
			return;

		for (var i = 0; i < row.Length; i++)
		{
			row[i].Object = null;
		}
		_rowStoragePool.Push(row);
	}

	DbValue[] MaterializeRow(DbValueStorage[] rowValues)
	{
		EnsureReusableRow();

		for (var i = 0; i < _reusableRow.Length; i++)
		{
			_reusableRow[i].ImportStorage(rowValues[i]);
		}

		ReturnRowStorage(rowValues);
		return _reusableRow;
	}

	protected virtual DbValueStorage[] ReadRowStorage()
	{
		var row = RentRowStorage();
		try
		{
			for (var i = 0; i < _fields.Count; i++)
			{
				var value = ReadRawValueStorage(_database.Xdr, _fields[i]);
				var sqlInd = _database.Xdr.ReadInt32();
				if (sqlInd == -1)
				{
					row[i] = default;
				}
				else if (sqlInd == 0)
				{
					row[i] = value;
				}
				else
				{
					throw IscException.ForStrParam($"Invalid {nameof(sqlInd)} value: {sqlInd}.");
				}
			}
		}
		catch (IOException ex)
		{
			ReturnRowStorage(row);
			throw IscException.ForIOException(ex);
		}
		return row;
	}
	protected virtual async ValueTask<DbValueStorage[]> ReadRowStorageAsync(CancellationToken cancellationToken = default)
	{
		var row = RentRowStorage();
		try
		{
			for (var i = 0; i < _fields.Count; i++)
			{
				var value = await ReadRawValueStorageAsync(_database.Xdr, _fields[i], cancellationToken).ConfigureAwait(false);
				var sqlInd = await _database.Xdr.ReadInt32Async(cancellationToken).ConfigureAwait(false);
				if (sqlInd == -1)
				{
					row[i] = default;
				}
				else if (sqlInd == 0)
				{
					row[i] = value;
				}
				else
				{
					throw IscException.ForStrParam($"Invalid {nameof(sqlInd)} value: {sqlInd}.");
				}
			}
		}
		catch (IOException ex)
		{
			ReturnRowStorage(row);
			throw IscException.ForIOException(ex);
		}
		return row;
	}

	protected virtual DbValue[] ReadRow()
	{
		var row = _fields.Count > 0 ? new DbValue[_fields.Count] : Array.Empty<DbValue>();
		try
		{
			for (var i = 0; i < _fields.Count; i++)
			{
				var value = ReadRawValue(_database.Xdr, _fields[i]);
				var sqlInd = _database.Xdr.ReadInt32();
				if (sqlInd == -1)
				{
					row[i] = new DbValue(this, _fields[i], null);
				}
				else if (sqlInd == 0)
				{
					row[i] = new DbValue(this, _fields[i], value);
				}
				else
				{
					throw IscException.ForStrParam($"Invalid {nameof(sqlInd)} value: {sqlInd}.");
				}
			}
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
		return row;
	}
	protected virtual async ValueTask<DbValue[]> ReadRowAsync(CancellationToken cancellationToken = default)
	{
		var row = _fields.Count > 0 ? new DbValue[_fields.Count] : Array.Empty<DbValue>();
		try
		{
			for (var i = 0; i < _fields.Count; i++)
			{
				var value = await ReadRawValueAsync(_database.Xdr, _fields[i], cancellationToken).ConfigureAwait(false);
				var sqlInd = await _database.Xdr.ReadInt32Async(cancellationToken).ConfigureAwait(false);
				if (sqlInd == -1)
				{
					row[i] = new DbValue(this, _fields[i], null);
				}
				else if (sqlInd == 0)
				{
					row[i] = new DbValue(this, _fields[i], value);
				}
				else
				{
					throw IscException.ForStrParam($"Invalid {nameof(sqlInd)} value: {sqlInd}.");
				}
			}
		}
		catch (IOException ex)
		{
			throw IscException.ForIOException(ex);
		}
		return row;
	}

	private static string TruncateStringByRuneCount(string s, DbField field)
	{
		if ((field.Length % field.Charset.BytesPerCharacter) != 0)
		{
			return s;
		}

		var runeCount = s.CountRunes();
		if (runeCount <= field.CharCount)
		{
			return s;
		}

		return new string(s.TruncateStringToRuneCount(field.CharCount));
	}

	#endregion

	#region Protected Internal Methods

	#endregion
}
