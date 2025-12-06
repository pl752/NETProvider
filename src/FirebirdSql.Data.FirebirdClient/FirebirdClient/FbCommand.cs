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
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FirebirdSql.Data.Common;
using FirebirdSql.Data.Logging;

namespace FirebirdSql.Data.FirebirdClient;

public sealed class FbCommand : DbCommand, IFbPreparedCommand, IDescriptorFiller, ICloneable {
		static readonly IFbLogger Log = FbLogManager.CreateLogger(nameof(FbCommand));

		#region Fields

		private CommandType _commandType;
		private UpdateRowSource _updatedRowSource;
		private FbConnection _connection;
		private FbTransaction _transaction;
		private FbParameterCollection _parameters;
		private StatementBase _statement;
		private FbDataReader _activeReader;
		private IReadOnlyList<string> _namedParameters;
		private string _commandText;
		private bool _disposed;
		private bool _designTimeVisible;
		private bool _implicitTransaction;
		private int? _commandTimeout;
		private int _fetchSize;
		private Type[] _expectedColumnTypes;

		#endregion

		#region Properties

		[Category("Data")]
		[DefaultValue("")]
		[RefreshProperties(RefreshProperties.All)]
		public override string CommandText {
				get => _commandText;
				set {
						if(_commandText != value && _statement != null) {
								Release();
						}

						_commandText = value;
				}
		}

		[Category("Data")]
		[DefaultValue(CommandType.Text)]
		[RefreshProperties(RefreshProperties.All)]
		public override CommandType CommandType { get => _commandType; set => _commandType = value;
		}

		[Category("Behavior")]
		[DefaultValue(ConnectionString.DefaultValueCommandTimeout)]
		public override int CommandTimeout {
				get => _commandTimeout != null
								? (int)_commandTimeout
								: _connection?.CommandTimeout >= 0 ? (int)_connection?.CommandTimeout : ConnectionString.DefaultValueCommandTimeout;
				set {
						if(value < 0) {
								throw new ArgumentException("The property value assigned is less than 0.");
						}

						_commandTimeout = value;
				}
		}

		[Category("Behavior")]
		[DefaultValue(null)]
		public new FbConnection Connection {
				get => _connection;
				set {
						if(_activeReader != null) {
								throw new InvalidOperationException("There is already an open DataReader associated with this Command which must be closed first.");
						}

						if(_transaction != null && _transaction.IsCompleted) {
								_transaction = null;
						}

						if(_connection != null &&
							_connection != value &&
							_connection.State == ConnectionState.Open) {
								Release();
						}

						_connection = value;
				}
		}

		[Category("Data")]
		[DesignerSerializationVisibility(DesignerSerializationVisibility.Content)]
		public new FbParameterCollection Parameters {
				get {
						_parameters ??= [];
						return _parameters;
				}
		}

		[Browsable(false)]
		[DesignerSerializationVisibility(DesignerSerializationVisibility.Hidden)]
		public new FbTransaction Transaction {
				get => _implicitTransaction ? null : _transaction;
				set {
						if(_activeReader != null) {
								throw new InvalidOperationException("There is already an open DataReader associated with this Command which must be closed first.");
						}

						RollbackImplicitTransaction();

						_transaction = value;

						_ = (_statement?.Transaction = _transaction?.Transaction);
				}
		}

		[Category("Behavior")]
		[DefaultValue(UpdateRowSource.Both)]
		public override UpdateRowSource UpdatedRowSource { get => _updatedRowSource; set => _updatedRowSource = value;
		}

		[Category("Behavior")]
		[DefaultValue(200)]
		public int FetchSize {
				get => _fetchSize;
				set {
						if(_activeReader != null) {
								throw new InvalidOperationException("There is already an open DataReader associated with this Command which must be closed first.");
						}
						_fetchSize = value;
				}
		}

		/// <summary>
		/// Gets collection of parameters parsed from the query text during command prepare.
		/// </summary>
		[Browsable(false)]
		[DesignerSerializationVisibility(DesignerSerializationVisibility.Hidden)]
		public IReadOnlyList<string> NamedParameters => _namedParameters;

		#endregion

		#region Protected DbCommand Properties

		protected override DbConnection DbConnection { get => Connection; set => Connection = (FbConnection)value;
		}

		protected override DbTransaction DbTransaction { get => Transaction; set => Transaction = (FbTransaction)value;
		}

		protected override DbParameterCollection DbParameterCollection => Parameters;

		#endregion

		#region Design-Time properties

		[Browsable(false)]
		[DesignOnly(true)]
		[DefaultValue(true)]
		public override bool DesignTimeVisible {
				get => _designTimeVisible;
				set {
						_designTimeVisible = value;
						TypeDescriptor.Refresh(this);
				}
		}

		#endregion

		#region Internal Properties

		internal int RecordsAffected => _statement != null && CommandType != CommandType.StoredProcedure ? _statement.RecordsAffected : -1;

		internal bool IsDisposed => _disposed;

		internal FbDataReader ActiveReader { get => _activeReader; set => _activeReader = value;
		}

		internal FbTransaction ActiveTransaction => _transaction;

		internal bool HasImplicitTransaction => _implicitTransaction;

		internal bool HasFields => _statement?.Fields?.Count > 0;

		internal bool HasParameters => _parameters != null && _parameters.Count > 0;

		internal bool IsDDLCommand => _statement?.StatementType == DbStatementType.DDL;

		internal Type[] ExpectedColumnTypes => _expectedColumnTypes;

		#endregion

		#region Constructors

		public FbCommand()
			: this(null, null, null) { }

		public FbCommand(string cmdText)
			: this(cmdText, null, null) { }

		public FbCommand(string cmdText, FbConnection connection)
			: this(cmdText, connection, null) { }

		public FbCommand(string cmdText, FbConnection connection, FbTransaction transaction) {
				_namedParameters = Array.Empty<string>();
				_updatedRowSource = UpdateRowSource.Both;
				_commandType = CommandType.Text;
				_designTimeVisible = true;
				_commandTimeout = null;
				_fetchSize = 200;
				_commandText = string.Empty;

				if(connection != null) {
						_fetchSize = connection.ConnectionOptions.FetchSize;
				}

				if(cmdText != null) {
						CommandText = cmdText;
				}

				Connection = connection;
				_transaction = transaction;
		}

		public static FbCommand CreateWithTypeCoercions(Type[] expectedColumnTypes) {
				var result = new FbCommand {
						_expectedColumnTypes = expectedColumnTypes
				};
				return result;
		}

		#endregion

		#region IDisposable, IAsyncDisposable methods

		protected override void Dispose(bool disposing) {
				if(disposing) {
						if(!_disposed) {
								_disposed = true;
								try {
										Release();
								}
								catch(IscException ex) {
										throw FbException.Create(ex);
								}
								_commandTimeout = null;
								_fetchSize = 0;
								_implicitTransaction = false;
								_commandText = null;
								_connection = null;
								_transaction = null;
								_parameters = null;
								_statement = null;
								_activeReader = null;
								_namedParameters = null;
						}
				}
				base.Dispose(disposing);
		}
#if !(NET48 || NETSTANDARD2_0)
		public override async ValueTask DisposeAsync() {
				if(!_disposed) {
						_disposed = true;
						try {
								await ReleaseAsync().ConfigureAwait(false);
						}
						catch(IscException ex) {
								throw FbException.Create(ex);
						}
						_commandTimeout = 0;
						_fetchSize = 0;
						_implicitTransaction = false;
						_commandText = null;
						_connection = null;
						_transaction = null;
						_parameters = null;
						_statement = null;
						_activeReader = null;
						_namedParameters = null;
				}
				await base.DisposeAsync().ConfigureAwait(false);
		}
#endif

		#endregion

		#region ICloneable Methods

		object ICloneable.Clone() {
				var command = new FbCommand {
						CommandText = CommandText,
						Connection = Connection,
						Transaction = Transaction,
						CommandType = CommandType,
						UpdatedRowSource = UpdatedRowSource,
						CommandTimeout = CommandTimeout,
						FetchSize = FetchSize
				};
				command.UpdatedRowSource = UpdatedRowSource;

				if(_expectedColumnTypes != null) {
						command._expectedColumnTypes = (Type[])_expectedColumnTypes.Clone();
				}

				for(int i = 0; i < Parameters.Count; i++) {
						_ = command.Parameters.Add(((ICloneable)Parameters[i]).Clone());
				}

				return command;
		}

		#endregion

		#region Methods

		public override void Cancel() => _connection.CancelCommand();

		public new static FbParameter CreateParameter() => new FbParameter();

		public override void Prepare() {
				CheckCommand();

				using(var explicitCancellation = ExplicitCancellation.Enter(CancellationToken.None, Cancel)) {
						try {
								Prepare(false);
						}
						catch(IscException ex) {
								RollbackImplicitTransaction();
								throw FbException.Create(ex);
						}
						catch {
								RollbackImplicitTransaction();
								throw;
						}
				}
		}
#if NET48 || NETSTANDARD2_0
	public async Task PrepareAsync(CancellationToken cancellationToken = default)
#else
		public override async Task PrepareAsync(CancellationToken cancellationToken = default)
#endif
		{
				CheckCommand();

				using(var explicitCancellation = ExplicitCancellation.Enter(cancellationToken, Cancel)) {
						try {
								await PrepareAsync(false, ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);
						}
						catch(IscException ex) {
								await RollbackImplicitTransactionAsync(ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);
								throw FbException.Create(ex);
						}
						catch {
								await RollbackImplicitTransactionAsync(ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);
								throw;
						}
				}
		}

		public override int ExecuteNonQuery() {
				CheckCommand();

				using(var explicitCancellation = ExplicitCancellation.Enter(CancellationToken.None, Cancel)) {
						try {
								ExecuteCommand(CommandBehavior.Default, false);

								if(_statement.StatementType == DbStatementType.StoredProcedure) {
										SetOutputParameters();
								}

								CommitImplicitTransaction();
						}
						catch(IscException ex) {
								RollbackImplicitTransaction();
								throw FbException.Create(ex);
						}
						catch {
								RollbackImplicitTransaction();
								throw;
						}
				}

				return _statement.StatementType switch {
						DbStatementType.Insert => RecordsAffected,
						DbStatementType.Update => RecordsAffected,
						DbStatementType.Delete => RecordsAffected,
						_ => -1,
				};
		}
		public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) {
				CheckCommand();

				using(var explicitCancellation = ExplicitCancellation.Enter(cancellationToken, Cancel)) {
						try {
								await ExecuteCommandAsync(CommandBehavior.Default, false, ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);

								if(_statement.StatementType == DbStatementType.StoredProcedure) {
										await SetOutputParametersAsync(ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);
								}

								await CommitImplicitTransactionAsync(ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);
						}
						catch(IscException ex) {
								await RollbackImplicitTransactionAsync(ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);
								throw FbException.Create(ex);
						}
						catch {
								await RollbackImplicitTransactionAsync(ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);
								throw;
						}
				}

				return _statement.StatementType switch {
						DbStatementType.Insert => RecordsAffected,
						DbStatementType.Update => RecordsAffected,
						DbStatementType.Delete => RecordsAffected,
						_ => -1,
				};
		}

		public new FbDataReader ExecuteReader() => ExecuteReader(CommandBehavior.Default);
		public new Task<FbDataReader> ExecuteReaderAsync() => ExecuteReaderAsync(CommandBehavior.Default);
		public new Task<FbDataReader> ExecuteReaderAsync(CancellationToken cancellationToken) => ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);

		public new FbDataReader ExecuteReader(CommandBehavior behavior) {
				CheckCommand();

				using(var explicitCancellation = ExplicitCancellation.Enter(CancellationToken.None, Cancel)) {
						try {
								ExecuteCommand(behavior, true);
						}
						catch(IscException ex) {
								RollbackImplicitTransaction();
								throw FbException.Create(ex);
						}
						catch {
								RollbackImplicitTransaction();
								throw;
						}
				}

				_activeReader = new FbDataReader(this, _connection, behavior);
				return _activeReader;
		}
		public new Task<FbDataReader> ExecuteReaderAsync(CommandBehavior behavior) => ExecuteReaderAsync(behavior, CancellationToken.None);
		public new async Task<FbDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) {
				CheckCommand();

				using(var explicitCancellation = ExplicitCancellation.Enter(cancellationToken, Cancel)) {
						try {
								await ExecuteCommandAsync(behavior, true, ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);
						}
						catch(IscException ex) {
								await RollbackImplicitTransactionAsync(ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);
								throw FbException.Create(ex);
						}
						catch {
								await RollbackImplicitTransactionAsync(ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);
								throw;
						}
				}

				_activeReader = new FbDataReader(this, _connection, behavior);
				return _activeReader;
		}

		public override object ExecuteScalar() {
				DbValue[] values = null;
				object val = null;

				CheckCommand();

				using(var explicitCancellation = ExplicitCancellation.Enter(CancellationToken.None, Cancel)) {
						try {
								ExecuteCommand(CommandBehavior.Default, false);

								// Gets	only the values	of the first row or
								// the output parameters values if command is an Stored Procedure
								if(_statement.StatementType == DbStatementType.StoredProcedure) {
										values = _statement.GetOutputParameters();
										SetOutputParameters(values);
								}
								else {
										values = _statement.Fetch();
								}

								// Get the return value
								if(values != null && values.Length > 0) {
										val = values[0].GetValue();
								}

								CommitImplicitTransaction();
						}
						catch(IscException ex) {
								RollbackImplicitTransaction();
								throw FbException.Create(ex);
						}
						catch {
								RollbackImplicitTransaction();
								throw;
						}
				}

				return val;
		}
		public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken) {
				DbValue[] values = null;
				object val = null;

				CheckCommand();

				using(var explicitCancellation = ExplicitCancellation.Enter(cancellationToken, Cancel)) {
						try {
								await ExecuteCommandAsync(CommandBehavior.Default, false, ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);

								// Gets	only the values	of the first row or
								// the output parameters values if command is an Stored Procedure
								if(_statement.StatementType == DbStatementType.StoredProcedure) {
										values = _statement.GetOutputParameters();
										await SetOutputParametersAsync(values, ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);
								}
								else {
										values = await _statement.FetchAsync(ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);
								}

								// Get the return value
								if(values != null && values.Length > 0) {
										val = await values[0].GetValueAsync(ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);
								}

								await CommitImplicitTransactionAsync(ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);
						}
						catch(IscException ex) {
								await RollbackImplicitTransactionAsync(ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);
								throw FbException.Create(ex);
						}
						catch {
								await RollbackImplicitTransactionAsync(ExplicitCancellation.ExplicitCancel.CancellationToken).ConfigureAwait(false);
								throw;
						}
				}

				return val;
		}

		public string GetCommandPlan() => _statement?.GetExecutionPlan();
		public Task<string> GetCommandPlanAsync(CancellationToken cancellationToken = default) => _statement == null ? Task.FromResult<string>(null) : _statement.GetExecutionPlanAsync(cancellationToken).AsTask();

		public string GetCommandExplainedPlan() => _statement?.GetExecutionExplainedPlan();
		public Task<string> GetCommandExplainedPlanAsync(CancellationToken cancellationToken = default) => _statement == null ? Task.FromResult<string>(null) : _statement.GetExecutionExplainedPlanAsync(cancellationToken).AsTask();

		#endregion

		#region DbCommand Protected Methods

		protected override DbParameter CreateDbParameter() => CreateParameter();

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => ExecuteReader(behavior);
		protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) => await ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);

		#endregion

		#region Internal Methods

		internal void DisposeReader() {
				if(_activeReader != null) {
#if NET48 || NETSTANDARD2_0
			_activeReader.Dispose();
#else
						_activeReader.Dispose();
#endif
				_activeReader = null;
		}
		}
		internal async Task DisposeReaderAsync(CancellationToken cancellationToken = default) {
				if(_activeReader != null) {
#if NET48 || NETSTANDARD2_0
			_activeReader.Dispose();
			await Task.CompletedTask.ConfigureAwait(false);
#else
						await _activeReader.DisposeAsync().ConfigureAwait(false);
#endif
						_activeReader = null;
				}
		}

		internal DbValue[] Fetch() {
				if(_statement != null) {
						try {
								return _statement.Fetch();
						}
						catch(IscException ex) {
								throw FbException.Create(ex);
						}
				}
				return null;
		}
		internal async Task<DbValue[]> FetchAsync(CancellationToken cancellationToken = default) {
				if(_statement != null) {
						try {
								return await _statement.FetchAsync(cancellationToken).ConfigureAwait(false);
						}
						catch(IscException ex) {
								throw FbException.Create(ex);
						}
				}
				return null;
		}

		internal Descriptor GetFieldsDescriptor() => _statement?.Fields;

		internal void SetOutputParameters() => SetOutputParameters(null);
		internal Task SetOutputParametersAsync(CancellationToken cancellationToken = default) => SetOutputParametersAsync(null, cancellationToken);

		internal void SetOutputParameters(DbValue[] outputParameterValues) {
				if(Parameters.Count > 0 && _statement != null) {
						if(_statement != null &&
							_statement.StatementType == DbStatementType.StoredProcedure) {
								var values = outputParameterValues;
								if(outputParameterValues == null) {
										values = _statement.GetOutputParameters();
								}

								if(values != null && values.Length > 0) {
										int i = 0;
										foreach(FbParameter parameter in Parameters) {
												if(parameter.Direction is ParameterDirection.Output or
													ParameterDirection.InputOutput or
													ParameterDirection.ReturnValue) {
														parameter.Value = values[i].GetValue();
														i++;
												}
										}
								}
						}
				}
		}
		internal async Task SetOutputParametersAsync(DbValue[] outputParameterValues, CancellationToken cancellationToken = default) {
				if(Parameters.Count > 0 && _statement != null) {
						if(_statement != null &&
							_statement.StatementType == DbStatementType.StoredProcedure) {
								var values = outputParameterValues;
								if(outputParameterValues == null) {
										values = _statement.GetOutputParameters();
								}

								if(values != null && values.Length > 0) {
										int i = 0;
										foreach(FbParameter parameter in Parameters) {
												if(parameter.Direction is ParameterDirection.Output or
													ParameterDirection.InputOutput or
													ParameterDirection.ReturnValue) {
														parameter.Value = await values[i].GetValueAsync(cancellationToken).ConfigureAwait(false);
														i++;
												}
										}
								}
						}
				}
		}

		internal void CommitImplicitTransaction() {
				if(HasImplicitTransaction && _transaction != null && _transaction.Transaction != null) {
						try {
								_transaction.Commit();
						}
						catch {
								RollbackImplicitTransaction();

								throw;
						}
						finally {
								if(_transaction != null) {
#if NET48 || NETSTANDARD2_0
					_transaction.Dispose();
#else
										_transaction.Dispose();
#endif
										_transaction = null;
										_implicitTransaction = false;
								}

								_statement?.Transaction = null;
						}
				}
		}
		internal async Task CommitImplicitTransactionAsync(CancellationToken cancellationToken = default) {
				if(HasImplicitTransaction && _transaction != null && _transaction.Transaction != null) {
						try {
								await _transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
						}
						catch {
								await RollbackImplicitTransactionAsync(cancellationToken).ConfigureAwait(false);

								throw;
						}
						finally {
								if(_transaction != null) {
#if NET48 || NETSTANDARD2_0
					_transaction.Dispose();
#else
										await _transaction.DisposeAsync().ConfigureAwait(false);
#endif
										_transaction = null;
										_implicitTransaction = false;
								}

								_statement?.Transaction = null;
						}
				}
		}

		internal void RollbackImplicitTransaction() {
				if(HasImplicitTransaction && _transaction != null && _transaction.Transaction != null) {
						int transactionCount = Connection.InnerConnection.Database.TransactionCount;

						try {
								_transaction.Rollback();
						}
						catch {
								if(Connection.InnerConnection.Database.TransactionCount == transactionCount) {
										Connection.InnerConnection.Database.TransactionCount--;
								}
						}
						finally {
#if NET48 || NETSTANDARD2_0
				_transaction.Dispose();
#else
								_transaction.Dispose();
#endif
								_transaction = null;
								_implicitTransaction = false;

								_statement?.Transaction = null;
						}
				}
		}
		internal async Task RollbackImplicitTransactionAsync(CancellationToken cancellationToken = default) {
				if(HasImplicitTransaction && _transaction != null && _transaction.Transaction != null) {
						int transactionCount = Connection.InnerConnection.Database.TransactionCount;

						try {
								await _transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
						}
						catch {
								if(Connection.InnerConnection.Database.TransactionCount == transactionCount) {
										Connection.InnerConnection.Database.TransactionCount--;
								}
						}
						finally {
#if NET48 || NETSTANDARD2_0
				_transaction.Dispose();
#else
								await _transaction.DisposeAsync().ConfigureAwait(false);
#endif
								_transaction = null;
								_implicitTransaction = false;

								_statement?.Transaction = null;
						}
				}
		}

		internal void Close() => _statement?.Close();
		internal Task CloseAsync(CancellationToken cancellationToken = default) => _statement != null ? _statement.CloseAsync(cancellationToken).AsTask() : Task.CompletedTask;

		void IFbPreparedCommand.Release() => Release();
		internal void Release() {
				RollbackImplicitTransaction();

				DisposeReader();

				if(_connection != null && _connection.State == ConnectionState.Open) {
						_connection.InnerConnection.RemovePreparedCommand(this);
				}

				_statement?.Dispose2();
				_statement = null;
		}
		Task IFbPreparedCommand.ReleaseAsync(CancellationToken cancellationToken) => ReleaseAsync(cancellationToken);
		internal async Task ReleaseAsync(CancellationToken cancellationToken = default) {
				await RollbackImplicitTransactionAsync(cancellationToken).ConfigureAwait(false);

				await DisposeReaderAsync(cancellationToken).ConfigureAwait(false);

				if(_connection != null && _connection.State == ConnectionState.Open) {
						_connection.InnerConnection.RemovePreparedCommand(this);
				}

				if(_statement != null) {
						await _statement.Dispose2Async(cancellationToken).ConfigureAwait(false);
						_statement = null;
				}
		}

		void IFbPreparedCommand.TransactionCompleted() => TransactionCompleted();
		internal void TransactionCompleted() {
				if(Transaction != null) {
						DisposeReader();
						Transaction = null;
				}
		}
		Task IFbPreparedCommand.TransactionCompletedAsync(CancellationToken cancellationToken) => TransactionCompletedAsync(cancellationToken);
		internal async Task TransactionCompletedAsync(CancellationToken cancellationToken = default) {
				if(Transaction != null) {
						await DisposeReaderAsync(cancellationToken).ConfigureAwait(false);
						Transaction = null;
				}
		}

		#endregion

		#region IDescriptorFiller

		void IDescriptorFiller.Fill(Descriptor descriptor, int index) => UpdateParameterValues(descriptor);
		private void UpdateParameterValues(Descriptor descriptor) {
				if(!HasParameters)
						return;

				for(int i = 0; i < descriptor.Count; i++) {
						var parameter = descriptor[i];
						int index = i;

						if(_namedParameters.Count > 0) {
								index = _parameters.IndexOf(_namedParameters[i], i);
								if(index == -1) {
										throw FbException.Create($"Must declare the variable '{_namedParameters[i]}'.");
								}
						}

						if(index != -1) {
								var commandParameter = _parameters[index];
								if(commandParameter.InternalValue == DBNull.Value || commandParameter.InternalValue == null) {
										parameter.NullFlag = -1;
										parameter.DbValue.SetValue(DBNull.Value);

										if(!parameter.AllowDBNull()) {
												parameter.DataType++;
										}
								}
								else {
										parameter.NullFlag = 0;

										switch(parameter.DbDataType) {
												case DbDataType.Binary: {
																var blob = _statement.CreateBlob();
																blob.Write((byte[])commandParameter.InternalValue);
																parameter.DbValue.SetValue(blob.Id);
														}
														break;

												case DbDataType.Text: {
																var blob = _statement.CreateBlob();
																if(commandParameter.InternalValue is byte[] v) {
																		blob.Write(v);
																}
																else {
																		blob.Write((string)commandParameter.InternalValue);
																}
																parameter.DbValue.SetValue(blob.Id);
														}
														break;

												case DbDataType.Array: {
																if(parameter.ArrayHandle == null) {
																		parameter.ArrayHandle = _statement.CreateArray(parameter.Relation, parameter.Name);
																}
																else {
																		parameter.ArrayHandle.Database = _statement.Database;
																		parameter.ArrayHandle.Transaction = _statement.Transaction;
																}

																parameter.ArrayHandle.Handle = 0;
																parameter.ArrayHandle.Write((Array)commandParameter.InternalValue);
																parameter.DbValue.SetValue(parameter.ArrayHandle.Handle);
														}
														break;

												case DbDataType.Guid:
														if(commandParameter.InternalValue is not Guid and not byte[]) {
																throw new InvalidOperationException("Incorrect Guid value.");
														}
														parameter.DbValue.SetValue(commandParameter.InternalValue);
														break;

												default:
														parameter.DbValue.SetValue(commandParameter.InternalValue);
														break;
										}
								}
						}
				}
		}
		ValueTask IDescriptorFiller.FillAsync(Descriptor descriptor, int index, CancellationToken cancellationToken) => UpdateParameterValuesAsync(descriptor, cancellationToken);
		private async ValueTask UpdateParameterValuesAsync(Descriptor descriptor, CancellationToken cancellationToken = default) {
				if(!HasParameters)
						return;

				for(int i = 0; i < descriptor.Count; i++) {
						var statementParameter = descriptor[i];
						int index = i;

						if(_namedParameters.Count > 0) {
								index = _parameters.IndexOf(_namedParameters[i], i);
								if(index == -1) {
										throw FbException.Create($"Must declare the variable '{_namedParameters[i]}'.");
								}
						}

						if(index != -1) {
								var commandParameter = _parameters[index];
								if(commandParameter.InternalValue == DBNull.Value || commandParameter.InternalValue == null) {
										statementParameter.NullFlag = -1;
										statementParameter.DbValue.SetValue(DBNull.Value);

										if(!statementParameter.AllowDBNull()) {
												statementParameter.DataType++;
										}
								}
								else {
										statementParameter.NullFlag = 0;

										switch(statementParameter.DbDataType) {
												case DbDataType.Binary: {
																var blob = _statement.CreateBlob();
																await blob.WriteAsync((byte[])commandParameter.InternalValue, cancellationToken).ConfigureAwait(false);
																statementParameter.DbValue.SetValue(blob.Id);
														}
														break;

												case DbDataType.Text: {
																var blob = _statement.CreateBlob();
																if(commandParameter.InternalValue is byte[] v) {
																		await blob.WriteAsync(v, cancellationToken).ConfigureAwait(false);
																}
																else {
																		await blob.WriteAsync((string)commandParameter.InternalValue, cancellationToken).ConfigureAwait(false);
																}
																statementParameter.DbValue.SetValue(blob.Id);
														}
														break;

												case DbDataType.Array: {
																if(statementParameter.ArrayHandle == null) {
																		statementParameter.ArrayHandle = await _statement.CreateArrayAsync(statementParameter.Relation, statementParameter.Name, cancellationToken).ConfigureAwait(false);
																}
																else {
																		statementParameter.ArrayHandle.Database = _statement.Database;
																		statementParameter.ArrayHandle.Transaction = _statement.Transaction;
																}

																statementParameter.ArrayHandle.Handle = 0;
																await statementParameter.ArrayHandle.WriteAsync((Array)commandParameter.InternalValue, cancellationToken).ConfigureAwait(false);
																statementParameter.DbValue.SetValue(statementParameter.ArrayHandle.Handle);
														}
														break;

												case DbDataType.Guid:
														if(commandParameter.InternalValue is not Guid and not byte[]) {
																throw new InvalidOperationException("Incorrect Guid value.");
														}
														statementParameter.DbValue.SetValue(commandParameter.InternalValue);
														break;

												default:
														statementParameter.DbValue.SetValue(commandParameter.InternalValue);
														break;
										}
								}
						}
				}
		}

		#endregion

		#region Private Methods

		private void Prepare(bool returnsSet) {
				var innerConn = _connection.InnerConnection;

				// Check if	we have	a valid	transaction
				if(_transaction == null) {
						if(innerConn.IsEnlisted) {
								_transaction = innerConn.ActiveTransaction;
						}
						else {
								_implicitTransaction = true;
								_transaction = new FbTransaction(_connection, _connection.ConnectionOptions.IsolationLevel);
								_transaction.BeginTransaction();

								// Update Statement	transaction
								_ = (_statement?.Transaction = _transaction.Transaction);
						}
				}

				// Check if	we have	a valid	statement handle
				_statement ??= innerConn.Database.CreateStatement(_transaction.Transaction);

				// Prepare the statement if	needed
				if(!_statement.IsPrepared) {
						// Close the inner DataReader if needed
						DisposeReader();

						// Reformat the SQL statement if needed
						string sql = _commandText;

						if(_commandType == CommandType.StoredProcedure) {
								sql = BuildStoredProcedureSql(sql, returnsSet);
						}

						try {
								(sql, _namedParameters) = NamedParametersParser.Parse(sql);
								// Try to prepare the command
								_statement.Prepare(sql);
						}
						catch {
								// Release the statement and rethrow the exception
								_statement.Release();
								_statement = null;

								throw;
						}

						// Add this	command	to the active command list
						innerConn.AddPreparedCommand(this);
				}
				else {
						// Close statement for subsequently	executions
						Close();
				}
		}
		private async Task PrepareAsync(bool returnsSet, CancellationToken cancellationToken = default) {
				var innerConn = _connection.InnerConnection;

				// Check if	we have	a valid	transaction
				if(_transaction == null) {
						if(innerConn.IsEnlisted) {
								_transaction = innerConn.ActiveTransaction;
						}
						else {
								_implicitTransaction = true;
								_transaction = new FbTransaction(_connection, _connection.ConnectionOptions.IsolationLevel);
								await _transaction.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

								// Update Statement	transaction
								_ = (_statement?.Transaction = _transaction.Transaction);
						}
				}

				// Check if	we have	a valid	statement handle
				_statement ??= innerConn.Database.CreateStatement(_transaction.Transaction);

				// Prepare the statement if	needed
				if(!_statement.IsPrepared) {
						// Close the inner DataReader if needed
						await DisposeReaderAsync(cancellationToken).ConfigureAwait(false);

						// Reformat the SQL statement if needed
						string sql = _commandText;

						if(_commandType == CommandType.StoredProcedure) {
								sql = BuildStoredProcedureSql(sql, returnsSet);
						}

						try {
								(sql, _namedParameters) = NamedParametersParser.Parse(sql);
								// Try to prepare the command
								await _statement.PrepareAsync(sql, cancellationToken).ConfigureAwait(false);
						}
						catch {
								// Release the statement and rethrow the exception
								await _statement.ReleaseAsync(cancellationToken).ConfigureAwait(false);
								_statement = null;

								throw;
						}

						// Add this	command	to the active command list
						innerConn.AddPreparedCommand(this);
				}
				else {
						// Close statement for subsequently	executions
						await CloseAsync(cancellationToken).ConfigureAwait(false);
				}
		}

		private void ExecuteCommand(CommandBehavior behavior, bool returnsSet) {
				LogMessages.CommandExecution(Log, this);

				Prepare(returnsSet);

				if((behavior & CommandBehavior.SequentialAccess) == CommandBehavior.SequentialAccess ||
					(behavior & CommandBehavior.SingleResult) == CommandBehavior.SingleResult ||
					(behavior & CommandBehavior.SingleRow) == CommandBehavior.SingleRow ||
					(behavior & CommandBehavior.CloseConnection) == CommandBehavior.CloseConnection ||
					behavior == CommandBehavior.Default) {
						// Set the fetch size
						_statement.FetchSize = _fetchSize;

						// Set if it's needed the Records Affected information
						_statement.ReturnRecordsAffected = _connection.ConnectionOptions.ReturnRecordsAffected;

						// Validate input parameter count
						if(_namedParameters.Count > 0 && !HasParameters) {
								throw FbException.Create("Must declare command parameters.");
						}

						// Execute
						_statement.Execute(CommandTimeout * 1000, this);
				}
		}
		private async Task ExecuteCommandAsync(CommandBehavior behavior, bool returnsSet, CancellationToken cancellationToken = default) {
				LogMessages.CommandExecution(Log, this);

				await PrepareAsync(returnsSet, cancellationToken).ConfigureAwait(false);

				if((behavior & CommandBehavior.SequentialAccess) == CommandBehavior.SequentialAccess ||
					(behavior & CommandBehavior.SingleResult) == CommandBehavior.SingleResult ||
					(behavior & CommandBehavior.SingleRow) == CommandBehavior.SingleRow ||
					(behavior & CommandBehavior.CloseConnection) == CommandBehavior.CloseConnection ||
					behavior == CommandBehavior.Default) {
						// Set the fetch size
						_statement.FetchSize = _fetchSize;

						// Set if it's needed the Records Affected information
						_statement.ReturnRecordsAffected = _connection.ConnectionOptions.ReturnRecordsAffected;

						// Validate input parameter count
						if(_namedParameters.Count > 0 && !HasParameters) {
								throw FbException.Create("Must declare command parameters.");
						}

						// Execute
						await _statement.ExecuteAsync(CommandTimeout * 1000, this, cancellationToken).ConfigureAwait(false);
				}
		}

		private string BuildStoredProcedureSql(string spName, bool returnsSet) {
				string sql = spName == null ? string.Empty : spName.Trim();

				if(sql.Length > 0 &&
					!sql.StartsWith("EXECUTE PROCEDURE ", StringComparison.InvariantCultureIgnoreCase) &&
					!sql.StartsWith("SELECT ", StringComparison.InvariantCultureIgnoreCase)) {
						var paramsText = new StringBuilder();

						// Append the stored proc parameter	name
						_ = paramsText.Append(sql);
						if(Parameters.Count > 0) {
								_ = paramsText.Append('(');
								for(int i = 0; i < Parameters.Count; i++) {
										if(Parameters[i].Direction is ParameterDirection.Input or
											ParameterDirection.InputOutput) {
												// Append parameter	name to parameter list
												_ = paramsText.Append(Parameters[i].InternalParameterName);
												if(i != Parameters.Count - 1) {
														paramsText = paramsText.Append(',');
												}
										}
								}
								_ = paramsText.Append(')');
								_ = paramsText.Replace(",)", ")");
								_ = paramsText.Replace("()", string.Empty);
						}

						sql = returnsSet ? "select * from " + paramsText.ToString() : "execute procedure " + paramsText.ToString();
				}

				return sql;
		}

		private void CheckCommand() {
				if(_transaction != null && _transaction.IsCompleted) {
						_transaction = null;
				}

				FbConnection.EnsureOpen(_connection);

				if(_activeReader != null) {
						throw new InvalidOperationException("There is already an open DataReader associated with this Command which must be closed first.");
				}

				if(_transaction == null &&
					_connection.InnerConnection.HasActiveTransaction &&
					!_connection.InnerConnection.IsEnlisted) {
						throw new InvalidOperationException("Execute requires the Command object to have a Transaction object when the Connection object assigned to the command is in a pending local transaction. The Transaction property of the Command has not been initialized.");
				}

				if(_transaction != null && !_transaction.IsCompleted &&
					!_connection.Equals(_transaction.Connection)) {
						throw new InvalidOperationException("Command Connection is not equal to Transaction Connection.");
				}

				if(_commandText == null || _commandText.Length == 0) {
						throw new InvalidOperationException("The command text for this Command has not been set.");
				}
		}

		#endregion
}
