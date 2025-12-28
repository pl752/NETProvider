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
using System.Buffers;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FirebirdSql.Data.Common;

namespace FirebirdSql.Data.Client.Managed.Version16;

internal class GdsBatch : BatchBase
{
	protected GdsStatement _statement;

	public override StatementBase Statement => _statement;

	public GdsDatabase Database => (GdsDatabase)_statement.Database;

	public GdsBatch(GdsStatement statement)
	{
		_statement = statement;
	}

	public override ExecuteResultItem[] Execute(int count, IDescriptorFiller descriptorFiller)
	{
		// this may throw error, so it needs to be before any writing
		var (buffer, lengths) = EncodeParametersData(count, descriptorFiller);

		try
		{
			Database.Xdr.Write(IscCodes.op_batch_create);
			Database.Xdr.Write(_statement.Handle); // p_batch_statement
			var blr = _statement.ParametersBlr;
			Database.Xdr.WriteBuffer(blr.Data); // p_batch_blr
			Database.Xdr.Write(blr.Length); // p_batch_msglen
			var pb = _statement.CreateBatchParameterBuffer();
			if (_statement.ReturnRecordsAffected)
			{
				pb.Append(IscCodes.Batch.TAG_RECORD_COUNTS, 1);
			}
			if (MultiError)
			{
				pb.Append(IscCodes.Batch.TAG_MULTIERROR, 1);
			}
			pb.Append(IscCodes.Batch.TAG_BUFFER_BYTES_SIZE, BatchBufferSize);
			Database.Xdr.WriteBuffer(pb.ToArray()); // p_batch_pb

			Database.Xdr.Write(IscCodes.op_batch_msg);
			Database.Xdr.Write(_statement.Handle); // p_batch_statement
			Database.Xdr.Write(count); // p_batch_messages

			var offset = 0;
			for (var i = 0; i < count; i++)
			{
				var length = lengths[i];
				Database.Xdr.WriteOpaque(buffer.AsSpan(offset, length), length); // p_batch_data
				offset += length;
			}

			Database.Xdr.Write(IscCodes.op_batch_exec);
			Database.Xdr.Write(_statement.Handle); // p_batch_statement
			Database.Xdr.Write(_statement.Transaction.Handle); // p_batch_transaction;		

			Database.Xdr.Flush();

			var numberOfResponses = 3;
			try
			{
				numberOfResponses--;
				var batchCreateResponse = Database.ReadResponse();
				numberOfResponses--;
				var batchMsgResponse = Database.ReadResponse();
				numberOfResponses--;
				var batchExecResponse = (BatchCompletionStateResponse)Database.ReadResponse();

				return BuildResult(batchExecResponse);
			}
			finally
			{
				Database.SafeFinishFetching(numberOfResponses);
			}
		}
		finally
		{
			ArrayPool<int>.Shared.Return(lengths);
			ArrayPool<byte>.Shared.Return(buffer);
		}
	}
	public override async ValueTask<ExecuteResultItem[]> ExecuteAsync(int count, IDescriptorFiller descriptorFiller, CancellationToken cancellationToken = default)
	{
		// this may throw error, so it needs to be before any writing
		var (buffer, lengths) = await EncodeParametersDataAsync(count, descriptorFiller, cancellationToken).ConfigureAwait(false);
		try
		{
			await Database.Xdr.WriteAsync(IscCodes.op_batch_create, cancellationToken).ConfigureAwait(false);
			await Database.Xdr.WriteAsync(_statement.Handle, cancellationToken).ConfigureAwait(false); // p_batch_statement
			var blr = _statement.ParametersBlr;
			await Database.Xdr.WriteBufferAsync(blr.Data, cancellationToken).ConfigureAwait(false); // p_batch_blr
			await Database.Xdr.WriteAsync(blr.Length, cancellationToken).ConfigureAwait(false); // p_batch_msglen
			var pb = _statement.CreateBatchParameterBuffer();
			if (_statement.ReturnRecordsAffected)
			{
				pb.Append(IscCodes.Batch.TAG_RECORD_COUNTS, 1);
			}
			if (MultiError)
			{
				pb.Append(IscCodes.Batch.TAG_MULTIERROR, 1);
			}
			pb.Append(IscCodes.Batch.TAG_BUFFER_BYTES_SIZE, BatchBufferSize);
			await Database.Xdr.WriteBufferAsync(pb.ToArray(), cancellationToken).ConfigureAwait(false); // p_batch_pb

			await Database.Xdr.WriteAsync(IscCodes.op_batch_msg, cancellationToken).ConfigureAwait(false);
			await Database.Xdr.WriteAsync(_statement.Handle, cancellationToken).ConfigureAwait(false); // p_batch_statement
			await Database.Xdr.WriteAsync(count, cancellationToken).ConfigureAwait(false); // p_batch_messages

			var offset = 0;
			for (var i = 0; i < count; i++)
			{
				var length = lengths[i];
				await Database.Xdr.WriteOpaqueAsync(buffer.AsMemory(offset, length), length, cancellationToken).ConfigureAwait(false); // p_batch_data
				offset += length;
			}

			await Database.Xdr.WriteAsync(IscCodes.op_batch_exec, cancellationToken).ConfigureAwait(false);
			await Database.Xdr.WriteAsync(_statement.Handle, cancellationToken).ConfigureAwait(false); // p_batch_statement
			await Database.Xdr.WriteAsync(_statement.Transaction.Handle, cancellationToken).ConfigureAwait(false); // p_batch_transaction;		

			await Database.Xdr.FlushAsync(cancellationToken).ConfigureAwait(false);

			var numberOfResponses = 3;
			try
			{
				numberOfResponses--;
				var batchCreateResponse = await Database.ReadResponseAsync(cancellationToken).ConfigureAwait(false);
				numberOfResponses--;
				var batchMsgResponse = await Database.ReadResponseAsync(cancellationToken).ConfigureAwait(false);
				numberOfResponses--;
				var batchExecResponse = (BatchCompletionStateResponse)await Database.ReadResponseAsync(cancellationToken).ConfigureAwait(false);

				return BuildResult(batchExecResponse);
			}
			finally
			{
				await Database.SafeFinishFetchingAsync(numberOfResponses, cancellationToken).ConfigureAwait(false);
			}
		}
		finally
		{
			ArrayPool<int>.Shared.Return(lengths);
			ArrayPool<byte>.Shared.Return(buffer);
		}
	}

	public override int ComputeBatchSize(int count, IDescriptorFiller descriptorFiller)
	{
		var total = 0;
		for(var i = 0; i < count; i++)
		{
			descriptorFiller.Fill(_statement.Parameters, i);
			total += _statement.EncodeCurrentParameters().Length;
		}
		return total;
	}
	public override async ValueTask<int> ComputeBatchSizeAsync(int count, IDescriptorFiller descriptorFiller, CancellationToken cancellationToken = default)
	{
		var total = 0;
		for(var i = 0; i < count; i++)
		{
			await descriptorFiller.FillAsync(_statement.Parameters, i, cancellationToken).ConfigureAwait(false);
			total += _statement.EncodeCurrentParameters().Length;
		}
		return total;
	}

	public override void Release()
	{
		Database.Xdr.Write(IscCodes.op_batch_rls);
		Database.Xdr.Write(_statement.Handle);
		Database.AppendDeferredPacket(ProcessReleaseResponse);
	}

	public override async ValueTask ReleaseAsync(CancellationToken cancellationToken = default)
	{
		await Database.Xdr.WriteAsync(IscCodes.op_batch_rls, cancellationToken).ConfigureAwait(false);
		await Database.Xdr.WriteAsync(_statement.Handle, cancellationToken).ConfigureAwait(false);
		Database.AppendDeferredPacket(ProcessReleaseResponseAsync);
	}

	protected void ProcessReleaseResponse(IResponse response)
	{ }
	protected ValueTask ProcessReleaseResponseAsync(IResponse response, CancellationToken cancellationToken = default)
	{
		return ValueTask.CompletedTask;
	}

	protected ExecuteResultItem[] BuildResult(BatchCompletionStateResponse response)
	{
		var detailedErrors = response.DetailedErrors.ToDictionary(x => x.Item1, x => x.Item2);
		var additionalErrorsPerMessage = response.AdditionalErrorsPerMessage.ToHashSet();
		var result = new ExecuteResultItem[response.ProcessedMessages];
		for (var i = 0; i < result.Length; i++)
		{
			var recordsAffected = i < response.UpdatedRecordsPerMessage.Length
				? response.UpdatedRecordsPerMessage[i]
				: -1;
			if (detailedErrors.TryGetValue(i, out var exception))
			{
				result[i] = new ExecuteResultItem()
				{
					RecordsAffected = recordsAffected,
					IsError = true,
					Exception = exception,
				};
			}
			else if (additionalErrorsPerMessage.Contains(i))
			{
				result[i] = new ExecuteResultItem()
				{
					RecordsAffected = recordsAffected,
					IsError = true,
					Exception = null,
				};
			}
			else
			{
				result[i] = new ExecuteResultItem()
				{
					RecordsAffected = recordsAffected,
					IsError = false,
					Exception = null,
				};
			}
		}
		return result;
	}

	(byte[] buffer, int[] lengths) EncodeParametersData(int count, IDescriptorFiller descriptorFiller)
	{
		var lengths = ArrayPool<int>.Shared.Rent(count);
		var buffer = ArrayPool<byte>.Shared.Rent(1024);
		var bufferCount = 0;

		try
		{
			for (var i = 0; i < count; i++)
			{
				descriptorFiller.Fill(_statement.Parameters, i);
				var item = _statement.EncodeCurrentParameters();
				lengths[i] = item.Length;
				EnsureCapacity(ref buffer, bufferCount, item.Length);
				item.CopyTo(buffer.AsSpan(bufferCount));
				bufferCount += item.Length;
			}

			return (buffer, lengths);
		}
		catch
		{
			ArrayPool<int>.Shared.Return(lengths);
			ArrayPool<byte>.Shared.Return(buffer);
			throw;
		}
	}

	async ValueTask<(byte[] buffer, int[] lengths)> EncodeParametersDataAsync(int count, IDescriptorFiller descriptorFiller, CancellationToken cancellationToken)
	{
		var lengths = ArrayPool<int>.Shared.Rent(count);
		var buffer = ArrayPool<byte>.Shared.Rent(1024);
		var bufferCount = 0;

		try
		{
			for (var i = 0; i < count; i++)
			{
				await descriptorFiller.FillAsync(_statement.Parameters, i, cancellationToken).ConfigureAwait(false);
				var item = _statement.EncodeCurrentParameters();
				lengths[i] = item.Length;
				EnsureCapacity(ref buffer, bufferCount, item.Length);
				item.CopyTo(buffer.AsSpan(bufferCount));
				bufferCount += item.Length;
			}

			return (buffer, lengths);
		}
		catch
		{
			ArrayPool<int>.Shared.Return(lengths);
			ArrayPool<byte>.Shared.Return(buffer);
			throw;
		}
	}

	static void EnsureCapacity(ref byte[] buffer, int bufferCount, int requiredAdditionalBytes)
	{
		if (requiredAdditionalBytes <= 0)
			return;

		var requiredTotal = checked(bufferCount + requiredAdditionalBytes);
		if (requiredTotal <= buffer.Length)
			return;

		var newCapacity = buffer.Length;
		while (newCapacity < requiredTotal)
		{
			newCapacity *= 2;
		}

		var newBuffer = ArrayPool<byte>.Shared.Rent(newCapacity);
		buffer.AsSpan(0, bufferCount).CopyTo(newBuffer);
		ArrayPool<byte>.Shared.Return(buffer);
		buffer = newBuffer;
	}
}
