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

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FirebirdSql.Data.Common;

namespace FirebirdSql.Data.FirebirdClient;

public sealed class FbTransactionInfo(FbTransaction transaction = null)
{
		#region Properties

		public FbTransaction Transaction { get; set; } = transaction;

		#endregion

		#region Methods

		public long GetTransactionSnapshotNumber() => GetValue<long>(IscCodes.fb_info_tra_snapshot_number);
		public Task<long> GetTransactionSnapshotNumberAsync(CancellationToken cancellationToken = default) => GetValueAsync<long>(IscCodes.fb_info_tra_snapshot_number, cancellationToken);

		#endregion
		#region Constructors

		#endregion

		#region Private Methods

		private T GetValue<T>(byte item)
		{
				FbTransaction.EnsureActive(Transaction);

				byte[] items =
				[
			item,
			IscCodes.isc_info_end
				];
				var info = Transaction.Transaction.GetTransactionInfo(items);
				return info.Any() ? InfoValuesHelper.ConvertValue<T>(info[0]) : default;
		}
		private async Task<T> GetValueAsync<T>(byte item, CancellationToken cancellationToken = default)
		{
				FbTransaction.EnsureActive(Transaction);

				byte[] items =
				[
			item,
			IscCodes.isc_info_end
				];
				var info = await Transaction.Transaction.GetTransactionInfoAsync(items, cancellationToken).ConfigureAwait(false);
				return info.Any() ? InfoValuesHelper.ConvertValue<T>(info[0]) : default;
		}

		private List<T> GetList<T>(byte item)
		{
				FbTransaction.EnsureActive(Transaction);

				byte[] items =
				[
			item,
			IscCodes.isc_info_end
				];

				return [.. Transaction.Transaction.GetTransactionInfo(items).Select(InfoValuesHelper.ConvertValue<T>)];
		}
		private async Task<List<T>> GetListAsync<T>(byte item, CancellationToken cancellationToken = default)
		{
				FbTransaction.EnsureActive(Transaction);

				byte[] items =
				[
			item,
			IscCodes.isc_info_end
				];

				return [.. (await Transaction.Transaction.GetTransactionInfoAsync(items, cancellationToken).ConfigureAwait(false)).Select(InfoValuesHelper.ConvertValue<T>)];
		}

		#endregion
}
