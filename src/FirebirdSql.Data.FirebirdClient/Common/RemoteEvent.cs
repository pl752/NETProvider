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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FirebirdSql.Data.Common;

internal class RemoteEvent(DatabaseBase database)
{
	const int MaxEventNameLength = 255;
	const int MaxEpbLength = 65535;

	readonly List<string> _events = [];
	readonly DatabaseBase _database = database;
	int[] _previousCounts;
	int[] _currentCounts;
	int _running;

	public int LocalId { get; set; } = 0;
	public int RemoteId { get; set; } = 0;
	public Action<string, int> EventCountsCallback { get; set; }
	public Action<Exception> EventErrorCallback { get; set; }

	public List<string> Events => _events;

	public DatabaseBase Database => _database;

	public void QueueEvents(ICollection<string> events)
	{
		EnsureNotRunning();
		EnsureEventsCollection(events);
		_events.AddRange(events);
		_database.QueueEvents(this);
	}
	public ValueTask QueueEventsAsync(ICollection<string> events, CancellationToken cancellationToken = default)
	{
		EnsureNotRunning();
		EnsureEventsCollection(events);
		_events.AddRange(events);
		return _database.QueueEventsAsync(this, cancellationToken);
	}

	public void CancelEvents()
	{
		_database.CancelEvents(this);
		_currentCounts = null;
		_previousCounts = null;
		_events.Clear();
		Volatile.Write(ref _running, 0);
	}
	public async ValueTask CancelEventsAsync(CancellationToken cancellationToken = default)
	{
		await _database.CancelEventsAsync(this, cancellationToken).ConfigureAwait(false);
		_currentCounts = null;
		_previousCounts = null;
		_events.Clear();
		Volatile.Write(ref _running, 0);
	}

	internal void EventCounts(byte[] buffer)
	{
		if (Volatile.Read(ref _running) == 0)
			return;

		_previousCounts = _currentCounts;
		_currentCounts = new int[_events.Count];

		int pos = 1;
		while (pos < buffer.Length)
		{
			byte length = buffer[pos++];
			string eventName = _database.Charset.GetString(buffer, pos, length);

			pos += length;

			int index = _events.IndexOf(eventName);
			Debug.Assert(index != -1);
			_currentCounts[index] = BitConverter.ToInt32(buffer, pos) - 1;

			pos += 4;
		}

		for (int i = 0; i < _events.Count; i++)
		{
			int count = _currentCounts[i] - _previousCounts[i];
			if (count == 0)
				continue;
			EventCountsCallback(_events[i], count);
		}
	}

	internal void EventError(Exception error) => EventErrorCallback(error);

	internal EventParameterBuffer BuildEpb()
	{
		_currentCounts ??= new int[_events.Count];
		return BuildEpb(_events, i => _currentCounts[i] + 1);
	}

	void EnsureNotRunning()
	{
		if (Interlocked.Exchange(ref _running, 1) == 1)
			throw new InvalidOperationException("Events are already running.");
	}

	EventParameterBuffer BuildEpb(IList<string> events, Func<int, int> countFactory)
	{
		var epb = Database.CreateEventParameterBuffer();
		epb.Append(IscCodes.EPB_version1);
		for (int i = 0; i < events.Count; i++)
		{
			epb.Append(events[i], countFactory(i));
		}
		return epb;
	}

	void EnsureEventsCollection(ICollection<string> events)
	{
		ArgumentNullException.ThrowIfNull(events);
		if (events.Count == 0)
			throw new ArgumentOutOfRangeException(nameof(events), "Need to provide at least one event.");
		if (events.Any(x => x.Length > MaxEventNameLength))
			throw new ArgumentOutOfRangeException(nameof(events), $"Some events are longer than {MaxEventNameLength}.");
		if (BuildEpb([.. events], _ => default).ToArray().Length > MaxEpbLength)
			throw new ArgumentOutOfRangeException(nameof(events), $"Whole events buffer is bigger than {MaxEpbLength}.");
	}
}
