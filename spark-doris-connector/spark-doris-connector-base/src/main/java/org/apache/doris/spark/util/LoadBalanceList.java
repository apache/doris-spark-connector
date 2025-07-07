// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


package org.apache.doris.spark.util;

import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.Random;
import java.util.Queue;
import java.util.PriorityQueue;
import java.util.Collections;

import java.util.concurrent.ConcurrentHashMap;


public class LoadBalanceList<T> implements Iterable<T> {

	private final List<T> list;

	private final Map<T, FailedServer<T>> failedServers;

	private static final long FAILED_TIME_OUT = 60 * 60 * 1000;

	public LoadBalanceList(List<T> servers) {
		this.list = Collections.unmodifiableList(servers);
		this.failedServers = new ConcurrentHashMap<>();
	}

	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>() {
			final int offset = Math.abs(new Random().nextInt());
			final Queue<FailedServer<T>> skipServers = new PriorityQueue<>();
			int index = 0;

			@Override
			public boolean hasNext() {
				return index < list.size() || !skipServers.isEmpty();
			}

			@Override
			public T next() {
				if (index < list.size()) {
					T server = list.get((offset + index++) % list.size());
					FailedServer failedEntry = failedServers.get(server);
					if (failedEntry != null) {
						if (System.currentTimeMillis() - failedEntry.failedTime > FAILED_TIME_OUT) {
							failedServers.remove(failedEntry.server);
						} else {
							skipServers.add(failedEntry);
							return next();
						}
					}
					return server;
				} else {
					return skipServers.poll().server;
				}
			}
		};
	}

	public List<T> getList() {
		return list;
	}

	public void reportFailed(T server) {
		this.failedServers.put(server, new FailedServer<T>(server));
	}

	private static class FailedServer<T> implements Comparable<FailedServer<T>> {

		protected final T server;

		protected final Long failedTime;

		public FailedServer(T t) {
			this.server = t;
			this.failedTime = System.currentTimeMillis();
		}


		@Override
		public int compareTo(FailedServer<T> o) {
			return this.failedTime.compareTo(o.failedTime);
		}
	}
}
