package org.apache.doris.spark.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;


public class LoadBalanceList<T> implements Iterable<T> {

	private final List<T> list;

	public LoadBalanceList(List<T> servers) {
		this.list = Collections.unmodifiableList(servers);
	}

	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>() {
			final int offset = Math.abs(new Random().nextInt());
			int index = 0;

			@Override
			public boolean hasNext() {
				return index < list.size();
			}

			@Override
			public T next() {
				return list.get((offset + index++) % list.size());
			}
		};
	}

	public List<T> getList() {
		return list;
	}
}
