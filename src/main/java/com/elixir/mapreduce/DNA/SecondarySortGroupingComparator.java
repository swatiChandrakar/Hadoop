package com.elixir.mapreduce.DNA;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortGroupingComparator extends WritableComparator {
	protected SecondarySortGroupingComparator() {
		super(CompositeKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeKey key1 = (CompositeKey) a;
		CompositeKey key2 = (CompositeKey) b;
		
		return key1.chrNo == key2.chrNo ? 0: key1.chrNo > key2.chrNo? +1:-1 ;
	}
}
