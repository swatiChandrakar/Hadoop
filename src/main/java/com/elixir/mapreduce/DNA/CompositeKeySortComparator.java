package com.elixir.mapreduce.DNA;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeySortComparator  extends WritableComparator {
	
	protected CompositeKeySortComparator(){
		super(CompositeKey.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeKey key1 = (CompositeKey) a;
		CompositeKey key2 = (CompositeKey) b;
		
		int result = key1.chrNo == key2.chrNo ? 0: key1.chrNo > key2.chrNo? +1:-1 ;
		if(result != 0) return result;
		
		//comparing data for same chromosome, so now compare on position
		return key1.position == key2.position ? 0: key1.position > key2.position? +1:-1 ;
	}
}
