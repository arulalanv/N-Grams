package org.alan.wc.profile3.Sorting;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordCountSorter extends WritableComparator{
	protected WordCountSorter() {
		super(CompositeKey.class,true);
	}
	
	public int compare(WritableComparable w1,WritableComparable w2) {
		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;
		
		return key2.getCount()-key1.getCount();
	}

}
