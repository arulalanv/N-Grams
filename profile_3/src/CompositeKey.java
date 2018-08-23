package org.alan.wc.profile3.Sorting;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKey implements WritableComparable{
	private String word;
	private int count;
	
	public CompositeKey() {
		
	}
	public CompositeKey(String word, int count) {
		super();
		this.word = word;
		this.count = count;
	}
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(out, word);
		WritableUtils.writeVInt(out, count);
		
	}
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		word = WritableUtils.readString(in);
		count = WritableUtils.readVInt(in);
		
	}
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		CompositeKey ck = (CompositeKey) o;
		return ck.word.compareToIgnoreCase(word);
	}
	
	public int haskCode() {
		return 23*word.hashCode();
	}
	
	public boolean equals(Object obj) {
		CompositeKey ck = (CompositeKey) obj;
		return ck.word.equals(word);
	}
	
	public String toString() {
		return word;
	}
	

}
