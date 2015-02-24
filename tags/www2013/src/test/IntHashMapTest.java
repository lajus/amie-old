package test;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;

public class IntHashMapTest {

	public IntHashMapTest() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		IntHashMap<ByteString> h1 = new IntHashMap<ByteString>();
		h1.put(ByteString.of("Whatever"), 10);
		for(ByteString value: h1){
			System.out.print(value);
		}
	}

}
