package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;

public class KBIntersection {

	public static void main(String[] args) throws IOException {
		FactDatabase kb1 = new FactDatabase();
		FactDatabase kb2 = new FactDatabase();
		kb1.load(new File(args[0]));
		kb2.load(new File(args[1]));
		ByteString[] triple = new ByteString[3];
		for (ByteString subject : kb1.subject2predicate2object.keySet()) {
			triple[0] = subject;
			Map<ByteString, IntHashMap<ByteString>> tail = kb1.subject2predicate2object.get(subject);			
			for (ByteString predicate : tail.keySet()) {
				triple[1] = predicate;
				for (ByteString object : tail.get(predicate)) {
					triple[2] = object;
					if (kb2.contains(triple)) {
						System.out.println(Arrays.toString(triple));
					}
				}
			}
		}
	}

}
