package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javatools.datatypes.*;

public class FilterSmallRelations {

	public static void main(String args[]) throws IOException {
		FactDatabase db = new FactDatabase();
		db.load(new File(args[0]));
		int threshold = Integer.parseInt(args[1]);
		
		//Now go through all relations and count the facts
		for (ByteString relation : db.predicateSize) {
			if (isBiggerThan(relation, threshold, db)) {
				outputFacts(relation, db);
			}
		}
	}

	private static void outputFacts(ByteString relation, FactDatabase db) {
		Map<ByteString, IntHashMap<ByteString>> tail = db.predicate2subject2object.get(relation);
		for (ByteString subject : tail.keySet()) {
			for (ByteString object : tail.get(subject)) {
				System.out.println(subject + "\t" + relation + "\t" + object);
			}
		}
	}

	private static boolean isBiggerThan(ByteString relation, int threshold,
			FactDatabase db) {
		int size = 0;
		Map<ByteString, IntHashMap<ByteString>> tail = db.predicate2subject2object.get(relation);
		for (ByteString subject : tail.keySet()) {
			size += tail.get(subject).size();
			if (size >= threshold) {
				return true;
			}
		}
		
		return false;		
	}
}
