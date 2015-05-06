package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import javatools.datatypes.ByteString;
import amie.data.FactDatabase;
import amie.data.KBSummarizer;

public class KBSummarizer2 {
	public static void main(String args[]) throws IOException {
		FactDatabase db1 = new FactDatabase();
		db1.load(new File(args[0]));
		FactDatabase db2 = new FactDatabase();
		db2.load(new File(args[1]));
		
		Set<ByteString> relationsInCommon = new LinkedHashSet<ByteString>();
		
		Set<ByteString> relationsDb1 = db1.selectDistinct(ByteString.of("?p"), 
				FactDatabase.triples(FactDatabase.triple(ByteString.of("?s"), 
						ByteString.of("?p"), ByteString.of("?o"))));
		Set<ByteString> relationsDb2 = db2.selectDistinct(ByteString.of("?p"), 
				FactDatabase.triples(FactDatabase.triple(ByteString.of("?s"), 
						ByteString.of("?p"), ByteString.of("?o"))));
		
		for (ByteString relation : relationsDb1) {
			if (relationsDb2.contains(relation)) {
				relationsInCommon.add(relation);
			}
		}
		
		KBSummarizer.summarize(db1, false);
		System.out.println();
		KBSummarizer.summarize(db2, false);
		System.out.println(relationsInCommon);
	}
}
