package amie.data.utils;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import javatools.datatypes.ByteString;
import amie.data.KB;

/**
 * Summarize 2 KBs and print their common relations.
 * @author galarrag
 *
 */
public class KBsSummarizer {
	
	public static void main(String args[]) throws IOException {
		KB db1 = new KB();
		db1.load(new File(args[0]));
		KB db2 = new KB();
		db2.load(new File(args[1]));
		
		Set<ByteString> relationsInCommon = new LinkedHashSet<ByteString>();
		
		Set<ByteString> relationsDb1 = db1.selectDistinct(ByteString.of("?p"), 
				KB.triples(KB.triple(ByteString.of("?s"), 
						ByteString.of("?p"), ByteString.of("?o"))));
		Set<ByteString> relationsDb2 = db2.selectDistinct(ByteString.of("?p"), 
				KB.triples(KB.triple(ByteString.of("?s"), 
						ByteString.of("?p"), ByteString.of("?o"))));
		
		for (ByteString relation : relationsDb1) {
			if (relationsDb2.contains(relation)) {
				relationsInCommon.add(relation);
			}
		}
		
		db1.summarize(false);
		System.out.println();
		db2.summarize(false);
		System.out.println(relationsInCommon);
	}
}
