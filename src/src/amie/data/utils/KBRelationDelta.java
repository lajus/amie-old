package amie.data.utils;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import amie.data.KB;
import javatools.datatypes.ByteString;

public class KBRelationDelta {

	/**
	 * Utility that verifies for every pair entity, relation whether there has been 
	 * a change between two versions of the KB.
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// Old KB
		KB db1 = new KB();
		// New KB
		KB db2 = new KB();
		
		db1.load(new File(args[0]));
		db2.load(new File(args[1]));
		
		List<ByteString> r1 = db1.getRelationsList();
		List<ByteString> r2 = db2.getRelationsList();
		
		r2.retainAll(r1);
				
		for (ByteString relation : r2) {
			List<ByteString[]> query = KB.triples(KB.triple(ByteString.of("?s"), relation, ByteString.of("?o")));
			ByteString[] query2 = KB.triple(ByteString.of("?s"), relation, ByteString.of("?o"));
			ByteString qVariable = null;
			boolean isFunctional = db2.isFunctional(relation);
			String relationlabel = null;
			if (isFunctional) {
				qVariable = query.get(0)[0];
				relationlabel = relation.toString();
			} else {
				qVariable = query.get(0)[2];
				relationlabel = relation.toString().replace(">", "-inv>");
			}
					
			Set<ByteString> e1 = new LinkedHashSet<ByteString>(db1.selectDistinct(qVariable, query));
			Set<ByteString> e2 = db2.selectDistinct(qVariable, query);
			e2.retainAll(e1);
			for (ByteString entity : e2) {
				if (isFunctional) {
					query2[0] = entity;
				} else {
					query2[2] = entity;
				}
				Set<ByteString> s1 = new LinkedHashSet<>(db1.resultsOneVariable(query2));
				Set<ByteString> s2 = db2.resultsOneVariable(query2);
				String outcome = null;
				if (s1.equals(s2)) {
					outcome = "No change";
					System.out.println(entity + "\t<hasNotChanged>\t" + relation);
				} else if (s2.containsAll(s1)) {
					outcome = "Addition";
					System.out.println(entity + "\t<hasChanged>\t" + relation);
				} else if (s1.containsAll(s2)) {
					outcome = "Deletion";
				} else {
					outcome = "Other";
				}
				System.out.println(entity + "\t" + relationlabel + "\t" + outcome);
			}
		}
	}
}
