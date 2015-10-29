package amie.data.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import amie.data.KB;
import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Triple;
import javatools.filehandlers.TSVFile;

/**
 * Given a file with the relevance score of a set of entities and a KB, 
 * it outputs the facts corresponding to entities on the top X% of the
 * relevance ranking.
 * 
 * @author galarrag
 *
 */
public class RelevanceFilter {

	public static void main(String[] args) throws IOException {
		List<Triple<String, String, Double>> relevanceList = new 
				ArrayList<>();
		try(TSVFile relevanceFile = new TSVFile(new File(args[0]))) {
			for (List<String> line : relevanceFile) {
				if (line.size() < 3)
					continue;
				relevanceList.add(new Triple<String, String, Double>(line.get(0), 
						line.get(1), Double.valueOf(line.get(2))));
			}
			
			Collections.sort(relevanceList, new Comparator<Triple<String, String, Double>>(){
				@Override
				public int compare(Triple<String, String, Double> o1, 
						Triple<String, String, Double> o2) {
					return o2.third.compareTo(o1.third);
				}
			});
		}
		
		KB kb = new KB();
		kb.load(new File(args[1]));

		double topPercentage = 0.05;
		if (args.length > 2)
			topPercentage = Double.parseDouble(args[2]);
		
		long top = Math.round(topPercentage * relevanceList.size());
		HashMap<ByteString, Double> relevanceMap = new HashMap<>();
		
		System.out.println(top);
		System.out.println("A sample of the top 10 entities");
		System.out.println(relevanceList.subList(0,  10));
		for (Triple<String, String, Double> t : relevanceList.subList(0, 10000)) {
			if (t.third.isNaN()) {
				System.err.println(t.first + " is Nan");
				System.exit(1);
			}
			relevanceMap.put(ByteString.of(t.first), t.third);
		}
		
		// Now filter the facts
		ByteString s = ByteString.of("?s");
		ByteString r = ByteString.of("?r");
		ByteString o = ByteString.of("?o");		
		List<ByteString[]> query =  KB.triples(KB.triple(s, r, o));
		for (ByteString relation : kb.selectDistinct(ByteString.of("?r"), query)) {			
			ByteString[] query2 = KB.triple(s, relation, o);
			Map<ByteString, IntHashMap<ByteString>> bindings = null;
			boolean inversed = false;
			if (kb.isFunctional(relation)) {				
				bindings = kb.resultsTwoVariables(s, o, query2);
			} else {
				inversed = true;
				bindings = kb.resultsTwoVariables(o, s, query2);			
			}
			
			for (ByteString argument : bindings.keySet()) {
				Double relevanceValue = relevanceMap.get(argument);
				if (relevanceValue != null) {
					//Output the entry
					outputEntry(argument, relation, bindings.get(argument), inversed);
				}
			}
		}
	}

	/**
	 * Output a set of triples when the relation and one of the arguments are 
	 * fixed.
	 * @param argument Either the subject or the object of the triples
	 * @param relation
	 * @param intHashMap
	 * @param inversed If true, then the object is fixed, otherwise the subject
	 */
	private static void outputEntry(ByteString argument, ByteString relation, 
			IntHashMap<ByteString> values, boolean inversed) {
		if (inversed) {
			for (ByteString value : values) {
				System.out.println(value + "\t" + relation + "\t" + argument);
			}	
		} else {
			for (ByteString value : values) {
				System.out.println(argument + "\t" + relation + "\t" + value);
			}				
		}
	}
}
