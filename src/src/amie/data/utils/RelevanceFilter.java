package amie.data.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import amie.data.KB;
import javatools.datatypes.ByteString;
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

		double topPercentage = 0.1;
		if (args.length > 2)
			topPercentage = Double.parseDouble(args[2]);
		
		long top = Math.round(topPercentage * relevanceList.size());
		HashMap<ByteString, Double> relevanceMap = new HashMap<>();
		
		for (Triple<String, String, Double> t : relevanceList.subList(0, (int)top)) {
			relevanceMap.put(ByteString.of(t.first), t.third);
		}
		
		// Now filter the facts
		List<ByteString[]> query =  KB.triples(KB.triple(ByteString.of("?s"), ByteString.of("?r"), ByteString.of("?o")));
		for (ByteString relation : kb.selectDistinct(ByteString.of("?r"), query)) {			
			if (kb.isFunctional(relation)) {
				
			}
		}
	}

}
