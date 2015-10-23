package amie.data.utils;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import amie.data.KB;
import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.filehandlers.TSVFile;

public class RelevanceCalculator {

	/**
	 * For each entity in an input KB (given as a TSV file), it outputs the relevance
	 * of the entities based on the formula:
	 * 
	 * log(wiki-length) * (ingoing-links + 2) * (number-facts) 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		try(TSVFile tsv = new TSVFile(new File(args[0]))) {
			KB kb = null;
			if (args.length > 1) {
				kb = new KB();
				kb.load(new File(args[1]));
			}
			
			IntHashMap<String> ingoingLinksMap = new IntHashMap<>();
			IntHashMap<String> wikiLengthMap = new IntHashMap<>();
			
			for (List<String> line : tsv) {
				String entity = line.get(0);
				String relation = line.get(1);
				String value = extractInteger(line.get(2));
				if (relation.equals("<hasIngoingLinks>")) {
					ingoingLinksMap.add(entity, Integer.parseInt(value));
				} else if (relation.equals("<hasWikipediaArticleLength>")) {
					wikiLengthMap.add(entity, Integer.parseInt(value));					
				}
			}
			
			Set<String> allEntities = new LinkedHashSet<>();
			allEntities.addAll(ingoingLinksMap.increasingKeys());
			allEntities.addAll(wikiLengthMap.increasingKeys());
			for (String entity : allEntities) {
				double nFacts = 1;
				int wikiLength = wikiLengthMap.get(entity);
				int ingoingLinks = ingoingLinksMap.get(entity);
				if (kb != null) {
					nFacts = kb.count(ByteString.of(entity), ByteString.of("?p"), ByteString.of("?o"));
				}
				
				double coefficient = (Math.log10(wikiLength) + 3) * (ingoingLinks + 1) * (nFacts + 1);
				System.out.println(entity + "\t<hasRelevance>\t" + coefficient);
			}
		}
	}
	
	private static String extractInteger(String xsdInteger) {
		return xsdInteger.replace("\"", "").replace("^^xsd:integer", "");
	}
}
