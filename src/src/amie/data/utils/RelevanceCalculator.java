package amie.data.utils;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javatools.datatypes.IntHashMap;
import javatools.filehandlers.TSVFile;

public class RelevanceCalculator {

	/**
	 * For each entity in an input KB (given as a TSV file), it outputs the relevance
	 * of the entities based on the formula:
	 * 
	 * log(wiki-length + 3) * (ingoing-links + 2)
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		try(TSVFile tsv = new TSVFile(new File(args[0]))) {
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
				int wikiLength = wikiLengthMap.get(entity);
				int ingoingLinks = ingoingLinksMap.get(entity);
				double coefficient = (Math.log10(wikiLength + 3.0)) * (ingoingLinks + 2);
				System.out.println(entity + "\t<hasRelevance>\t" + coefficient);
			}
		}
	}
	
	private static String extractInteger(String xsdInteger) {
		return xsdInteger.replace("\"", "").replace("^^xsd:integer", "");
	}
}
