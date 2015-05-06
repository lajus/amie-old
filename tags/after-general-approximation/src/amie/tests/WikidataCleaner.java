package amie.tests;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

public class WikidataCleaner {
	
	public static final String labelProperty = "http://www.w3.org/2000/01/rdf-schema#label";
	
	public static final String wikidataPrefix = "http://www.wikidata.org/entity/";

	public static Map<String, String> idsToNames(String fileName) throws FileNotFoundException, IOException {
		NxParser nxp = new NxParser(new FileInputStream(fileName), false);
		Map<String, String> resultMap = new HashMap<String, String>();

		while (nxp.hasNext()) {
			Node[] ns = nxp.next();
			if (ns.length == 3) {
			    //Only Process Triples  
			    //Replace the print statements with whatever you want
				if (ns[1].toString().equals(labelProperty) && ns[2].toN3().endsWith("@en")) {
					String cleanSubject = ns[0].toString();
					String cleanObject = ns[2].toString().replaceAll("[^a-zA-Z0-9]", "_");
					resultMap.put(cleanSubject, "<" + cleanObject + "_" + cleanSubject.replace(wikidataPrefix, "") + ">");
				}
			}
		}
		
		return resultMap;
	}
	
	private static void cleanWikidata(String file, Map<String, String> relationsMap, Map<String, String> entitiesMap) 
			throws FileNotFoundException, IOException {
		NxParser nxp = new NxParser(new FileInputStream(file), false);
		
		while (nxp.hasNext()) {
			Node[] ns = nxp.next();
			if (ns.length == 3) {
				if (ns[0].toString().startsWith(wikidataPrefix) 
						&& ns[2].toString().startsWith(wikidataPrefix)) {
					String subject = entitiesMap.get(ns[0].toString());
					String object = entitiesMap.get(ns[2].toString());
					String relation = relationsMap.get(ns[1].toString().substring(0, ns[1].toString().length() - 1));
					if (subject != null && object != null && relation != null) {
						String cleanSubject = subject.replace(wikidataPrefix, "");
						String cleanObject = object.replace(wikidataPrefix, "");
						System.out.println(cleanSubject + "\t" + relation + "\t" + cleanObject);
					}
				}
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		// Build a map from property identifiers to more human readable names
		Map<String, String> relationsMap = idsToNames(args[0]);
		Map<String, String>	entitiesMap = idsToNames(args[1]);
		// Parse the big file
		cleanWikidata(args[2], relationsMap, entitiesMap);
	
	}
}
