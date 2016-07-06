package amie.rosa;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import amie.data.KB;
import amie.mining.AMIE;
import amie.rules.Metric;
import amie.rules.Rule;
import javatools.datatypes.ByteString;
import javatools.filehandlers.TSVFile;

public class AlignKBs {
	
	public static String prefixkb1 = "<db:";
	
	public static String prefixkb2 = "";
	
	public static String wikidataMappings = "/infres/ic2/galarrag/AMIE/Data/wikidata/mapping_yago_wikidata.tsv";
	
	/**
	 * Determines if a rule r => r' is a cross-ontology mapping, i.e., 
	 * r and r' belong to different ontologies.
	 * @param rule
	 * @return
	 */
	public static boolean isCrossOntology(Rule rule) {
		ByteString r1, r2;
		r1 = rule.getHead()[1];
		r2 = rule.getBody().get(0)[1];
		return (r1.toString().startsWith(prefixkb1) && !r2.toString().startsWith(prefixkb1)) || 
			(r2.toString().startsWith(prefixkb1) && !r1.toString().startsWith(prefixkb1));
	}

	public static KB loadFiles(String args[]) throws IOException {
		KB kb = new KB();
		for (String fileName : args) {
			TSVFile file = new TSVFile(new File(fileName));
			for (List<String> line : file) {
				String[] parts = line.get(0).split("> <");
				if (parts.length == 3) {
					kb.add(parts[0] + ">", "<" + parts[1] + ">", "<" + parts[2]);
				} else if (parts.length == 2) {
					String[] tailParts = parts[1].split("> \"");
					if (tailParts.length == 2) {
						kb.add(parts[0] + ">", "<" + tailParts[0] + ">", "\"" + tailParts[1]);
					}
				}
			}
			file.close();
		}
		return kb;
	}
	
	public static String map(Map<String, String> map, String val) {
		if (map.containsKey(val)) 
			return map.get(val);
		else
			return val;
	}
	
	public static KB loadAndMapFiles(String args[]) throws IOException {
		KB kb = new KB();
		Map<String, String> yago2wiki = new HashMap<>();
		// Load the mappings 
		try(TSVFile mappings = new TSVFile(new File(wikidataMappings))) {
			for (List<String> mapping : mappings) {
				yago2wiki.put(mapping.get(0), mapping.get(2));
			}
		}
		
		for (int i = 1; i < args.length; ++i) {
			String fileName = args[i];
			TSVFile file = new TSVFile(new File(fileName));
			for (List<String> line : file) {
				String[] parts = line.get(0).split("> <");
				if (parts.length == 3) {
					kb.add(map(yago2wiki, parts[0] + ">"), "<" + parts[1] + ">", map(yago2wiki, "<" + parts[2]));
				} else if (parts.length == 2) {
					String[] tailParts = parts[1].split("> \"");
					if (tailParts.length == 2) {
						kb.add(map(yago2wiki, parts[0] + ">"), "<" + tailParts[0] + ">", map(yago2wiki, "\"" + tailParts[1]));
					}
				}
			}
			file.close();
		}
		return kb;
	}


	
	public static void main(String[] args) throws Exception {	
		KB kb = null;
		if (args[0].equals("wikidata"))
			kb = loadAndMapFiles(args);
		else
			kb = loadFiles(args);
		AMIE amie = AMIE.getVanillaSettingInstance(kb);
		amie.getAssistant().setMaxDepth(2);
		amie.setPruningMetric(Metric.Support);
		amie.setMinSignificanceThreshold(10);
		amie.setMinInitialSupport(10);
		amie.setRealTime(false);		
		List<Rule> rules = amie.mine();
		List<Rule> crossOntologyRules = new ArrayList<>();
		for (Rule rule : rules) {
			if (isCrossOntology(rule)) {
				crossOntologyRules.add(rule);			
			}				
		}
		
		List<ROSAEquivalence> rosaEquivalences = 
				EquivalenceRulesBuilder.findEquivalences(kb, crossOntologyRules);
		
		Collections.sort(rosaEquivalences, new Comparator<ROSAEquivalence>() {
			@Override
			public int compare(ROSAEquivalence o1, ROSAEquivalence o2) {
				return Double.compare(o2.getConfidence(), o1.getConfidence());
			}			
		});
		
		System.out.println("Mappings");
		for (ROSAEquivalence rule : rosaEquivalences) {
			rule.prefix1 = prefixkb1;
			System.out.println(rule.toShortString());
		}
		
	}

}
