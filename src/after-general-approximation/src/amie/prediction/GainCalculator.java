package amie.prediction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javatools.datatypes.ByteString;
import javatools.filehandlers.TSVFile;
import amie.data.EquivalenceChecker2;
import amie.query.AMIEreader;
import amie.query.Query;

public class GainCalculator {

	public static final ByteString typeStr = ByteString.of("rdf:type");
	
	public static List<Query> parseRules(File f) throws IOException {
		List<Query> rules = new ArrayList<Query>();
		try(TSVFile specific = new TSVFile(f)) {
			for (List<String> line : specific) {
				Query rule = AMIEreader.rule(line.get(0));
				/*if (rule.containsRelationTimes(typeStr) < 2 || !rule.containsRelation(ByteString.of("<linksTo>"))) {
					continue;
				}*/
				rule.setSupport(Long.parseLong(line.get(5)));
				rule.setPcaConfidence(Double.parseDouble(line.get(4)));
				rules.add(rule);
			}
		}
		return rules;
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub	
		List<Query> specificRules = parseRules(new File(args[0]));
		List<Query> generalRules = parseRules(new File(args[1]));
		int matched = 0;
		double gain = 0.0;
		double confidenceGain = 0.0;
		double gain2 = 0.0;
		double totalSupport = 0.0;
		double maxConfDelta = -1.0;
		int overFiftyPercentGain = 0; 
		for (Query rule : specificRules) {
			removeLinksAtomTo(rule);
			for (Query generalRule : generalRules) {
				if (EquivalenceChecker2.equal(rule.getTriples(), generalRule.getTriples())) {
					// Bingo. I found the general version
					++matched;
					double deltaConf = rule.getPcaConfidence() - generalRule.getPcaConfidence();
					gain2 += rule.getSupport() * deltaConf;
					totalSupport += rule.getSupport(); 
					confidenceGain += deltaConf;
					if (deltaConf > maxConfDelta) {
						maxConfDelta = deltaConf;
					}
					double deltaSupp = rule.getSupport() - generalRule.getSupport();					
					if (deltaSupp > 0) {
						System.out.println("Error");
						System.out.println(generalRule);
					}
					if (deltaConf > 0.2) {
						System.out.println("General rule");
						System.out.println(generalRule.getFullRuleString());
						System.out.println("Specific rule");
						System.out.println(rule.getFullRuleString());
						overFiftyPercentGain++;
					}
					gain += -1.0 / ((deltaSupp - 1) * deltaConf);
				}
			}
		}
		
		System.out.println("Specific rules: " + specificRules.size());
		System.out.println("General rules: " + generalRules.size());
		System.out.println("Matched rules: " + matched);
		System.out.println("Average gain: " + gain / matched);
		System.out.println("Average confidence gain: " + (confidenceGain / matched));
		System.out.println("Weighted confidence gain: " + (gain2 / totalSupport));
		System.out.println("Maximum confidence: " + maxConfDelta);
		System.out.println("Over 50% gain: " + overFiftyPercentGain);

	}

	private static void removeLinksAtomTo(Query rule) {
		List<ByteString[]> triples = rule.getTriples();
		Iterator<ByteString[]> it = triples.iterator();
		while(it.hasNext()) {
			ByteString[] current = it.next();
			if (current[1].equals(ByteString.of("<linksTo>"))) {
				it.remove();
			}
		}
	}

}
