package amie.prediction;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Triple;
import amie.data.FactDatabase;
import amie.data.eval.PredictionsSampler;
import amie.mining.assistant.HeadVariablesMiningAssistant;
import amie.query.Query;

public class RuleJointDistribution {
	
	FactDatabase dataSource;
	
	public RuleJointDistribution(FactDatabase db) {
		dataSource = db;
	}
	
	public static Set getIntersection(Set set1, Set set2) {
	    boolean set1IsLarger = set1.size() > set2.size();
	    Set cloneSet = new HashSet(set1IsLarger ? set2 : set1);
	    cloneSet.retainAll(set1IsLarger ? set1 : set2);
	    return cloneSet;
	}
	
	public Map<BitSet, Double> getJointDistributionOnIntersection(Query r1, Query r2) {
		IntHashMap<Integer> distribution = new IntHashMap<>();
		PredictionsSampler sampler = new PredictionsSampler(dataSource);
		Map<BitSet, Double> finalDistribution = new HashMap<>();
		Set<Triple<ByteString, ByteString, ByteString>> triples1 = sampler.generateBodyTriples(r1, false);
		Set<Triple<ByteString, ByteString, ByteString>> triples2 = sampler.generateBodyTriples(r2, false);
		distribution.put(0, 0);
		distribution.put(1, 0);
		Set<Triple<ByteString, ByteString, ByteString>> intersection = getIntersection(triples1, triples2);
		for (Triple<ByteString, ByteString, ByteString> triple : intersection) {
			if (dataSource.contains(FactDatabase.triple2Array(triple))) {
				distribution.increase(1);
			} else {
				distribution.increase(0);
			}
		}
		
		double normalizationConstant = 0.0;
		for (Integer val : distribution) {
			normalizationConstant += distribution.get(val);
		}
		System.out.println(normalizationConstant + " triples in the intersection");
		
		for (Integer i : distribution) {
			finalDistribution.put(toBitSet(i.intValue(), 1), 
					new Double(distribution.get(i) / normalizationConstant));
		}
		
		return finalDistribution;
	}
	
	public Map<BitSet, Double> getJointDistributionOnUnion(Query r1, Query r2) {
		IntHashMap<Integer> distribution = new IntHashMap<>();
		PredictionsSampler sampler = new PredictionsSampler(dataSource);
		Map<BitSet, Double> finalDistribution = new HashMap<>();
		Set<Triple<ByteString, ByteString, ByteString>> triples1 = sampler.generateBodyTriples(r1, false);
		Set<Triple<ByteString, ByteString, ByteString>> triples2 = sampler.generateBodyTriples(r2, false);
		distribution.put(0, 0);
		distribution.put(1, 0);
		distribution.put(2, 0);
		distribution.put(3, 0);
		for (Triple<ByteString, ByteString, ByteString> triple : triples1) {
			ByteString[] arrayTriple = FactDatabase.triple2Array(triple);
			if (dataSource.contains(arrayTriple)) {
				distribution.increase(3);
				triples2.remove(triple);
			} else {
				if (triples2.contains(triple)) {
					distribution.increase(0);
					triples2.remove(triple);
				} else {
					distribution.increase(1);	
				}
			}
		}
		
		for (Triple<ByteString, ByteString, ByteString> triple : triples2) {
			if (dataSource.contains(FactDatabase.triple2Array(triple))) {
				distribution.increase(3);
			} else {
				distribution.increase(2);
			}
		}
		
		double normalizationConstant = 0.0;
		for (Integer val : distribution) {
			normalizationConstant += distribution.get(val);
		}
		System.out.println(normalizationConstant + " triples in the union");
		
		for (Integer i : distribution) {
			finalDistribution.put(toBitSet(i.intValue(), 2), 
					new Double(distribution.get(i) / normalizationConstant));
		}
		
		return finalDistribution;
	}
	
	/**
	 * Calculates the joint distribution between 2 rules
	 * @param r1
	 * @param r2
	 * @return
	 */
	public Map<BitSet, Double> getJointDistribution(Query r1, Query r2, boolean onUnion) {
		if (onUnion) {
			return getJointDistributionOnUnion(r1, r2);
		} else {
			return getJointDistributionOnIntersection(r1, r2);
		}
	}
	
	/**
	 * Given a list of rules A1 => X1, ... An => Xn, having the same head relation, it returns the combined rule
	 * A1,..., An => X', where X' is the most specific atom. For example given the rules A1 => livesIn(x, y) and 
	 * A2 => livesIn(x, USA), the method returns A1, A2 => livesIn(x, USA). 
	 * @param rules
	 * @return
	 */
	public Query getCombinedRule(List<Query> rules) {
		Query combinedRule = Query.combineRules(rules);
		HeadVariablesMiningAssistant assistant = new HeadVariablesMiningAssistant(dataSource);
		assistant.computeCardinality(combinedRule);
		assistant.computeStandardConfidence(combinedRule);
		assistant.computePCAConfidence(combinedRule);
		return combinedRule;
	}


	private BitSet toBitSet(int i, int size) {
		int flag = 1;
		BitSet bitset = new BitSet(size);
		for (int k = 0 ; k < size; ++k) {
			int cflag = flag << k;
			int val = i & cflag;
			val = val >> k;
			bitset.set(k, val == 1);
		}
		return bitset;
	}

	public void printDistribution(Map<BitSet, Double> dist, List<String> labels) {
		for (int i = 0; i < labels.size() - 1; ++i) {
			System.out.print(labels.get(i) + "\t");
		}
		System.out.println(labels.get(labels.size() - 1));
		
		for (BitSet distCase : dist.keySet()) {
			System.out.println(tabularRepresentation(distCase) + "\t" + dist.get(distCase));
		}
	}

	private String tabularRepresentation(BitSet distCase) {
		StringBuilder strBuilder = new StringBuilder();
		for (int i = 0; i < distCase.size() - 1; ++i) {
			strBuilder.append(distCase.get(i) + "\t");
		}
		strBuilder.append(distCase.get(distCase.size() - 1) + "\t");
		return strBuilder.toString();
	}

	public static void main(String[] args) throws IOException {
		List<ByteString[]> triples1 = FactDatabase.triples(FactDatabase.triple("?a", "<wasBornIn>", "?c"), FactDatabase.triple("?c", "<isLocatedIn>", "?e"));
		ByteString[] head1 = FactDatabase.triple("?a", "<isCitizenOf>", "?e");
		
		List<ByteString[]> triples2 = FactDatabase.triples(FactDatabase.triple("?a", "<livesIn>", "?c"), FactDatabase.triple("?c", "<isLocatedIn>", "?e"));
		ByteString[] head2 = FactDatabase.triple("?a", "<isCitizenOf>", "?e");
		
/*		List<ByteString[]> triples1 = FactDatabase.triples(FactDatabase.triple("?a", "<dealsWith>", "?b"));
		ByteString[] head1 = FactDatabase.triple("?b", "<dealsWith>", "?a");
		
		List<ByteString[]> triples2 = FactDatabase.triples(FactDatabase.triple("<United_Kingdom>", "<dealsWith>", "?a"));
		ByteString[] head2 = FactDatabase.triple("?a", "<dealsWith>", "<United_Kingdom>");*/
		
		Query r1 = new Query(head1, triples1);
		Query r2 = new Query(head2, triples2);
		List<Query> rules = Arrays.asList(r1, r2);
		
		FactDatabase db = new FactDatabase();
		db.load(new File(args[0]));
		RuleJointDistribution distribution = new RuleJointDistribution(db);
		Query r12 = distribution.getCombinedRule(rules);
		System.out.println(r12.getBasicRuleString());
/*		
		Map<BitSet, Double> joinDistribution = distribution.getJointDistribution(r1, r2, true);
		distribution.printDistribution(joinDistribution, Arrays.asList("R1", "R2", "P(R1 ^ R2)"));
		distribution.printMarginals(joinDistribution, Arrays.asList("R1", "R2", "P(R1 ^ R2)"));
		HeadVariablesMiningAssistant assistant = new HeadVariablesMiningAssistant(db);
		assistant.computeCardinality(r1);
		assistant.computeCardinality(r2);
		assistant.computeStandardConfidence(r1);
		assistant.computeStandardConfidence(r2);
		assistant.computePCAConfidence(r1);
		assistant.computePCAConfidence(r2);
		System.out.println(r1.getRuleFullString());
		System.out.println(r2.getRuleFullString());		*/
		
	}
}
