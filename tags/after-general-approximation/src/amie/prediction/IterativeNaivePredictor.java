package amie.prediction;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Triple;
import amie.data.FactDatabase;
import amie.data.HistogramFactDatabase;
import amie.mining.AMIE;
import amie.mining.assistant.HeadVariablesMiningAssistant;
import amie.mining.assistant.MiningAssistant;
import amie.mining.assistant.TypedMiningAssistant;
import amie.query.Query;
import amie.utils.Utils;

public class IterativeNaivePredictor {
	private HistogramFactDatabase trainingDb;
	
	private FactDatabase testingDb;
	
	public static final double DefaultPCAConfidenceThreshold = 0.4;
	
	public static final int DefaultSupportThreshold = 100;
	
	public IterativeNaivePredictor(HistogramFactDatabase training) {
		this.trainingDb = training;
	}
	
	public IterativeNaivePredictor(HistogramFactDatabase training, FactDatabase testing) {
		this.trainingDb = training;
		this.testingDb = testing;
	}
	
	public List<Prediction> predict(int numberIterations, boolean onlyHits, double confidenceThreshold, boolean allowTypes) throws Exception {
		MiningAssistant assistant = null;
		if (allowTypes) {
			assistant = new TypedMiningAssistant(trainingDb);
		} else {
			assistant = new HeadVariablesMiningAssistant(trainingDb);
		}
		return predict(numberIterations, onlyHits, DefaultPCAConfidenceThreshold, assistant);
	}
	
	public List<Prediction> predict(int numberIterations, boolean onlyHits, 
			double confidenceThreshold, MiningAssistant miningAssistant) throws Exception {
		List<Prediction> resultingPredictions = new ArrayList<>();
		AMIE amieMiner = AMIE.getLossyInstance(trainingDb, confidenceThreshold, DefaultSupportThreshold);
		Map<Triple<ByteString, ByteString, ByteString>, Double> scores = new HashMap<>();
		if (onlyHits) {
			System.out.println("Including only hits");
		}
		for (int i = 0; i < numberIterations; ++i) {
			System.out.println("Inference round #" + (i + 1));
			// Mine rules using AMIE
			long startTime = System.currentTimeMillis();
			List<Query> rules = amieMiner.mine(false, Collections.EMPTY_LIST);
			System.out.println("Rule mining took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
			System.out.println(rules.size() + " rules found");
			// Build the uncertaint version of the rules
			List<Query> uncertainRules = new ArrayList<Query>();
			for(Query rule: rules) {
				// Readjust the scores
				Query newRule = getUncertainVersion(rule, scores);
				// This means the rule drops the support threshold when the uncertainty is considered.
				if (newRule != null) {
					uncertainRules.add(newRule);
					System.out.println(newRule.getBasicRuleString());
				}
			}
			// Get the predictions
			List<Prediction> predictions = JointPredictions.getPredictions(rules, trainingDb, testingDb, true);
			// First calculate the confidence of the combined rule
			int addedPredictions = 0;
			for (Prediction prediction : predictions) {
				Query combinedRule = prediction.getJointRule();
				// If there is only rule then the scores are already computed
				if (combinedRule != prediction.getRules().get(0)) {
					miningAssistant.computeCardinality(combinedRule);
					miningAssistant.computePCAConfidence(combinedRule);
				}
				computeCardinalityScore(prediction);
				double finalConfidence = prediction.getFullScore();
				if (finalConfidence >= confidenceThreshold) {
					++addedPredictions;
					scores.put(prediction.getTripleObj(), finalConfidence);
					resultingPredictions.add(prediction);
					prediction.setIterationId(i + 1);
					trainingDb.add(prediction.getTriple());
				}
			}
			System.out.println(addedPredictions + " new predictions added to the KB");
		}
		
		//PredictionsComparator predictionsCmp = new PredictionsComparator();
		//Collections.sort(resultingPredictions, predictionsCmp);
		return resultingPredictions;
	}

	/**
	 * Given a rule mined from a KB containing non-scored uncertain facts, it recalculates its metrics by considering the scores
	 * of the uncertain facts.
	 * @param rule
	 * @param scores
	 */
	private Query getUncertainVersion(Query rule,
			Map<Triple<ByteString, ByteString, ByteString>, Double> scores) {
		return rule;
/*		double uncertainCardinality = 0.0;
		double uncertainBodySize = 0.0;
		double uncertainBodyStarSize = 0.0;
		double uncertainConfidence = 0.0;
		double uncertainPcaConfidence = 0.0;
		List<ByteString[]> query = new ArrayList<>();
		for (ByteString[] triple : rule.getTriples()) {
			query.add(triple.clone());
		}
		
		double probabilityHead = 1.0;
		ByteString[] head = rule.getHead();
		if (FactDatabase.numVariables(head) == 2) {			
			try(FactDatabase.Instantiator inst1 = new FactDatabase.Instantiator(query, head[0])) {
				Set<ByteString> bindings1 = trainingDb.selectDistinct(head[0], query);
				for (ByteString val1 : bindings1) {
					List<ByteString[]> boundQuery1 = inst1.instantiate(val1);
					try(FactDatabase.Instantiator inst2 = new FactDatabase.Instantiator(boundQuery1, head[2])) {
						Set<ByteString> bindings2 = trainingDb.selectDistinct(head[2], boundQuery1);
						// At this point we have the head's score
						for (ByteString val2 : bindings2) {
							Triple<ByteString, ByteString, ByteString> triple = new Triple<ByteString, ByteString, ByteString>(val1, head[1], val2);
							Double score = scores.get(triple);
							if (score != null) {
								probabilityHead = score.doubleValue();
							}
							
							// Verify if there are still variables to bind
							List<ByteString[]> boundQuery2 = inst2.instantiate(val2);
							uncertainCardinality = probabilityHead;
							// Now compute the score for all paths that allow to conclude this value.
							List<Double> pathsScore = calculateScorePaths(boundQuery2, scores);
							double probabilityNoneOfBodyPathHolds = 1.0;
							for (Double probabilityOfPath : pathsScore) {
								probabilityNoneOfBodyPathHolds *= (1.0 - probabilityOfPath);
							}
							uncertainCardinality = probabilityHead * (1.0 - probabilityNoneOfBodyPathHolds);	
						}
					}
				}
			}			
		} else {
			System.out.println("Not yet implemented for rules with constants");
		}
		
		UncertainQuery newQuery = new UncertainQuery(rule, (int)rule.getCardinality());
		newQuery.setUncertainCardinality(uncertainCardinality);
		newQuery.setUncertainBodySize(uncertainBodySize);
		newQuery.setUncertainBodySizeStar(uncertainBodyStarSize);
		newQuery.setConfidence(uncertainConfidence);
		newQuery.setPcaConfidence(uncertainPcaConfidence);
		return newQuery;*/
	}
	
	private double uncertainSelect(ByteString var1, 
			ByteString var2, 
			List<ByteString[]> query, Map<Triple<ByteString, ByteString, ByteString>, Double> scores,
			boolean boundHead) {
		double total = 0.0;
		try(FactDatabase.Instantiator inst1 = new FactDatabase.Instantiator(query, var1)) {
			Set<ByteString> bindings1 = trainingDb.selectDistinct(var1, query);
			for (ByteString val1 : bindings1) {
				List<ByteString[]> boundQuery1 = inst1.instantiate(val1);
				try(FactDatabase.Instantiator inst2 = new FactDatabase.Instantiator(boundQuery1, var2)) {
					Set<ByteString> bindings2 = trainingDb.selectDistinct(var2, boundQuery1);
					// At this point we have the head's score
					for (ByteString val2 : bindings2) {
						// It stores the probability that some path is fullfilled
						double partialResult = 1.0;
						// Verify if there are still variables to bind
						List<ByteString[]> boundQuery2 = inst2.instantiate(val2);
						if (!FactDatabase.containsVariables(boundQuery2)) {
							// This is the good case, i.e., we found a full bound path
							// Lookup the scores in the map
							// Here we calculate the probability that none of the paths is fullfilled
							for (int i = 0; i < query.size(); ++i) {
								partialResult *= lookupScore(query.get(i), scores);
							}
							// This is the probability that some path is fullfilled
							partialResult = 1 - partialResult;
							
						} else {
							// There are still variables, so we still have to find the paths associated to those
						}
						// This is the score provide by one binding of the query variables.
						total += partialResult;
					}
				}
			}
		}
		
		return total;
	}

	private double lookupScore(ByteString[] triple,
			Map<Triple<ByteString, ByteString, ByteString>, Double> scores) {
		Double score = scores.get(new Triple(triple[0], triple[1], triple[2]));
		if (score == null) {
			return 1.0;
		} else {
			return score.doubleValue();
		}
	}

	private List<Double> calculateScorePaths(List<ByteString[]> instantiate,
			Map<Triple<ByteString, ByteString, ByteString>, Double> scores) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * For a given prediction, it computes the probability that meets soft cardinality constraints.
	 * @param prediction
	 */
	private void computeCardinalityScore(Prediction prediction) {
		// Take the most functional side of the prediction
		ByteString relation = prediction.getTriple()[1];
		long cardinality = 0;
		if (trainingDb.functionality(relation) > trainingDb.inverseFunctionality(relation)) {
			ByteString subject = prediction.getTriple()[0];
			List<ByteString[]> query = FactDatabase.triples(FactDatabase.triple(subject, relation, ByteString.of("?x")));
			cardinality = trainingDb.countDistinct(ByteString.of("?x"), query);
		} else {
			ByteString object = prediction.getTriple()[2];
			List<ByteString[]> query = FactDatabase.triples(FactDatabase.triple(ByteString.of("?x"), relation, object));
			cardinality = trainingDb.countDistinct(ByteString.of("?x"), query);
		}
		
		double functionalityScore = trainingDb.probabilityOfCardinalityGreaterThan(relation, (int)cardinality);
		prediction.setCardinalityScore(functionalityScore);		
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("IterativeNaivePredictor <trainingDb> <testingDb> <n-iterations> <onlyHitsInTest> [confidenceThreshold = 0] [allowTypes = false]");
			System.exit(1);
		}
		
		HistogramFactDatabase training = new HistogramFactDatabase();
		FactDatabase testing = new FactDatabase();
		training.load(new File(args[0]));
		double confidenceThreshold = 0.0;
		boolean allowTypes = false;
		
		if (!args[1].equals("-")) {
			testing.load(new File(args[1]));
		}
		IterativeNaivePredictor predictor = new IterativeNaivePredictor(training, testing);
		
		List<Prediction> predictions = null;
		if (args.length > 4) {
			confidenceThreshold = Double.parseDouble(args[4]);
		}
		
		if (args.length > 5) {
			allowTypes = Boolean.parseBoolean(args[5]);
		}
		
		predictions = predictor.predict(Integer.parseInt(args[2]), 
				Boolean.parseBoolean(args[3]), confidenceThreshold, allowTypes);
		
		IntHashMap<Integer> hitsInTargetHistogram = new IntHashMap<>();
		IntHashMap<Integer> hitsInTargetNotInSourceHistogram = new IntHashMap<>();
		IntHashMap<Integer> predictionsHistogram = new IntHashMap<>();
		for (Prediction prediction : predictions) {
			System.out.println(prediction);
			predictionsHistogram.increase(prediction.getRules().size());
			
			if (prediction.isHitInTarget()) {
				hitsInTargetHistogram.increase(prediction.getRules().size());
				if (!prediction.isHitInTraining()) {
					hitsInTargetNotInSourceHistogram.increase(prediction.getRules().size());
				}
			}
		}
		

		System.out.println("Predictions histogram");
		Utils.printHistogram(predictionsHistogram);
		
		System.out.println("Hits in target histogram");
		Utils.printHistogram(hitsInTargetHistogram);
		
		System.out.println("Hits In target but not in training histogram");
		Utils.printHistogram(hitsInTargetNotInSourceHistogram);
	}
}
