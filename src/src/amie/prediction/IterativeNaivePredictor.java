package amie.prediction;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import amie.data.FactDatabase;
import amie.mining.AMIE;
import amie.prediction.assistant.ProbabilisticHeadVariablesMiningAssistant;
import amie.prediction.assistant.TypedProbabilisticMiningAssistant;
import amie.prediction.data.HistogramTupleIndependentProbabilisticFactDatabase;
import amie.query.Query;
import amie.utils.Utils;

public class IterativeNaivePredictor {
	private HistogramTupleIndependentProbabilisticFactDatabase trainingDb;
	
	private FactDatabase testingDb;
	
	public static final double DefaultPCAConfidenceThreshold = 0.4;
	
	public static final int DefaultSupportThreshold = 100;
	
	public IterativeNaivePredictor(HistogramTupleIndependentProbabilisticFactDatabase training) {
		this.trainingDb = training;
	}
	
	public IterativeNaivePredictor(HistogramTupleIndependentProbabilisticFactDatabase training, FactDatabase testing) {
		this.trainingDb = training;
		this.testingDb = testing;
	}
	
	public List<Prediction> predict(int numberIterations, boolean onlyHits, double confidenceThreshold, boolean allowTypes) throws Exception {
		ProbabilisticHeadVariablesMiningAssistant assistant = null;
		if (allowTypes) {
			assistant = new TypedProbabilisticMiningAssistant(trainingDb);
		} else {
			assistant = new ProbabilisticHeadVariablesMiningAssistant(trainingDb);
		}
		return predict(numberIterations, onlyHits, confidenceThreshold, assistant);
	}
	
	public List<Prediction> predict(int numberIterations, boolean onlyHits, 
			double confidenceThreshold, ProbabilisticHeadVariablesMiningAssistant miningAssistant) throws Exception {
		List<Prediction> resultingPredictions = new ArrayList<>();
		AMIE amieMiner = AMIE.getLossyInstance(trainingDb, confidenceThreshold, DefaultSupportThreshold);
		if (onlyHits) {
			System.out.println("Including only hits");
		}
		for (int i = 0; i < numberIterations; ++i) {
			System.out.println("Inference round #" + (i + 1));
			// Mine rules using AMIE
			long startTime = System.currentTimeMillis();
			List<Query> rules = amieMiner.mine(false, Collections.EMPTY_LIST);
			System.out.println("Rule mining took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
			// Build the uncertain version of the rules
			List<Query> finalRules = new ArrayList<Query>();
			for(Query rule: rules) {
				// Readjust the scores
				if (i > 0) { // Only the latter iterations will contain fuzzy facts.
					miningAssistant.computeProbabilisticMetrics(rule);
					if (rule.getProbabilisticSupport() >= DefaultSupportThreshold
							&& rule.getProbabilisticPCAConfidence() >= confidenceThreshold) {
						finalRules.add(rule);
						System.out.println(rule.getFullRuleString());
					}
				} else {
					finalRules.add(rule);
					System.out.println(rule.getFullRuleString());
				}
			}
			System.out.println(rules.size() + " rules found");
			// Get the predictions
			List<Prediction> predictions = JointPredictions.getPredictions(finalRules, trainingDb, testingDb, true);
			int addedPredictions = 0;
			for (Prediction prediction : predictions) {
				// First calculate the confidence of the combined rule
				Query combinedRule = prediction.getJointRule();
				if (combinedRule != prediction.getRules().get(0)) {
					if (i == 0) {
						miningAssistant.computeCardinality(combinedRule);
						miningAssistant.computePCAConfidence(combinedRule);						
						computeCardinalityScore(prediction, false);
					} else {
						miningAssistant.computeProbabilisticMetrics(combinedRule);
						computeCardinalityScore(prediction, true);
					}
				}
				
				double finalConfidence = prediction.getFullScore();
				if (finalConfidence >= confidenceThreshold) {
					++addedPredictions;
					resultingPredictions.add(prediction);
					prediction.setIterationId(i + 1);
					ByteString[] triple = prediction.getTriple();
					trainingDb.add(triple[0], triple[1], triple[2], finalConfidence);
				}
			}
			System.out.println(addedPredictions + " new predictions added to the KB");
		}
		
		PredictionsComparator predictionsCmp = new PredictionsComparator();
		Collections.sort(resultingPredictions, predictionsCmp);
		return resultingPredictions;
	}
	
	/**
	 * For a given prediction, it computes the probability that meets soft cardinality constraints.
	 * @param prediction
	 */
	private void computeCardinalityScore(Prediction prediction, boolean probabilistic) {
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
		
		HistogramTupleIndependentProbabilisticFactDatabase training = new HistogramTupleIndependentProbabilisticFactDatabase();
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
