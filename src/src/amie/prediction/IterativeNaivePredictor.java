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
import amie.data.eval.Evaluator;
import amie.data.eval.PredictionsSampler;
import amie.mining.AMIE;
import amie.mining.assistant.HeadVariablesMiningAssistant;
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
	
	private double pcaConfidenceThreshold;
	
	public boolean allowTypes;
	
	private ProbabilisticHeadVariablesMiningAssistant miningAssistant;
	
	public IterativeNaivePredictor(HistogramTupleIndependentProbabilisticFactDatabase training, 
			boolean allowTypes) {
		this.trainingDb = training;
		this.pcaConfidenceThreshold = DefaultPCAConfidenceThreshold;
		this.allowTypes = allowTypes;
		if (allowTypes) {
			miningAssistant = new TypedProbabilisticMiningAssistant(trainingDb);
		} else {
			miningAssistant = new ProbabilisticHeadVariablesMiningAssistant(trainingDb);
		}
	}
	
	public IterativeNaivePredictor(HistogramTupleIndependentProbabilisticFactDatabase training, 
			FactDatabase testing, boolean allowTypes) {
		this.trainingDb = training;
		this.testingDb = testing;
		this.pcaConfidenceThreshold = DefaultPCAConfidenceThreshold;
		this.allowTypes = allowTypes;
	}

	
	public double getPcaConfidenceThreshold() {
		return pcaConfidenceThreshold;
	}

	public void setPcaConfidenceThreshold(double pcaConfidenceThreshold) {
		this.pcaConfidenceThreshold = pcaConfidenceThreshold;
	}

	private List<Prediction> predict(int numberIterations, boolean onlyHits) throws Exception {
		List<Prediction> resultingPredictions = new ArrayList<>();
		AMIE amieMiner = AMIE.getLossyInstance(trainingDb, pcaConfidenceThreshold, DefaultSupportThreshold);
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
				if (i > 0) { 
					// Override the support and the PCA confidence to store the
					// probabilistic version
					miningAssistant.computeProbabilisticMetrics(rule);
					if (rule.getProbabilisticSupport() >= DefaultSupportThreshold
							&& rule.getProbabilisticPCAConfidence() >= pcaConfidenceThreshold) {
						finalRules.add(rule);
						System.out.println(rule.getFullRuleString());
					}
				}
			}
			System.out.println(rules.size() + " rules found");
			
			// Get the predictions
			int newPredictions = getPredictions(finalRules, true, resultingPredictions, i);
			System.out.println(newPredictions + " new predictions");			
			if (newPredictions == 0) {
				break;
			}
		}
		
		PredictionsComparator predictionsCmp = new PredictionsComparator();
		Collections.sort(resultingPredictions, predictionsCmp);
		return resultingPredictions;
	}
	
	/**
	 * Add a pair triple/query to a map.
	 * @param predictions
	 * @param prediction
	 * @param q
	 */
	private void add2Map(
			Map<Triple<ByteString, ByteString, ByteString>, List<Query>> predictions,
			Triple<ByteString, ByteString, ByteString> prediction, Query q) {
		// TODO Auto-generated method stub
		List<Query> queries = predictions.get(prediction);
		if (queries == null) {
			queries = new ArrayList<>();
			predictions.put(prediction, queries);
		}
		queries.add(q);
	}
	

	/**
	 * Given a set of rules, it runs an iteration of deduction and returns a map
	 * prediction -> {list of rules that deduced this prediction}
	 * @param queries
	 * @param trainingDataset
	 * @param targetDataset
	 * @param notInTraining 
	 * @return
	 */
	private Map<Triple<ByteString, ByteString, ByteString>, List<Query>> 
	calculatePredictions2RulesMap(List<Query> queries, boolean notInTraining) {
		
		Map<Triple<ByteString, ByteString, ByteString>, List<Query>> predictions = new HashMap<>();
		HeadVariablesMiningAssistant assistant = new HeadVariablesMiningAssistant(trainingDb);
		PredictionsSampler predictor = new PredictionsSampler(trainingDb);
		
		for (Query q : queries) {
			ByteString[] head = q.getHead();
			q.setFunctionalVariablePosition(Query.findFunctionalVariable(q, trainingDb));
			assistant.computeCardinality(q);
			assistant.computePCAConfidence(q);
			 
			Object bindings = null;
			try {
				if (notInTraining) {
					bindings = predictor.generatePredictions(q);
				} else {
					bindings = predictor.generateBodyBindings(q);
				}
			} catch (Exception e) {
				continue;
			}
			
			if(FactDatabase.numVariables(head) == 1){
				Set<ByteString> oneVarBindings = (Set<ByteString>)bindings;
				for(ByteString binding: oneVarBindings){
					Triple<ByteString, ByteString, ByteString> t = 
							new Triple<>(ByteString.of("?a"), head[1], ByteString.of("?b"));
					if (q.getFunctionalVariablePosition() == 0) {
						t.first = binding;
					} else {
						t.third = binding;
					}
					// Add the pair prediction, query
					add2Map(predictions, t, q);
				}
			}else{
				Map<ByteString, IntHashMap<ByteString>> twoVarsBindings = 
						(Map<ByteString, IntHashMap<ByteString>>)bindings;
				for(ByteString value1: twoVarsBindings.keySet()){
					for(ByteString value2: twoVarsBindings.get(value1)){
						Triple<ByteString, ByteString, ByteString> t = 
								new Triple<>(ByteString.of("?a"), head[1], ByteString.of("?b"));
						if(q.getFunctionalVariablePosition() == 0){
							t.first = value1;
							t.third = value2;
						}else{
							t.first = value2;
							t.third = value1;					
						}
						add2Map(predictions, t, q);
					}
				}
			}
		}
		
		return predictions;
	}
	
	/**
	 * Returns the list of all the predictions made by the given rules on the training dataset.
	 * The correctness of the predictions is verified in the target dataset. 
	 * @param queries
	 * @param trainingDataset
	 * @param targetDataset
	 * @return int
	 */
	private int getPredictions(List<Query> queries, 
			boolean notInTraining, List<Prediction> result, 
			int iteration) {
		Map<Triple<ByteString, ByteString, ByteString>, List<Query>> predictions =
				calculatePredictions2RulesMap(queries, notInTraining);
		int count = 0;
		
		for (Triple<ByteString, ByteString, ByteString> t : predictions.keySet()) {
			Prediction prediction = new Prediction(t);
			prediction.getRules().addAll(predictions.get(t));
			ByteString triple[] = prediction.getTriple();
			int eval = Evaluator.evaluate(triple, testingDb, trainingDb);
			if(eval == 0) { 
				prediction.setHitInTarget(true);
			}
			
			if(trainingDb.count(triple) > 0) {
				prediction.setHitInTraining(true);
			}
			
			// First calculate the confidence of the combined rule
			Query combinedRule = prediction.getJointRule();
			
			if (combinedRule != prediction.getRules().get(0)) {
				if (iteration == 0) {
					miningAssistant.computeCardinality(combinedRule);
					miningAssistant.computePCAConfidence(combinedRule);						
					computeCardinalityScore(prediction, false);
				} else {
					miningAssistant.computeProbabilisticMetrics(combinedRule);
					computeCardinalityScore(prediction, true);
				}
			}
			
			double finalConfidence = prediction.getFullScore();
			if (finalConfidence >= pcaConfidenceThreshold) {
				prediction.setIterationId(iteration);
				ByteString[] tripleArray = prediction.getTriple();
				trainingDb.add(tripleArray[0], tripleArray[1], tripleArray[2], finalConfidence);
				result.add(prediction);
				++count;
			}
		}
		
		return count;
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
		
		List<Prediction> predictions = null;
		if (args.length > 4) {
			confidenceThreshold = Double.parseDouble(args[4]);
		}
		
		if (args.length > 5) {
			allowTypes = Boolean.parseBoolean(args[5]);
		}
		
		IterativeNaivePredictor predictor = 
				new IterativeNaivePredictor(training, testing, allowTypes);
		predictor.setPcaConfidenceThreshold(confidenceThreshold);
		predictions = predictor.predict(Integer.parseInt(args[2]), 
				Boolean.parseBoolean(args[3]));
		
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
