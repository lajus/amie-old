package amie.prediction;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Triple;
import amie.data.FactDatabase;
import amie.data.eval.Evaluator;
import amie.data.eval.PredictionsSampler;
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
	
	public static final int DefaultInitialSupportThreshold = 10;
	
	public static final double DefaultHeadCoverageThreshold = 0.01;
	
	private double pcaConfidenceThreshold;
	
	private double headCoverageThreshold;
	
	public boolean allowTypes;
	
	private ProbabilisticHeadVariablesMiningAssistant miningAssistant;
	
	public IterativeNaivePredictor(HistogramTupleIndependentProbabilisticFactDatabase training, 
			boolean allowTypes) {
		this.trainingDb = training;
		this.pcaConfidenceThreshold = DefaultPCAConfidenceThreshold;
		this.headCoverageThreshold = DefaultHeadCoverageThreshold;
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
		this.headCoverageThreshold = DefaultHeadCoverageThreshold;
		this.allowTypes = allowTypes;
		if (allowTypes) {
			miningAssistant = new TypedProbabilisticMiningAssistant(trainingDb);
		} else {
			miningAssistant = new ProbabilisticHeadVariablesMiningAssistant(trainingDb);
		}
	}

	
	public double getPcaConfidenceThreshold() {
		return pcaConfidenceThreshold;
	}

	public void setPcaConfidenceThreshold(double pcaConfidenceThreshold) {
		this.pcaConfidenceThreshold = pcaConfidenceThreshold;
	}

	public double getHeadCoverageThreshold() {
		return headCoverageThreshold;
	}

	public void setHeadCoverageThreshold(double headCoverageThreshold) {
		this.headCoverageThreshold = headCoverageThreshold;
	}

	private List<Prediction> predict(int numberIterations, boolean onlyHits) throws Exception {
		List<Prediction> resultingPredictions = new ArrayList<>();
		//AMIE amieMiner = AMIE.getLossyInstance(trainingDb, pcaConfidenceThreshold, DefaultSupportThreshold);
		AMIE amieMiner = AMIE.getLossyVanillaSettingInstance(trainingDb, pcaConfidenceThreshold, DefaultInitialSupportThreshold);
		if (onlyHits) {
			System.out.println("Including only hits");
		}
		for (int i = 0; i < numberIterations; ++i) {
			System.out.println("Inference round #" + (i + 1));
			List<Prediction> predictionsAtIterationI = new ArrayList<>();
			// Mine rules using AMIE
			long startTime = System.currentTimeMillis();
			List<Query> rules = amieMiner.mine(false, Collections.EMPTY_LIST);
			System.out.println("Rule mining took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
			System.out.println(rules.size() + " rules found");
			// Build the uncertain version of the rules
			List<Query> finalRules = new ArrayList<Query>();
			int ruleId = 1; // We will assign each rule a unique identifier for hashing purposes.
			for (Query rule: rules) {
				if (i > 0) { 
					// Override the support and the PCA confidence to store the
					// probabilistic version
					miningAssistant.computeProbabilisticMetrics(rule);					
					if (rule.getSupport() >= DefaultInitialSupportThreshold
							&& rule.getPcaConfidence() >= pcaConfidenceThreshold) {
						rule.setId(ruleId); // Assign an integer identifier for hashing purposes
						++ruleId;
						finalRules.add(rule);
					}
				} else {
					rule.setId(ruleId); // Assign an integer identifier for hashing purposes
					++ruleId;
					finalRules.add(rule);					
				}
			}
			
			System.out.println(finalRules.size() + " used to fire predictions");
			
			// Get the predictions
			getPredictions(finalRules, true, i, predictionsAtIterationI);
			System.out.println(predictionsAtIterationI.size() + " new predictions");			
			resultingPredictions.addAll(predictionsAtIterationI);
			if (predictionsAtIterationI.size() == 0) {
				break;
			}
		}
		System.out.println("Iterations are over");
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
		PredictionsSampler predictor = new PredictionsSampler(trainingDb);
		
		for (Query q : queries) {
			ByteString[] head = q.getHead();
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
	
	class PredictionsBuilder implements Runnable {
		private Map<Triple<ByteString, ByteString, ByteString>, List<Query>> items;
		private List<Prediction> result;
		private int iteration;
		
		public PredictionsBuilder(Map<Triple<ByteString, ByteString, ByteString>, List<Query>> items,
				List<Prediction> result, Map<List<Integer>, Query> combinedRulesMap, int iteration) {
			this.items = items;			
			this.result = result;
			this.iteration = iteration;
		}
		
		@Override
		public void run() {
			Map<List<Integer>, Query> combinedRulesMap = new HashMap<List<Integer>, Query>();
			while (true) {
				Triple<ByteString, ByteString, ByteString> nextTriple = null;
				List<Query> rules = null;
				synchronized (items) {
					Iterator<Triple<ByteString, ByteString, ByteString>> it = items.keySet().iterator();
					try {
						nextTriple = it.next();
						rules = items.get(nextTriple);
						items.remove(nextTriple);
					} catch (NoSuchElementException e) {
						return;
					}
				}
				Prediction prediction = new Prediction(nextTriple);		
				List<Integer> ruleIds = new ArrayList<Integer>();
				for (Query rule : rules) {
					ruleIds.add(rule.getId());
					prediction.getRules().add(rule);
				}
				// Avoid recomputing joint rules for each prediction (there can be many)
				Collections.sort(ruleIds);
				
				Query combinedRule = combinedRulesMap.get(ruleIds);
				if (combinedRule != null) {
					prediction.setJointRule(combinedRule);
				} else {
					combinedRule = prediction.getJointRule();
					combinedRulesMap.put(ruleIds, combinedRule);
					if (iteration == 0) {
						miningAssistant.computeCardinality(combinedRule);
						miningAssistant.computePCAConfidence(combinedRule);
					} else {
						miningAssistant.computeProbabilisticMetrics(combinedRule);
					}
				}	
				combinedRule = prediction.getJointRule();				
				computeCardinalityScore(prediction);
				
				ByteString triple[] = prediction.getTriple();
				int eval = Evaluator.evaluate(triple, trainingDb, testingDb);
				if(eval == 0) { 
					prediction.setHitInTarget(true);
				}
				
				if(trainingDb.count(triple) > 0) {
					prediction.setHitInTraining(true);
				}
				
				double finalConfidence = prediction.getFullScore();
				if (finalConfidence >= pcaConfidenceThreshold) {
					prediction.setIterationId(iteration);
					synchronized (result) {
						result.add(prediction);	
					}
				}
			}
		}
	}
	
	/**
	 * Returns the list of all the predictions made by the given rules on the training dataset.
	 * The correctness of the predictions is verified in the target dataset. 
	 * @param queries
	 * @param trainingDataset
	 * @param targetDataset
	 * @return int
	 * @throws InterruptedException 
	 */
	private void getPredictions(List<Query> queries, 
			boolean notInTraining, int iteration, 
			List<Prediction> output) throws InterruptedException {
		long timeStamp1 = System.currentTimeMillis();
		Map<Triple<ByteString, ByteString, ByteString>, List<Query>> predictions =
				calculatePredictions2RulesMap(queries, notInTraining);
		System.out.println((System.currentTimeMillis() - timeStamp1) + " milliseconds for calculatePredictions2RulesMap");
		System.out.println(predictions.size() + " predictions from the KB");
		// We keep a map with the combined rules in order to avoid combining rules
		// multiple times.
		Map<List<Integer>, Query> combinedRulesMap = new HashMap<List<Integer>, Query>();
		timeStamp1 = System.currentTimeMillis();
		Thread[] threads = new Thread[2];
		for (int i = 0; i < threads.length; ++i) {
			threads[i] = new Thread(new PredictionsBuilder(predictions, output, combinedRulesMap, iteration));
			threads[i].start();
		}
		
		for (int i = 0; i < threads.length; ++i) {
			threads[i].join();
		}
		
		for (Prediction prediction : output) {
			ByteString[] t = prediction.getTriple();
			trainingDb.add(t[0], t[1], t[2], prediction.getFullScore());
		}
		
		System.out.println((System.currentTimeMillis() - timeStamp1) + " to calculate the scores for predictions");
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
