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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Triple;
import amie.data.FactDatabase;
import amie.data.eval.Evaluator;
import amie.data.eval.PredictionsSampler;
import amie.mining.AMIE;
import amie.mining.Metric;
import amie.prediction.assistant.ProbabilisticHeadVariablesMiningAssistant;
import amie.prediction.assistant.TypedProbabilisticMiningAssistant;
import amie.prediction.data.HistogramTupleIndependentProbabilisticFactDatabase;
import amie.query.Query;
import amie.utils.Utils;

public class IterativeNaivePredictor {
	private HistogramTupleIndependentProbabilisticFactDatabase trainingDb;
	
	private FactDatabase testingDb;
	
	private AMIE amieMiner;

	public static final double DefaultPCAConfidenceThreshold = 0.4;
	
	public static final int DefaultInitialSupportThreshold = 10;
	
	public static final int DefaultSupportThreshold = 10;
	
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
	
	public AMIE getAmieMiner() {
		return amieMiner;
	}

	public void setAmieMiner(AMIE amieMiner) {
		this.amieMiner = amieMiner;
	}

	/**
	 * 
	 * @param numberIterations
	 * @param onlyHits
	 * @return
	 * @throws Exception
	 */
	private List<Prediction> predict(int numberIterations, boolean onlyHits) throws Exception {
		List<Prediction> resultingPredictions = new ArrayList<>();
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
					if (rule.getHeadCoverage() < DefaultHeadCoverageThreshold
							|| rule.getPcaConfidence() < pcaConfidenceThreshold) {
						continue;
					}
				}
				rule.setId(ruleId); // Assign an integer identifier for hashing purposes
				++ruleId;
				finalRules.add(rule);
			}
			
			System.out.println(finalRules.size() + " used to fire predictions");
			
			// Get the predictions
			getPredictions(finalRules, true, i, predictionsAtIterationI);
			System.out.println(predictionsAtIterationI.size() + " predictions added to the KB");			
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
		System.out.println(predictions.size() + " predictions deduced from the KB");
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
		
		System.out.println((System.currentTimeMillis() - timeStamp1) + " milliseconds to calculate the scores for predictions");
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
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cli = null;
        // create the command line parser
        CommandLineParser parser = new PosixParser();
        // create the Options
        Options options = new Options();
		
        // Arguments
        double minMetricValue = 0.0;
        int minInitialSupport = DefaultInitialSupportThreshold;
        int minSupport = DefaultSupportThreshold;
        double minHeadCoverage = DefaultHeadCoverageThreshold;
		double confidenceThreshold = DefaultPCAConfidenceThreshold;
		boolean allowTypes = false;
		Metric metric = Metric.HeadCoverage;
		int numberOfIterations = Integer.MAX_VALUE;
		boolean addOnlyVerifiedPredictions = false;
		List<Prediction> predictions = null;
        
        Option supportOpt = OptionBuilder.withArgName("min-support")
                .hasArg()
                .withDescription("Minimum absolute support. Default: 100 positive examples")
                .create("mins");
       
        Option initialSupportOpt = OptionBuilder.withArgName("min-initial-support")
                .hasArg()
                .withDescription("Minimum size of the relations to be considered as head relations. Default: 100 (facts or entities depending on the bias)")
                .create("minis");

        Option headCoverageOpt = OptionBuilder.withArgName("min-head-coverage")
                .hasArg()
                .withDescription("Minimum head coverage. Default: 0.01")
                .create("minhc");

        Option pruningMetricOpt = OptionBuilder.withArgName("pruning-metric")
                .hasArg()
                .withDescription("Metric used for pruning of intermediate queries: support|headcoverage. Default: headcoverage")
                .create("pm");
        
        Option pcaConfThresholdOpt = OptionBuilder.withArgName("min-pca-confidence")
                .hasArg()
                .withDescription("Minimum PCA confidence threshold. This value is not used for pruning, only for filtering of the results. Default: 0.0")
                .create("minpca");
        
        Option testingKBOption = OptionBuilder.withArgName("testing-kb")
        		.hasArg()
        		.withDescription("List of files containing the testing KB, used to validate the predictions")
        		.create("tkb");
        
        Option iterationsOption = OptionBuilder.withArgName("number-iterations")
        		.hasArg()
        		.withDescription("Number of iterations")
        		.create("ni");
        
        options.addOption(supportOpt);
        options.addOption(initialSupportOpt);
        options.addOption(headCoverageOpt);
        options.addOption(pruningMetricOpt);
        options.addOption(pcaConfThresholdOpt);
        options.addOption(testingKBOption);
        options.addOption(iterationsOption);
        
        try {
            cli = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println("Unexpected exception: " + e.getMessage());
            formatter.printHelp("AMIE", options);
            System.exit(1);
        }
        
        String[] inputFileArgs = cli.getArgs();
        if (inputFileArgs.length < 1) {
            System.err.println("No input file has been provided");
            System.err.println("AMIEPredictor [OPTIONS] <.tsv INPUT FILES>");
            formatter.printHelp("AMIEPredictor", options);
            System.exit(1);
        }
		HistogramTupleIndependentProbabilisticFactDatabase training = new HistogramTupleIndependentProbabilisticFactDatabase();
        FactDatabase testing = new FactDatabase();
        
		File[] inputFilesArray = new File[inputFileArgs.length];
		for (int i = 0; i < inputFileArgs.length; ++i) {
        	inputFilesArray[i] = new File(inputFileArgs[i]);
        }
		training.load(inputFilesArray);
		
		if (args.length < 4) {
			System.err.println("IterativeNaivePredictor <trainingDb> <testingDb> <n-iterations> <onlyHitsInTest> [confidenceThreshold = 0] [allowTypes = false]");
			System.exit(1);
		}
		
		if (cli.hasOption("tkb")) {
			String[] testingFileArgs = cli.getOptionValue("tkb").split(":");
			File[] testingFilesArray = new File[testingFileArgs.length];
			for (int i = 0; i < testingFileArgs.length; ++i) {
				testingFilesArray[i] = new File(testingFileArgs[i]);
			}
			testing.load(testingFilesArray);
		}
		
	
		if (cli.hasOption("minpca")) {
			try {
				confidenceThreshold = Double.parseDouble(cli.getOptionValue("minpca"));
			} catch (NumberFormatException e) {
				System.err.println("The argument minpca (minimal confidence threshold) expects a real number between 0 and 1");
				formatter.printHelp("AMIEPredictor", options);
				System.exit(1);
			}
			
		}
		
		if (cli.hasOption("minis")) {
            try {
                minInitialSupport = Integer.parseInt(cli.getOptionValue("minis"));
            } catch (NumberFormatException e) {
				System.err.println("The argument minis (minimal initial support) expects a non-negative positive integer");
				formatter.printHelp("AMIEPredictor", options);
				System.exit(1);
            }
        }

        if (cli.hasOption("minhc")) {
            try {
                minHeadCoverage = Double.parseDouble(cli.getOptionValue("minhc"));
            } catch (NumberFormatException e) {
            	System.err.println("The argument minhc (minimal head coverage) expects a real number between 0 and 1");
				formatter.printHelp("AMIEPredictor", options);
				System.exit(1);
            }
        }
        
        if (cli.hasOption("pm")) {
            switch (cli.getOptionValue("pm")) {
                case "support":
                    metric = Metric.Support;
                    System.err.println("Using " + metric + " as pruning metric with threshold " + minSupport);
                    minMetricValue = minSupport;
                    minInitialSupport = minSupport;
                    break;
                default:
                    metric = Metric.HeadCoverage;
                    System.err.println("Using " + metric + " as pruning metric with threshold " + minHeadCoverage);
                    minMetricValue = minHeadCoverage;
                    break;
            }
        } else {
        	System.out.println("Using " + metric + " as pruning metric with minimum threshold " + minHeadCoverage);
            minMetricValue = minHeadCoverage;
            minInitialSupport = minSupport;
        }
		
		if (args.length > 5) {
			allowTypes = Boolean.parseBoolean(args[5]);
		}
		
		try {
			numberOfIterations = Integer.parseInt(args[2]);
		} catch(NumberFormatException e) {
			
		}
		
		addOnlyVerifiedPredictions = Boolean.parseBoolean(args[3]);
		
		IterativeNaivePredictor predictor = 
				new IterativeNaivePredictor(training, testing, allowTypes);
		predictor.setPcaConfidenceThreshold(confidenceThreshold);
		predictions = predictor.predict(numberOfIterations, addOnlyVerifiedPredictions);
		
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