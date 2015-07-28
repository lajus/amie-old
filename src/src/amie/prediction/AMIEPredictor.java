package amie.prediction;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Triple;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import telecom.util.collections.MultiMap;
import amie.data.FactDatabase;
import amie.data.eval.Evaluator;
import amie.data.eval.PredictionsSampler;
import amie.mining.AMIE;
import amie.mining.Metric;
import amie.mining.assistant.DefaultMiningAssistant;
import amie.mining.assistant.MiningAssistant;
import amie.mining.assistant.RelationSignatureDefaultMiningAssistant;
import amie.prediction.data.HistogramTupleIndependentProbabilisticFactDatabase;
import amie.prediction.data.TupleIndependentFactDatabase;
import amie.query.Query;
import amie.utils.Utils;

public class AMIEPredictor {
	private HistogramTupleIndependentProbabilisticFactDatabase trainingKb;
	
	private FactDatabase testingKb;
	
	private AMIE ruleMiner;

	public static final double DefaultPCAConfidenceThreshold = 0.4;
	
	public static final int DefaultInitialSupportThreshold = 10;
	
	public static final int DefaultSupportThreshold = 10;
	
	public static final double DefaultHeadCoverageThreshold = 0.01;
	
	public static final int DefaultSampleSize = 30;
	
	private double pcaConfidenceThreshold;
	
	private Metric pruningMetric;
	
	private double pruningThreshold;
	
	private int numberOfCoresEvaluation;
	
	private boolean rewriteProbabilities;
		
	private MiningAssistant miningAssistant;
	
	private boolean standardMining;

	private PredictionMetric predictionsMetric;
	
	public AMIEPredictor(AMIE miner) {
		this.miningAssistant = miner.getAssistant();
		this.trainingKb = (HistogramTupleIndependentProbabilisticFactDatabase) this.miningAssistant.getKb();
		this.pcaConfidenceThreshold = this.miningAssistant.getPcaConfidenceThreshold();
		this.pruningMetric = miner.getPruningMetric();
		this.ruleMiner = miner;
		this.numberOfCoresEvaluation = 1;
		this.rewriteProbabilities = true;
	}
	
	public AMIEPredictor(AMIE miner, FactDatabase testing) {
		this.miningAssistant = miner.getAssistant();
		this.trainingKb = (HistogramTupleIndependentProbabilisticFactDatabase) this.miningAssistant.getKb();
		this.pcaConfidenceThreshold = this.miningAssistant.getPcaConfidenceThreshold();
		this.pruningMetric = miner.getPruningMetric();
		this.ruleMiner = miner;
		this.testingKb = testing;
		this.numberOfCoresEvaluation = 1;
		this.rewriteProbabilities = true;
	}
	
	public double getPcaConfidenceThreshold() {
		return pcaConfidenceThreshold;
	}

	public void setPcaConfidenceThreshold(double pcaConfidenceThreshold) {
		this.pcaConfidenceThreshold = pcaConfidenceThreshold;
	}

	public double getPruningThreshold() {
		return pruningThreshold;
	}
	
	public void setPruningThreshold(double threshold) {
		this.pruningThreshold = threshold;
	}

	public void setHeadCoverageThreshold(double headCoverageThreshold) {
		this.pruningThreshold = headCoverageThreshold;
	}
	
	public AMIE getRuleMiner() {
		return ruleMiner;
	}

	public void setRuleMiner(AMIE amieMiner) {
		this.ruleMiner = amieMiner;
	}
	
	public int getNumberOfCoresForEvaluation() {
		return numberOfCoresEvaluation;
	}
	
	public void setNumberOfCoresForEvaluation(int numberOfCoresEvaluation) {
		this.numberOfCoresEvaluation = numberOfCoresEvaluation;
	}

	public boolean isStandardMining() {
		return standardMining;
	}

	public void setStandardMining(boolean standardMining) {
		this.standardMining = standardMining;
	}
	
	public PredictionMetric getPredictionMetric() {
		return this.predictionsMetric;
	}
	
	public void setPredictionMetric(PredictionMetric pmetric) {
		this.predictionsMetric = pmetric;
	}
	
	public void setRewriteProbabilities(boolean rewriteProbabilities) {
		this.rewriteProbabilities = rewriteProbabilities;
	}
	
	public boolean getRewriteProbabilities() {
		return this.rewriteProbabilities;
	}

	/**
	 * 
	 * @param numberIterations
	 * @param onlyHits
	 * @return
	 * @throws Exception
	 */
	private Set<Prediction> predict(int numberIterations, boolean onlyHits) throws Exception {
		Set<Prediction> resultingPredictions = new LinkedHashSet<Prediction>();
		Set<Prediction> previousPredictions = new LinkedHashSet<Prediction>();
		if (onlyHits) {
			System.out.println("Including only hits");
		}
		for (int i = 0; i < numberIterations; ++i) {
			System.out.println("Inference round #" + (i + 1));
			// Mine rules using AMIE
			long startTime = System.currentTimeMillis();
			List<Query> rules = null;
			if (this.standardMining) {
				rules = ruleMiner.mine(false, Collections.EMPTY_LIST);
			} else {
				rules = ruleMiner.mine(false, Collections.EMPTY_LIST);
			}
			System.out.println("Rule mining took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
			System.out.println(rules.size() + " rules found");
			System.out.println("Using " + this.numberOfCoresEvaluation + " threads to re-evaluate the rules (probabilistic scores).");
			// Build the uncertain version of the rules
			List<Query> finalRules = new ArrayList<Query>();
			AtomicInteger ruleId = new AtomicInteger(1); // We will assign each rule a unique identifier for hashing purposes.
			Thread[] threads = new Thread[this.numberOfCoresEvaluation]; 
			for (int k = 0; k < threads.length; ++k) {
				threads[k] = new Thread(new RuleEvaluator(rules, i, finalRules, ruleId));
				threads[k].start();
			}
			
			for (int k = 0; k < threads.length; ++k) {
				threads[k].join();
			}
			
			System.out.println(finalRules.size() + " used to fire predictions");
			for (Query r : finalRules)
				System.out.println(r.getBasicRuleString());
			
			// Get the predictions
			resultingPredictions = new LinkedHashSet<>();
			IterationInformation iInfo = getPredictions(finalRules, true, i, resultingPredictions);	
			
			System.out.println(iInfo);
			System.out.println("Size of the KB: " + trainingKb.size());
			
			if (this.rewriteProbabilities) {
				if (iInfo.averageChange() < 0.00001) {					
					break;
				}
			} else {
				if (iInfo.newFacts == 0) {
					break;
				}
			}
			
			// If there is no convergence, remove those predictions that remained
			// without any support.
			if (!previousPredictions.isEmpty()) {
				System.out.println("Removing unsupported predictions");
				previousPredictions.removeAll(resultingPredictions);
				System.out.println(previousPredictions.size() + " predictions will be removed");
				// The remaining predictions have to removed
				for (Prediction prediction : previousPredictions) {
					ByteString[] triple = prediction.getTriple();
					trainingKb.delete(triple[0], triple[1], triple[2]);
				}
			}
			
			previousPredictions = resultingPredictions;
		}
		
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
		PredictionsSampler predictor = new PredictionsSampler(trainingKb);
		
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
						t.third = head[2];
					} else {
						t.third = binding;
						t.first = head[0];
					}
					// Add the pair prediction, rule
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
	
	class RuleEvaluator implements Runnable {

		private Collection<Query> rules;
		private int iteration;
		private Collection<Query> output;
		private AtomicInteger ruleId;
		
		
		public RuleEvaluator(Collection<Query> rules, int iteration, Collection<Query> output, AtomicInteger ruleId) {
			this.rules = rules;
			this.iteration = iteration;
			this.output = output;
			this.ruleId = ruleId;
		}
		
		@Override
		public void run() {
			Query rule = null;
			while (true) {
				synchronized (this.rules) {
					rule = telecom.util.collections.Collections.poll(this.rules);
				}				
				
				if (rule == null) {
					break;
				}
				
				if (iteration > 0) { 
					// Override the support and the PCA confidence to store the
					// probabilistic version
					Utilities.computeProbabilisticMetrics(rule, trainingKb);
					boolean pruningCondition = false;
					if (pruningMetric == Metric.HeadCoverage) {
						pruningCondition = rule.getHeadCoverage() < pruningThreshold;
					} else if (pruningMetric == Metric.Support) {
						pruningCondition = rule.getSupport() < pruningThreshold;
					}
					pruningCondition = pruningCondition 
							|| rule.getPcaConfidence() < pcaConfidenceThreshold;
					if (pruningCondition) {
						continue;
					}
				}				
				rule.setId(ruleId.incrementAndGet()); // Assign an integer identifier for hashing purposes
				synchronized(output) {
					output.add(rule);	
				}
			}			
		}
	}
	
	class PredictionsBuilder implements Runnable {
		private Map<Triple<ByteString, ByteString, ByteString>, List<Query>> items;
		private List<Prediction> result;
		private int iteration;
		
		public PredictionsBuilder(Map<Triple<ByteString, ByteString, ByteString>, List<Query>> items,
				List<Prediction> result, int iteration) {
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
						break;
					}
				}
				
				if (trainingKb.isCertain(nextTriple)) {
					continue;
				}
				
				Prediction prediction = new Prediction(nextTriple);		
				List<Integer> ruleIds = new ArrayList<Integer>();
				for (Query rule : rules) {
					ruleIds.add(rule.getId());
					prediction.getRules().add(rule);
				}
				// Avoid recomputing joint rules for each prediction (there can be many)
				Collections.sort(ruleIds);

				if (predictionsMetric == PredictionMetric.JointConfidence ||
						predictionsMetric == PredictionMetric.JointScoreTimesFuncScore) {
					Query combinedRule = combinedRulesMap.get(ruleIds);				
					if (combinedRule != null) {
						prediction.setJointRule(combinedRule);
					} else {
						combinedRule = prediction.getJointRule();
						if (iteration == 0 || combinedRule.getLength() > 6) {
							miningAssistant.computeCardinality(combinedRule);
							miningAssistant.computePCAConfidence(combinedRule);
						} else {
							Utilities.computeProbabilisticMetrics(combinedRule, 
									(TupleIndependentFactDatabase) miningAssistant.getKb());
						}
						combinedRulesMap.put(ruleIds, combinedRule);
					}	
				}
				computeCardinalityScore(prediction);
				
				ByteString triple[] = prediction.getTriple();
				int eval = Evaluator.evaluate(triple, trainingKb, testingKb);
				if(eval == 0) { 
					prediction.setHitInTarget(true);
				}
				
				prediction.setIterationId(iteration);
				synchronized (result) {
					result.add(prediction);	
				}
			}
		}
	}
	
	class IterationInformation {
		public int newFacts;
		
		public int totalConclusions;
		
		public double totalChange;
		
		public double maximumChange;
		
		public IterationInformation() {
			this.newFacts = this.totalConclusions = 0;
			this.totalChange = 0.0;
		}
		
		public double averageChange() {
			return (double) this.totalChange / (this.totalConclusions + 1);
		}
		
		public String toString() {
			StringBuilder strBuilder = new StringBuilder();
			
			strBuilder.append("New facts: " + this.newFacts + "\n");
			strBuilder.append("Conclusions: " + this.totalConclusions + "\n");
			strBuilder.append("Total change: " + this.totalChange + "\n");
			strBuilder.append("Maximum change: " + this.maximumChange + "\n");
			strBuilder.append("Average change: " + this.averageChange());
			
			return strBuilder.toString();
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
	private IterationInformation getPredictions(List<Query> queries, 
			boolean notInTraining, int iteration, Collection<Prediction> output) throws InterruptedException {
		long timeStamp1 = System.currentTimeMillis();
		List<Prediction> deductions = new ArrayList<Prediction>();
		IterationInformation iInfo = new IterationInformation();
		Map<Triple<ByteString, ByteString, ByteString>, List<Query>> predictions =
				calculatePredictions2RulesMap(queries, !this.rewriteProbabilities);
		System.out.println((System.currentTimeMillis() - timeStamp1) + " milliseconds for calculatePredictions2RulesMap");
		if (this.rewriteProbabilities) {
			System.out.println(predictions.size() + " conclusions deduced from the KB");
		} else {
			System.out.println(predictions.size() + " predictions deduced from the KB");			
		}
		
		System.out.println("Using " + this.numberOfCoresEvaluation + " threads to score predictions.");
		timeStamp1 = System.currentTimeMillis();
		Thread[] threads = new Thread[this.numberOfCoresEvaluation];
		for (int i = 0; i < threads.length; ++i) {
			threads[i] = new Thread(new PredictionsBuilder(predictions, deductions, iteration));
			threads[i].start();
		}
		
		for (int i = 0; i < threads.length; ++i) {
			threads[i].join();
		}
		
		System.out.println("Adding predictions to KB");
		iInfo.maximumChange = -1.0;
		for (Prediction prediction : deductions) {
			ByteString[] t = prediction.getTriple();			
			
			if (!trainingKb.contains(t)) {
				++iInfo.newFacts;
			}
			
			++iInfo.totalConclusions;
			double probability = trainingKb.probabilityOfFact(t);
			double newProbability = getScoreForPrediction(prediction);			
			double change = Math.abs(probability - newProbability);
			if (Double.isNaN(change)) {
				System.out.println(prediction + " has a problem");
				System.out.println(prediction.getJointRule().getBasicRuleString());
			}
			iInfo.totalChange += change;
			if (change > iInfo.maximumChange) {
				iInfo.maximumChange = change;
			}
			trainingKb.add(t[0], t[1], t[2], newProbability);
			output.add(prediction);
		}
		
		System.out.println("Rebuilding overlap tables");
		trainingKb.buildOverlapTables();
		System.out.println("Done");
		
		System.out.println((System.currentTimeMillis() - timeStamp1) + " milliseconds to calculate the scores for predictions");
		return iInfo;
	}
	
	private double getScoreForPrediction(Prediction p) {
		Query jointRule = p.getJointRule();
		double score = 0.0;
		if (this.pruningMetric == Metric.HeadCoverage) {
			score = jointRule.getHeadCoverage();
		} else {
			score = jointRule.getSupport();
		}
			
		if ( (this.predictionsMetric == PredictionMetric.JointConfidence || 
				this.predictionsMetric == PredictionMetric.JointScoreTimesFuncScore)
				&& score < this.pruningThreshold) {
			return this.predictionsMetric == PredictionMetric.JointConfidence ?
					p.get(PredictionMetric.NaiveIndependenceConfidence) :
						p.get(PredictionMetric.NaiveIndependenceConfidenceTimesFuncScore);
		} else {
			return p.get(this.predictionsMetric);
		}
	}
	
	/**
	 * For a given prediction, it computes the probability that meets soft cardinality constraints.
	 * @param prediction
	 */
	private void computeCardinalityScore(Prediction prediction) {
		// Take the most functional side of the prediction
		ByteString relation = prediction.getTriple()[1];
		long cardinality = 0;
		if (trainingKb.functionality(relation) > trainingKb.inverseFunctionality(relation)) {
			ByteString subject = prediction.getTriple()[0];
			List<ByteString[]> query = FactDatabase.triples(FactDatabase.triple(subject, relation, ByteString.of("?x")));
			cardinality = trainingKb.countDistinct(ByteString.of("?x"), query);
		} else {
			ByteString object = prediction.getTriple()[2];
			List<ByteString[]> query = FactDatabase.triples(FactDatabase.triple(ByteString.of("?x"), relation, object));
			cardinality = trainingKb.countDistinct(ByteString.of("?x"), query);
		}
		
		double functionalityScore = trainingKb.probabilityOfCardinalityGreaterThan(relation, (int)cardinality);
		prediction.setFunctionalityScore(functionalityScore);		
	}

	public static void main(String[] args) throws Exception {
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cli = null;
        // create the command line parser
        CommandLineParser parser = new PosixParser();
        // create the Options
        Options options = new Options();
		
        // Arguments
        double pruningThreshold = 0.0;
        int minInitialSupport = DefaultInitialSupportThreshold;
        int minSupport = DefaultSupportThreshold;
        double minHeadCoverage = DefaultHeadCoverageThreshold;
		double confidenceThreshold = DefaultPCAConfidenceThreshold;
		boolean allowTypes = false;
		Metric pruningMetric = Metric.HeadCoverage;
		PredictionMetric pmetric = PredictionMetric.JointScoreTimesFuncScore;
		int numberOfIterations = Integer.MAX_VALUE;
		int numberOfCoresEvaluation = 1;
		boolean addOnlyVerifiedPredictions = false;
		Set<Prediction> predictions = null;
		boolean outputSample = false;
		int sampleSize = DefaultSampleSize;
		String miningTechniqueStr = "standard";
		boolean rewriteProbabilities = false;
        
        Option supportOpt = OptionBuilder.withArgName("min-support")
                .hasArg()
                .withDescription("Minimum absolute support. Default: " + DefaultSupportThreshold + " positive examples")
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
        
        Option nCoresEvaluationOption = OptionBuilder.withArgName("number-cores-evaluation")
        		.hasArg()
        		.withDescription("Number of cores used to calculate the scores of predictions in each iteration. "
        				+ "A high number may cause memory exhaustion. A low number may slow down the process."
        				+ "Default value: 1")
        		.create("nce");
        
        Option enableTypesOption = OptionBuilder.withArgName("enable-types")
        		.withDescription("It enforces type constraints in the head variables of rules, that is,"
        				+ "it generates rules of the form B ^ type(x, C) ^ type(y, C') => rh(x, y)")
        		.create("et");
        
        Option outputSampleOption = OptionBuilder.withArgName("output-sample")
        		.withDescription("It outputs a sample of the predictions for confidence evaluation.")
        		.create("os");
        
        Option sampleSizeOption = OptionBuilder.withArgName("sample-size")
        		.hasArg()
        		.withDescription("Bucket size for sample. The sampler creates buckets of width 0.1 with the given number of samples.")
        		.create("ss");
        
        Option sampleOutputFile = OptionBuilder.withArgName("sample-output-file")
        		.hasArg()
        		.withDescription("Print the sample in this file.")
        		.create("sout");
        
        Option miningTechniqueOp = OptionBuilder.withArgName("mining-technique")
                .withDescription("AMIE offers 2 multi-threading strategies: standard (traditional) and solidary (experimental)")
                .hasArg()
                .create("mt");
        
        Option pmetricOp =  OptionBuilder.withArgName("score-metric")
                .withDescription("Metric used score predictions: NaiveConfidence | JointConfidence | NaiveJointScore | FullJointScore")
                .hasArg()
                .create("sm");
        
        Option rewriteProbabilitiesOp = OptionBuilder.withArgName("rewrite-mode")
        		.withDescription("The method can override the score associated to a prediction.")
        		.create("rm");
                
        options.addOption(supportOpt);
        options.addOption(initialSupportOpt);
        options.addOption(headCoverageOpt);
        options.addOption(pruningMetricOpt);
        options.addOption(pcaConfThresholdOpt);
        options.addOption(testingKBOption);
        options.addOption(iterationsOption);
        options.addOption(nCoresEvaluationOption);
        options.addOption(enableTypesOption);
        options.addOption(outputSampleOption);
        options.addOption(sampleSizeOption);       
        options.addOption(sampleOutputFile);
        options.addOption(miningTechniqueOp);
        options.addOption(pmetricOp);
        options.addOption(rewriteProbabilitiesOp);
        
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
        training.buildOverlapTables();
		
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
	        	System.out.println("Using PCA confidence threshold " + confidenceThreshold);
			} catch (NumberFormatException e) {
				System.err.println("The argument minpca (minimal confidence threshold) expects a real number between 0 and 1");
				formatter.printHelp("AMIEPredictor", options);
				System.exit(1);
			}
		}
		
		if (cli.hasOption("minis")) {
            try {
                minInitialSupport = Integer.parseInt(cli.getOptionValue("minis"));
        		System.out.println("Using an initial support threshold of " + minInitialSupport);
            } catch (NumberFormatException e) {
				System.err.println("The argument minis (minimal initial support) expects a non-negative positive integer");
				formatter.printHelp("AMIEPredictor", options);
				System.exit(1);
            }
        }
		
		if (cli.hasOption("mins")) {
            try {
                minSupport = Integer.parseInt(cli.getOptionValue("mins"));
            } catch (NumberFormatException e) {
				System.err.println("The argument mins (minimal support) expects a non-negative positive integer");
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
                    pruningMetric = Metric.Support;
                    System.err.println("Using " + pruningMetric + " as pruning metric with threshold " + minSupport);
                    pruningThreshold = minSupport;
                    minInitialSupport = minSupport;
                    break;
                default:
                    pruningMetric = Metric.HeadCoverage;
                    System.err.println("Using " + pruningMetric + " as pruning metric with threshold " + minHeadCoverage);
                    pruningThreshold = minHeadCoverage;
                    break;
            }
        } else {
        	System.out.println("Using " + pruningMetric + " as pruning metric with minimum threshold " + minHeadCoverage);
            pruningThreshold = minHeadCoverage;
            minInitialSupport = minSupport;
        }
		
		allowTypes = cli.hasOption("et");
		
		if (cli.hasOption("ni")) {
			try {
				numberOfIterations = Integer.parseInt(cli.getOptionValue("ni"));
			} catch (NumberFormatException e) {
				System.err.println("The argument ni (number of iterations) expects a positive number.");
				formatter.printHelp("AMIEPredictor", options);
				System.exit(1);
			}
			
		}
		
		if (cli.hasOption("nce")) {
			try {
				numberOfCoresEvaluation = Integer.parseInt(cli.getOptionValue("nce"));
			} catch (NumberFormatException e) {
				System.err.println("The argument nce (number of cores for evaluation) expects a positive number.");
				formatter.printHelp("AMIEPredictor", options);
				System.exit(1);
			}
			
		}
		
		if (cli.hasOption("mt")) {
            miningTechniqueStr = cli.getOptionValue("mt").toLowerCase();
            if (!miningTechniqueStr.equals("solidary")
                    && !miningTechniqueStr.equals("standard")) {
                miningTechniqueStr = "standard";
            }
        }
		
		if (cli.hasOption("sm")) {
			try {				
				pmetric = PredictionMetric.valueOf(cli.getOptionValue("sm"));
			} catch (Exception e) {
				pmetric = PredictionMetric.JointScoreTimesFuncScore;
			}
		}
		System.out.println("Using " + pmetric + " as prediction score metric");
		
		rewriteProbabilities = cli.hasOption("rm");
				
        System.out.println("Using " + miningTechniqueStr + " multi-threading strategy.");
        boolean standardMining = !miningTechniqueStr.equals("solidary"); 
				
        MiningAssistant assistant = null;
        if (allowTypes) {
        	assistant = new RelationSignatureDefaultMiningAssistant(training);
    		List<ByteString> excludedRelationsSignatured = Arrays.asList(ByteString.of("rdf:type"), 
    				ByteString.of("rdfs:domain"), ByteString.of("rdfs:range"));
    		assistant.setHeadExcludedRelations(excludedRelationsSignatured);
    		assistant.setBodyExcludedRelations(excludedRelationsSignatured);
    		System.out.println("Mining rules with type constraints, i.e., of form B ^ is(x, D) ^ is(y, R) => rh(x, y)");
        } else {
        	assistant = new DefaultMiningAssistant(training);
        }
        assistant.setPcaConfidenceThreshold(confidenceThreshold);
        assistant.setEnabledConfidenceUpperBounds(true);
        assistant.setEnabledFunctionalityHeuristic(true);
        AMIE miner = new AMIE(assistant, minInitialSupport, pruningThreshold, pruningMetric, Runtime.getRuntime().availableProcessors());
		AMIEPredictor predictor = new AMIEPredictor(miner, testing);
		predictor.setStandardMining(standardMining);
		predictor.setNumberOfCoresForEvaluation(numberOfCoresEvaluation);
		predictor.setPredictionMetric(pmetric);
		predictor.setRewriteProbabilities(rewriteProbabilities);
		predictor.setPruningThreshold(pruningThreshold);
		long timeStamp1 = System.currentTimeMillis();
		predictions = predictor.predict(numberOfIterations, addOnlyVerifiedPredictions);

		IntHashMap<Integer> hitsInTargetHistogram = new IntHashMap<>();
		IntHashMap<Integer> hitsInTargetNotInSourceHistogram = new IntHashMap<>();
		IntHashMap<Integer> predictionsHistogram = new IntHashMap<>();
		for (Prediction prediction : predictions) {
			predictionsHistogram.increase(prediction.getRules().size());
			if (prediction.isHitInTarget()) {
				hitsInTargetHistogram.increase(prediction.getRules().size());
				if (!prediction.isHitInTraining()) {
					hitsInTargetNotInSourceHistogram.increase(prediction.getRules().size());
				}
			}
		}
		System.out.println("Inference took " + ((System.currentTimeMillis() - timeStamp1) / 1000.0) + " seconds");
		System.out.println("Predictions histogram");
		Utils.printHistogram(predictionsHistogram);
		
		System.out.println("Hits in target histogram");
		Utils.printHistogram(hitsInTargetHistogram);
		
		System.out.println("Hits In target but not in training histogram");
		Utils.printHistogram(hitsInTargetNotInSourceHistogram);
		
		outputSample = cli.hasOption("os");
		if (outputSample) {
			if (cli.hasOption("ss")) {
				try {
					sampleSize = Integer.parseInt(cli.getOptionValue("ss"));
				} catch (NumberFormatException e) {
					System.err.println("Sample size");
					formatter.printHelp("AMIEPredictor", options);
					System.err.println("Using default sample size: " + sampleSize);
				}				
			}
			PrintStream stream = null;			
			if (cli.hasOption("sout")) {
				stream = new PrintStream(cli.getOptionValue("sout"));
				System.out.println("Outputing a sample of size " 
				+ sampleSize + " for evaluation in file " + cli.getOptionValue("sout"));				
			} else {
				stream = new PrintStream(System.out);
				System.out.println("Outputing a sample " + sampleSize + " for evaluation in the standard output");
			}
			sampleBucketizedPredictions(predictions, stream, pmetric, sampleSize);
		}
	}

	/**
	 * It outputs in the given stream a bucketized sample of the input predictions. 
	 * A bucket corresponds to a group of predictions within a score interval.
	 * @param predictions
	 * @param stream
	 */
	private static void sampleBucketizedPredictions(
			Collection<Prediction> predictions, PrintStream stream, PredictionMetric metric,
			int sampleSize) {
		MultiMap<Integer, Prediction> buckets = new MultiMap<>();
		
		for (Prediction prediction : predictions) {
			int key = (int) Math.ceil(prediction.get(metric) * 10);
			buckets.add(key, prediction);
		}
		
		List<Integer> keys = new ArrayList<Integer>(buckets.keySet());
		Collections.sort(keys);
		for (Integer key : keys) {
			List<Prediction> bucket = buckets.get(key);
			Collection<Prediction> sample = 
					telecom.util.collections.Collections.reservoirSampling(bucket, sampleSize);
			stream.println("Bucket [" + key / 10.0 + ", " + (key - 1.0) / 10.0 + ") " + bucket.size() + " predictions");
			for (Prediction prediction : sample) {
				stream.println(prediction);
			}
		}
	}
}