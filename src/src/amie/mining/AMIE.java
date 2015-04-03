/**
 * @author lgalarra
 * @date Aug 8, 2012
 * AMIE Version 0.1
 */
package amie.mining;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javatools.administrative.Announce;
import javatools.datatypes.ByteString;
import javatools.datatypes.MultiMap;
import javatools.parsers.NumberFormatter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import amie.data.EquivalenceChecker2;
import amie.data.FactDatabase;
import amie.mining.assistant.ExistentialRulesHeadVariablesMiningAssistant;
import amie.mining.assistant.FullRelationSignatureMiningAssistant;
import amie.mining.assistant.HeadVariablesImprovedMiningAssistant;
import amie.mining.assistant.HeadVariablesMiningAssistant;
import amie.mining.assistant.InstantiatedHeadMiningAssistant;
import amie.mining.assistant.MiningAssistant;
import amie.mining.assistant.RelationSignatureMiningAssistant;
import amie.mining.assistant.SeedsCountMiningAssistant;
import amie.mining.assistant.TypedMiningAssistant;
import amie.mining.assistant.WikilinksHeadVariablesMiningAssistant;
import amie.query.Query;


/**
 * @author lgalarra
 *
 */
public class AMIE {
	/**
	 * It implements all the operators defined for the mining process: ADD-EDGE, INSTANTIATION, SPECIALIZATION and
	 * CLOSE-CIRCLE
	 */
	private MiningAssistant assistant;
	
	/**
	 * Support threshold for relations. 
	 */
	private int minInitialSupport;
	
	/**
	 * Head coverage threshold for refinements
	 */
	private double minHeadCoverage;
		
	/**
	 * Metric used to prune the mining tree
	 */
	private Metric pruningMetric;
	
	/**
	 * Preferred number of threads
	 */
	private int nThreads;
	
	/**
	 *  Time spent in applying the operators. 
	 **/
	private long specializationTime;
			
	/** 
	 * Time spent in calculating confidence scores. 
	 **/
	private long scoringTime;
			
	/** 
	 * Time spent in duplication elimination 
	 **/
	private long queueingAndDuplicateElimination;
	
	/**
	 * Time spent in confidence approximations
	 */
	private long approximationTime;
		
	/**
	 * 
	 * @param assistant
	 * @param minInitialSupport If head coverage is defined as pruning metric, it is the minimum size for 
	 * a relation to be considered in the mining.
	 * @param threshold The minimum support threshold: it can be either a head coverage ratio threshold or
	 * an absolute number.
	 * @param metric Head coverage or support.
	 */
	public AMIE(MiningAssistant assistant, int minInitialSupport, double threshold, Metric metric, int nThreads){
		this.assistant = assistant;
		this.minInitialSupport = minInitialSupport;
		this.minHeadCoverage = threshold;
		this.pruningMetric = metric;
		this.nThreads = nThreads;
		this.specializationTime = 0l;
		this.scoringTime = 0l;
		this.queueingAndDuplicateElimination = 0l;
		this.approximationTime = 0l;
	}
	
	public long getSpecializationTime() {
		return specializationTime;
	}

	public long getScoringTime() {
		return scoringTime;
	}

	public long getQueueingAndDuplicateElimination() {
		return queueingAndDuplicateElimination;
	}
	
	public long getApproximationTime() {
		return approximationTime;
	}

		
	/**
	 * The key method which returns a set of rules mined from the KB.
	 * @param realTime If true, the rules are printed as they are discovered, otherwise they are 
	 * just returned.
	 * @param seeds A collection of target head relations. If empty, the methods considers all
	 * possible head relations in the KB.
	 * @return
	 * @throws Exception 
	 */
	public List<Query> mine(boolean realTime, Collection<ByteString> seeds) throws Exception{
		List<Query> result = new ArrayList<>();
		MultiMap<Integer, Query> indexedResult = new MultiMap<>();
		RuleConsumer consumerObj = null;
		Thread consumerThread = null;
		Lock resultsLock = new ReentrantLock();
	    Condition resultsCondVar = resultsLock.newCondition();
	    AtomicInteger sharedCounter = new AtomicInteger(0);
	    		
	    Query rootQuery = new Query();
		Collection<Query> seedsPool = new LinkedHashSet<>();		
				
		if(seeds == null || seeds.isEmpty())
			assistant.getDanglingEdges(rootQuery, minInitialSupport, seedsPool);
		else
			assistant.getDanglingEdges(rootQuery, seeds, minInitialSupport, seedsPool);
		
		if(realTime){
			consumerObj = new RuleConsumer(result, resultsLock, resultsCondVar);
			consumerThread = new Thread(consumerObj);
			consumerThread.start();
		}		
		
        if(nThreads > 1) {
            System.out.println("Using " + nThreads + " threads");
			//Create as many threads as available cores
        	ArrayList<Thread> currentJobs = new ArrayList<>();
        	ArrayList<RDFMinerJob> jobObjects = new ArrayList<>();
        	for(int i = 0; i < nThreads; ++i){
        		RDFMinerJob jobObject = new RDFMinerJob(seedsPool, result, resultsLock, resultsCondVar, sharedCounter, indexedResult);
        		Thread job = new Thread(jobObject);
        		currentJobs.add(job);
        		jobObjects.add(jobObject);
        	}
        	
        	for(Thread job: currentJobs) {
        		job.start();
        	}
        	
        	for(Thread job: currentJobs) {
        		job.join();
        	}
        	
        	for (RDFMinerJob jobObject : jobObjects) {
        		this.specializationTime += jobObject.getSpecializationTime();
        		this.scoringTime += jobObject.getScoringTime();
        		this.queueingAndDuplicateElimination += jobObject.getQueueingAndDuplicateElimination();
        		this.approximationTime += jobObject.getApproximationTime();
        	}
        }else{
        	RDFMinerJob jobObject = new RDFMinerJob(seedsPool, result, resultsLock, resultsCondVar, sharedCounter, indexedResult);
        	Thread job = new Thread(jobObject);
        	job.run();
    		this.specializationTime += jobObject.getSpecializationTime();
    		this.scoringTime += jobObject.getScoringTime();
    		this.queueingAndDuplicateElimination += jobObject.getQueueingAndDuplicateElimination();
    		this.approximationTime += jobObject.getApproximationTime();    		
        }
		
        if(realTime){
        	consumerObj.finish();
        	consumerThread.interrupt();
        }
		
        return result;
	}

	/**
	 * It removes and prints rules from a shared list (a list accessed by multiple threads).
	 * @author galarrag
	 *
	 */
	private class RuleConsumer implements Runnable{

		private List<Query> consumeList;
		
		private volatile boolean finished;
		
		private int lastConsumedIndex;
		
		private Lock consumeLock;
		
		private Condition conditionVariable;
		
		public RuleConsumer(List<Query> consumeList, Lock consumeLock, Condition conditionVariable){
			this.consumeList = consumeList;
			this.lastConsumedIndex = -1;
			this.consumeLock = consumeLock;
			this.conditionVariable = conditionVariable;
			finished = false;
		}
		
		@Override
		public void run(){
			Query.printRuleHeaders();
			while(!finished){
				consumeLock.lock();
				try {
					while(lastConsumedIndex == consumeList.size() - 1){
						conditionVariable.await();
						for(int i = lastConsumedIndex + 1; i < consumeList.size(); ++i){
							System.out.println(consumeList.get(i).getFullRuleString());		
						}
						
						lastConsumedIndex = consumeList.size() - 1;
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
				} finally {
					consumeLock.unlock();					
				}					
			}
		}
		
		public void finish(){
			finished = true;
		}
	}
	
	/**
	 * Given a seed query, this class mines rules involving queries which extend from
	 * the seed query. An example of a seed query is <br />
	 * <pre>?s <happenedIn> ?o</pre>
	 * @author lgalarra
	 */
	private class RDFMinerJob implements Runnable{
		
		private List<Query> outputSet;
		
		// A version of the output set thought for search.
		private MultiMap<Integer, Query> indexedOutputSet;
		
		private Collection<Query> queryPool;
					
		private Lock resultsLock;
		
		private Condition resultsCondition;
		
		private AtomicInteger sharedCounter;
		
		private boolean idle;
		
		// Time spent in applying the operators.
		private long specializationTime;
		
		// Time spent in calculating confidence scores.
		private long scoringTime;
		
		// Time spent in duplication elimination
		private long queueingAndDuplicateElimination;
		
		// Time to calculate confidence approximations
		private long approximationTime;
		
								
		public RDFMinerJob(Collection<Query> seedsPool, 
				List<Query> outputSet, Lock resultsLock, 
				Condition resultsCondition, 
				AtomicInteger sharedCounter,
				MultiMap<Integer, Query> indexedOutputSet){
			this.queryPool = seedsPool;
			this.outputSet = outputSet;
			this.resultsLock = resultsLock;
			this.resultsCondition = resultsCondition;
			this.sharedCounter = sharedCounter;
			this.indexedOutputSet = indexedOutputSet;
			this.idle = false;
			this.specializationTime = 0l;
			this.scoringTime = 0l;
			this.queueingAndDuplicateElimination = 0l;
			this.approximationTime = 0l;
		}
		
		private Query pollQuery(){
			long timeStamp1 = System.currentTimeMillis();
			Query nextQuery = null;
			if(!queryPool.isEmpty()){
				Iterator<Query> iterator = queryPool.iterator();
				nextQuery = iterator.next();
				iterator.remove();
			}
			long timeStamp2 = System.currentTimeMillis();
			this.queueingAndDuplicateElimination += (timeStamp2 - timeStamp1);
			return nextQuery;
		}
	
		@Override
		public void run() {
			while(true) {				
				Query currentRule = null;
				
				synchronized(queryPool){
					currentRule = pollQuery();
				}
				
				if(currentRule != null){
					if(idle){
						idle = false;
						sharedCounter.decrementAndGet();
					}
					
					// Check if the rule meets the language bias and confidence thresholds and
					// decide whether to output it.
					boolean outputRule = false;
					if (currentRule.isClosed()){
						long timeStamp1 = System.currentTimeMillis();
						boolean ruleSatisfiesConfidenceBounds = 
								assistant.calculateConfidenceBoundsAndApproximations(currentRule);
						this.approximationTime += (System.currentTimeMillis() - timeStamp1);
						if (ruleSatisfiesConfidenceBounds) {
							resultsLock.lock();
							setAdditionalParents2(currentRule);
							resultsLock.unlock();
							// Calculate the metrics
							assistant.calculateConfidenceMetrics(currentRule);					
							// Check the confidence threshold and skyline technique.
							outputRule = assistant.testConfidenceThresholds(currentRule);
						} else {
							outputRule = false;
						}
						long timeStamp2 = System.currentTimeMillis();
						this.scoringTime += (timeStamp2 - timeStamp1);
					}
					
					// Check if we should further refine the rule
					boolean furtherRefined = true;
					if (assistant.isEnablePerfectRules()) {
						furtherRefined = !currentRule.isPerfect();
					}
					
					if (furtherRefined) {
						long timeStamp1 = System.currentTimeMillis();
						int minCount = getCountThreshold(currentRule);
						List<Query> temporalOutput = new ArrayList<Query>();
						assistant.getCloseCircleEdges(currentRule, minCount, temporalOutput);
						assistant.getDanglingEdges(currentRule, minCount, temporalOutput);
						long timeStamp2 = System.currentTimeMillis();
						this.specializationTime += (timeStamp2 - timeStamp1);
						synchronized(queryPool){
							timeStamp1 = System.currentTimeMillis();
							queryPool.addAll(temporalOutput);
							timeStamp2 = System.currentTimeMillis();
							this.queueingAndDuplicateElimination += (timeStamp2 - timeStamp1);
						}
					}
					
					// Output the rule
					if (outputRule) {
						resultsLock.lock();
						long timeStamp1 = System.currentTimeMillis();
						Set<Query> outputQueries = indexedOutputSet.get(currentRule.alternativeParentHashCode());
						if (outputQueries != null) {
							if (!outputQueries.contains(currentRule)) {
								outputSet.add(currentRule);
								outputQueries.add(currentRule);								
							}
						} else {
							outputSet.add(currentRule);
							indexedOutputSet.put(currentRule.alternativeParentHashCode(), currentRule);
						}
						long timeStamp2 = System.currentTimeMillis();
						this.queueingAndDuplicateElimination += (timeStamp2 - timeStamp1);
						resultsCondition.signal();
						resultsLock.unlock();
					}					
				}else{
					if(!idle){
						idle = true;
						boolean leave;
						synchronized(sharedCounter){
							leave = sharedCounter.incrementAndGet() >= nThreads;
						}					
						if(leave) break;
					}else{
						if(sharedCounter.get() >= nThreads)
							break;
					}
				}
			}
		}
		
		/**
		 * It finds all potential parents of a rule in the output set of 
		 * indexed rules.
		 * @param currentQuery
		 */
		private void setAdditionalParents2(Query currentQuery) {
			int parentHashCode = currentQuery.alternativeParentHashCode();
			Set<Query> candidateParents = indexedOutputSet.get(parentHashCode);
			if (candidateParents != null) {
				List<ByteString[]> queryPattern = currentQuery.getRealTriples();
				// No need to look for parents of rules of size 2
				if (queryPattern.size() <= 2) {
					return;
				}
				List<List<ByteString[]>> parentsOfSizeI = new ArrayList<>();
				Query.getParentsOfSize(queryPattern.subList(1, queryPattern.size()), queryPattern.get(0), queryPattern.size() - 2, parentsOfSizeI);
				for (List<ByteString[]> parent : parentsOfSizeI) {
					for (Query candidate : candidateParents) {
						List<ByteString[]> candidateParentPattern = candidate.getRealTriples();
						if (EquivalenceChecker2.equal(parent, candidateParentPattern)) {
							currentQuery.setParent(candidate);
						}
					}
				}
			}
		}

		private int getCountThreshold(Query query) {			
			switch(pruningMetric){
			case Support:
				return (int)minHeadCoverage;
			case HeadCoverage:
				return (int)Math.ceil((minHeadCoverage * (double)assistant.getHeadCardinality(query)));
			default:
				return 0;
			}
		}

		public long getSpecializationTime() {
			return specializationTime;
		}

		public long getScoringTime() {
			return scoringTime;
		}


		public long getQueueingAndDuplicateElimination() {
			return queueingAndDuplicateElimination;
		}

		public long getApproximationTime() {
			return approximationTime;
		}

	}
	
	public static void run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		List<File> dataFiles = new ArrayList<File>();
		List<File> targetFiles = new ArrayList<File>();
		List<File> schemaFiles = new ArrayList<File>();

		CommandLine cli = null;
		double minStdConf = 0.0;
		double minPCAConf = 0.0;
		int minSup = 100;
		int minInitialSup = 100;
		double minHeadCover = 0.01;
		int maxDepth = 3;
		int recursivityLimit = 3;
		boolean realTime = true;
		boolean countAlwaysOnSubject = false;
		double minMetricValue = 0.0;
		boolean allowConstants = false;
		boolean enableConfidenceUpperBounds = true;
		boolean enableFunctionalityHeuristic = true;
		boolean silent = false;
		boolean pcaOptimistic = false;
		boolean enforceConstants = false;
		boolean avoidUnboundTypeAtoms = true;
		// = Requested by the reviewers of AMIE+ ==
		boolean exploitMaxLengthForRuntime = true;
		boolean enableQueryRewriting = true;
		boolean enablePerfectRulesPruning = true;
		long sourcesLoadingTime = 0l;
		// ========================================
		int nProcessors = Runtime.getRuntime().availableProcessors();
		String bias = "headVars";
		Metric metric = Metric.HeadCoverage;
		MiningAssistant mineAssistant = null;		
		Collection<ByteString> bodyExcludedRelations = null;
		Collection<ByteString> headExcludedRelations = null;
		Collection<ByteString> headTargetRelations = null;
		Collection<ByteString> bodyTargetRelations = null;
		FactDatabase targetSource = null;
		FactDatabase schemaSource = null;
		int nThreads = nProcessors;
		HelpFormatter formatter = new HelpFormatter();
		
		// create the command line parser
		CommandLineParser parser = new PosixParser();
		// create the Options
		Options options = new Options();
				
		Option supportOpt = OptionBuilder.withArgName("min-support")
                .hasArg()
                .withDescription("Minimum absolute support. Default: 100 positive examples")
                .create( "mins");
		
		Option initialSupportOpt = OptionBuilder.withArgName("min-initial-support")
                .hasArg()
                .withDescription("Minimum size of the relations to be considered as head relations. Default: 100 (facts or entities depending on the bias)" )
                .create( "minis");
		
		Option headCoverageOpt = OptionBuilder.withArgName("min-head-coverage")
                .hasArg()
                .withDescription("Minimum head coverage. Default: 0.01")
                .create( "minhc");
				
		Option pruningMetricOpt = OptionBuilder.withArgName("pruning-metric")
				.hasArg()
                .withDescription("Metric used for pruning of intermediate queries: support|headcoverage. Default: headcoverage" )
                .create( "pm");

		Option realTimeOpt = OptionBuilder.withArgName("output-at-end")
				 .withDescription("Print the rules at the end and not while they are discovered. Default: false")
				 .create("oute");
		
		Option bodyExcludedOpt = OptionBuilder.withArgName("body-excluded-relations")
                 .hasArg()
				 .withDescription("Do not use these relations as atoms in the body of rules. Example: <livesIn>,<bornIn>")
				 .create("bexr");
		
		Option headExcludedOpt = OptionBuilder.withArgName("head-excluded-relations")
                .hasArg()
				.withDescription("Do not use these relations as atoms in the head of rules (incompatible with head-target-relations). Example: <livesIn>,<bornIn>")
				.create("hexr");
		
		Option headTargetRelationsOpt = OptionBuilder.withArgName("head-target-relations")
                .hasArg()
				 .withDescription("Mine only rules with these relations in the head. Provide a list of relation names separated by commas (incompatible with head-excluded-relations). Example: <livesIn>,<bornIn>")
				 .create("htr");
		
		Option bodyTargetRelationsOpt = OptionBuilder.withArgName("body-target-relations")
                .hasArg()
				 .withDescription("Allow only these relations in the body. Provide a list of relation names separated by commas (incompatible with body-excluded-relations). Example: <livesIn>,<bornIn>")
				 .create("btr");
		
		Option maxDepthOpt = OptionBuilder.withArgName("max-depth")
                .hasArg()
				.withDescription("Maximum number of atoms in the antecedent and succedent of rules. Default: 3")
				.create("maxad");
		
		Option maxImprovedConfOpt = OptionBuilder.withArgName("min-pca-confidence")
                .hasArg()
				.withDescription("Minimum PCA confidence threshold. This value is not used for pruning, only for filtering of the results. Default: 0.0")
				.create("minpca");
		
		Option allowConstantsOpt = OptionBuilder.withArgName("allow-constants")
				.withDescription("Enable rules with constants. Default: false")
				.create("const");
		
		Option enforceConstantsOpt = OptionBuilder.withArgName("only-constants")
				.withDescription("Enforce constants in all atoms. Default: false")
				.create("fconst");
		
		Option assistantOp = OptionBuilder.withArgName("bias-name")
                .hasArg()
                .withDescription("Syntatic/semantic bias: oneVar|headVars|typed|signatured|headVarsImproved. Default: headVars")
                .create( "bias");

		Option countOnSubjectOpt = OptionBuilder.withArgName("count-always-on-subject")
				.withDescription("If a single variable bias is used (oneVar), force to count support always on the subject position.")
				.create("caos");
				
		Option coresOp = OptionBuilder.withArgName("n-threads")
                .hasArg()
                .withDescription("Preferred number of cores. Round down to the actual number of cores in the system if a higher value is provided.")
                .create("nc");
		
		Option stdConfidenceOpt = OptionBuilder.withArgName("min-std-confidence")
                .hasArg()
                .withDescription("Minimum standard confidence threshold. This value is not used for pruning, only for filtering of the results. Default: 0.0")
                .create("minc");
		
		Option confidenceBoundsOp = OptionBuilder.withArgName("optim-confidence-bounds")
			 	.withDescription("Enable the calculation of confidence upper bounds to prune rules.")
			 	.create("optimcb");
		
		Option funcHeuristicOp = OptionBuilder.withArgName("optim-func-heuristic")
			 	.withDescription("Enable functionality heuristic to identify potential low confident rules for pruning.")
			 	.create("optimfh");
		
		Option optimisticApproxOp = OptionBuilder.withArgName("optim-func-heuristic-optimistic")
			 	.withDescription("Optimistic approximation for functionality heuristic.")
			 	.create("optimistic");
		
		Option silentOp = OptionBuilder.withArgName("silent")
			 	.withDescription("Minimal verbosity")
			 	.create("silent");
		
		Option recursivityLimitOpt = OptionBuilder.withArgName("recursivity-limit")
				.withDescription("Recursivity limit")
				.hasArg()
				.create("rl");
		
		Option avoidUnboundTypeAtomsOpt = OptionBuilder.withArgName("avoid-unbound-type-atoms")
				.withDescription("Avoid unbound type atoms, e.g., type(x, y), i.e., bind always 'y' to a type")
				.create("auta");
		
		Option doNotExploitMaxLengthOp = OptionBuilder.withArgName("do-not-exploit-max-length")
				.withDescription("Do not exploit max length for speedup (requested by the reviewers of AMIE+). False by default.")
				.create("deml");
		
		Option disableQueryRewriteOp = OptionBuilder.withArgName("disable-query-rewriting")
				.withDescription("Disable query rewriting and caching.")
				.create("dqrw");
		
		Option disablePerfectRulesOp = OptionBuilder.withArgName("disable-perfect-rules")
				.withDescription("Disable perfect rules.")
				.create("dpr");
						
		options.addOption(stdConfidenceOpt);
		options.addOption(supportOpt);
		options.addOption(initialSupportOpt);
		options.addOption(headCoverageOpt);
		options.addOption(pruningMetricOpt);
		options.addOption(realTimeOpt);
		options.addOption(bodyExcludedOpt);
		options.addOption(headExcludedOpt);
		options.addOption(maxDepthOpt);
		options.addOption(maxImprovedConfOpt);
		options.addOption(headTargetRelationsOpt);
		options.addOption(bodyTargetRelationsOpt);		
		options.addOption(allowConstantsOpt);
		options.addOption(enforceConstantsOpt);
		options.addOption(countOnSubjectOpt);
		options.addOption(assistantOp);
		options.addOption(coresOp);
		options.addOption(confidenceBoundsOp);
		options.addOption(silentOp);
		options.addOption(funcHeuristicOp);
		options.addOption(optimisticApproxOp);
		options.addOption(recursivityLimitOpt);
		options.addOption(avoidUnboundTypeAtomsOpt);
		options.addOption(doNotExploitMaxLengthOp);
		options.addOption(disableQueryRewriteOp);
		options.addOption(disablePerfectRulesOp);
		
		try {
			cli = parser.parse(options, args);
		} catch(ParseException e) {
			System.out.println( "Unexpected exception: " + e.getMessage());
			formatter.printHelp( "AMIE", options );
			System.exit(1);
		}
		
		if (cli.hasOption("htr") && cli.hasOption("hexr")) {
			System.err.println("The options head-target-relations and head-excluded-relations cannot appear at the same time");
			formatter.printHelp( "AMIE", options );
			System.exit(1);
		}
		
		if (cli.hasOption("btr") && cli.hasOption("bexr")) {
			System.err.println("The options body-target-relations and body-excluded-relations cannot appear at the same time");
			formatter.printHelp( "AMIE", options );
			System.exit(1);			
		}
										
		if(cli.hasOption("mins")) {
			String minSupportStr = cli.getOptionValue("mins");
			try{
				minSup = Integer.parseInt(minSupportStr);
			}catch(NumberFormatException e){
				System.err.println("The option -mins (support threshold) requires an integer as argument");
				System.err.println("AMIE [OPTIONS] <.tsv INPUT FILES>");
				formatter.printHelp( "AMIE", options );
				System.exit(1);
			}
		}
		
		if(cli.hasOption("minhc")) {
			String minHeadCoverage = cli.getOptionValue("minhc");
			try{
				minHeadCover = Double.parseDouble(minHeadCoverage);
			}catch(NumberFormatException e){
				System.err.println("The option -minhc (head coverage threshold) requires a real number as argument");
				System.err.println("AMIE [OPTIONS] <.tsv INPUT FILES>");
				formatter.printHelp( "AMIE", options );
				System.exit(1);				
			}
		}

		if (cli.hasOption("minc")) {
			String minConfidenceStr = cli.getOptionValue("minc");
			try{
				minStdConf = Double.parseDouble(minConfidenceStr);
			}catch(NumberFormatException e){
				System.err.println("The option -minc (confidence threshold) requires a real number as argument");
				System.err.println("AMIE [OPTIONS] <.tsv INPUT FILES>");
				formatter.printHelp( "AMIE", options );
				System.exit(1);
			}
		}
		
		if (cli.hasOption("minpca")) {
			String minicStr = cli.getOptionValue("minpca");
			try{
				minPCAConf = Double.parseDouble(minicStr);
			}catch(NumberFormatException e){
				System.err.println("The argument for option -minpca (PCA confidence threshold) must be an integer greater than 2");
				System.err.println("AMIE [OPTIONS] <.tsv INPUT FILES>");
				formatter.printHelp( "AMIE", options );
				System.exit(1);				
			}
		}		
				
		if (cli.hasOption("bexr")) {
			bodyExcludedRelations = new ArrayList<>();			
			String excludedValuesStr = cli.getOptionValue("bexr");
			String[] excludedValueArr = excludedValuesStr.split(",");
			for(String excludedValue: excludedValueArr)
				bodyExcludedRelations.add(ByteString.of(excludedValue.trim()));
		}
	
		if (cli.hasOption("btr")) {
			bodyTargetRelations = new ArrayList<>();			
			String targetBodyValuesStr = cli.getOptionValue("btr");
			String[] bodyTargetRelationsArr = targetBodyValuesStr.split(",");
			for (String targetString : bodyTargetRelationsArr) {
				bodyTargetRelations.add(ByteString.of(targetString.trim()));
			}
		}
		
		if (cli.hasOption("htr")) {
			headTargetRelations = new ArrayList<>();
			String targetValuesStr = cli.getOptionValue("htr");
			String[] targetValueArr = targetValuesStr.split(",");
			for(String targetValue: targetValueArr)
				headTargetRelations.add(ByteString.of(targetValue.trim()));
		}
		
		if (cli.hasOption("hexr")) {
			headExcludedRelations = new ArrayList<>();
			String excludedValuesStr = cli.getOptionValue("hexr");
			String[] excludedValueArr = excludedValuesStr.split(",");
			for(String excludedValue : excludedValueArr)
				headExcludedRelations.add(ByteString.of(excludedValue.trim()));
		}
		
		if (cli.hasOption("maxad")) {
			String maxDepthStr = cli.getOptionValue("maxad");
			try{
				maxDepth = Integer.parseInt(maxDepthStr);
			}catch(NumberFormatException e){
				System.err.println("The argument for option -maxad (maximum depth) must be an integer greater than 2");
				System.err.println("AMIE [OPTIONS] <.tsv INPUT FILES>");
				formatter.printHelp( "AMIE", options );
				System.exit(1);
			}
			
			if(maxDepth < 2){
				System.err.println("The argument for option -maxad (maximum depth) must be greater or equal than 2");
				System.err.println("AMIE [OPTIONS] <.tsv INPUT FILES>");
				formatter.printHelp( "AMIE", options );
				System.exit(1);
			}
		}
				
		if (cli.hasOption("nc")) {
			String nCoresStr = cli.getOptionValue("nc");
			try{
				nThreads = Integer.parseInt(nCoresStr);
			}catch(NumberFormatException e){
				System.err.println("The argument for option -nc (number of threads) must be an integer");
				System.err.println("AMIE [OPTIONS] <.tsv INPUT FILES>");						
				formatter.printHelp( "AMIE", options );
				System.exit(1);
			}
			
			if(nThreads > nProcessors) 
				nThreads = nProcessors;
		}
		
		pcaOptimistic = cli.hasOption("optimistic");		
		avoidUnboundTypeAtoms = cli.hasOption("auta");
		exploitMaxLengthForRuntime = !cli.hasOption("deml");
		enableQueryRewriting = !cli.hasOption("dqrw");
		enablePerfectRulesPruning = !cli.hasOption("dpr");
		String[] leftOverArgs = cli.getArgs();
		
		if (leftOverArgs.length < 1) {
			System.err.println("No input file has been provided");
			System.err.println("AMIE [OPTIONS] <.tsv INPUT FILES>");			
			formatter.printHelp( "amie", options );
			System.exit(1);
		}
		
		//Load database
		for (int i = 0; i < leftOverArgs.length; ++i) {
			if(leftOverArgs[i].startsWith(":t"))
				targetFiles.add(new File(leftOverArgs[i].substring(2)));
			else if(leftOverArgs[i].startsWith(":s"))
				schemaFiles.add(new File(leftOverArgs[i].substring(2)));
			else				
				dataFiles.add(new File(leftOverArgs[i]));
		}
				
		FactDatabase dataSource = new FactDatabase();
		long timeStamp1 = System.currentTimeMillis();
		dataSource.load(dataFiles, cli.hasOption("optimfh"));
		long timeStamp2 = System.currentTimeMillis();
		sourcesLoadingTime = timeStamp2 - timeStamp1;
		
		if (!targetFiles.isEmpty()) {
			targetSource = new FactDatabase();
			targetSource.load(targetFiles, cli.hasOption("optimfh"));
		}
		
		if (!schemaFiles.isEmpty()) {
			schemaSource = new FactDatabase();
			schemaSource.load(schemaFiles);
		}
		
		if (cli.hasOption("pm")) {
			switch(cli.getOptionValue("pm")){
			case "support":
				metric = Metric.Support;
				System.err.println("Using " + metric + " as pruning metric with threshold " + minSup);
				minMetricValue = minSup;
				minInitialSup = minSup;
				break;
			default:
				metric = Metric.HeadCoverage;
				System.err.println("Using " + metric + " as pruning metric with threshold " + minHeadCover);				
				minMetricValue = minHeadCover;
				if(cli.hasOption("minis")){
					String minInitialSupportStr = cli.getOptionValue("minis");
					minInitialSup = Integer.parseInt(minInitialSupportStr);
				}				
				break;
			}
		} else {
			System.out.println("Using " + metric + " as pruning metric with minimum threshold " + minHeadCover);
			minMetricValue = minHeadCover;
			minInitialSup = minSup;
			if(cli.hasOption("minis")){
				String minInitialSupportStr = cli.getOptionValue("minis");
				minInitialSup = Integer.parseInt(minInitialSupportStr);
			}			
		}
		
		
		if (cli.hasOption("bias")) {
			bias = cli.getOptionValue("bias");
		}
		
		silent = cli.hasOption("silent");
		
		if (cli.hasOption("rl")) {
			try {
				recursivityLimit = Integer.parseInt(cli.getOptionValue("rl"));
			} catch (NumberFormatException e) {
				System.err.println("The argument for option -rl (recursivity limit) must be an integer");
				System.err.println("AMIE [OPTIONS] <.tsv INPUT FILES>");						
				formatter.printHelp( "AMIE", options );
				System.exit(1);
			}
		}
		
		switch(bias) {
		case "seedsCount" :
			mineAssistant = new SeedsCountMiningAssistant(dataSource, schemaSource);
			break;
		case "headVars" : default:
			mineAssistant = new HeadVariablesMiningAssistant(dataSource);
			System.out.println("Counting on both head variables");
			break;
		case "signatured" :
			mineAssistant = new RelationSignatureMiningAssistant(dataSource);
			System.out.println("Counting on both head variables and using relation signatures (domain and range types) [EXPERIMENTAL]");			
			break;
		case "typed" :
			mineAssistant = new TypedMiningAssistant(dataSource);
			System.out.println("Counting on both head variables and using all available data types [EXPERIMENTAL]");			
			break;
		case "headVarsImproved" :
			mineAssistant = new HeadVariablesImprovedMiningAssistant(dataSource);
			System.out.println("Counting on both head variables");
			break;
		case "instantiatedHeadVars" :
			mineAssistant = new InstantiatedHeadMiningAssistant(dataSource);
			System.out.println("Counting on one variable. Head relation is always instantiated in one argument");
			break;
		case "fullSignatures" :
			mineAssistant = new FullRelationSignatureMiningAssistant(dataSource);
			System.out.println("Rules of the form type(x, C) r(x, y) => type(y, C') or type(y, C) r(x, y) => type(x, C')");
			break;
		case "wikilinks":
			mineAssistant = new WikilinksHeadVariablesMiningAssistant(dataSource);
			headExcludedRelations = Arrays.asList(ByteString.of(WikilinksHeadVariablesMiningAssistant.wikiLinkProperty), ByteString.of("rdf:type"));
			bodyExcludedRelations = Arrays.asList(ByteString.of(WikilinksHeadVariablesMiningAssistant.wikiLinkProperty), ByteString.of("rdf:type"));
			System.out.println("Rules of the form .... linksTo(x, y) type(x, C) type(y, C') => r(x, y)");
			break;
		case "existential" :
			mineAssistant = new ExistentialRulesHeadVariablesMiningAssistant(dataSource);
			System.out.println("Reporting also existential rules. Counting on both head variables.");
			break;
		case "oneVar" :
			mineAssistant = new MiningAssistant(dataSource);			
			if(countAlwaysOnSubject)
				System.out.println("Counting on the subject variable of the head relation");
			else
				System.out.println("Counting on the most functional variable of the head relation");
		}
		mineAssistant.setSchemaSource(schemaSource);
		
		enableConfidenceUpperBounds = cli.hasOption("optimcb");
		if(enableConfidenceUpperBounds) {
			System.out.println("Enabling standard and PCA confidences upper bounds for pruning [EXPERIMENTAL]");
		}
		
		enableFunctionalityHeuristic = cli.hasOption("optimfh");
		if(enableFunctionalityHeuristic) {
			System.out.println("Enabling functionality heuristic with ratio for pruning of low confident rules [EXPERIMENTAL]");			
		}
		
		allowConstants = cli.hasOption("const");
		countAlwaysOnSubject = cli.hasOption("caos");
		realTime = !cli.hasOption("oute");
		enforceConstants = cli.hasOption("fconst");
		
		mineAssistant.setEnabledConfidenceUpperBounds(enableConfidenceUpperBounds);
		mineAssistant.setEnabledFunctionalityHeuristic(enableFunctionalityHeuristic);
		mineAssistant.setMaxDepth(maxDepth);
		mineAssistant.setMinStdConfidence(minStdConf);
		mineAssistant.setMinPcaConfidence(minPCAConf);
		mineAssistant.setAllowConstants(allowConstants);
		mineAssistant.setEnforceConstants(enforceConstants);
		mineAssistant.setBodyExcludedRelations(bodyExcludedRelations);
		mineAssistant.setHeadExcludedRelations(headExcludedRelations);
		mineAssistant.setTargetBodyRelations(bodyTargetRelations);
		mineAssistant.setCountAlwaysOnSubject(countAlwaysOnSubject);
		mineAssistant.setSilent(silent);
		mineAssistant.setPcaOptimistic(pcaOptimistic);
		mineAssistant.setRecursivityLimit(recursivityLimit);
		mineAssistant.setAvoidUnboundTypeAtoms(avoidUnboundTypeAtoms);
		mineAssistant.setExploitMaxLengthOption(exploitMaxLengthForRuntime);
		mineAssistant.setEnableQueryRewriting(enableQueryRewriting);
		mineAssistant.setEnablePerfectRules(enablePerfectRulesPruning);
		
		AMIE miner = new AMIE(mineAssistant, minInitialSup, minMetricValue, metric, nThreads);
		if(minStdConf > 0.0) {
			System.out.println("Filtering on standard confidence with minimum threshold " + minStdConf);
		} else {
			System.out.println("No minimum threshold on standard confidence");
		}
		
		if (minPCAConf > 0.0) {
			System.out.println("Filtering on PCA confidence with minimum threshold " + minPCAConf);
		}else{
			System.out.println("No minimum threshold on PCA confidence");
		}
		
		if (enforceConstants) {
			System.out.println("Constants in the arguments of relations are enforced");
		} else if (allowConstants) {
			System.out.println("Constants in the arguments of relations are enabled");			
		} else {
			System.out.println("Constants in the arguments of relations are disabled");
		}
		
		if (exploitMaxLengthForRuntime && enableQueryRewriting && enablePerfectRulesPruning) {
			System.out.println("Lossless heuristics enabled");
		} else {			
			if (!exploitMaxLengthForRuntime) {
				System.out.println("Pruning by maximum rule length disabled");
			}
			
			if (!enableQueryRewriting) {
				System.out.println("Query rewriting and caching disabled");
			}
			
			if (!enablePerfectRulesPruning) {
				System.out.println("Perfect rules pruning disabled");
			}
		}
		
		Announce.doing("Starting the mining phase");		
		
		long time = System.currentTimeMillis();
		List<Query> rules = null;
		
		rules = miner.mine(realTime, headTargetRelations);
		
		if(!realTime) {
			Query.printRuleHeaders();
			for(Query rule: rules)
				System.out.println(rule.getFullRuleString());
		}
		System.out.println("Specialization time: " + (miner.getSpecializationTime() / 1000.0) + "s");
		System.out.println("Scoring time: " + (miner.getScoringTime() / 1000.0) + "s");
		System.out.println("Queueing and duplicate elimination: " + (miner.getQueueingAndDuplicateElimination() / 1000.0) + "s");
		System.out.println("Approximation time: " + (miner.getApproximationTime() / 1000.0) + "s");
		System.out.println(rules.size() + " rules mined.");
		long miningTime = System.currentTimeMillis() - time;
		System.out.println("Mining done in " + NumberFormatter.formatMS(miningTime));
		Announce.done("Total time " + NumberFormatter.formatMS(miningTime + sourcesLoadingTime));
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		run(args);
	}

	public static AMIE getVanillaSettingInstance(FactDatabase db) {
		return new AMIE(new HeadVariablesImprovedMiningAssistant(db), 
				100, // Do not look at relations smaller than 100 facts 
				0.01, // Head coverage 1%
				Metric.HeadCoverage,
				Runtime.getRuntime().availableProcessors());
	}
	
	public static AMIE getVanillaSettingInstance(FactDatabase db, double minPCAConfidence) {
		HeadVariablesImprovedMiningAssistant miningAssistant = new HeadVariablesImprovedMiningAssistant(db);
		miningAssistant.setMinPcaConfidence(minPCAConfidence);
		return new AMIE(miningAssistant, 
				100, // Do not look at relations smaller than 100 facts 
				0.01, // Head coverage 1%
				Metric.HeadCoverage,
				Runtime.getRuntime().availableProcessors());
	}
	
	public static AMIE getLossyVanillaSettingInstance(FactDatabase db, double minPCAConfidence) {
		HeadVariablesImprovedMiningAssistant miningAssistant = new HeadVariablesImprovedMiningAssistant(db);
		miningAssistant.setMinPcaConfidence(minPCAConfidence);
		miningAssistant.setEnabledConfidenceUpperBounds(true);
		miningAssistant.setEnabledFunctionalityHeuristic(true);
		return new AMIE(miningAssistant, 
				100, // Do not look at relations smaller than 100 facts 
				0.01, // Head coverage 1%
				Metric.HeadCoverage,
				Runtime.getRuntime().availableProcessors());
	}
	
	public static AMIE getLossyInstance(FactDatabase db, double minPCAConfidence, int minSupport) {
		HeadVariablesImprovedMiningAssistant miningAssistant = new HeadVariablesImprovedMiningAssistant(db);
		miningAssistant.setMinPcaConfidence(minPCAConfidence);
		miningAssistant.setEnabledConfidenceUpperBounds(true);
		miningAssistant.setEnabledFunctionalityHeuristic(true);
		return new AMIE(miningAssistant, 
				minSupport, // Do not look at relations smaller than 100 facts 
				minSupport, // Head coverage 1%
				Metric.Support,
				Runtime.getRuntime().availableProcessors());
	}
}