/**
 * @author lgalarra
 * @date Aug 8, 2012
 * AMIE Version 0.1
 */
package amie.mining;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javatools.administrative.Announce;
import javatools.datatypes.ByteString;
import javatools.parsers.NumberFormatter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

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
	 * 
	 * @param assistant
	 * @param minInitialSupport
	 * @param headCoverage
	 * @param metric 
	 * @param seeds
	 */
	public AMIE(MiningAssistant assistant, int minInitialSupport, double headCoverage, Metric metric, int nThreads){
		this.assistant = assistant;
		this.minInitialSupport = minInitialSupport;
		this.minHeadCoverage = headCoverage;
		this.pruningMetric = metric;
		this.nThreads = nThreads;
	}
		
	/**
	 * The key method which returns a set of rules
	 * @return
	 * @throws Exception 
	 */
	public List<Query> mine(boolean realTime, Collection<ByteString> seeds) throws Exception{
		List<Query> result = new ArrayList<Query>();
		RuleConsumer consumerObj = null;
		Thread consumerThread = null;
		Lock resultsLock = new ReentrantLock();
	    Condition resultsCondVar = resultsLock.newCondition();
	    AtomicInteger sharedCounter = new AtomicInteger(0);
	    		
	    Query rootQuery = new Query();
		Collection<Query> seedsPool = new LinkedHashSet<Query>();		
				
		if(seeds == null || seeds.isEmpty())
			assistant.getDanglingEdges(rootQuery, minInitialSupport, seedsPool);
		else
			assistant.getDanglingEdges(rootQuery, seeds, minInitialSupport, seedsPool);
		
		if(realTime){
			consumerObj = new RuleConsumer(result, resultsLock, resultsCondVar);
			consumerThread = new Thread(consumerObj);
			consumerThread.start();
		}		
		
        if(nThreads > 1){
            System.out.println("Using " + nThreads + " threads");
			//Create as many threads as available cores
        	ArrayList<Thread> currentJobs = new ArrayList<Thread>();
        	for(int i = 0; i < nThreads; ++i){
        		Thread job = new Thread(new RDFMinerJob(seedsPool, result, resultsLock, resultsCondVar, sharedCounter));
        		currentJobs.add(job);
        	}
        	
        	for(Thread job: currentJobs){
        		job.start();
        	}
        	
        	for(Thread job: currentJobs){
        		job.join();
        	}
        }else{
        	Thread job = new Thread(new RDFMinerJob(seedsPool, result, resultsLock, resultsCondVar, sharedCounter));
        	job.run();
        }
		
        if(realTime){
        	consumerObj.finish();
        	consumerThread.interrupt();
        }
		
        return result;
	}

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
		
		private Collection<Query> queryPool;
					
		private Lock resultsLock;
		
		private Condition resultsCondition;
		
		private AtomicInteger sharedCounter;
		
		private boolean idle;
		
								
		public RDFMinerJob(Collection<Query> seedsPool, List<Query> outputSet, Lock resultsLock, Condition resultsCondition, AtomicInteger sharedCounter){
			this.queryPool = seedsPool;
			this.outputSet = outputSet;
			this.resultsLock = resultsLock;
			this.resultsCondition = resultsCondition;
			this.sharedCounter = sharedCounter;
			this.idle = false;
		}
		
		private Query pollQuery(){
			Query nextQuery = null;
			if(!queryPool.isEmpty()){
				Iterator<Query> iterator = queryPool.iterator();
				nextQuery = iterator.next();
				iterator.remove();
			}
			
			return nextQuery;
		}
	
		@Override
		public void run() {
			while(true){				
				Query currentQuery = null;
				
				synchronized(queryPool){
					currentQuery = pollQuery();
				}
				
				if(currentQuery != null){
					if(idle){
						idle = false;
						sharedCounter.decrementAndGet();
					}
					
					// Check if the rule meets the language bias and confidence thresholds.
					boolean outputRule = false;
					if (currentQuery.isSafe()){
						boolean ruleSatisfiesConfBounds = assistant.calculateConfidenceBounds(currentQuery);
						if (ruleSatisfiesConfBounds) {
							assistant.calculateConfidenceMetrics(currentQuery);					
							outputRule = assistant.testConfidenceThresholds(currentQuery);
						} else {
							outputRule = false;
						}
					}
					
					// Specialize the rule
					if (!currentQuery.isSafe() || currentQuery.getPcaConfidence() < 1.0) {
						int minCount = getCountThreshold(currentQuery);
						List<Query> temporalOutput = new ArrayList<Query>();
						assistant.getCloseCircleEdges(currentQuery, minCount, temporalOutput);
						assistant.getDanglingEdges(currentQuery, minCount, temporalOutput);
						synchronized(queryPool){									
							queryPool.addAll(temporalOutput);
						}												
					}
					
					// Output the rule
					if (outputRule) {
						resultsLock.lock();
						outputSet.add(currentQuery);
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
	}
	
	public static void run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		List<File> dataFiles = new ArrayList<File>();
		List<File> targetFiles = new ArrayList<File>();
		List<File> schemaFiles = new ArrayList<File>();

		CommandLine cli = null;
		double minStdConf = 0.0;
		int minSup = 100;
		int minInitialSup = 100;
		double minHeadCover = 0.01;
		double minPCAConf = 0.0;
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
				bodyExcludedRelations.add(ByteString.of(excludedValue));
		}
	
		if (cli.hasOption("btr")) {
			bodyTargetRelations = new ArrayList<>();			
			String targetBodyValuesStr = cli.getOptionValue("btr");
			String[] bodyTargetRelationsArr = targetBodyValuesStr.split(",");
			for (String targetString : bodyTargetRelationsArr) {
				bodyTargetRelations.add(ByteString.of(targetString));
			}
		}
		
		if (cli.hasOption("htr")) {
			headTargetRelations = new ArrayList<>();
			String targetValuesStr = cli.getOptionValue("htr");
			String[] targetValueArr = targetValuesStr.split(",");
			for(String targetValue: targetValueArr)
				headTargetRelations.add(ByteString.of(targetValue));
		}
		
		if (cli.hasOption("hexr")) {
			headExcludedRelations = new ArrayList<>();
			String excludedValuesStr = cli.getOptionValue("hexr");
			String[] excludedValueArr = excludedValuesStr.split(",");
			for(String excludedValue : excludedValueArr)
				headExcludedRelations.add(ByteString.of(excludedValue));
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
		dataSource.load(dataFiles, cli.hasOption("optimfh"));
		
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
		
		Announce.doing("Starting the mining phase");		
		
		long time = System.currentTimeMillis();
		List<Query> rules = null;
		
		rules = miner.mine(realTime, headTargetRelations);
		
		if(!realTime) {
			Query.printRuleHeaders();
			for(Query rule: rules)
				System.out.println(rule.getFullRuleString());
		}
		
		Announce.done("Mining done in " + NumberFormatter.formatMS(System.currentTimeMillis() - time) + " seconds" );
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
				100, // Do not look at relations smaller than 100 facts 
				0.01, // Head coverage 1%
				Metric.Support,
				Runtime.getRuntime().availableProcessors());
	}
}