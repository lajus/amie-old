/**
 * @author lgalarra
 * @date Aug 8, 2012
 */
package arm.mining;

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

import arm.data.FactDatabase;
import arm.mining.assistant.MiningAssistant;
import arm.mining.assistant.MiningAssistantTwoVars;
import arm.mining.assistant.SeedsCountMiningAssistant;
import arm.mining.assistant.TypedMiningAssistant;
import arm.query.Query;


/**
 * @author lgalarra
 *
 */
public class AMIEMiner {
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
	 * Flag to tell the miner to use multiple threads
	 */
	private boolean parallel;
	
	/**
	 * Head coverage threshold for refinements
	 */
	private double minPruningRatio;
		
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
	 * @param parallel
	 * @param metric 
	 * @param seeds
	 */
	public AMIEMiner(MiningAssistant assistant, int minSupport, double minPruningRatio, boolean parallel, Metric metric, int nThreads){
		this.assistant = assistant;
		this.minInitialSupport = minSupport;
		this.minPruningRatio = minPruningRatio;
		this.parallel = parallel;
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
        int nCores = parallel ? nThreads : 1;
	    		
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
		
		//Register the head cardinalities in the assistant map
		for(Object head: seedsPool)
			assistant.registerHeadRelation((Query)head);
		
        if(parallel){
            System.out.println("Using " + nCores + " threads");
			//Create as many threads as available cores
        	ArrayList<Thread> currentJobs = new ArrayList<Thread>();
        	for(int i = 0; i < nCores; ++i){
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
						for(int i = lastConsumedIndex + 1; i < consumeList.size(); ++i)
							System.out.println(consumeList.get(i).getRuleFullString2());
						
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
					
					int minCount = getCountThreshold(currentQuery);
					List<Query> temporalOutput = new ArrayList<Query>();
					assistant.getCloseCircleEdges(currentQuery, minCount, temporalOutput);
					assistant.getDanglingEdges(currentQuery, minCount, temporalOutput);
					
					synchronized(queryPool){
						queryPool.addAll(temporalOutput);
					}
										
					if(assistant.testRule(currentQuery)){
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
				return (int)minPruningRatio;
			case HeadCoverage:
				return (int)Math.ceil((minPruningRatio * (double)assistant.getHeadCardinality(query)));
			default:
				return 0;
			}
		}
	}
	
	public static MiningResult run(String[] args) throws Exception{
		// TODO Auto-generated method stub
		List<File> dataFiles = new ArrayList<File>();
		List<File> targetFiles = new ArrayList<File>();
		List<File> schemaFiles = new ArrayList<File>();

		CommandLine cli = null;
		double minConf = 0.25;
		int minSup = 100;
		int minInitialSup = 100;
		double minHeadCover = 0.25;
		double minImprovedConf = 0.25;
		double minPredictiveness = 0.0;
		double maxPredictiveness = 0.5;
		int maxDepth = Integer.MAX_VALUE;
		boolean parallel = false;
		boolean realTime = false;
		boolean countAlwaysOnSubject = false;
		double minMetricValue = 0.0;
		boolean allowConstants = false;
		boolean enableOptimizations = true;
		Metric metric = Metric.HeadCoverage;
		MiningAssistant mineAssistant = null;
		Collection<ByteString> excludedRelations = new ArrayList<ByteString>();
		Collection<ByteString> targetRelations = new ArrayList<ByteString>();
		FactDatabase targetSource = null;
		FactDatabase schemaSource = null;
		int nThreads;
		
		// create the command line parser
		CommandLineParser parser = new PosixParser();
		// create the Options
		Options options = new Options();
		
		Option confidenceOpt = OptionBuilder.withArgName("min-conf")
                .hasArg()
                .withDescription("Minimum confidence" )
                .create( "minc");
		
		Option supportOpt = OptionBuilder.withArgName("min-supp")
                .hasArg()
                .withDescription("Minimum support")
                .create( "mins");
		
		Option initialSupportOpt = OptionBuilder.withArgName("min-ini-supp")
                .hasArg()
                .withDescription(  "Minimum initial support " )
                .create( "minis");
		
		Option headCoverageOpt = OptionBuilder.withArgName("min-headcover")
                .hasArg()
                .withDescription(  "Minimum head coverage" )
                .create( "minhc");
				
		Option pruningMetricOpt = OptionBuilder.withArgName("pruning-metric")
				.hasArg()
                .withDescription(  "Metric used for pruning" )
                .create( "pm");

		Option parOpt = OptionBuilder.withArgName("parallel")
					 .withDescription("Use multiple threads")
					 .create("pl");
				
		Option realTimeOpt = OptionBuilder.withArgName("real-time")
				 .withDescription("Report rules as they are mined")
				 .create("rlt");
		
		Option excludedOpt = OptionBuilder.withArgName("excluded-relations")
                 .hasArg()
				 .withDescription("Do not produce queries starting with these relations")
				 .create("ex");
		
		Option targetRelationsOpt = OptionBuilder.withArgName("target-relations")
                .hasArg()
				 .withDescription("Mine only these relations")
				 .create("tr");
		
		Option maxDepthOpt = OptionBuilder.withArgName("max-depth")
                .hasArg()
				.withDescription("Rules maximum depth: maximum total number of atoms in the antecedent and succedent of rules")
				.create("maxad");
		
		Option maxImprovedConfOpt = OptionBuilder.withArgName("min-improved-conf")
                .hasArg()
				.withDescription("Minimum improved (non-existential) confidence. Applicable only for certain types of biases")
				.create("minic");
		
		Option allowConstantsOpt = OptionBuilder.withArgName("allow-constants")
				.withDescription("Allow constants in the antecedent")
				.create("const");
		
		Option maxPredictivenessOpt = OptionBuilder.withArgName("max-predictive")
                .hasArg()
				.withDescription("Rules maximum predictiviness")
				.create("maxp");
		
		Option minPredictivenessOpt = OptionBuilder.withArgName("min-predictive")
                .hasArg()
				.withDescription("Rules minimum predictiviness")
				.create("minp");
		
		Option countOnSubjectOpt = OptionBuilder.withArgName("count-always-on-subject")
				.withDescription("Always count on subject")
				.create("caos");

		Option supportConfidenceBucketsOp = OptionBuilder.withArgName("support-conf-buckets")
                .hasArg()
                .withDescription("Support buckets" )
                .create( "sb");
		
		Option confidenceBucketsOp = OptionBuilder.withArgName("conf-buckets")
                .hasArg()
                .withDescription("Confidence buckets" )
                .create( "cb");
		
		Option improvedConfidenceBucketsOp = OptionBuilder.withArgName("improved-conf-buckets")
                .hasArg()
                .withDescription("Improved Confidence buckets" )
                .create( "icb");
		
		Option improvedPredictivenessBucketsOp = OptionBuilder.withArgName("improved-pred-buckets")
                .hasArg()
                .withDescription("Improved Predictiveness buckets" )
                .create( "ipb");
		
		Option headCoverageBucketsOp = OptionBuilder.withArgName("head-coverage-buckets")
                .hasArg()
                .withDescription("Head coverage buckets" )
                .create( "hcb");
		
		Option assistantOp = OptionBuilder.withArgName("assistant")
                .hasArg()
                .withDescription("Mining assistant class" )
                .create( "a");
		
		Option rankOp = OptionBuilder.withArgName("rank-by")
                .hasArg()
                .withDescription("Rank rules by certain metric" )
                .create( "rb");
		
		Option coresOp = OptionBuilder.withArgName("n-cores")
                .hasArg()
                .withDescription("Preferred number of cores. Round down to number of processors if higher" )
                .create( "nc");
		
		Option silentOp = OptionBuilder.withArgName("silent")
                .withDescription("Do not output the rules right after the mining phase" )
                .create( "silent");
		
		Option enableOptimOp = OptionBuilder.withArgName("optimized")
							 	.withDescription("Enable query rewriting and approximations for hard queries")
							 	.create("optim");
						
		options.addOption(confidenceOpt);
		options.addOption(supportOpt);
		options.addOption(initialSupportOpt);
		options.addOption(headCoverageOpt);
		options.addOption(pruningMetricOpt);
		options.addOption(parOpt);
		options.addOption(realTimeOpt);
		options.addOption(excludedOpt);
		options.addOption(maxDepthOpt);
		options.addOption(maxImprovedConfOpt);
		options.addOption(targetRelationsOpt);
		options.addOption(allowConstantsOpt);
		options.addOption(maxPredictivenessOpt);
		options.addOption(minPredictivenessOpt);
		options.addOption(countOnSubjectOpt);
		options.addOption(supportConfidenceBucketsOp);
		options.addOption(improvedConfidenceBucketsOp);
		options.addOption(headCoverageBucketsOp);
		options.addOption(confidenceBucketsOp);
		options.addOption(assistantOp);
		options.addOption(rankOp);
		options.addOption(silentOp);
		options.addOption(improvedPredictivenessBucketsOp);
		options.addOption(coresOp);
		options.addOption(enableOptimOp);
		
		try{
			cli = parser.parse(options, args);
		}catch(ParseException e){
			System.out.println( "Unexpected exception: " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "rdfminer", options );
			System.exit(1);
		}
						
		if(cli.hasOption("minc")){
			String minConfidenceStr = cli.getOptionValue("minc");
			minConf = Double.parseDouble(minConfidenceStr);
		}
		
		if(cli.hasOption("mins")){
			String minSupportStr = cli.getOptionValue("mins");
			minSup = Integer.parseInt(minSupportStr);
		}
		
		if(cli.hasOption("minhc")){
			String minHeadCoverage = cli.getOptionValue("minhc");
			minHeadCover = Double.parseDouble(minHeadCoverage);
		}
		
		if(cli.hasOption("maxp")){
			String maxPredictivenessStr = cli.getOptionValue("maxp");
			maxPredictiveness = Double.parseDouble(maxPredictivenessStr);
		}
		
		if(cli.hasOption("minp")){
			String minPredictivenessStr = cli.getOptionValue("minp");
			minPredictiveness = Double.parseDouble(minPredictivenessStr);
		}
		
		
		if(cli.hasOption("const")){
			allowConstants = true;
		}
		
		if(cli.hasOption("caos")){
			countAlwaysOnSubject = true;
		}
		
		if(cli.hasOption("pm")){
			switch(cli.getOptionValue("pm")){
			case "support":
				metric = Metric.Support;
				System.out.println("Using " + metric + " as pruning metric with threshold " + minSup);
				minMetricValue = minSup;
				minInitialSup = minSup;
				break;
			default:
				metric = Metric.HeadCoverage;
				System.out.println("Using " + metric + " as pruning metric with threshold " + minHeadCover);				
				minMetricValue = minHeadCover;
				if(cli.hasOption("minis")){
					String minInitialSupportStr = cli.getOptionValue("minis");
					minInitialSup = Integer.parseInt(minInitialSupportStr);
				}				
				break;
			}
		}else{
			System.out.println("Using " + metric + " as pruning metric with threshold " + minHeadCover);
			minMetricValue = minHeadCover;
			minInitialSup = minSup;
		}
		
		if(cli.hasOption("pl")){
			parallel = true;
		}
		
		if(cli.hasOption("rlt")){
			realTime = true;
		}
		
		if(cli.hasOption("ex") && cli.hasOption("tr")){
			System.err.println("Options excluded-relations and target-relations cannot be at the same time");
			System.exit(2);
		}
		
		if(cli.hasOption("ex")){
			String excludedValuesStr = cli.getOptionValue("ex");
			String[] excludedValueArr = excludedValuesStr.split(",");
			for(String excludedValue: excludedValueArr)
				excludedRelations.add(ByteString.of(excludedValue));
		}
		
		if(cli.hasOption("tr")){
			String targetValuesStr = cli.getOptionValue("tr");
			String[] targetValueArr = targetValuesStr.split(",");
			for(String targetValue: targetValueArr)
				targetRelations.add(ByteString.of(targetValue));
		}
		
		if(cli.hasOption("maxad")){
			String maxDepthStr = cli.getOptionValue("maxad");
			maxDepth = Integer.parseInt(maxDepthStr);
			if(maxDepth < 2)
				throw new IllegalArgumentException("The value for argument max-depth cannot be smaller than 2");
		}
		
		if(cli.hasOption("minic")){
			String minicStr = cli.getOptionValue("minic");
			minImprovedConf = Double.parseDouble(minicStr);
		}
		
		int nProcessors = Runtime.getRuntime().availableProcessors();
		if(cli.hasOption("nc")){
			String nCoresStr = cli.getOptionValue("nc");
			nThreads = Integer.parseInt(nCoresStr);
			if(nThreads > nProcessors) 
				nThreads = nProcessors;
		}else{
			nThreads = nProcessors;
		}
		
		String[] leftOverArgs = cli.getArgs();
		
		if(leftOverArgs.length < 1){
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "rdfminer", options );
			System.exit(1);
		}
		
		//Load database
		for(int i = 0; i < leftOverArgs.length; ++i){
			if(leftOverArgs[i].startsWith(":t"))
				targetFiles.add(new File(leftOverArgs[i].substring(2)));
			else if(leftOverArgs[i].startsWith(":s"))
				schemaFiles.add(new File(leftOverArgs[i].substring(2)));
			else				
				dataFiles.add(new File(leftOverArgs[i]));
		}
				
		FactDatabase dataSource = new FactDatabase();
		dataSource.load(dataFiles);
		
		if(!targetFiles.isEmpty()){
			targetSource = new FactDatabase();
			targetSource.load(targetFiles);
		}
		
		if(!schemaFiles.isEmpty()){
			schemaSource = new FactDatabase();
			schemaSource.load(schemaFiles);
		}
		
		if(cli.hasOption("a")){
			switch(cli.getOptionValue("a")){
			case "seedsCount":
				mineAssistant = new SeedsCountMiningAssistant(dataSource, schemaSource);
				break;
			case "twoVars":
				mineAssistant = new MiningAssistantTwoVars(dataSource);
				mineAssistant.setSchemaSource(schemaSource);
				break;
			case "typed":
				mineAssistant = new TypedMiningAssistant(dataSource);
				mineAssistant.setSchemaSource(schemaSource);
				break;
			default:
				mineAssistant = new MiningAssistant(dataSource);			
				mineAssistant.setSchemaSource(schemaSource);
			}
		}else{
			mineAssistant = new MiningAssistant(dataSource);			
			mineAssistant.setSchemaSource(schemaSource);			
		}
		
		enableOptimizations = cli.hasOption("optim");
		
		mineAssistant.setEnableOptimizations(enableOptimizations);
		mineAssistant.setMaxDepth(maxDepth);
		mineAssistant.setMinConfidence(minConf);
		mineAssistant.setMinImprovedConfidence(minImprovedConf);
		mineAssistant.setAllowConstants(allowConstants);
		mineAssistant.setExcludedRelations(excludedRelations);
		mineAssistant.setMinPredictiveness(minPredictiveness);
		mineAssistant.setMaxPredictiveness(maxPredictiveness);
		mineAssistant.setCountAlwaysOnSubject(countAlwaysOnSubject);
		
				
		AMIEMiner miner = new AMIEMiner(mineAssistant, minInitialSup, minMetricValue, parallel, metric, nThreads);
		Announce.doing("Starting the mining phase");
		System.out.println("");
		long time = System.currentTimeMillis();
		List<Query> rules = null;
		
		
		rules = miner.mine(realTime, targetRelations);
		
		if(!realTime){
			if(!cli.hasOption("silent")){
				for(Query rule: rules)
					System.out.println(rule.getRuleFullString2());
			}
		}
		
		Announce.done("Mining done in " + NumberFormatter.formatMS(System.currentTimeMillis() - time) + " seconds" );
		MiningResult mr =  new MiningResult(dataSource, targetSource, rules);
		mr.setCli(cli);
		return mr;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		run(args);
	}
}