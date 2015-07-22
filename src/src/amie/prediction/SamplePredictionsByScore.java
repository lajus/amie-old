package amie.prediction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javatools.datatypes.ByteString;
import javatools.datatypes.Triple;
import javatools.filehandlers.TSVFile;
import amie.data.FactDatabase;
import amie.data.eval.PredictionsSampler;
import amie.mining.assistant.experimental.HeadVariablesImprovedMiningAssistant;
import amie.prediction.Prediction.Metric;
import amie.query.AMIEreader;
import amie.query.Query;

public class SamplePredictionsByScore {

	public static final int sampleSize = 100;
	
	public static void main(String[] args) throws IOException {
		if(args.length < 5){
			System.err.println("SamplePredictionsyByScore <rules> <trainingDb> <targetDb> <random> <naive> [allrules=false]");
			System.exit(1);
		}
		
		File inputFile = new File(args[0]);		
		FactDatabase trainingDataset = new FactDatabase();
		FactDatabase targetDataset = new FactDatabase();		
		TSVFile tsvFile = new TSVFile(inputFile);
		boolean random = Boolean.parseBoolean(args[3]);
		boolean allRules = false;
		boolean naive = Boolean.parseBoolean(args[4]);
		
		if (args.length > 5) {
			allRules = Boolean.parseBoolean(args[5]);
		}
		
		List<List<Prediction>> buckets = initializeBuckets();
	
		// Load the data
		trainingDataset.load(new File(args[1]));
		if (!args[2].equals("-")) {
			targetDataset.load(new File(args[2]));
		}
		
		List<Query> queries = new ArrayList<>();
		HeadVariablesImprovedMiningAssistant miningAssistant = new HeadVariablesImprovedMiningAssistant(trainingDataset);
		// Parse the rules
		for(List<String> record: tsvFile) {
			Query q = AMIEreader.rule(record.get(0));
			if(q == null) {
				continue;
			}
			miningAssistant.computeCardinality(q);
			miningAssistant.computeStandardConfidence(q);
			miningAssistant.computePCAConfidence(q);
			queries.add(q);			
		}
		tsvFile.close();
		Prediction.setConfidenceMetric(Metric.PCAConfidence);
		List<Prediction> predictions = null;
		predictions = JointPredictions.getPredictionsWithoutLinks(queries, trainingDataset, targetDataset, true);
		
		int predictionsConsidered = 0;
		System.out.println(predictions.size() + " predictions");
		for (Prediction prediction : predictions) {
			double naiveConfidence = prediction.getNaiveIndependentConfidence();
			if (!allRules && prediction.getRules().size() == 1) {
				continue;
			}
			++predictionsConsidered;
			if (!naive) {
				Query combinedRule = prediction.getJointRule();
				miningAssistant.computeCardinality(combinedRule);
				miningAssistant.computePCAConfidence(combinedRule);
			}
			
			if (naiveConfidence < 0.0 || naiveConfidence > 1.0) {
				System.err.println(prediction.toNaiveEvaluationString());
				System.exit(1);
			}
			int bucketId = Math.max(0, 9 - (int)(naiveConfidence * 10));
			buckets.get(bucketId).add(prediction);
		}
		if (predictionsConsidered != predictions.size()) {
			System.out.println(predictionsConsidered + " considered for sampling.");
		}
		if (random) {
			for (int i = 0; i < buckets.size(); ++i) {
				double a = 1.0 - i * 0.1;
				double b = 1.0 - (i + 1) * 0.1;
				System.out.println("Confidence [" + a + ", " + b + ")");
				System.out.println(buckets.get(i).size() + " predictions");
				printPredictions(samplePredictions(buckets.get(i)));
			}
		} else {
			for (int i = 0; i < buckets.size(); ++i) {
				double a = 1.0 - i * 0.1;
				double b = 1.0 - (i + 1) * 0.1;
				System.out.println("Confidence [" + a + ", " + b + ")");
				System.out.println(buckets.get(i).size() + " predictions");
				printPredictions(buckets.get(i).subList(0, Math.min(sampleSize, buckets.get(i).size())));
			}
		}
	}

	private static List<List<Prediction>> initializeBuckets() {
		List<List<Prediction>> buckets = new ArrayList<>(10);
		for (int i = 0; i < 10; ++i) {
			buckets.add(new ArrayList<Prediction>());
		}
		
		return buckets;
	}

	private static List<Prediction> samplePredictions(
			List<Prediction> predictions) {
		Map<Triple<ByteString, ByteString, ByteString>, Prediction> triple2Prediction = new HashMap<>();
		for (Prediction prediction : predictions) {
			ByteString[] triple = prediction.getTriple();
			triple2Prediction.put(new Triple<>(triple[0], triple[1], triple[2]), prediction);
		}
		
		Collection<Triple<ByteString, ByteString, ByteString>> sample = 
				PredictionsSampler.sample((Collection<Triple<ByteString, ByteString, ByteString>>)triple2Prediction.keySet(), sampleSize);
	
		List<Prediction> finalPredictions = new ArrayList<>();
		for (Triple<ByteString, ByteString, ByteString> triple : sample) {
			finalPredictions.add(triple2Prediction.get(triple));
		}
		
		return finalPredictions;
	}

	private static void printPredictions(List<Prediction> list) {
		for (Prediction prediction : list) {
			System.out.println(prediction.toNaiveEvaluationString());
		}
	}
}