package amie.prediction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javatools.datatypes.ByteString;
import javatools.datatypes.Triple;
import javatools.filehandlers.TSVFile;
import amie.data.FactDatabase;
import amie.data.eval.PredictionsSampler;
import amie.mining.assistant.HeadVariablesImprovedMiningAssistant;
import amie.prediction.Prediction.Metric;
import amie.query.AMIEreader;
import amie.query.Query;

public class SamplePredictionsByScore {

	public static final int sampleSize = 100;
	
	public static void main(String[] args) throws IOException {
		if(args.length < 3){
			System.err.println("JointPredictions <rules> <trainingDb> <targetDb> <random> <naive>");
			System.exit(1);
		}
		
		File inputFile = new File(args[0]);		
		FactDatabase trainingDataset = new FactDatabase();
		FactDatabase targetDataset = new FactDatabase();		
		TSVFile tsvFile = new TSVFile(inputFile);
		boolean random = Boolean.parseBoolean(args[3]);
		boolean naive = Boolean.parseBoolean(args[4]);
		List<Prediction> bucket_0_8_to_1_0 = new ArrayList<>();
		List<Prediction> bucket_0_6_to_0_8 = new ArrayList<>();
		List<Prediction> bucket_0_4_to_0_6 = new ArrayList<>();
		List<Prediction> bucket_0_4 = new ArrayList<>();
	
		// Load the data
		trainingDataset.load(new File(args[1]));
		targetDataset.load(new File(args[2]));
		
		List<Query> queries = new ArrayList<>();
		
		// Parse the rules
		for(List<String> record: tsvFile) {
			Query q = AMIEreader.rule(record.get(0));
			if(q == null) {
				continue;
			}
			queries.add(q);			
		}
		tsvFile.close();
		Prediction.setConfidenceMetric(Metric.StdConfidence);
		List<Prediction> predictions = JointPredictions.getPredictions(queries, trainingDataset, targetDataset, true);
		HeadVariablesImprovedMiningAssistant miningAssistant = new HeadVariablesImprovedMiningAssistant(trainingDataset);
		int predictionsConsidered = 0;
		for (Prediction prediction : predictions) {
			double naiveConfidence = prediction.getNaiveConfidence();
			if (prediction.getRules().size() == 1) {
				continue;
			}
			++predictionsConsidered;
			Query combinedRule = prediction.getJointRule();
			miningAssistant.computeCardinality(combinedRule);
			miningAssistant.computePCAConfidence(combinedRule);
			if (naiveConfidence >= 0.8) {
				bucket_0_8_to_1_0.add(prediction);
			} else if (naiveConfidence >= 0.6) {
				bucket_0_6_to_0_8.add(prediction);
			} else if (naiveConfidence >= 0.4) {
				bucket_0_4_to_0_6.add(prediction);
			} else {
				bucket_0_4.add(prediction);
			}
		}
		System.out.println(predictionsConsidered + " predictions in total.");
		if (random) {
			System.out.println("Confidence [0.8-1.0]");
			printPredictions(samplePredictions(bucket_0_8_to_1_0));
			System.out.println("Confidence [0.6-0.8]");
			printPredictions(samplePredictions(bucket_0_6_to_0_8));
			System.out.println("Confidence [0.4-0.6]");
			printPredictions(samplePredictions(bucket_0_4_to_0_6));
			System.out.println("Confidence [0-0.4]");
			printPredictions(samplePredictions(bucket_0_4));
		} else {
			System.out.println("Confidence [0.8-1.0]");
			Collections.sort(bucket_0_8_to_1_0, naive ? new NaivePredictionsComparator() : new PredictionsComparator());
			
			printPredictions(bucket_0_8_to_1_0.subList(0, Math.min(sampleSize, bucket_0_8_to_1_0.size())));
			System.out.println("Confidence [0.6-0.8]");
			Collections.sort(bucket_0_6_to_0_8, naive ? new NaivePredictionsComparator() : new PredictionsComparator());
			printPredictions(bucket_0_6_to_0_8.subList(0, Math.min(sampleSize, bucket_0_6_to_0_8.size())));
			System.out.println("Confidence [0.4-0.6]");
			Collections.sort(bucket_0_4_to_0_6, naive ? new NaivePredictionsComparator() : new PredictionsComparator());
			printPredictions(bucket_0_4_to_0_6.subList(0, Math.min(sampleSize, bucket_0_4_to_0_6.size())));			
			System.out.println("Confidence [0-0.4]");
			Collections.sort(bucket_0_4, naive ? new NaivePredictionsComparator() : new PredictionsComparator());
			printPredictions(bucket_0_4.subList(0, Math.min(sampleSize, bucket_0_4.size())));
		}
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
			System.out.println(prediction.toEvaluationString());
		}
	}
}