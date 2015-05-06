package amie.prediction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Pair;
import javatools.datatypes.Triple;
import javatools.filehandlers.TSVFile;
import amie.data.FactDatabase;
import amie.mining.assistant.HeadVariablesImprovedMiningAssistant;
import amie.prediction.Prediction.Metric;
import amie.query.AMIEreader;
import amie.query.Query;

public class SampleWikiLinksByScore {
	
	private static int SampleSize = 200000;

	private static List<List<Pair<Pair<ByteString, ByteString>, List<Prediction>>>> initializeBuckets() {
		List<List<Pair<Pair<ByteString, ByteString>, List<Prediction>>>> buckets = new ArrayList<>(10);
		for (int i = 0; i < 10; ++i) {
			buckets.add(new ArrayList<Pair<Pair<ByteString, ByteString>, List<Prediction>>>());
		}
		
		return buckets;
	}
	
	public static void main(String[] args) throws IOException {
		if(args.length < 2){
			System.err.println("SampleLinksByScore <rules> <trainingDb> bindingsWithoutLinks=false [testingDb]");
			System.exit(1);
		}
		
		File inputFile = new File(args[0]);		
		FactDatabase trainingDataset = new FactDatabase();	
		FactDatabase testingDataset = new FactDatabase();
		trainingDataset.load(new File(args[1]));
		TSVFile tsvFile = new TSVFile(inputFile);
		boolean bindingsWithoutLinks = false;

		if (args.length > 2) {
			bindingsWithoutLinks = Boolean.parseBoolean(args[2]);
		}
		
		if (args.length > 3) {
			testingDataset.load(new File(args[3]));
		}
		
		List<List<Pair<Pair<ByteString, ByteString>, List<Prediction>>>> buckets = initializeBuckets();
			
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
		Map<Pair<ByteString, ByteString>, List<Prediction>> linksToLabels = null;
		System.out.println("BindingsWithoutLinks = " + bindingsWithoutLinks);
		if (bindingsWithoutLinks)
			linksToLabels = getPredictionsWithoutLinks(queries, trainingDataset, true, testingDataset);
		else
			linksToLabels = getPredictions(queries, trainingDataset, true, testingDataset);
		
		System.out.println(linksToLabels.size() + " links");
		PredictionsComparator cmp = new PredictionsComparator(true);
		int numberOfHits = 0; // Number of triples found in the testing dataset.
		int numberOfAutomSemantified = 0; // Number of automatically semantified wikilinks
		for (Pair<ByteString, ByteString> link : linksToLabels.keySet()) {
			List<Prediction> labels = linksToLabels.get(link);
			Collections.sort(labels, cmp);
			// Take the prediction with the highest score
			Prediction representativePrediction = labels.get(0);
			boolean automaticallySemantified = false;
			for (Prediction prediction : labels) {
				if (prediction.isHitInTarget()) {
					++numberOfHits;
					automaticallySemantified = true;
				}
			}
			if (automaticallySemantified) {
				++numberOfAutomSemantified;
			}
			
			double naiveConfidence = representativePrediction.getNaiveConfidence();
			
			if (naiveConfidence < 0.0 || naiveConfidence > 1.0) {
				System.err.println(representativePrediction.toNaiveEvaluationString());
				System.exit(1);
			}
			int bucketId = Math.max(0, 9 - (int)(naiveConfidence * 10));
			buckets.get(bucketId).add(new Pair<>(link, labels));
		}

		for (int i = 0; i < buckets.size(); ++i) {
			double a = 1.0 - i * 0.1;
			double b = 1.0 - (i + 1) * 0.1;
			System.out.println("Confidence [" + a + ", " + b + ")");
			System.out.println(buckets.get(i).size() + " links");
			printPredictions(samplePredictions(buckets.get(i)));
		}
		System.out.println("Number of hits: " + numberOfHits);
		System.out.println("Number of automatically semantified wikilinks: " + numberOfAutomSemantified);		
		
	}

	private static void printPredictions(List<Pair<Pair<ByteString, ByteString>, List<Prediction>>> predictions) {
		for (int i = 0; i < predictions.size(); ++i) {
			Pair<Pair<ByteString, ByteString>, List<Prediction>> pair = predictions.get(i);
			System.out.print(pair.first.first + "\t<linksTo>\t" + pair.first.second + "\t");
			for (Prediction prediction : pair.second) {
				Triple<ByteString, ByteString, ByteString> triple = prediction.getTripleObj();
				// Order is preserved
				boolean checked = prediction.isHitInTarget();
				if (pair.first.first.equals(triple.first)) {
					System.out.print(triple.second + "[" + prediction.getNaiveConfidence() + ", " + checked + "]" +  ", ");	
				} else {
					System.out.print(triple.second + "-inv[" + prediction.getNaiveConfidence() + ", " + checked + "]" +  ", ");	
				}
			}
			System.out.println();
		}
		
	}

	private static List<Pair<Pair<ByteString, ByteString>, List<Prediction>>> samplePredictions(
			List<Pair<Pair<ByteString, ByteString>, List<Prediction>>> list) {
		return (List<Pair<Pair<ByteString, ByteString>, List<Prediction>>>) telecom.util.collections.Collections.reservoirSampling(list, SampleSize);
	}

	private static Map<Pair<ByteString, ByteString>, List<Prediction>> getPredictions(List<Query> queries,
			FactDatabase trainingDataset, boolean b, FactDatabase testingDataset) {
		Map<Pair<ByteString, ByteString>, List<Prediction>> result = new HashMap<Pair<ByteString, ByteString>, List<Prediction>>();
		Map<Triple<ByteString, ByteString, ByteString>, List<Query>> predictions =
				JointPredictions.findPredictionsForRules(queries, trainingDataset, new FactDatabase(), true);
		List<ByteString[]> linksQuery = FactDatabase.triples(FactDatabase.triple(ByteString.of("?s"), ByteString.of("<linksTo>"), ByteString.of("?o")));
		Map<ByteString, IntHashMap<ByteString>> links = trainingDataset.selectDistinct(ByteString.of("?s"), ByteString.of("?o"), linksQuery);
	
		for (Triple<ByteString, ByteString, ByteString> t : predictions.keySet()) {
			Prediction prediction = new Prediction(t);
			prediction.getRules().addAll(predictions.get(t));
			if (containsLink(links, t.first, t.third)) {
				if (testingDataset.contains(prediction.getTriple())) {
					prediction.setHitInTarget(true);
				}
				addToMap(result, new Pair<ByteString, ByteString>(t.first, t.third), prediction);
			}
		}
		
		return result;
	}

	private static Map<Pair<ByteString, ByteString>, List<Prediction>> getPredictionsWithoutLinks(List<Query> queries,
			FactDatabase trainingDataset, boolean b, FactDatabase testingDataset) {
		Map<Pair<ByteString, ByteString>, List<Prediction>> result = new HashMap<Pair<ByteString, ByteString>, List<Prediction>>();
		Map<Triple<ByteString, ByteString, ByteString>, List<Query>> predictions =
				JointPredictions.findPredictionsForRules(queries, trainingDataset, new FactDatabase(), true);
		List<ByteString[]> linksQuery = FactDatabase.triples(FactDatabase.triple(ByteString.of("?s"), ByteString.of("<linksTo>"), ByteString.of("?o")));
		Map<ByteString, IntHashMap<ByteString>> links = trainingDataset.selectDistinct(ByteString.of("?s"), ByteString.of("?o"), linksQuery);
	
		for (Triple<ByteString, ByteString, ByteString> t : predictions.keySet()) {
			Prediction prediction = new Prediction(t);
			prediction.getRules().addAll(predictions.get(t));
			if (containsLink(links, t.first, t.third)) {
				List<ByteString[]> noLinkQuery = FactDatabase.triples(
						FactDatabase.triple(t.first, ByteString.of("?p"), t.third),
						FactDatabase.triple(ByteString.of("?p"), FactDatabase.DIFFERENTFROMbs, ByteString.of("<linksTo>"))
						);	
				long nRelations = trainingDataset.countDistinct(ByteString.of("?p"), noLinkQuery);
				if (nRelations == 0) {
					if (testingDataset.contains(prediction.getTriple())) {
						prediction.setHitInTarget(true);
					}
					// Bingo
					addToMap(result, new Pair<ByteString, ByteString>(t.first, t.third), prediction);
				}
			}
		}
		
		return result;
	}

	private static void addToMap(
			Map<Pair<ByteString, ByteString>, List<Prediction>> result,
			Pair<ByteString, ByteString> pair, Prediction prediction) {
		List<Prediction> predictions = result.get(pair);
		if (predictions == null) {
			predictions = new ArrayList<Prediction>();
			result.put(pair, predictions);
		}
		predictions.add(prediction);
	}

	private static boolean containsLink(
			Map<ByteString, IntHashMap<ByteString>> links, ByteString first,
			ByteString second) {
		if (links.containsKey(first)) {
			return links.get(first).contains(second);
		} else {
			return false;
		}
	}

}
