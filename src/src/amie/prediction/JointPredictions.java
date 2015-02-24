package amie.prediction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Triple;
import javatools.filehandlers.TSVFile;
import amie.data.FactDatabase;
import amie.data.eval.Evaluator;
import amie.data.eval.PredictionsSampler;
import amie.mining.assistant.HeadVariablesMiningAssistant;
import amie.query.AMIEreader;
import amie.query.Query;
import amie.utils.Utils;

public class JointPredictions {

	/**
	 * Given a set of rules, it runs an iteration of deduction and returns a map
	 * prediction -> {list of rules that deduced this prediction}
	 * @param queries
	 * @param trainingDataset
	 * @param targetDataset
	 * @return
	 */
	private static Map<Triple<ByteString, ByteString, ByteString>, List<Query>> findPredictions2Rules(List<Query> queries, 
			FactDatabase trainingDataset, FactDatabase targetDataset) {
		Map<Triple<ByteString, ByteString, ByteString>, List<Query>> predictions = new HashMap<>();
		HeadVariablesMiningAssistant assistant = new HeadVariablesMiningAssistant(trainingDataset);
		PredictionsSampler predictor = new PredictionsSampler(trainingDataset);
		for (Query q : queries) {
			ByteString[] head = q.getHead();
			q.setFunctionalVariable(q.getHead()[Query.findFunctionalVariable(q, trainingDataset)]);
			assistant.computeCardinality(q);
			assistant.computePCAConfidence(q);
			
			Object bindings = null;
			try {
				bindings = predictor.generateBodyBindings(q);
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
	 * @return
	 */
	public static List<Prediction> getPredictions(List<Query> queries, 
			FactDatabase trainingDataset, FactDatabase targetDataset) {
		List<Prediction> result = new ArrayList<>();
		Map<Triple<ByteString, ByteString, ByteString>, List<Query>> predictions =
				findPredictions2Rules(queries, trainingDataset, targetDataset);
		for (Triple<ByteString, ByteString, ByteString> t : predictions.keySet()) {
			Prediction prediction = new Prediction(t);
			prediction.getRules().addAll(predictions.get(t));
			ByteString triple[] = prediction.getTriple();
			int eval = Evaluator.evaluate(triple, trainingDataset, targetDataset);
			if(eval == 0) { 
				prediction.setHitInTarget(true);
			}
			
			if(trainingDataset.count(triple) > 0) {
				prediction.setHitInTraining(true);
			}
			
			result.add(prediction);
		}
		
		return result;
	}
	
	public static void main(String[] args) throws IOException {
		if(args.length < 3){
			System.err.println("JointPredictions <inputfile> <trainingDb> <targetDb>");
			System.exit(1);
		}
		
		File inputFile = new File(args[0]);		
		FactDatabase trainingDataset = new FactDatabase();
		FactDatabase targetDataset = new FactDatabase();		
		TSVFile tsvFile = new TSVFile(inputFile);
		
		IntHashMap<Integer> hitsInTargetHistogram = new IntHashMap<>();
		IntHashMap<Integer> hitsInTargetNotInSourceHistogram = new IntHashMap<>();
		IntHashMap<Integer> predictionsHistogram = new IntHashMap<>();
		
		int rawHitsInTargetNotInTraining = 0;
		int hitsInTarget = 0;
		int rawHitsInTarget = 0;
		int rawHitsInTraining = 0;
		int hitsInTraining = 0;
		int hitsInTargetNotInTraining = 0;
		
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
		
		List<Prediction> predictions = getPredictions(queries, trainingDataset, targetDataset);
		
		for (Prediction prediction : predictions) {
			predictionsHistogram.increase(prediction.getRules().size());
			
			if(prediction.isHitInTarget()) { 
				++hitsInTarget;
				rawHitsInTarget += prediction.getRules().size();
				hitsInTargetHistogram.increase(prediction.getRules().size());
			}
			
			if(prediction.isHitInTraining()) {
				++hitsInTraining;
				rawHitsInTraining += prediction.getRules().size();
			} else {
				if (prediction.isHitInTarget()) {
					++hitsInTargetNotInTraining;
					rawHitsInTargetNotInTraining += prediction.getRules().size();
					hitsInTargetNotInSourceHistogram.increase(prediction.getRules().size());
				}
			}
			
			System.out.println(prediction);
		}
		
		System.out.println("Total unique predictions\tTotal Hits in target"
				+ "\tTotal unique hits in target\tTotal hits on training"
				+ "\tTotal unique hits in training\tTotal hits in target not in training"
				+ "\tTotal unique hits in target not in training");
		System.out.println(predictions.size() + 
				"\t" + rawHitsInTarget + "\t" + hitsInTarget + 
				"\t" + rawHitsInTraining + "\t" + hitsInTraining + 
				"\t" + rawHitsInTargetNotInTraining + "\t" + hitsInTargetNotInTraining);
		
		System.out.println("Predictions histogram");
		Utils.printHistogram(predictionsHistogram);
		
		System.out.println("Hits in target histogram");
		Utils.printHistogram(hitsInTargetHistogram);
		
		System.out.println("Hits In target but not in training histogram");
		Utils.printHistogram(hitsInTargetNotInSourceHistogram);
	}

	private static void add2Map(
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

}
