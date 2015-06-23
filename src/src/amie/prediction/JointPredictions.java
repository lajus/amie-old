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
import amie.prediction.assistant.ProbabilisticHeadVariablesMiningAssistant;
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
	 * @param notInTraining 
	 * @return
	 */
	static Map<Triple<ByteString, ByteString, ByteString>, List<Query>> findPredictionsForRules(List<Query> queries, 
			FactDatabase trainingDataset, FactDatabase targetDataset, boolean notInTraining) {
		Map<Triple<ByteString, ByteString, ByteString>, List<Query>> predictions = new HashMap<>();
		HeadVariablesMiningAssistant assistant = new HeadVariablesMiningAssistant(trainingDataset);
		PredictionsSampler predictor = new PredictionsSampler(trainingDataset);
		for (Query q : queries) {
			ByteString[] head = q.getHead();
			q.setFunctionalVariablePosition(Query.findFunctionalVariable(q, trainingDataset));
			assistant.computeCardinality(q);
			assistant.computePCAConfidence(q);
			 
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
	
	/**
	 * Returns the list of all the predictions made by the given rules on the training dataset.
	 * The correctness of the predictions is verified in the target dataset. 
	 * @param queries
	 * @param trainingDataset
	 * @param targetDataset
	 * @param miningAssistant 
	 * @return
	 */
	public static List<Prediction> getPredictions(List<Query> queries, 
			FactDatabase trainingDataset, FactDatabase targetDataset, 
			ProbabilisticHeadVariablesMiningAssistant miningAssistant, boolean notInTraining) {
		List<Prediction> result = new ArrayList<>();
		Map<Triple<ByteString, ByteString, ByteString>, List<Query>> predictions =
				findPredictionsForRules(queries, trainingDataset, targetDataset, notInTraining);
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
			
			// First calculate the confidence of the combined rule
			/*Query combinedRule = prediction.getJointRule();
			
			if (combinedRule != prediction.getRules().get(0)) {
				if (i == 0) {
					miningAssistant.computeCardinality(combinedRule);
					miningAssistant.computePCAConfidence(combinedRule);						
					computeCardinalityScore(prediction, false);
				} else {
					miningAssistant.computeProbabilisticMetrics(combinedRule);
					computeCardinalityScore(prediction, true);
				}
			}
			
			double finalConfidence = prediction.getFullScore();
			if (finalConfidence >= confidenceThreshold) {
				++addedPredictions;
				resultingPredictions.add(prediction);
				prediction.setIterationId(i + 1);
				ByteString[] triple = prediction.getTriple();
				trainingDb.add(triple[0], triple[1], triple[2], finalConfidence);
			
			result.add(prediction);*/
		}
		
		return result;
	}
	
	/**
	 * Returns the list of all the predictions made by the given rules on the training dataset.
	 * It restricts the output to those paris of entities for which there is no link in the 
	 * KB.
	 * @param queries
	 * @param trainingDataset
	 * @param targetDataset
	 * @param notInTraining
	 * @return
	 */
	public static List<Prediction> getPredictionsWithoutLinks(List<Query> queries, 
			FactDatabase trainingDataset, FactDatabase targetDataset, boolean notInTraining) {
		List<Prediction> result = new ArrayList<>();
		Map<Triple<ByteString, ByteString, ByteString>, List<Query>> predictions =
				findPredictionsForRules(queries, trainingDataset, targetDataset, notInTraining);
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
			
			List<ByteString[]> noLinkQuery = FactDatabase.triples(
					FactDatabase.triple(t.first, ByteString.of("?p"), t.third),
					FactDatabase.triple(ByteString.of("?p"), FactDatabase.DIFFERENTFROMbs, ByteString.of("<linksTo>"))
					);
			long nRelations = trainingDataset.countDistinct(ByteString.of("?p"), noLinkQuery);
			System.out.println(FactDatabase.toString(noLinkQuery) + ": " + nRelations);
			if (nRelations == 0) {
				// Bingo
				result.add(prediction);
			}
		}
		
		return result;
	}
	/**
	 * Add a pair triple/query to a map.
	 * @param predictions
	 * @param prediction
	 * @param q
	 */
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

	public static void main(String[] args) throws IOException {

	}
}
