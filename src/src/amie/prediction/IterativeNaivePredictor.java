package amie.prediction;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javatools.datatypes.IntHashMap;
import amie.data.FactDatabase;
import amie.mining.AMIE;
import amie.mining.assistant.HeadVariablesMiningAssistant;
import amie.query.Query;
import amie.utils.Utils;

public class IterativeNaivePredictor {
	private FactDatabase trainingDb;
	
	private FactDatabase testingDb;
	
	public static final double DefaultPCAConfidenceThreshold = 0.4;
	
	public static final int DefaultSupportThreshold = 100;
	
	public IterativeNaivePredictor(FactDatabase training) {
		this.trainingDb = training;
	}
	
	public IterativeNaivePredictor(FactDatabase training, FactDatabase testing) {
		this.trainingDb = training;
		this.testingDb = testing;
	}
	
	public List<Prediction> predict(int numberIterations, boolean onlyHits) throws Exception {
		return predict(numberIterations, onlyHits, DefaultPCAConfidenceThreshold);
	}
	
	public List<Prediction> predict(int numberIterations, boolean onlyHits, double confidenceThreshold) throws Exception {
		List<Prediction> resultingPredictions = new ArrayList<>();
		AMIE amieMiner = AMIE.getLossyInstance(trainingDb, confidenceThreshold, DefaultSupportThreshold);		
		HeadVariablesMiningAssistant miningAssistant = new HeadVariablesMiningAssistant(trainingDb);
		if (onlyHits) {
			System.out.println("Including only hits");
		}
		for (int i = 0; i < numberIterations; ++i) {
			System.out.println("Inference round #" + (i + 1));
			// Mine rules
			long startTime = System.currentTimeMillis();
			List<Query> rules = amieMiner.mine(false, Collections.EMPTY_LIST);
			System.out.println("Rule mining took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
			System.out.println(rules.size() + " rules found");
			for(Query rule: rules)
				System.out.println(rule.getBasicRuleString());
			// Get the predictions
			List<Prediction> predictions = JointPredictions.getPredictions(rules, trainingDb, testingDb, false);
			System.out.println("Adding " + predictions.size() + " predictions");
			int countAboveThreshold = 0;
			for (Prediction prediction : predictions) {
				if (onlyHits) {
					if (prediction.inTargetButNotInTraining()) {
						resultingPredictions.add(prediction);
						++countAboveThreshold;
					}
				} else {
					if (!prediction.isHitInTraining()) {
						resultingPredictions.add(prediction);
						++countAboveThreshold;
					}
				}
			}
			System.out.println(countAboveThreshold + " new predictions added to the KB");
			updateDb(resultingPredictions, trainingDb);
		}
		
		for (Prediction prediction : resultingPredictions) {
			Query combinedRule = prediction.getJointRule();
			miningAssistant.computeCardinality(combinedRule);
			miningAssistant.computePCAConfidence(combinedRule);
		}
		PredictionsComparator predictionsCmp = new PredictionsComparator();
		Collections.sort(resultingPredictions, predictionsCmp);
		return resultingPredictions;
	}
	
	private void updateDb(List<Prediction> predictions, FactDatabase trainingDb) {
		for (Prediction prediction : predictions) {
			trainingDb.add(prediction.getTriple());
		}
	}

	public static void main(String[] args) throws Exception {
		FactDatabase training = new FactDatabase();
		FactDatabase testing = new FactDatabase();
		training.load(new File(args[0]));
		if (!args[1].equals("-")) {
			testing.load(new File(args[1]));
		}
		IterativeNaivePredictor predictor = new IterativeNaivePredictor(training, testing);
		
		List<Prediction> predictions = null;
		if (args.length > 4) {
			predictions = predictor.predict(Integer.parseInt(args[2]), 
					Boolean.parseBoolean(args[3]), Double.parseDouble(args[4]));
		}else {
			predictions = predictor.predict(Integer.parseInt(args[2]), 
					Boolean.parseBoolean(args[3]));
		}	
		
		IntHashMap<Integer> hitsInTargetHistogram = new IntHashMap<>();
		IntHashMap<Integer> hitsInTargetNotInSourceHistogram = new IntHashMap<>();
		IntHashMap<Integer> predictionsHistogram = new IntHashMap<>();
		for (Prediction prediction : predictions) {
			System.out.println(prediction.getCompressedString());
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
