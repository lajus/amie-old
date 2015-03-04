package amie.prediction;

import java.util.ArrayList;
import java.util.List;

import javatools.datatypes.ByteString;
import javatools.datatypes.Triple;
import amie.data.FactDatabase;
import amie.query.Query;

public class Prediction {
	
	private ByteString[] triple;
	
	private boolean hitInTarget;
	
	private boolean hitInTraining;
	
	// The rules that infer the prediction
	private List<Query> rules;
	
	private Query combinedRule;
	
	private double naiveConfidence;
	
	public enum Metric {PCAConfidence, StdConfidence};
	
	public static Metric metric = Metric.PCAConfidence;
	
	public static void setConfidenceMetric(Metric m) {
		metric = m;
	}
	 
	public Prediction(ByteString[] triple) {
		this.triple = triple;
		hitInTarget = false;
		rules = new ArrayList<>();
		naiveConfidence = Double.NEGATIVE_INFINITY;
		combinedRule = null;
	}
	
	public Prediction(Triple<ByteString, ByteString, ByteString> triple) {
		this.triple = new ByteString[]{triple.first, triple.second, triple.third};
		hitInTarget = false;
		rules = new ArrayList<>();
		naiveConfidence = Double.NEGATIVE_INFINITY;
		combinedRule = null;
	}
	
	public ByteString[] getTriple() {
		return triple;
	}
	
	public boolean isHitInTarget() {
		return hitInTarget;
	}

	public void setHitInTarget(boolean hit) {
		this.hitInTarget = hit;
	}

	public boolean isHitInTraining() {
		return hitInTraining;
	}

	public void setHitInTraining(boolean hitInTraining) {
		this.hitInTraining = hitInTraining;
	}

	/**
	 * Returns the confidence of the prediction as 1 - (1 - PCA(R1))...(1 - PCA(Rn))
	 * It assumes rules are independent.
	 * @return
	 */
	public double getNaiveConfidence() {
		if (naiveConfidence == Double.NEGATIVE_INFINITY) {
			double product = 1.0;
			for (Query rule : rules) {
				if (Prediction.metric == Metric.PCAConfidence) {
					product *= (1.0 - rule.getPcaConfidence());
				} else {
					product *= (1.0 - rule.getConfidence());					
				}
			}
			naiveConfidence = 1.0 - product;
		}
		return naiveConfidence;
	}
	
	/**
	 * Returns the combined rule constructed by merging the antecedents of 
	 * the predicting rules and keeping the most specific head relation. The PCA
	 * confidence of this rule is the predictions confidence score without independence
	 * assumptions.
	 * @return
	 */
	public Query getJointRule() {
		if (rules.size() == 1) {
			return rules.get(0);
		}
		
		if (combinedRule == null) {
			combinedRule = Query.combineRules(rules);
		}
		
		return combinedRule;
	}
	
	public double getConfidence() {
		if (Prediction.metric == Metric.PCAConfidence)
			return getJointRule().getPcaConfidence();
		else
			return getJointRule().getConfidence();
	}
	
	public List<Query> getRules() {
		return rules;
	}
	
	public String toEvaluationString() {
		StringBuilder builder = new StringBuilder();
		builder.append(triple[0]);
		builder.append("\t");		
		builder.append(triple[1]);
		builder.append("\t");		
		builder.append(triple[2]);
		builder.append("\t");	
		builder.append(getNaiveConfidence());
		builder.append("\t");
		builder.append(getConfidence());
		builder.append("\t");
		
		if (hitInTarget) {
			builder.append("TargetSource\tTrue");
		} else {
			builder.append("ManualEvaluation\t");
		}
		
		return builder.toString();
	}
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(FactDatabase.toString(triple));
		if (hitInTarget) {
			builder.append("\tHit in target");
		}
		
		if (hitInTraining) {
			builder.append("\tHit in training");
		}
		
		builder.append("\t" + getNaiveConfidence());
		builder.append("\t" + getConfidence());
		
		for (Query q : rules) {
			builder.append("\t" + q.getRuleString() + "[" + q.getPcaConfidence() + "]");
		}
		return builder.toString(); 
	}

	public String getCompressedString() {
		StringBuilder builder = new StringBuilder();
		builder.append(FactDatabase.toString(triple));
		if (hitInTarget) {
			builder.append("\tHit in target");
		}
		if (hitInTraining) {
			builder.append("\tHit in training");
		}
	
		builder.append("\t" + rules.size());
		builder.append("\t" + getNaiveConfidence());
		builder.append("\t" + getConfidence());
		
		return builder.toString(); 
	}

	public boolean inTargetButNotInTraining() {
		return hitInTarget && !hitInTraining;
	}
}
