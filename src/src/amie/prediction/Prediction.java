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
	
	/**
	 * The rule consisting of the intersection of all the rules
	 * that make the prediction.
	 */
	private Query jointRule;
	
	/**
	 * 1 - (Conf(R1) * Conf(R2) ... Conf(Rn))
	 * where Conf can be either the standard or the PCA confidence
	 */
	private double naiveConfidence;
	
	/**
	 * Based on soft cardinality constraints, it is the probability that
	 * the prediction meets a cardinality constraint. For instance, if this prediction
	 * postulates a third child for a person, the cardinality score is the probability
	 * that people has more than 2 children in the KB. 
	 */
	private double cardinalityScore;
	
	/**
	 * If the predictions are drawn from an iterative process, this attribute stores
	 * the number of the iteration in which this prediction was drawn for the first
	 * time.
	 */
	private int iterationId;
	
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
		cardinalityScore = 1.0;
		jointRule = null;
		setIterationId(-1);
	}
	
	public Prediction(Triple<ByteString, ByteString, ByteString> triple) {
		this.triple = new ByteString[]{triple.first, triple.second, triple.third};
		hitInTarget = false;
		rules = new ArrayList<>();
		naiveConfidence = Double.NEGATIVE_INFINITY;
		cardinalityScore = 1.0;
		jointRule = null;
		setIterationId(-1);
	}
	
	public ByteString[] getTriple() {
		return triple;
	}
	
	public Triple<ByteString, ByteString, ByteString> getTripleObj() {
		return new Triple<ByteString, ByteString, ByteString>(triple[0], triple[1], triple[2]);
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
					product *= (1.0 - rule.getStdConfidence());					
				}
			}
			naiveConfidence = 1.0 - product;
		}
		return naiveConfidence;
	}
	
	public double getCardinalityScore() {
		return cardinalityScore;
	}

	public void setCardinalityScore(double cardinalityScore) {
		this.cardinalityScore = cardinalityScore;
	}
	
	public double getNaiveFullScore() {
		return getNaiveConfidence() * getCardinalityScore();
	}
	
	public double getFullScore() {
		return getConfidence() * getCardinalityScore();
	}
	
	public void setJointRule(Query rule) {
		this.jointRule = rule;
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
		
		if (this.jointRule == null) {
			this.jointRule = 
					Query.combineRules(rules);
		}
		
		return this.jointRule;
	}
	
	public double getConfidence() {
		Query jointRule = getJointRule();
		if (Prediction.metric == Metric.PCAConfidence) {
			if (jointRule.getPcaConfidence() < 0.0)
				return jointRule.getPcaConfidence();
			else 
				return jointRule.getPcaConfidence();
		} else {
			return getJointRule().getStdConfidence();
		}
	}
	
	public int getIterationId() {
		return iterationId;
	}

	public void setIterationId(int iterationId) {
		this.iterationId = iterationId;
	}

	public List<Query> getRules() {
		return rules;
	}
	
	public String toNaiveEvaluationString() {
		StringBuilder builder = new StringBuilder();
		builder.append(triple[0]);
		builder.append("\t");		
		builder.append(triple[1]);
		builder.append("\t");		
		builder.append(triple[2]);
		builder.append("\t");	
		builder.append(getNaiveConfidence());
		builder.append("\t");
		
		if (hitInTarget) {
			builder.append("TargetSource\tTrue");
		} else {
			builder.append("ManualEvaluation\t");
		}
		
		return builder.toString();
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
		builder.append("\t" + getNaiveFullScore());
		builder.append("\t" + getFullScore());
		
		for (Query q : rules) {
			builder.append("\t" + q.getRuleString() + "[" + q.getSupport() + ", " + q.getPcaConfidence() + "]");
		}
		
		Query combinedRule = getJointRule();
		builder.append("\t" + combinedRule.getRuleString() + "[" + combinedRule.getSupport() + ", " + combinedRule.getPcaConfidence() + "]");
		
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
