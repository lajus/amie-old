package amie.prediction.assistant;

import javatools.datatypes.ByteString;
import amie.data.FactDatabase;
import amie.mining.assistant.DefaultMiningAssistant;
import amie.prediction.data.HistogramTupleIndependentProbabilisticFactDatabase;
import amie.query.Query;

public class ProbabilisticHeadVariablesMiningAssistant extends DefaultMiningAssistant {

	public ProbabilisticHeadVariablesMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
	}
	
	/**
	 * It calculates the probabilistic versions of support and PCA confidence for the 
	 * given rule and stores them in the corresponding fields in the rule object
	 * (see getProbabilisticSupport() and getProbabilisticPCAConfidence())
	 * @param rule
	 */
	public void computeProbabilisticMetrics(Query rule) {
		HistogramTupleIndependentProbabilisticFactDatabase kb = 
				(HistogramTupleIndependentProbabilisticFactDatabase) this.source;
		ByteString[] head = rule.getHead();
		ByteString valueToReplace = null;
		if (!FactDatabase.isVariable(head[2]) 
				|| this.source.isFunctional(head[1])) {
			valueToReplace = head[0];
		} else {
			valueToReplace = head[2];
		}
		
		double[] supports = kb.probabilitiesOf(rule.getBody(), head, valueToReplace);
		rule.setSupport(supports[0]);
		rule.setPcaBodySize(supports[1]);
	}
}
