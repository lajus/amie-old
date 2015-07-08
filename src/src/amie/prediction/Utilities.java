package amie.prediction;

import javatools.datatypes.ByteString;
import amie.data.FactDatabase;
import amie.prediction.data.TupleIndependentFactDatabase;
import amie.query.Query;

public class Utilities {

	/**
	 * Given a rule and tuple independent fact database, it computes the probabilistic support and 
	 * PCA body size of the rule according to the fact database. The method updates the fields in the 
	 * rules and also returns the values.
	 * @param rule
	 * @param kb
	 * @return An array with two real numbers: probabilistic support and probabilistic PCA denominator (PCA body size)
	 */
	public static double[] computeProbabilisticMetrics(Query rule, TupleIndependentFactDatabase kb) {
		ByteString[] head = rule.getHead();
		ByteString valueToReplace = null;
		if (!FactDatabase.isVariable(head[2]) || kb.isFunctional(head[1])) {
			valueToReplace = head[0];
		} else {
			valueToReplace = head[2];
		}
		
		double[] supports = kb.probabilitiesOf(rule.getBody(), head, valueToReplace);
		double size = kb.probabilisticSize(head[1]);
		rule.setSupport(supports[0]);
		rule.setHeadCoverage(supports[0] / size);
		rule.setPcaBodySize(supports[1]);
		return supports;
	}
}
