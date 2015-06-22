package amie.prediction.assistant;

import java.util.ArrayList;
import java.util.List;

import javatools.datatypes.ByteString;
import amie.data.FactDatabase;
import amie.mining.assistant.HeadVariablesMiningAssistant;
import amie.prediction.data.HistogramTupleIndependentProbabilisticFactDatabase;
import amie.query.Query;

public class ProbabilisticHeadVariablesMiningAssistant extends HeadVariablesMiningAssistant {

	public ProbabilisticHeadVariablesMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
	}

	/**
	 * It computes the probabilistic version of support of the given rule.
	 * @param rule
	 * @return
	 */
	public long computeProbabilisticCardinality(Query rule) {
		HistogramTupleIndependentProbabilisticFactDatabase db = 
				(HistogramTupleIndependentProbabilisticFactDatabase)source;
		double probabilisticSupport = db.probabilityOf(rule.getBody(), rule.getHead());
		rule.setProbabilisticSupport(probabilisticSupport);
		return (long)probabilisticSupport;
	}
	
	/**
	 * It computes the probabilistic version of the PCA for the given rule.
	 */
	public double computeProbabilisticPCAConfidence(Query rule) {
		HistogramTupleIndependentProbabilisticFactDatabase db = 
				(HistogramTupleIndependentProbabilisticFactDatabase)source;
		ByteString[] head = rule.getHead();
		ByteString[] existentialHead = head.clone();
		if (source.isFunctional(head[1])) {
			existentialHead[0] = ByteString.of("?x");
		} else {
			existentialHead[2] = ByteString.of("?x");
		}
		List<ByteString[]> bodyPCA = new ArrayList<ByteString[]>();
		bodyPCA.add(existentialHead);
		for (ByteString[] atom : rule.getBody()) {
			bodyPCA.add(atom);
		}
		double probabilisticSupport = db.probabilityOf(rule.getBody(), head);
		double probabilisticPCABodySize = db.probabilityOf(bodyPCA, head);
		rule.setProbabilisticSupport(probabilisticSupport);
		rule.setProbabilisticPCABodySize(probabilisticPCABodySize);
		return rule.getProbabilisticPCAConfidence();
	}
}
