package amie.prediction.assistant;

import java.util.Collection;

import amie.data.FactDatabase;
import amie.mining.assistant.experimental.TypedMiningAssistant;
import amie.query.Query;

public class TypedProbabilisticMiningAssistant extends ProbabilisticHeadVariablesMiningAssistant{

	TypedMiningAssistant realAssistant;
	
	public TypedProbabilisticMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
		realAssistant = new TypedMiningAssistant(dataSource);
	}
	
	/**
	 * 
	 */
	public void getDanglingAtoms(Query query, double minCardinality, Collection<Query> output){	
		super.getDanglingAtoms(query, minCardinality, output);
	}
	
	/**
	 * 
	 * @param query
	 * @param minCardinality
	 * @param output
	 */
	public void getSpecializationCandidates(Query query, int minCardinality, Collection<Query> output) {
		realAssistant.getSpecializationCandidates(query, minCardinality, output);
	}
	
	/**
	 * 
	 */
	protected boolean testLength(Query candidate) {
		return testLength(candidate);
	}

}
