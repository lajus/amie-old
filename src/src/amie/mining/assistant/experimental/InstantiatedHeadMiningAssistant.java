package amie.mining.assistant.experimental;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import amie.data.FactDatabase;
import amie.mining.assistant.DefaultMiningAssistant;
import amie.query.Query;

public class InstantiatedHeadMiningAssistant extends DefaultMiningAssistant {

	public InstantiatedHeadMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
	}
	
	@Override
	public void getInitialAtomsFromSeeds(Collection<ByteString> relations, double minCardinality, Collection<Query> output) {
		Query query = new Query();
		//The query must be empty
		if(!query.isEmpty()) {
			throw new IllegalArgumentException("Expected an empty query");
		}
		
		ByteString[] newEdge = query.fullyUnboundTriplePattern();		
		query.getTriples().add(newEdge);
		
		for(ByteString relation: relations) {
			newEdge[1] = relation;
			
			int countVarPos = countAlwaysOnSubject ? 0 : findCountingVariable(newEdge);
			List<ByteString[]> emptyList = Collections.emptyList();
			long cardinality = kb.countProjection(query.getHead(), emptyList);
			
			ByteString[] succedent = newEdge.clone();
			Query candidate = new Query(succedent, cardinality);
			candidate.setFunctionalVariablePosition(countVarPos);
			registerHeadRelation(candidate);
			ArrayList<Query> tmpOutput = new ArrayList<>();
			getInstantiatedAtoms(candidate, null, query.getLastTriplePattern(), countVarPos == 0 ? 2 : 0, minCardinality, tmpOutput);			
			output.addAll(tmpOutput);
		}
		
		query.getTriples().remove(0);		
	}
	
	@Override
	public void getInitialAtoms(double minSupportThreshold, Collection<Query> output) {
		Query query = new Query();
		ByteString[] newEdge = query.fullyUnboundTriplePattern();
		
		if(query.isEmpty()) {
			//Initial case
			query.getTriples().add(newEdge);
			List<ByteString[]> emptyList = Collections.emptyList();
			IntHashMap<ByteString> relations = kb.countProjectionBindings(query.getHead(), emptyList, newEdge[1]);
			for(ByteString relation : relations){
				if(headExcludedRelations != null && headExcludedRelations.contains(relation))
					continue;
				
				int cardinality = relations.get(relation);
				if (cardinality >= minSupportThreshold) {
					ByteString[] succedent = newEdge.clone();
					succedent[1] = relation;
					int countVarPos = countAlwaysOnSubject ? 0 : findCountingVariable(succedent);
					Query candidate = new Query(succedent, cardinality);
					candidate.setFunctionalVariablePosition(countVarPos);
					registerHeadRelation(candidate);
					getInstantiatedAtoms(candidate, null, query.getLastTriplePattern(), countVarPos == 0 ? 2 : 0, minSupportThreshold, output);
					output.add(candidate);
				}
			}			
			query.getTriples().remove(0);
		}
		
	}
	
	@Override
	public void getDanglingAtoms(Query query, double minCardinality, Collection<Query> output) {
		ByteString[] newEdge = query.fullyUnboundTriplePattern();
		
		if(query.isEmpty()) {
			//Initial case
			query.getTriples().add(newEdge);
			List<ByteString[]> emptyList = Collections.emptyList();
			IntHashMap<ByteString> relations = kb.countProjectionBindings(query.getHead(), emptyList, newEdge[1]);
			for(ByteString relation : relations){
				if(headExcludedRelations != null && headExcludedRelations.contains(relation))
					continue;
				
				int cardinality = relations.get(relation);
				if (cardinality >= minCardinality) {
					ByteString[] succedent = newEdge.clone();
					succedent[1] = relation;
					int countVarPos = countAlwaysOnSubject ? 0 : findCountingVariable(succedent);
					Query candidate = new Query(succedent, cardinality);
					candidate.setFunctionalVariablePosition(countVarPos);
					registerHeadRelation(candidate);
					getInstantiatedAtoms(candidate, null, query.getLastTriplePattern(), countVarPos == 0 ? 2 : 0, minCardinality, output);
					output.add(candidate);
				}
			}			
			query.getTriples().remove(0);
		} else {
			if (!testLength(query)) {
				return;
			}
			
			// Enforce this only for n > 2
			if (maxDepth > 2) {
				if(query.getRealLength() == maxDepth - 1) {
					if(!query.getOpenVariables().isEmpty() && (!allowConstants && !enforceConstants)) {
						return;
					}
				}
			}
			
			getDanglingAtoms(query, newEdge, minCardinality, output);
		}
	}
	
	/**
	 * Simplified version that calculates only standard confidence.
	 */
	public void calculateConfidenceMetrics(Query candidate) {	
		if (candidate.getLength() == 2) {
			List<ByteString[]> antecedent = new ArrayList<ByteString[]>();
			antecedent.addAll(candidate.getTriples().subList(1, candidate.getTriples().size()));
			List<ByteString[]> succedent = new ArrayList<ByteString[]>();
			succedent.addAll(candidate.getTriples().subList(0, 1));
			double denominator = 0.0;
			denominator = (double) kb.countDistinct(candidate.getFunctionalVariable(), antecedent);
			candidate.setBodySize((int)denominator);
		} else {
			super.calculateConfidenceMetrics(candidate);
		}
	}
	
	public boolean testConfidenceThresholds(Query candidate) {
		if (candidate.getLength() == 2) {
			calculateConfidenceMetrics(candidate);
			return true;
		} else {
			return super.testConfidenceThresholds(candidate);
		}
	}
}
