package amie.mining.assistant.experimental;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import amie.data.FactDatabase;
import amie.mining.assistant.DefaultMiningAssistant;
import amie.query.Query;

/**
 * Implements a subclass of the mining assistant that is optimized to mine keys. It extends
 * the standard head variables mining assistant to mine rules of the form
 * r(x, z1) r(y, z1) ....  r'(x, zk) r'(y, zk) => equals(x, y)
 * @author galarrag
 *
 */
public class KeyMinerMiningAssistant extends DefaultMiningAssistant {

	public KeyMinerMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
		recursivityLimit = 2; // The maximum number of atoms of a relation in the query
	}
	
	/**
	 * We enforce always an equals relation on the head, no matter if the user provides
	 * seeds relations.
	 */
	public void getInitialAtomsFromSeeds(Collection<ByteString> seedsRelations, double minSupportThreshold, Collection<Query> output) {
		Collection<ByteString> equalsRelation = Arrays.asList(FactDatabase.EQUALSbs);
		super.getInitialAtomsFromSeeds(equalsRelation, minSupportThreshold, output);
	}
	
	@Override
	public void getDanglingEdges(Query query, double minCardinality, Collection<Query> output) {		
		if (query.isEmpty()) {
			getInitialAtomsFromSeeds(Collections.EMPTY_LIST, minCardinality, output);
			return;
		}
		// Do nothing as getCloseCircleEdges takes care of building rules for keys.
	}
	
	/**
	 * Returns all candidates obtained by adding a composite closing edge of the form
	 * r(x, z), r(y, z) where x, y are the head variables of the rule sent as argument.
	 * @param currentNode
	 * @param minSupportThreshold
	 * @param omittedVariables
	 * @return
	 */
	@Override
	public void getCloseCircleEdges(Query query, double minSupportThreshold, Collection<Query> output) {
		if (this.enforceConstants) {
			return;
		}
		
		int nPatterns = query.getTriples().size();

		if(query.isEmpty())
			return;
		
		if(!testLength(query))
			return;
		// We first add a dangling atom of the form r(x, z) where x is one of the head variables		
		List<ByteString> joinVariables = query.getHeadVariables();
		if (joinVariables.size() < 2) {
			return;
		}
		
		ByteString[] newEdge = query.fullyUnboundTriplePattern();			
		int joinPosition = 0;
		ByteString[] newEdge1 = newEdge.clone();
		ByteString[] newEdge2 = newEdge.clone();
		
		newEdge1[joinPosition] = joinVariables.get(0);
		newEdge2[joinPosition] = joinVariables.get(1);
		query.getTriples().add(newEdge1);
		IntHashMap<ByteString> promisingRelations = this.kb.countProjectionBindings(query.getHead(), query.getAntecedent(), newEdge1[1]);
		query.getTriples().remove(nPatterns);
		
		for(ByteString relation: promisingRelations){
			int cardinality = promisingRelations.get(relation);
			
			if (cardinality < minSupportThreshold) {
				continue;
			}			
			
			// Language bias test
			if (query.cardinalityForRelation(relation) >= recursivityLimit) {
				continue;
			}
			
			if (bodyExcludedRelations != null 
					&& bodyExcludedRelations.contains(relation)) {
				continue;
			}
			
			if (bodyTargetRelations != null 
					&& !bodyTargetRelations.contains(relation)) {
				continue;
			}
			
			newEdge1[1] = relation;
			newEdge2[1] = relation;
			
			Query candidate = query.addEdges(newEdge1, newEdge2);
			computeCardinality(candidate);
			if (candidate.getSupport() < minSupportThreshold) {
				continue;
			}
			candidate.setParent(query);	
			output.add(candidate);
		}
	}
}
