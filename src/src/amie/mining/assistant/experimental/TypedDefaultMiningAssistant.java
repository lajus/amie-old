/**
 * @author lgalarra
 * @date Nov 25, 2012
 */
package amie.mining.assistant.experimental;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import amie.data.FactDatabase;
import amie.mining.assistant.DefaultMiningAssistant;
import amie.query.Query;

/**
 * This class overrides the default mining assistant and adds to the rule
 * all possible types constraints on the head variables, that is, it mines
 * rules of the form B ^ is(x, C) ^ is(y, C') => rh(x, y).
 * @author lgalarra
 *
 */
public class TypedDefaultMiningAssistant extends DefaultMiningAssistant {

	/**
	 * @param dataSource
	 */
	public TypedDefaultMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
	}
		
	/**
	 * Returns all candidates obtained by adding a new triple pattern to the query
	 * @param query
	 * @param minCardinality
	 * @return
	 */
	public void getDanglingAtoms(Query query, double minCardinality, Collection<Query> output){		
		if(query.getRealLength() == 1){
			//Add the types at the beginning of the query.
			getSpecializationCandidates(query, minCardinality, output);
		} else {
			super.getDanglingAtoms(query, minCardinality, output);	
		}
	}
		
	public void getSpecializationCandidates(Query query, double minSupportThreshold, Collection<Query> output) {
		List<Query> tmpCandidates = new ArrayList<Query>();
		ByteString[] head = query.getHead();
		
		//Specialization by type
		if(FactDatabase.isVariable(head[0])){
			ByteString[] newEdge = query.fullyUnboundTriplePattern();
			newEdge[0] = head[0];
			newEdge[1] = typeString;				
			query.getTriples().add(newEdge);
			IntHashMap<ByteString> subjectTypes = kb.countProjectionBindings(query.getHead(), query.getAntecedent(), newEdge[2]);
			if(!subjectTypes.isEmpty()){
				for(ByteString type: subjectTypes){
					int cardinality = subjectTypes.get(type);
					if(cardinality >= minSupportThreshold){
						Query newCandidate = new Query(query, cardinality);
						newCandidate.getLastTriplePattern()[2] = type;
						tmpCandidates.add(newCandidate);
					}
				}
			}
			
			query.getTriples().remove(query.getTriples().size() - 1);
			tmpCandidates.add(query);
		}
		
		if(FactDatabase.isVariable(head[2])){
			for(Query candidate: tmpCandidates){
				ByteString[] newEdge = query.fullyUnboundTriplePattern();
				newEdge[0] = head[2];
				newEdge[1] = typeString;
				candidate.getTriples().add(newEdge);
				IntHashMap<ByteString> objectTypes = kb.countProjectionBindings(candidate.getHead(), candidate.getAntecedent(), newEdge[2]);
				if(!objectTypes.isEmpty()){
					for(ByteString type: objectTypes){
						int cardinality = objectTypes.get(type);
						if(cardinality >= minSupportThreshold){
							Query newCandidate = new Query(candidate, cardinality);
							newCandidate.getLastTriplePattern()[2] = type;
							newCandidate.setParent(query);
							output.add(newCandidate);
						}
					}
				}else{
					if(candidate != query){
						output.add(candidate);
						candidate.setParent(query);
					}
				}
				candidate.getTriples().remove(candidate.getTriples().size() - 1);
			}
		}
	}

	@Override
	protected boolean testLength(Query candidate) {
		return candidate.getLengthWithoutTypes(typeString) < maxDepth;
	}
}