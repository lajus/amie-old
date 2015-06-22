/**
 * @author lgalarra
 * @date Nov 25, 2012
 */
package amie.mining.assistant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import amie.data.FactDatabase;
import amie.query.Query;

/**
 * @author lgalarra
 *
 */
public class TypedMiningAssistant extends HeadVariablesMiningAssistant {

	/**
	 * @param dataSource
	 */
	public TypedMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
	}
		
	/**
	 * Returns all candidates obtained by adding a new triple pattern to the query
	 * @param query
	 * @param minCardinality
	 * @return
	 */
	public void getDanglingEdges(Query query, int minCardinality, Collection<Query> output){		
		if(query.getRealLength() == 1){
			//Add the types at the beginning of the query.
			getSpecializationCandidates(query, minCardinality, output);
		} else {
			super.getDanglingEdges(query, minCardinality, output);	
		}
	}
		
	public void getSpecializationCandidates(Query query, int minCardinality, Collection<Query> output) {
		List<Query> tmpCandidates = new ArrayList<Query>();
		ByteString[] head = query.getHead();
		
		//Specialization by type
		if(FactDatabase.isVariable(head[0])){
			ByteString[] newEdge = query.fullyUnboundTriplePattern();
			newEdge[0] = head[0];
			newEdge[1] = typeString;				
			query.getTriples().add(newEdge);
			IntHashMap<ByteString> subjectTypes = source.countProjectionBindings(query.getHead(), query.getAntecedent(), newEdge[2]);
			if(!subjectTypes.isEmpty()){
				for(ByteString type: subjectTypes){
					int cardinality = subjectTypes.get(type);
					if(cardinality >= minCardinality){
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
				IntHashMap<ByteString> objectTypes = source.countProjectionBindings(candidate.getHead(), candidate.getAntecedent(), newEdge[2]);
				if(!objectTypes.isEmpty()){
					for(ByteString type: objectTypes){
						int cardinality = objectTypes.get(type);
						if(cardinality >= minCardinality){
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

	protected boolean testLength(Query candidate) {
		return candidate.getLengthWithoutTypes(typeString) < maxDepth;
	}
}