package amie.mining.assistant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import amie.data.FactDatabase;
import amie.query.Query;

public class WikilinksHeadVariablesMiningAssistant extends HeadVariablesMiningAssistant {
	
	public static String wikiLinkProperty = "<linksTo>";
	
	public WikilinksHeadVariablesMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
	}
	
	public void getDanglingEdges(Query query, int minCardinality, Collection<Query> output) {		
		if(query.isEmpty()){
			//Initial case
			ByteString[] newEdge = query.fullyUnboundTriplePattern();
			query.getTriples().add(newEdge);
			List<ByteString[]> emptyList = Collections.emptyList();
			IntHashMap<ByteString> relations = source.countProjectionBindings(query.getHead(), emptyList, newEdge[1]);
			for(ByteString relation: relations){
				// Language bias test
				if (query.cardinalityForRelation(relation) >= recursivityLimit) {
					continue;
				}
				
				if(headExcludedRelations != null && headExcludedRelations.contains(relation)) {
					continue;
				}
				
				int cardinality = relations.get(relation);
				if(cardinality >= minCardinality){
					ByteString[] succedent = newEdge.clone();
					succedent[1] = relation;
					int countVarPos = countAlwaysOnSubject? 0 : findCountingVariable(succedent);
					Query candidate = new Query(succedent, cardinality);
					candidate.setFunctionalVariable(succedent[countVarPos]);
					registerHeadRelation(candidate);
					output.add(candidate);
				}
			}			
			query.getTriples().remove(0);
		}else{
			super.getDanglingEdges(query, minCardinality, output);
			if(query.isSafe() && !query.containsRelation(ByteString.of(wikiLinkProperty)) && !query.containsRelation(typeString)){
				//Add the types when the query is long enough
				getSpecializationCandidates(query, minCardinality, output);
			}
		}
	}
	
	public void getSpecializationCandidates(Query query, int minCardinality, Collection<Query> output){
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
				for(ByteString type: objectTypes){
					int cardinality = objectTypes.get(type);
					if(cardinality >= minCardinality){
						Query newCandidate = new Query(candidate, cardinality);
						newCandidate.setHeadCoverage((double)cardinality / (double)headCardinalities.get(newCandidate.getHeadRelation()));
						newCandidate.setSupport((double)cardinality / (double)source.size());
						newCandidate.setParent(query);
						newCandidate.getLastTriplePattern()[2] = type;
						newCandidate.setParent(query);
						output.add(newCandidate);
					}
				}
				
				if (candidate != query) {
					output.add(candidate);
					candidate.setParent(query);
				}
				candidate.getTriples().remove(candidate.getTriples().size() - 1);
			}
		}
	}
	
	public void getCloseCircleEdges(Query query, int minCardinality, Collection<Query> output) {
		int length = query.getLengthWithoutTypesAndLinksTo(typeString, ByteString.of(wikiLinkProperty));
		ByteString[] head = query.getHead();
		if (length == maxDepth - 1) {
			List<ByteString> openVariables = query.getOpenVariables();
			for (ByteString openVar : openVariables) {
				if (FactDatabase.isVariable(head[0]) && !openVar.equals(head[0])) {
					return;
				}
				
				if (FactDatabase.isVariable(head[2]) && !openVar.equals(head[2])) {
					return;
				}
			}
		}
		
		if (!query.containsRelation(ByteString.of(wikiLinkProperty))) {
			ByteString[] newEdge = head.clone();
			newEdge[1] = ByteString.of(wikiLinkProperty);
			List<ByteString[]> queryAtoms = new ArrayList<>();
			queryAtoms.addAll(query.getTriples());
			queryAtoms.add(newEdge);
			long cardinality = source.countPairs(head[0], head[2], queryAtoms);
			if (cardinality >= minCardinality) {
				Query candidate1 = query.closeCircle(newEdge, (int)cardinality);
				candidate1.setHeadCoverage((double)cardinality / (double)headCardinalities.get(candidate1.getHeadRelation()));
				candidate1.setSupport((double)cardinality / (double)source.size());
				candidate1.setParent(query);			
				output.add(candidate1);	
			}
			
			ByteString tmp = newEdge[0];
			newEdge[0] = newEdge[2];
			newEdge[2] = tmp;
			cardinality = source.countPairs(head[0], head[2], queryAtoms);
			if (cardinality >= minCardinality) {
				Query candidate2 = query.closeCircle(newEdge, (int)cardinality);
				candidate2.setHeadCoverage((double)cardinality / (double)headCardinalities.get(candidate2.getHeadRelation()));
				candidate2.setSupport((double)cardinality / (double)source.size());
				candidate2.setParent(query);			
				output.add(candidate2);	
			}
		}
			
		super.getCloseCircleEdges(query, minCardinality, output);
	}

	protected boolean testLength(Query candidate){
		boolean passed = candidate.getLengthWithoutTypesAndLinksTo(typeString, ByteString.of(wikiLinkProperty)) < maxDepth;
		return passed;
	}
}