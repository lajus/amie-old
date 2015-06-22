package amie.mining.assistant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Pair;
import amie.data.FactDatabase;
import amie.query.Query;

public class SeedsCountMiningAssistant extends MiningAssistant {

	protected long subjectSchemaCount;
	
	private Set<ByteString> allSubjects;
	
	public SeedsCountMiningAssistant(FactDatabase dataSource, FactDatabase schemaSource) {
		super(dataSource);
		this.schemaSource = schemaSource;
		ByteString[] rootPattern = Query.fullyUnboundTriplePattern1();
		List<ByteString[]> triples = new ArrayList<ByteString[]>();
		triples.add(rootPattern);
		allSubjects = this.schemaSource.selectDistinct(rootPattern[0], triples);
		subjectSchemaCount = allSubjects.size();
	}
	
	public long getTotalCount(Query candidate){
		return subjectSchemaCount;
	}

	protected void getInstantiatedEdges(Query query, Query originalQuery, ByteString[] danglingEdge, int danglingPosition, int minCardinality, Collection<Query> output) {
		IntHashMap<ByteString> constants = source.frequentBindingsOf(danglingEdge[danglingPosition], query.getFunctionalVariable(), query.getTriples());
		for(ByteString constant: constants){
			ByteString tmp = danglingEdge[danglingPosition];
			danglingEdge[danglingPosition] = constant;
			int cardinality = seedsCardinality(query);
			danglingEdge[danglingPosition] = tmp;
			if(cardinality >= minCardinality){
				ByteString[] lastPatternCopy = query.getLastTriplePattern().clone();
				lastPatternCopy[danglingPosition] = constant;
				long cardLastEdge = source.count(lastPatternCopy);
				if(cardLastEdge < 2)
					continue;
				
				Query candidate = query.unify(danglingPosition, constant, cardinality);
				if(candidate.getRedundantAtoms().isEmpty()){
					candidate.setHeadCoverage((double)cardinality / headCardinalities.get(candidate.getHeadRelation()));
					candidate.setSupportRatio((double)cardinality / (double)getTotalCount(candidate));
					candidate.setParent(originalQuery);					
					output.add(candidate);
				}
			}
		}
	}
	
	/**
	 * Returns all candidates obtained by binding two values
	 * @param currentNode
	 * @param minCardinality
	 * @param omittedVariables
	 * @return
	 */
	public void getCloseCircleEdges(Query query, int minCardinality, Collection<Query> output){		
		int nPatterns = query.getTriples().size();

		if(query.isEmpty())
			return;
		
		if(!testLength(query))
			return;
		
		List<ByteString> sourceVariables = null;
		List<ByteString> allVariables = query.getVariables();
		List<ByteString> targetVariables = null;		
		List<ByteString> openVariables = query.getOpenVariables();
		
		if(query.isClosed()){
			sourceVariables = allVariables;
			targetVariables = allVariables;
		}else{
			sourceVariables = openVariables; 
			if(sourceVariables.size() > 1){
				if (this.exploitMaxLengthOption) {
					// Pruning by maximum length for the \mathcal{O}_C operator.
					if (sourceVariables.size() > 2 
							&& query.getRealLength() == this.maxDepth - 1) {
						return;
					}
				}
				//Give preference to the non-closed variables
				targetVariables = sourceVariables;
			}else{
				targetVariables = allVariables;
			}
		}
		
		Pair<Integer, Integer>[] varSetups = new Pair[2];
		varSetups[0] = new Pair<Integer, Integer>(0, 2);
		varSetups[1] = new Pair<Integer, Integer>(2, 0);
		ByteString[] newEdge = query.fullyUnboundTriplePattern();
		ByteString relationVariable = newEdge[1];
		
		for(Pair<Integer, Integer> varSetup: varSetups){			
			int joinPosition = varSetup.first.intValue();
			int closeCirclePosition = varSetup.second.intValue();
			ByteString joinVariable = newEdge[joinPosition];
			ByteString closeCircleVariable = newEdge[closeCirclePosition];
						
			for(ByteString sourceVariable: sourceVariables){					
				newEdge[joinPosition] = sourceVariable;
				
				for(ByteString variable: targetVariables){
					if(!variable.equals(sourceVariable)){
						newEdge[closeCirclePosition] = variable;
						
						query.getTriples().add(newEdge);
						IntHashMap<ByteString> promisingRelations = source.frequentBindingsOf(newEdge[1], query.getFunctionalVariable(), query.getTriples());
						query.getTriples().remove(nPatterns);
						
						for(ByteString relation: promisingRelations){
							if(bodyExcludedRelations != null && bodyExcludedRelations.contains(relation))
								continue;
							
							//Here we still have to make a redundancy check
							newEdge[1] = relation;
							query.getTriples().add(newEdge);
							int cardinality = seedsCardinality(query);
							query.getTriples().remove(nPatterns);
							if(cardinality >= minCardinality){										
								Query candidate = query.closeCircle(newEdge, cardinality);
								if(!candidate.isRedundantRecursive()){
									candidate.setHeadCoverage((double)cardinality / (double)headCardinalities.get(candidate.getHeadRelation()));
									candidate.setSupportRatio((double)cardinality / (double)getTotalCount(candidate));
									candidate.setParent(query);
									output.add(candidate);
								}
							}
						}
					}
					newEdge[1] = relationVariable;
				}
				newEdge[closeCirclePosition] = closeCircleVariable;
				newEdge[joinPosition] = joinVariable;
			}
		}
	}
	
	private int seedsCardinality(Query query) {
		// TODO Auto-generated method stub
		Set<ByteString> subjects = new HashSet<ByteString>(source.selectDistinct(query.getFunctionalVariable(), query.getTriples()));
		subjects.retainAll(allSubjects);
		return subjects.size();
	}

	/**
	 * Returns all candidates obtained by adding a new triple pattern to the query
	 * @param query
	 * @param minCardinality
	 * @return
	 */
	public void getDanglingEdges(Query query, int minCardinality, Collection<Query> output){		
		ByteString[] newEdge = query.fullyUnboundTriplePattern();
		List<ByteString> openVariables = query.getOpenVariables();
		
		if(query.isEmpty()){
			//Initial case
			query.getTriples().add(newEdge);
			IntHashMap<ByteString> relations = 
					source.frequentBindingsOf(newEdge[1], newEdge[0], query.getTriples());
			for(ByteString relation: relations){
				if(headExcludedRelations != null && headExcludedRelations.contains(newEdge[1]))
					continue;

				//int cardinality = relations.get(relation);
				newEdge[1] = relation;
				int countVarPos = countAlwaysOnSubject? 0 : findCountingVariable(newEdge);
				query.setFunctionalVariable(newEdge[countVarPos]);
				int cardinality = seedsCardinality(query);
				query.setSupport(cardinality);
				if(cardinality >= minCardinality){
					ByteString[] succedent = newEdge.clone();					
					Query candidate = new Query(succedent, cardinality);
					candidate.setFunctionalVariable(succedent[countVarPos]);
					registerHeadRelation(candidate);
					if(allowConstants)
						getInstantiatedEdges(candidate, null, candidate.getLastTriplePattern(), countVarPos == 0 ? 2 : 0, minCardinality, output);
					
					output.add(candidate);
				}
			}			
			query.getTriples().remove(0);
		}else{
			//General case
			if(!testLength(query))
				return;
			
			//General case
			if(query.getLength() == maxDepth - 1) {
				if (this.exploitMaxLengthOption) {
					if(!openVariables.isEmpty() 
							&& !this.allowConstants 
							&& !this.enforceConstants) {
						return;
					}
				}
			}
			
			List<ByteString> joinVariables = null;
			
			//Then do it for all values
			if(query.isClosed()){
				joinVariables = query.getVariables();
			}else{
				joinVariables = query.getOpenVariables();
			}

			int nPatterns = query.getTriples().size();
			ByteString originalRelationVariable = newEdge[1];		
			
			for(int joinPosition = 0; joinPosition <= 2; joinPosition += 2){
				ByteString originalFreshVariable = newEdge[joinPosition];
				
				for(ByteString joinVariable: joinVariables){					
					newEdge[joinPosition] = joinVariable;
					query.getTriples().add(newEdge);
					IntHashMap<ByteString> promisingRelations = source.frequentBindingsOf(newEdge[1], query.getFunctionalVariable(), query.getTriples());
					query.getTriples().remove(nPatterns);
					
					int danglingPosition = (joinPosition == 0 ? 2 : 0);
					boolean boundHead = !FactDatabase.isVariable(query.getTriples().get(0)[danglingPosition]);
					for(ByteString relation: promisingRelations){
						if(bodyExcludedRelations != null && bodyExcludedRelations.contains(relation))
							continue;
						//Here we still have to make a redundancy check		
						newEdge[1] = relation;
						query.getTriples().add(newEdge);
						int cardinality = seedsCardinality(query);
						query.getTriples().remove(nPatterns);						
						if(cardinality >= minCardinality){
							Query candidate = query.addEdge(newEdge, cardinality, newEdge[joinPosition], newEdge[danglingPosition]);
							if(candidate.containsUnifiablePatterns()){
								//Verify whether dangling variable unifies to a single value (I do not like this hack)
								if(boundHead && source.countDistinct(newEdge[danglingPosition], candidate.getTriples()) < 2)
									continue;
							}
							
							candidate.setHeadCoverage((double)candidate.getSupport() / headCardinalities.get(candidate.getHeadRelation()));
							candidate.setSupportRatio((double)candidate.getSupport() / (double)getTotalCount(candidate));
							candidate.setParent(query);
							if (canAddInstantiatedAtom()) {
								// Pruning by maximum length for the \mathcal{O}_E operator.
								if (this.exploitMaxLengthOption) {
									if (query.getRealLength() < this.maxDepth - 1 
											|| openVariables.size() < 2) {
										getInstantiatedEdges(candidate, candidate, candidate.getLastTriplePattern(), danglingPosition, minCardinality, output);
									}
								} else {
									getInstantiatedEdges(candidate, candidate, candidate.getLastTriplePattern(), danglingPosition, minCardinality, output);							
								}
							}
							
							output.add(candidate);
						}
					}
					
					newEdge[1] = originalRelationVariable;
				}
				newEdge[joinPosition] = originalFreshVariable;
			}
		}
	}
	
	public void calculateConfidenceMetrics(Query candidate) {		
		// TODO Auto-generated method stub
		List<ByteString[]> antecedent = new ArrayList<ByteString[]>();
		antecedent.addAll(candidate.getTriples().subList(1, candidate.getTriples().size()));
		List<ByteString[]> succedent = new ArrayList<ByteString[]>();
		succedent.addAll(candidate.getTriples().subList(0, 1));
		double improvedDenominator = 0.0;
		double denominator = 0.0;
		double stdConfidence = 0.0;
		double pcaConfidence = 0.0;
		ByteString[] existentialTriple = succedent.get(0).clone();
		int freeVarPos = 0;
		
		if(FactDatabase.numVariables(existentialTriple) == 1){
			freeVarPos = FactDatabase.firstVariablePos(existentialTriple);
		}else{
			if(existentialTriple[0].equals(candidate.getFunctionalVariable()))
				freeVarPos = 2;
			else
				freeVarPos = 0;
		}

		existentialTriple[freeVarPos] = ByteString.of("?x");
				
		if(antecedent.isEmpty()){
			candidate.setStdConfidence(1.0);
			candidate.setPcaConfidence(1.0);
		}else{
			//Confidence
			try{
				denominator = (double)source.countDistinct(candidate.getFunctionalVariable(), antecedent);
				stdConfidence = (double)candidate.getSupport() / denominator;
				candidate.setStdConfidence(stdConfidence);
				candidate.setBodySize((int)denominator);
			}catch(UnsupportedOperationException e){
				
			}
			
			//Improved confidence: Add an existential version of the head
			antecedent.add(existentialTriple);
			try{
				improvedDenominator = (double)source.countDistinct(candidate.getFunctionalVariable(), antecedent);
				pcaConfidence = (double)candidate.getSupport() / improvedDenominator;
				candidate.setPcaConfidence(pcaConfidence);
			}catch(UnsupportedOperationException e){
				
			}
			antecedent.remove(antecedent.size() - 1);
		}
	}
}