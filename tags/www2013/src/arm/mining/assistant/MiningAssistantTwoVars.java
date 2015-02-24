package arm.mining.assistant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Pair;
import arm.data.FactDatabase;
import arm.query.Query;

public class MiningAssistantTwoVars extends MiningAssistant{
	/**
	 * Store counts for hard queries
	 */
	protected Map<Pair<ByteString, Boolean>, Long> hardQueries;
	
	
	public MiningAssistantTwoVars(FactDatabase dataSource) {
		super(dataSource);
		hardQueries = Collections.synchronizedMap(new HashMap<Pair<ByteString, Boolean>, Long>());
		// TODO Auto-generated constructor stub
	}
	
	public int getTotalCount(Query query){
		return source.size();
	}
	
	public void getDanglingEdges(Query query, Collection<ByteString> relations, int minCardinality, Collection<Query> output) {
		//The query must be empty
		if(!query.isEmpty()){
			throw new IllegalArgumentException("Expected an empty query");
		}
		
		ByteString[] newEdge = query.fullyUnboundTriplePattern();		
		query.getTriples().add(newEdge);
		
		for(ByteString relation: relations){
			newEdge[1] = relation;
			
			int countVarPos = countAlwaysOnSubject? 0 : findCountingVariable(newEdge);
			ByteString countingVariable = newEdge[countVarPos];
			List<ByteString[]> emptyList = Collections.emptyList();
			int cardinality = source.countProjection(query.getHead(), emptyList);
			
			ByteString[] succedent = newEdge.clone();
			Query candidate = new Query(succedent, cardinality);
			candidate.setProjectionVariable(countingVariable);
			ArrayList<Query> tmpOutput = new ArrayList<>();
			if(allowConstants){
				getInstantiatedEdges(candidate, null, candidate.getLastTriplePattern(), countVarPos == 0 ? 2 : 0, minCardinality, tmpOutput);			
				output.addAll(tmpOutput);
			}
			output.add(candidate);
		}
		
		query.getTriples().remove(0);
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
		
		if(query.isSafe()){
			sourceVariables = query.getVariables();
		}else{
			sourceVariables = query.getMustBindVariables(); 
		}
		
		if(sourceVariables.size() < 2)
			return;
		
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
				
				for(ByteString variable: allVariables){
					if(!variable.equals(sourceVariable)){
						newEdge[closeCirclePosition] = variable;
						
						query.getTriples().add(newEdge);
						IntHashMap<ByteString> promisingRelations = null;
						if(this.enableOptimizations){
							Query rewrittenQuery = rewriteProjectionQuery(query, nPatterns, closeCirclePosition);
							if(rewrittenQuery == null){
								long t1 = System.currentTimeMillis();
								promisingRelations = source.countProjectionBindings(query.getHead(), query.getAntecedent(), newEdge[1]);
								long t2 = System.currentTimeMillis();
								if((t2 - t1) > 20000)
									System.out.println("countProjectionBindings var=" + newEdge[1] + " "  + query + " has taken " + (t2 - t1) + " ms");
							}else{
								long t1 = System.currentTimeMillis();
								promisingRelations = source.countProjectionBindings(rewrittenQuery.getHead(), rewrittenQuery.getAntecedent(), newEdge[1]);
								long t2 = System.currentTimeMillis();
								if((t2 - t1) > 20000)
									System.out.println("countProjectionBindings on rewritten query var=" + newEdge[1] + " "  + rewrittenQuery + " has taken " + (t2 - t1) + " ms");						
							}
						}else{
							promisingRelations = source.countProjectionBindings(query.getHead(), query.getAntecedent(), newEdge[1]);
						}
						query.getTriples().remove(nPatterns);
						
						for(ByteString relation: promisingRelations){
							if(excludedRelations != null && excludedRelations.contains(relation))
								continue;
							
							//Here we still have to make a redundancy check
							int cardinality = promisingRelations.get(relation);
							newEdge[1] = relation;
							if(cardinality >= minCardinality){
								Query candidate = query.closeCircle(newEdge, cardinality);
								if(!candidate.isRedundantRecursive()){
									candidate.setHeadCoverage((double)cardinality / (double)headCardinalities.get(candidate.getHeadKey()));
									candidate.setSupport((double)cardinality / (double)source.size());
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
	
	/**
	 * Returns all candidates obtained by adding a new triple pattern to the query
	 * @param query
	 * @param minCardinality
	 * @return
	 */
	public void getDanglingEdges(Query query, int minCardinality, Collection<Query> output){		
		ByteString[] newEdge = query.fullyUnboundTriplePattern();
		
		if(query.isEmpty()){
			//Initial case
			query.getTriples().add(newEdge);
			List<ByteString[]> emptyList = Collections.emptyList();
			IntHashMap<ByteString> relations = source.countProjectionBindings(query.getHead(), emptyList, newEdge[1]);
			for(ByteString relation: relations){
				if(excludedRelations != null && excludedRelations.contains(relation))
					continue;
				
				int cardinality = relations.get(relation);
				if(cardinality >= minCardinality){
					ByteString[] succedent = newEdge.clone();
					succedent[1] = relation;
					int countVarPos = countAlwaysOnSubject? 0 : findCountingVariable(succedent);
					Query candidate = new Query(succedent, cardinality);
					candidate.setProjectionVariable(succedent[countVarPos]);
					List<Query> tmpOutput = new ArrayList<Query>();
					if(allowConstants){
						getInstantiatedEdges(candidate, null, candidate.getLastTriplePattern(), countVarPos == 0 ? 2 : 0, minCardinality, tmpOutput);
						output.addAll(tmpOutput);
					}
					output.add(candidate);
				}
			}			
			query.getTriples().remove(0);
		}else{
			//General case
			if(!testLength(query))
				return;
			
			List<ByteString> joinVariables = null;
						
			//Then do it for all values
			if(query.isSafe()){
				joinVariables = query.getVariables();
			}else{
				joinVariables = query.getMustBindVariables();
			}

			int nPatterns = query.getLength();
			ByteString originalRelationVariable = newEdge[1];		
			
			for(int joinPosition = 0; joinPosition <= 2; joinPosition += 2){
				ByteString originalFreshVariable = newEdge[joinPosition];
				
				for(ByteString joinVariable: joinVariables){					
					newEdge[joinPosition] = joinVariable;
					query.getTriples().add(newEdge);
					IntHashMap<ByteString> promisingRelations = null;

					//Rewrite query
					if(enableOptimizations){
						Query rewrittenQuery = rewriteProjectionQuery(query, nPatterns, joinPosition == 0 ? 0 : 2);
						if(rewrittenQuery == null){
							long t1 = System.currentTimeMillis();
							promisingRelations = source.countProjectionBindings(query.getHead(), query.getAntecedent(), newEdge[1]);
							long t2 = System.currentTimeMillis();
							if((t2 - t1) > 20000)
								System.out.println("countProjectionBindings var=" + newEdge[1] + " "  + query + " has taken " + (t2 - t1) + " ms");
						}else{
							long t1 = System.currentTimeMillis();
							promisingRelations = source.countProjectionBindings(rewrittenQuery.getHead(), rewrittenQuery.getAntecedent(), newEdge[1]);
							long t2 = System.currentTimeMillis();
							if((t2 - t1) > 20000)
								System.out.println("countProjectionBindings on rewritten query var=" + newEdge[1] + " "  + rewrittenQuery + " has taken " + (t2 - t1) + " ms");						
						}
					}else{
						promisingRelations = source.countProjectionBindings(query.getHead(), query.getAntecedent(), newEdge[1]);						
					}
					
					query.getTriples().remove(nPatterns);					
					int danglingPosition = (joinPosition == 0 ? 2 : 0);
					boolean boundHead = !FactDatabase.isVariable(query.getTriples().get(0)[danglingPosition]);
					for(ByteString relation: promisingRelations){
						if(excludedRelations != null && excludedRelations.contains(relation))
							continue;
						
						//Here we still have to make a redundancy check						
						int cardinality = promisingRelations.get(relation);
						if(cardinality >= minCardinality){
							newEdge[1] = relation;
							//Before adding the edge, verify whether it leads to the hard case
							if(containsHardCase(query, newEdge))
								continue;
							
							Query candidate = query.addEdge(newEdge, cardinality, newEdge[joinPosition], newEdge[danglingPosition]);
							List<ByteString[]> recursiveAtoms = candidate.getRedundantAtoms();
							if(!recursiveAtoms.isEmpty()){
								if(allowConstants){
									for(ByteString[] triple: recursiveAtoms){										
										if(!FactDatabase.isVariable(triple[danglingPosition])){
											candidate.getTriples().add(FactDatabase.triple(newEdge[danglingPosition], FactDatabase.DIFFERENTFROMbs, triple[danglingPosition]));
										}
									}
									int finalCardinality;
									if(boundHead){
										//Single variable in head
										finalCardinality = source.countDistinct(candidate.getProjectionVariable(), candidate.getTriples());
									}else{
										//Still pending
										finalCardinality = source.countProjection(candidate.getHead(), candidate.getAntecedent());
									}
									
									if(finalCardinality < minCardinality)
										continue;
									
									candidate.setCardinality(finalCardinality);
								}
							}
							
							candidate.setHeadCoverage((double)candidate.getCardinality() / headCardinalities.get(candidate.getHeadKey()));
							candidate.setSupport((double)candidate.getCardinality() / (double)source.size());
							candidate.setParent(query);		
							List<Query> tmpOutput = new ArrayList<Query>();
							if(allowConstants){
								getInstantiatedEdges(candidate, candidate, nPatterns, danglingPosition, minCardinality, tmpOutput);
								output.addAll(tmpOutput);								
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

	private boolean containsHardCase(Query query, ByteString[] newEdge) {
		// TODO Auto-generated method stub
		int[] hardnessInfo = source.identifyHardQuery(query.getTriples());
		if(hardnessInfo == null) return false;
		ByteString[] hardAtom1 = query.getTriples().get(hardnessInfo[2]);
		ByteString[] hardAtom2 = query.getTriples().get(hardnessInfo[3]);
		List<ByteString[]> subquery = new ArrayList<ByteString[]>(2);
		subquery.add(newEdge);
		subquery.add(hardAtom1);
		if(source.identifyHardQuery(subquery) != null) return true;
		subquery.set(1, hardAtom2);
		if(source.identifyHardQuery(subquery) != null) return true;		
		return false;
	}

	protected void getInstantiatedEdges(Query query, Query originalQuery, int bindingTriplePos, int danglingPosition, int minCardinality, Collection<Query> output) {
		ByteString[] danglingEdge = query.getTriples().get(bindingTriplePos);
		Query rewrittenQuery = rewriteProjectionQuery(query, bindingTriplePos, danglingPosition == 0 ? 2 : 0);
		IntHashMap<ByteString> constants = null;
		if(rewrittenQuery != null){
			long t1 = System.currentTimeMillis();		
			constants = source.countProjectionBindings(rewrittenQuery.getHead(), rewrittenQuery.getAntecedent(), danglingEdge[danglingPosition]);
			long t2 = System.currentTimeMillis();
			if((t2 - t1) > 20000)
				System.out.println("countProjectionBindings var=" + danglingEdge[danglingPosition] + " in " + query + " (rewriten to " + rewrittenQuery + ") has taken " + (t2 - t1) + " ms");						
		}else{
			long t1 = System.currentTimeMillis();		
			constants = source.countProjectionBindings(query.getHead(), query.getAntecedent(), danglingEdge[danglingPosition]);
			long t2 = System.currentTimeMillis();
			if((t2 - t1) > 20000)
				System.out.println("countProjectionBindings var=" + danglingEdge[danglingPosition] + " in " + query + " has taken " + (t2 - t1) + " ms");			
		}
		
		int joinPosition = (danglingPosition == 0 ? 2 : 0);
		for(ByteString constant: constants){
			int cardinality = constants.get(constant);
			if(cardinality >= minCardinality){
				ByteString[] targetEdge = danglingEdge.clone();
				targetEdge[danglingPosition] = constant;
				
				Query candidate = query.unify(bindingTriplePos, danglingPosition, constant, cardinality);				
				//If the new edge does not contribute with anything
				int cardLastEdge = source.countDistinct(targetEdge[joinPosition], candidate.getTriples());
				if(cardLastEdge < 2)
					continue;
				
				if(candidate.getRedundantAtoms().isEmpty()){
					candidate.setHeadCoverage((double)cardinality / headCardinalities.get(candidate.getHeadKey()));
					candidate.setSupport((double)cardinality / (double)source.size());
					candidate.setParent(originalQuery);
					output.add(candidate);
				}
			}
		}
	}
	
	private Query rewriteProjectionQuery(Query query, int bindingTriplePos, int bindingVarPos) {
		int hardnessInfo[] = source.identifyHardQuery(query.getTriples());
		ByteString[] targetTriple = query.getTriples().get(bindingTriplePos);
		int nonFreshVarPos = bindingVarPos;
		ByteString[] toRemove = null;
		Query rewrittenQuery = null;
		
		if(hardnessInfo != null){
			ByteString[] t1 = query.getTriples().get(hardnessInfo[2]);
			ByteString[] t2 = query.getTriples().get(hardnessInfo[3]);
			
			ByteString nonFreshVar = targetTriple[nonFreshVarPos];
			int victimVarPos, victimTriplePos = -1, targetTriplePos = -1;
			victimVarPos= hardnessInfo[1];
						
			if(FactDatabase.varpos(nonFreshVar, t1) == -1){
				toRemove = t1;
				victimTriplePos = hardnessInfo[2];
				targetTriplePos = hardnessInfo[3];
			}else if(FactDatabase.varpos(nonFreshVar, t2) == -1){
				toRemove = t2;
				victimTriplePos = hardnessInfo[3];
				targetTriplePos = hardnessInfo[2];
			}
			
			if(toRemove == null){
				//Check which one is suitable
				if(query.variableCanBeDeleted(hardnessInfo[2], hardnessInfo[1])){
					rewrittenQuery = query.rewriteQuery(t1, t2, t1[hardnessInfo[1]], t2[hardnessInfo[1]]);
				}else if(query.variableCanBeDeleted(hardnessInfo[3], hardnessInfo[1])){
					rewrittenQuery = query.rewriteQuery(t2, t1, t2[hardnessInfo[1]], t1[hardnessInfo[1]]);
				}else{
					return null;
				}
				
			}else{
				ByteString[] target = toRemove == t1 ? t2 : t1;
				//Check for the triple that can be deleted
				if(!query.variableCanBeDeleted(victimTriplePos, victimVarPos)){
					return null;
				}else{
					rewrittenQuery = query.rewriteQuery(toRemove, target, toRemove[victimVarPos], query.getTriples().get(targetTriplePos)[victimVarPos]);
				}					
			}
		}

		return rewrittenQuery;
	}

	protected long computeAntecedentCount(ByteString var1, ByteString var2, Query query){
		List<ByteString[]> antecedent = query.getAntecedent();
		int[] hardnessInfo = source.identifyHardQuery(antecedent);
		long result;
		if(hardnessInfo == null || !enableOptimizations){
			result = source.countPairs(var1, var2, antecedent);
		}else{
			//Use the cache
			ByteString relation = antecedent.get(0)[1];
			Boolean joinBySubject = new Boolean(hardnessInfo[1] == 0); 
			Pair<ByteString, Boolean> key = new Pair<ByteString, Boolean>(relation, joinBySubject);
			Long tmpResult = hardQueries.get(key);
			
			if(tmpResult == null){
				long t1 = System.currentTimeMillis();
				result = source.countPairs(var1, var2, antecedent, hardnessInfo);
				long t2 = System.currentTimeMillis();
				if((t2 - t1) > 20000)
					System.out.println("Query " + toString(antecedent) + " has taken " + (t2 - t1) + " ms");			

				hardQueries.put(key, result);	
			}else{
				System.out.println("Hitting cache for query " + toString(antecedent));				
				result = tmpResult.longValue();	
			}
		}
		
		return result;
	}
	
	private long computePCAAntecedentCount(ByteString var1, ByteString var2, List<ByteString[]> antecedent, ByteString[] existentialTriple, int nonExistentialPosition) {
		int[] hardnessInfo = source.identifyHardQuery(antecedent);
		long result;
		
		if(hardnessInfo == null || !enableOptimizations){
			antecedent.add(existentialTriple);
			result = source.countPairs(var1, var2, antecedent);
		}else{
			long t1 = System.currentTimeMillis();			
			List<ByteString[]> tmp = new ArrayList<ByteString[]>();
			tmp.add(existentialTriple);
			result = source.countPairs(var1, var2, antecedent, hardnessInfo, existentialTriple, nonExistentialPosition);
			long t2 = System.currentTimeMillis();
			if((t2 - t1) > 20000)
				System.out.println("Query " + toString(antecedent) + " has taken " + (t2 - t1) + " ms");		
		}
		
		return result;
	}


	protected void calculateMetrics(Query candidate) {
		// TODO Auto-generated method stub
		List<ByteString[]> antecedent = new ArrayList<ByteString[]>();
		antecedent.addAll(candidate.getAntecedent());
		List<ByteString[]> succedent = new ArrayList<ByteString[]>();
		succedent.addAll(candidate.getTriples().subList(0, 1));
		double improvedDenominator = 0.0;
		double denominator = 0.0;
		double confidence = 0.0;
		double improvedConfidence = 0.0;
		double predictiveness = 0.0;
		ByteString[] head = candidate.getHead();
		ByteString[] existentialTriple = head.clone();
		int freeVarPos, countVarPos;
		
		countVarPos = candidate.getProjectionVariablePosition();
		if(FactDatabase.numVariables(existentialTriple) == 1){
			freeVarPos = FactDatabase.firstVariablePos(existentialTriple) == 0 ? 2 : 0;
		}else{
			freeVarPos = existentialTriple[0].equals(candidate.getProjectionVariable()) ? 2 : 0;
		}

		existentialTriple[freeVarPos] = ByteString.of("?x");
				
		if(antecedent.isEmpty()){
			candidate.setConfidence(1.0);
			candidate.setImprovedConfidence(1.0);
		}else{
			//Confidence
			try{
				if(FactDatabase.numVariables(head) == 2){
					ByteString var1, var2;
					var1 = head[FactDatabase.firstVariablePos(head)];
					var2 = head[FactDatabase.secondVariablePos(head)];
					denominator = computeAntecedentCount(var1, var2, candidate);
				}else{					
					denominator = (double)source.countDistinct(candidate.getProjectionVariable(), antecedent);
				}				
				confidence = (double)candidate.getCardinality() / denominator;
				candidate.setConfidence(confidence);
				candidate.setBodySize((int)denominator);
			}catch(UnsupportedOperationException e){
				
			}
							
			try{
				List<ByteString[]> redundantAtoms = Query.redundantAtoms(existentialTriple, antecedent);
				boolean existentialQueryRedundant = false;
				
				//If the counting variable is in the same position of any of the unifiable patterns => redundant
				for(ByteString[] atom: redundantAtoms){
					if(existentialTriple[countVarPos].equals(atom[countVarPos]))
						existentialQueryRedundant = true;
				}
					
				if(existentialQueryRedundant){
					improvedConfidence = confidence;
				}else{
					if(FactDatabase.numVariables(head) == 2){
						ByteString var1, var2;
						var1 = head[FactDatabase.firstVariablePos(head)];
						var2 = head[FactDatabase.secondVariablePos(head)];
						improvedDenominator = computePCAAntecedentCount(var1, var2, antecedent, existentialTriple, candidate.getProjectionVariablePosition());
					}else{
						antecedent.add(existentialTriple);
						improvedDenominator = (double)source.countDistinct(candidate.getProjectionVariable(), antecedent);
					}
					
					improvedConfidence = (double)candidate.getCardinality() / improvedDenominator;					
				}
				
				candidate.setImprovedConfidence(improvedConfidence);				
			}catch(UnsupportedOperationException e){
				
			}
			
			predictiveness = 1 - (improvedDenominator / denominator);
			candidate.setPredictiveness(predictiveness);
			candidate.setImprovedPredictiveness(predictiveness * improvedConfidence);
			candidate.setImprovedStdPredictiveness(predictiveness * confidence);
			candidate.setPredConfBodySize(confidence * denominator * predictiveness);
			candidate.setPredImprConfBodySize(confidence * denominator * predictiveness);
		}
	}
	
	private String toString(List<ByteString[]> triples){
		StringBuilder strBuilder = new StringBuilder();
		for(int i = 0; i < triples.size(); ++i){
			ByteString[] pattern = triples.get(i);
			strBuilder.append(pattern[0]);
			strBuilder.append("  ");
			strBuilder.append(pattern[1]);
			strBuilder.append("  ");			
			strBuilder.append(pattern[2]);
			strBuilder.append("  ");
		}
		
		return strBuilder.toString();
	}
}
