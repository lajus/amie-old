package amie.mining.assistant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Pair;
import amie.data.FactDatabase;
import amie.query.Query;

public class HeadVariablesMiningAssistant extends MiningAssistant{
	/**
	 * Store counts for hard queries
	 */
	protected Map<Pair<ByteString, Boolean>, Long> hardQueries;
	
	
	public HeadVariablesMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
		hardQueries = Collections.synchronizedMap(new HashMap<Pair<ByteString, Boolean>, Long>());
		// TODO Auto-generated constructor stub
	}
	
	public long getTotalCount(Query query){
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
			long cardinality = source.countProjection(query.getHead(), emptyList);
			
			ByteString[] succedent = newEdge.clone();
			Query candidate = new Query(succedent, cardinality);
			candidate.setFunctionalVariable(countingVariable);
			registerHeadRelation(candidate);
			ArrayList<Query> tmpOutput = new ArrayList<>();
			if(allowConstants || enforceConstants) {
				getInstantiatedEdges(candidate, null, candidate.getLastTriplePattern(), countVarPos == 0 ? 2 : 0, minCardinality, tmpOutput);			
				output.addAll(tmpOutput);
			}
			
			if (!enforceConstants) {
				output.add(candidate);
			}
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
	public void getCloseCircleEdges(Query query, int minCardinality, Collection<Query> output) {
		if (enforceConstants) {
			return;
		}
		
		int nPatterns = query.getTriples().size();

		if(query.isEmpty())
			return;
		
		if(!testLength(query))
			return;
		
		List<ByteString> sourceVariables = null;
		List<ByteString> targetVariables = null;
		List<ByteString> openVariables = query.getOpenVariables();
		List<ByteString> allVariables = query.getVariables();
		
		if (allVariables.size() < 2) {
			return;
		}
		
		if(query.isSafe()){
			sourceVariables = allVariables;
			targetVariables = allVariables;
		}else{
			sourceVariables = openVariables; 
			if(sourceVariables.size() > 1){
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
						IntHashMap<ByteString> promisingRelations = null;
						if (this.enabledFunctionalityHeuristic) {
							Query rewrittenQuery = rewriteProjectionQuery(query, nPatterns, closeCirclePosition);
							if(rewrittenQuery == null){
								long t1 = System.currentTimeMillis();
								promisingRelations = source.countProjectionBindings(query.getHead(), query.getAntecedent(), newEdge[1]);
								long t2 = System.currentTimeMillis();
								if((t2 - t1) > 20000 && !silent)
									System.out.println("countProjectionBindings var=" + newEdge[1] + " "  + query + " has taken " + (t2 - t1) + " ms");
							}else{
								long t1 = System.currentTimeMillis();
								promisingRelations = source.countProjectionBindings(rewrittenQuery.getHead(), rewrittenQuery.getAntecedent(), newEdge[1]);
								long t2 = System.currentTimeMillis();
								if((t2 - t1) > 20000 && !silent)
									System.out.println("countProjectionBindings on rewritten query var=" + newEdge[1] + " "  + rewrittenQuery + " has taken " + (t2 - t1) + " ms");						
							}
						} else {
							promisingRelations = source.countProjectionBindings(query.getHead(), query.getAntecedent(), newEdge[1]);
						}
						query.getTriples().remove(nPatterns);
						
						for(ByteString relation: promisingRelations){
							// Language bias test
							if (query.cardinalityForRelation(relation) >= recursivityLimit) {
								continue;
							}
							
							if (bodyExcludedRelations != null && bodyExcludedRelations.contains(relation)) {
								continue;
							}
							
							if (bodyTargetRelations != null && !bodyTargetRelations.contains(relation)) {
								continue;
							}
							
							//Here we still have to make a redundancy check
							int cardinality = promisingRelations.get(relation);
							newEdge[1] = relation;
							if (cardinality >= minCardinality) {
								Query candidate = query.closeCircle(newEdge, cardinality);
								if(!candidate.isRedundantRecursive()){
									candidate.setHeadCoverage((double)cardinality / (double)headCardinalities.get(candidate.getHeadRelation()));
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
	 * @param queryand will therefore predict too many new facts with scarce evidence, 
	 * @param minCardinality
	 * @return
	 */
	public void getDanglingEdges(Query query, int minCardinality, Collection<Query> output) {		
		ByteString[] newEdge = query.fullyUnboundTriplePattern();
		
		if(query.isEmpty()){
			//Initial case
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
					if(allowConstants || enforceConstants){
						getInstantiatedEdges(candidate, null, candidate.getLastTriplePattern(), countVarPos == 0 ? 2 : 0, minCardinality, output);
					}
					
					if (!enforceConstants) {
						output.add(candidate);
					}
				}
			}			
			query.getTriples().remove(0);
		}else{
			if(!testLength(query))
				return;
						
			//General case
			if(query.getLength() == maxDepth - 1) {
				if(!query.getOpenVariables().isEmpty() 
						&& !allowConstants && !enforceConstants) {
					return;
				}
			}
			
			addDanglingEdge(query, newEdge, minCardinality, output);
		}
	}
	
	protected void addDanglingEdge(Query query, ByteString[] edge, int minCardinality, Collection<Query> output) {
		List<ByteString> joinVariables = null;
		
		//Then do it for all values
		if(query.isSafe()) {				
			joinVariables = query.getVariables();
		} else {
			joinVariables = query.getOpenVariables();
		}
		
		int nPatterns = query.getLength();
		
		for(int joinPosition = 0; joinPosition <= 2; joinPosition += 2){			
			for(ByteString joinVariable: joinVariables){
				ByteString[] newEdge = edge.clone();
				
				newEdge[joinPosition] = joinVariable;
				query.getTriples().add(newEdge);
				IntHashMap<ByteString> promisingRelations = null;

				Query rewrittenQuery = rewriteProjectionQuery(query, nPatterns, joinPosition == 0 ? 0 : 2);
				if(rewrittenQuery == null){
					long t1 = System.currentTimeMillis();
					promisingRelations = source.countProjectionBindings(query.getHead(), query.getAntecedent(), newEdge[1]);
					long t2 = System.currentTimeMillis();
					if((t2 - t1) > 20000 && !silent) {
						System.out.println("countProjectionBindings var=" + newEdge[1] + " "  + query + " has taken " + (t2 - t1) + " ms");
					}
				}else{
					long t1 = System.currentTimeMillis();
					promisingRelations = source.countProjectionBindings(rewrittenQuery.getHead(), rewrittenQuery.getAntecedent(), newEdge[1]);
					long t2 = System.currentTimeMillis();
					if((t2 - t1) > 20000 && !silent)
					System.out.println("countProjectionBindings on rewritten query var=" + newEdge[1] + " "  + rewrittenQuery + " has taken " + (t2 - t1) + " ms");						
				}
				
				query.getTriples().remove(nPatterns);					
				int danglingPosition = (joinPosition == 0 ? 2 : 0);
				boolean boundHead = !FactDatabase.isVariable(query.getTriples().get(0)[danglingPosition]);
				for(ByteString relation: promisingRelations){
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
							if(allowConstants || enforceConstants){
								for(ByteString[] triple: recursiveAtoms){										
									if(!FactDatabase.isVariable(triple[danglingPosition])){
										candidate.getTriples().add(
												FactDatabase.triple(newEdge[danglingPosition], 
												FactDatabase.DIFFERENTFROMbs, 
												triple[danglingPosition]));
									}
								}
								long finalCardinality;
								if(boundHead){
									//Single variable in head
									finalCardinality = source.countDistinct(candidate.getFunctionalVariable(), candidate.getTriples());
								}else{
									//Still pending
									finalCardinality = source.countProjection(candidate.getHead(), candidate.getAntecedent());
								}
								
								if(finalCardinality < minCardinality)
									continue;
								
								candidate.setCardinality(finalCardinality);
							}
						}
						
						candidate.setHeadCoverage((double)candidate.getCardinality() / headCardinalities.get(candidate.getHeadRelation()));
						candidate.setSupport((double)candidate.getCardinality() / (double)source.size());
						candidate.setParent(query);		
						if(allowConstants || enforceConstants) {
							getInstantiatedEdges(candidate, candidate, nPatterns, danglingPosition, minCardinality, output);
						}
						
						if (!enforceConstants) {
							// If this rule will not be refined anyway.
							if (candidate.getLength() == maxDepth 
									&& !candidate.isSafe()) {
								continue;
							}								
							output.add(candidate);
						}
					}
				}
			}
		}
	}

	protected boolean containsHardCase(Query query, ByteString[] newEdge) {
		// TODO Auto-generated method stub
		int[] hardnessInfo = source.identifyHardQueryTypeI(query.getTriples());
		if(hardnessInfo == null) return false;
		ByteString[] hardAtom1 = query.getTriples().get(hardnessInfo[2]);
		ByteString[] hardAtom2 = query.getTriples().get(hardnessInfo[3]);
		List<ByteString[]> subquery = new ArrayList<ByteString[]>(2);
		subquery.add(newEdge);
		subquery.add(hardAtom1);
		if(source.identifyHardQueryTypeI(subquery) != null) return true;
		subquery.set(1, hardAtom2);
		if(source.identifyHardQueryTypeI(subquery) != null) return true;		
		return false;
	}

	protected void getInstantiatedEdges(Query query, Query originalQuery, int bindingTriplePos, int danglingPosition, int minCardinality, Collection<Query> output) {
		ByteString[] danglingEdge = query.getTriples().get(bindingTriplePos);
		Query rewrittenQuery = null;
		rewrittenQuery = rewriteProjectionQuery(query, bindingTriplePos, danglingPosition == 0 ? 2 : 0);
		
		IntHashMap<ByteString> constants = null;
		if(rewrittenQuery != null){
			long t1 = System.currentTimeMillis();		
			constants = source.countProjectionBindings(rewrittenQuery.getHead(), rewrittenQuery.getAntecedent(), danglingEdge[danglingPosition]);
			long t2 = System.currentTimeMillis();
			if((t2 - t1) > 20000 && !silent)
				System.out.println("countProjectionBindings var=" + danglingEdge[danglingPosition] + " in " + query + " (rewritten to " + rewrittenQuery + ") has taken " + (t2 - t1) + " ms");						
		}else{
			long t1 = System.currentTimeMillis();		
			constants = source.countProjectionBindings(query.getHead(), query.getAntecedent(), danglingEdge[danglingPosition]);
			long t2 = System.currentTimeMillis();
			if((t2 - t1) > 20000 && !silent)
				System.out.println("countProjectionBindings var=" + danglingEdge[danglingPosition] + " in " + query + " has taken " + (t2 - t1) + " ms");			
		}
		
		int joinPosition = (danglingPosition == 0 ? 2 : 0);
		for(ByteString constant: constants){
			int cardinality = constants.get(constant);
			if(cardinality >= minCardinality){
				ByteString[] targetEdge = danglingEdge.clone();
				targetEdge[danglingPosition] = constant;
				assert(FactDatabase.isVariable(targetEdge[joinPosition]));
				
				Query candidate = query.unify(bindingTriplePos, danglingPosition, constant, cardinality);				
				//If the new edge does not contribute with anything
				long cardLastEdge = source.countDistinct(targetEdge[joinPosition], candidate.getTriples());
				if(cardLastEdge < 2)
					continue;
				
				if(candidate.getRedundantAtoms().isEmpty()){
					candidate.setHeadCoverage((double)cardinality / headCardinalities.get(candidate.getHeadRelation()));
					candidate.setSupport((double)cardinality / (double)source.size());
					candidate.setParent(originalQuery);
					output.add(candidate);
				}
			}
		}
	}
	
	protected Query rewriteProjectionQuery(Query query, int bindingTriplePos, int bindingVarPos) {
		int hardnessInfo[] = source.identifyHardQueryTypeI(query.getTriples());
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
						
			if (FactDatabase.varpos(nonFreshVar, t1) == -1) {
				toRemove = t1;
				victimTriplePos = hardnessInfo[2];
				targetTriplePos = hardnessInfo[3];
			} else if(FactDatabase.varpos(nonFreshVar, t2) == -1) {
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
		long t1 = System.currentTimeMillis();		
		long result = source.countPairs(var1, var2, query.getAntecedent());
		long t2 = System.currentTimeMillis();	
		query.setPcaConfidenceRunningTime(t2 - t1);
		if((t2 - t1) > 20000 && !silent)
			System.out.println("countPairs vars " + var1 + ", " + var2 + " in " + FactDatabase.toString(query.getAntecedent()) + " has taken " + (t2 - t1) + " ms");		
		
		return result;
	}
	
	protected long computePCAAntecedentCount(ByteString var1, ByteString var2, Query query, List<ByteString[]> antecedent, ByteString[] existentialTriple, int nonExistentialPosition) {		
		antecedent.add(existentialTriple);
		long t1 = System.currentTimeMillis();
		long result = source.countPairs(var1, var2, antecedent);
		long t2 = System.currentTimeMillis();
		query.setConfidenceRunningTime(t2 - t1);
		if((t2 - t1) > 20000 && !silent)
			System.out.println("countPairs vars " + var1 + ", " + var2 + " in " + FactDatabase.toString(antecedent) + " has taken " + (t2 - t1) + " ms");		
		
		return result;		
	}

	public long computeCardinality(Query rule) {
		if (rule.isEmpty()) {
			rule.setCardinality(0l);
			rule.setHeadCoverage(0.0);
			rule.setSupport(0.0);
		} else {
			ByteString[] head = rule.getHead();
			if (FactDatabase.numVariables(head) == 2) {
				rule.setCardinality(source.countPairs(head[0], head[2], rule.getTriples()));
			} else {
				rule.setCardinality(source.countDistinct(rule.getFunctionalVariable(), rule.getTriples()));
			}
			rule.setSupport((double) rule.getCardinality() / source.size());
			Long relationSize = headCardinalities.get(head[1].toString());
			if (relationSize != null) {
				rule.setHeadCoverage(rule.getCardinality() / relationSize.doubleValue());
			}
		}
		return rule.getCardinality();
	}
	
	@Override
	public double computePCAConfidence(Query rule) {
		if (rule.isEmpty()) {
			return rule.getPcaConfidence();
		}
		
		List<ByteString[]> antecedent = new ArrayList<ByteString[]>();
		antecedent.addAll(rule.getTriples().subList(1, rule.getTriples().size()));
		ByteString[] succedent = rule.getTriples().get(0);
		double pcaDenominator = 0.0;
		double pcaConfidence = 0.0;
		ByteString[] existentialTriple = succedent.clone();
		int freeVarPos = 0;
		int noOfHeadVars = FactDatabase.numVariables(succedent);
		
		if(noOfHeadVars == 1){
			freeVarPos = FactDatabase.firstVariablePos(succedent) == 0 ? 2 : 0;
		}else{
			if(existentialTriple[0].equals(rule.getFunctionalVariable()))
				freeVarPos = 2;
			else
				freeVarPos = 0;
		}

		existentialTriple[freeVarPos] = ByteString.of("?x");
		if(antecedent.isEmpty()){
			rule.setPcaConfidence(1.0);
		}else{
			//Improved confidence: Add an existential version of the head
			antecedent.add(existentialTriple);
			try{
				if (noOfHeadVars == 1) {
					pcaDenominator = (double)source.countDistinct(rule.getFunctionalVariable(), antecedent);
				} else {
					pcaDenominator = (double)source.countPairs(succedent[0], succedent[2], antecedent);					
				}
				pcaConfidence = (double)rule.getCardinality() / pcaDenominator;
				rule.setPcaConfidence(pcaConfidence);
				rule.setBodyStarSize((long)pcaDenominator);
			}catch(UnsupportedOperationException e){
				
			}
		}
		
		return rule.getPcaConfidence();
	}	
	
	@Override
	public double computeStandardConfidence(Query candidate) {
		if (candidate.isEmpty()) {
			return candidate.getConfidence();
		}
		// TODO Auto-generated method stub
		List<ByteString[]> antecedent = new ArrayList<ByteString[]>();
		antecedent.addAll(candidate.getAntecedent());
		double denominator = 0.0;
		double confidence = 0.0;
		ByteString[] head = candidate.getHead();
		
		if(antecedent.isEmpty()){
			candidate.setConfidence(1.0);
		}else{
			//Confidence
			try{
				if(FactDatabase.numVariables(head) == 2){
					ByteString var1, var2;
					var1 = head[FactDatabase.firstVariablePos(head)];
					var2 = head[FactDatabase.secondVariablePos(head)];
					denominator = (double)computeAntecedentCount(var1, var2, candidate);
				} else {					
					denominator = (double)source.countDistinct(candidate.getFunctionalVariable(), antecedent);
				}				
				confidence = (double)candidate.getCardinality() / denominator;
				candidate.setConfidence(confidence);
				candidate.setBodySize((long)denominator);
			}catch(UnsupportedOperationException e){
				
			}
		}
		
		return candidate.getConfidence();
	}

	public void calculateConfidenceMetrics(Query candidate) {
		computeStandardConfidence(candidate);
		computePCAConfidence(candidate);
	}
}
