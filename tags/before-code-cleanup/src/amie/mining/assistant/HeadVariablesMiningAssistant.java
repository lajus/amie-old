package amie.mining.assistant;

import java.io.File;
import java.io.IOException;
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

/**
 * Mining assistant that defines support and confidence as the number of 
 * distinct bindings of the head variables.
 * @author galarrag
 *
 */
public class HeadVariablesMiningAssistant extends MiningAssistant{
	/**
	 * Store counts for hard queries
	 */
	protected Map<Pair<ByteString, Boolean>, Long> hardQueries;
	
	
	public HeadVariablesMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
		this.hardQueries = Collections.synchronizedMap(new HashMap<Pair<ByteString, Boolean>, Long>());
		// TODO Auto-generated constructor stub
	}
	
	public long getTotalCount(Query query){
		return source.size();
	}
	
	public void getInitialDanglingEdgesFromSeeds(Query query, Collection<ByteString> relations, int minCardinality, Collection<Query> output) {
		//The query must be empty
		if (!query.isEmpty()){
			throw new IllegalArgumentException("Expected an empty query");
		}
		
		ByteString[] newEdge = query.fullyUnboundTriplePattern();		
		query.getTriples().add(newEdge);
		
		for(ByteString relation: relations){
			newEdge[1] = relation;
			
			int countVarPos = this.countAlwaysOnSubject? 0 : findCountingVariable(newEdge);
			ByteString countingVariable = newEdge[countVarPos];
			List<ByteString[]> emptyList = Collections.emptyList();
			long cardinality = this.source.countProjection(query.getHead(), emptyList);
			
			ByteString[] succedent = newEdge.clone();
			Query candidate = new Query(succedent, cardinality);
			candidate.setFunctionalVariable(countingVariable);
			registerHeadRelation(candidate);
			ArrayList<Query> tmpOutput = new ArrayList<>();
			if(canAddInstantiatedAtom()) {
				getInstantiatedEdges(candidate, null, candidate.getLastTriplePattern(), countVarPos == 0 ? 2 : 0, minCardinality, tmpOutput);			
				output.addAll(tmpOutput);
			}
			
			if (!this.enforceConstants) {
				output.add(candidate);
			}
		}
		
		query.getTriples().remove(0);
	}

	
	/**
	 * Returns all candidates obtained by adding a closing edge (an edge with two existing variables).
	 * @param currentNode
	 * @param minCardinality
	 * @param omittedVariables
	 * @return
	 */
	public void getCloseCircleEdges(Query query, int minCardinality, Collection<Query> output) {
		if (this.enforceConstants) {
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
						if (this.enabledFunctionalityHeuristic && this.enableQueryRewriting) {
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
							promisingRelations = this.source.countProjectionBindings(query.getHead(), query.getAntecedent(), newEdge[1]);
						}
						query.getTriples().remove(nPatterns);
						List<ByteString> listOfPromisingRelations = promisingRelations.decreasingKeys();
						for(ByteString relation: listOfPromisingRelations){
							int cardinality = promisingRelations.get(relation);
							if (cardinality < minCardinality) {
								break;
							}
							
							// Language bias test
							if (query.cardinalityForRelation(relation) >= this.recursivityLimit) {
								continue;
							}
							
							if (this.bodyExcludedRelations != null 
									&& this.bodyExcludedRelations.contains(relation)) {
								continue;
							}
							
							if (this.bodyTargetRelations != null 
									&& !this.bodyTargetRelations.contains(relation)) {
								continue;
							}
							
							//Here we still have to make a redundancy check							
							newEdge[1] = relation;
							Query candidate = query.closeCircle(newEdge, cardinality);
							if(!candidate.isRedundantRecursive()){
								candidate.setHeadCoverage((double)cardinality / (double)this.headCardinalities.get(candidate.getHeadRelation()));
								candidate.setSupportRatio((double)cardinality / (double)this.source.size());
								candidate.setParent(query);
								output.add(candidate);
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
			IntHashMap<ByteString> relations = this.source.countProjectionBindings(query.getHead(), emptyList, newEdge[1]);
			for(ByteString relation: relations){
				// Language bias test
				if (query.cardinalityForRelation(relation) >= this.recursivityLimit) {
					continue;
				}
				
				if (this.headExcludedRelations != null 
						&& this.headExcludedRelations.contains(relation)) {
					continue;
				}
				
				int cardinality = relations.get(relation);
				if(cardinality >= minCardinality){
					ByteString[] succedent = newEdge.clone();
					succedent[1] = relation;
					int countVarPos = this.countAlwaysOnSubject? 0 : findCountingVariable(succedent);
					Query candidate = new Query(succedent, cardinality);
					candidate.setFunctionalVariable(succedent[countVarPos]);
					registerHeadRelation(candidate);
					if(canAddInstantiatedAtom()){
						getInstantiatedEdges(candidate, null, 
								candidate.getLastTriplePattern(), 
								countVarPos == 0 ? 2 : 0, 
										minCardinality, output);
					}
					
					if (!this.enforceConstants) {
						output.add(candidate);
					}
				}
			}			
			query.getTriples().remove(0);
		}else{
			if(!testLength(query))
				return;
						
			// Pruning by maximum length for the \mathcal{O}_D operator.
			if(query.getRealLength() == this.maxDepth - 1) {
				if (this.exploitMaxLengthOption) {
					if(!query.getOpenVariables().isEmpty() 
							&& !this.allowConstants 
							&& !this.enforceConstants) {
						return;
					}
				}
			}
			
			addDanglingEdge(query, newEdge, minCardinality, output);
		}
	}
	
	/**
	 * It adds to the output all the rules resulting from adding dangling atom instantiation of "edge"
	 * to the query.
	 * @param query
	 * @param edge
	 * @param minCardinality Minimum support threshold.
	 * @param output
	 */
	protected void addDanglingEdge(Query query, ByteString[] edge, int minCardinality, Collection<Query> output) {
		List<ByteString> joinVariables = null;
		List<ByteString> openVariables = query.getOpenVariables();
		
		//Then do it for all values
		if(query.isClosed()) {				
			joinVariables = query.getVariables();
		} else {
			joinVariables = openVariables;
		}
		
		int nPatterns = query.getLength();
		
		for(int joinPosition = 0; joinPosition <= 2; joinPosition += 2){			
			for(ByteString joinVariable: joinVariables){
				ByteString[] newEdge = edge.clone();
				
				newEdge[joinPosition] = joinVariable;
				query.getTriples().add(newEdge);
				IntHashMap<ByteString> promisingRelations = null;
				Query rewrittenQuery = null;
				if (this.enableQueryRewriting) {
					rewrittenQuery = rewriteProjectionQuery(query, nPatterns, joinPosition == 0 ? 0 : 2);	
				}
				
				if(rewrittenQuery == null){
					long t1 = System.currentTimeMillis();
					promisingRelations = this.source.countProjectionBindings(query.getHead(), query.getAntecedent(), newEdge[1]);
					long t2 = System.currentTimeMillis();
					if((t2 - t1) > 20000 && !silent) {
						System.out.println("countProjectionBindings var=" + newEdge[1] + " "  + query + " has taken " + (t2 - t1) + " ms");
					}
				}else{
					long t1 = System.currentTimeMillis();
					promisingRelations = this.source.countProjectionBindings(rewrittenQuery.getHead(), rewrittenQuery.getAntecedent(), newEdge[1]);
					long t2 = System.currentTimeMillis();
					if((t2 - t1) > 20000 && !silent)
					System.out.println("countProjectionBindings on rewritten query var=" + newEdge[1] + " "  + rewrittenQuery + " has taken " + (t2 - t1) + " ms");						
				}
				
				query.getTriples().remove(nPatterns);					
				int danglingPosition = (joinPosition == 0 ? 2 : 0);
				boolean boundHead = !FactDatabase.isVariable(query.getTriples().get(0)[danglingPosition]);
				List<ByteString> listOfPromisingRelations = promisingRelations.decreasingKeys();				
				// The relations are sorted by support, therefore we can stop once we have reached
				// the minimum support.
				for(ByteString relation: listOfPromisingRelations){
					int cardinality = promisingRelations.get(relation);
					
					if (cardinality < minCardinality) {
						break;
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
					
					newEdge[1] = relation;
					//Before adding the edge, verify whether it leads to the hard case
					if(containsHardCase(query, newEdge))
						continue;
					
					Query candidate = query.addEdge(newEdge, cardinality, newEdge[joinPosition], newEdge[danglingPosition]);
					List<ByteString[]> recursiveAtoms = candidate.getRedundantAtoms();
					if(!recursiveAtoms.isEmpty()){
						if(canAddInstantiatedAtom()){
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
								finalCardinality = this.source.countDistinct(candidate.getFunctionalVariable(), candidate.getTriples());
							}else{
								//Still pending
								finalCardinality = this.source.countProjection(candidate.getHead(), candidate.getAntecedent());
							}
							
							if(finalCardinality < minCardinality)
								continue;
							
							candidate.setSupport(finalCardinality);
						}
					}
					
					candidate.setHeadCoverage((double)candidate.getSupport() / this.headCardinalities.get(candidate.getHeadRelation()));
					candidate.setSupportRatio((double)candidate.getSupport() / (double)this.source.size());
					candidate.setParent(query);		
					if (canAddInstantiatedAtom()) {
						// Pruning by maximum length for the \mathcal{O}_E operator.
						if (this.exploitMaxLengthOption) {
							if (query.getRealLength() < this.maxDepth - 1 
									|| openVariables.size() < 2) {
								getInstantiatedEdges(candidate, candidate, nPatterns, danglingPosition, minCardinality, output);	
							}
						} else {
							getInstantiatedEdges(candidate, candidate, nPatterns, danglingPosition, minCardinality, output);							
						}
					}
					
					if (!this.enforceConstants) {
						// If this rule will not be refined anyway.
						if (candidate.getRealLength() == this.maxDepth 
								&& !candidate.isClosed()) {
							continue;
						}
						// If the assistant has been told to avoid atoms of the form type(x, y)
						if (relation.equals(this.typeString) 
								&& this.avoidUnboundTypeAtoms) {
							continue;
						}
						output.add(candidate);
					}
				}
			}
		}
	}

	/**
	 * It determines whether the rule contains an expensive query patterns of the forms
	 * #(x, y) : r(z, x) r(z, y) or #(x, y) : r(y, z) r(x, z). Such query patterns will be approximated
	 * by AMIE.
	 * 
	 * @param query
	 * @param newEdge
	 * @return
	 */
	protected boolean containsHardCase(Query query, ByteString[] newEdge) {
		// TODO Auto-generated method stub
		int[] hardnessInfo = this.source.identifyHardQueryTypeI(query.getTriples());
		if(hardnessInfo == null) return false;
		ByteString[] hardAtom1 = query.getTriples().get(hardnessInfo[2]);
		ByteString[] hardAtom2 = query.getTriples().get(hardnessInfo[3]);
		List<ByteString[]> subquery = new ArrayList<ByteString[]>(2);
		subquery.add(newEdge);
		subquery.add(hardAtom1);
		if (this.source.identifyHardQueryTypeI(subquery) != null) return true;
		subquery.set(1, hardAtom2);
		if (this.source.identifyHardQueryTypeI(subquery) != null) return true;		
		return false;
	}

	/**
	 * Application of the "Add instantiated atom" operator. It takes a rule of the form
	 * r(x, w) ^ ..... => rh(x, y), where r(x, w) is recently added atom and adds to the
	 * output all the derived rules where "w" is bound to a constant that keeps the whole
	 * pattern above the minCardinality threshold.
	 * @param query
	 * @param originalQuery
	 * @param bindingTriplePos
	 * @param danglingPosition
	 * @param minCardinality
	 * @param output
	 */
	protected void getInstantiatedEdges(Query query, Query originalQuery, int bindingTriplePos, int danglingPosition, int minCardinality, Collection<Query> output) {
		ByteString[] danglingEdge = query.getTriples().get(bindingTriplePos);
		Query rewrittenQuery = null;
		if (this.enableQueryRewriting) {
			rewrittenQuery = rewriteProjectionQuery(query, bindingTriplePos, danglingPosition == 0 ? 2 : 0);
		}
		
		IntHashMap<ByteString> constants = null;
		if(rewrittenQuery != null){
			long t1 = System.currentTimeMillis();		
			constants = this.source.countProjectionBindings(rewrittenQuery.getHead(), rewrittenQuery.getAntecedent(), danglingEdge[danglingPosition]);
			long t2 = System.currentTimeMillis();
			if((t2 - t1) > 20000 && !silent)
				System.out.println("countProjectionBindings var=" + danglingEdge[danglingPosition] + " in " + query + " (rewritten to " + rewrittenQuery + ") has taken " + (t2 - t1) + " ms");						
		}else{
			long t1 = System.currentTimeMillis();		
			constants = this.source.countProjectionBindings(query.getHead(), query.getAntecedent(), danglingEdge[danglingPosition]);
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
				long cardLastEdge = this.source.countDistinct(targetEdge[joinPosition], candidate.getTriples());
				if(cardLastEdge < 2)
					continue;
				
				if(candidate.getRedundantAtoms().isEmpty()){
					candidate.setHeadCoverage((double)cardinality / headCardinalities.get(candidate.getHeadRelation()));
					candidate.setSupportRatio((double)cardinality / (double)source.size());
					candidate.setParent(originalQuery);
					output.add(candidate);
				}
			}
		}
	}
	
	/**
	 * It identifies redundant patterns in queries and rewrites them accordingly 
	 * so that they become less expensive to evaluate. This function targets exclusively queries
	 * of the form: 
	 * r(x, z) r(x, w) => r'(x, y)
	 * where the newly added atom r(x, z) does not really make the query more selective and it is
	 * therefore redundant.
	 * @param query
	 * @param bindingTriplePos
	 * @param bindingVarPos
	 * @return
	 */
	protected Query rewriteProjectionQuery(Query query, int bindingTriplePos, int bindingVarPos) {
		int hardnessInfo[] = this.source.identifyHardQueryTypeI(query.getTriples());
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
		long result = this.source.countPairs(var1, var2, query.getAntecedent());
		long t2 = System.currentTimeMillis();	
		query.setPcaConfidenceRunningTime(t2 - t1);
		if((t2 - t1) > 20000 && !this.silent)
			System.out.println("countPairs vars " + var1 + ", " + var2 + " in " + FactDatabase.toString(query.getAntecedent()) + " has taken " + (t2 - t1) + " ms");		
		
		return result;
	}
	
	protected long computePCAAntecedentCount(ByteString var1, ByteString var2, Query query, List<ByteString[]> antecedent, ByteString[] existentialTriple, int nonExistentialPosition) {		
		antecedent.add(existentialTriple);
		long t1 = System.currentTimeMillis();
		long result = this.source.countPairs(var1, var2, antecedent);
		long t2 = System.currentTimeMillis();
		query.setConfidenceRunningTime(t2 - t1);
		if((t2 - t1) > 20000 && !this.silent)
			System.out.println("countPairs vars " + var1 + ", " + var2 + " in " + FactDatabase.toString(antecedent) + " has taken " + (t2 - t1) + " ms");		
		
		return result;		
	}

	public long computeCardinality(Query rule) {
		if (rule.isEmpty()) {
			rule.setSupport(0l);
			rule.setHeadCoverage(0.0);
			rule.setSupportRatio(0.0);
		} else {
			ByteString[] head = rule.getHead();
			if (FactDatabase.numVariables(head) == 2) {
				rule.setSupport(this.source.countPairs(head[0], head[2], rule.getTriples()));
			} else {
				rule.setSupport(this.source.countDistinct(rule.getFunctionalVariable(), rule.getTriples()));
			}
			rule.setSupportRatio((double) rule.getSupport() / this.source.size());
			Long relationSize = this.headCardinalities.get(head[1].toString());
			if (relationSize != null) {
				rule.setHeadCoverage(rule.getSupport() / relationSize.doubleValue());
			}
		}
		return rule.getSupport();
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
					pcaDenominator = (double)this.source.countDistinct(rule.getFunctionalVariable(), antecedent);
				} else {
					pcaDenominator = (double)this.source.countPairs(succedent[0], succedent[2], antecedent);					
				}
				pcaConfidence = (double)rule.getSupport() / pcaDenominator;
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
			return candidate.getStdConfidence();
		}
		// TODO Auto-generated method stub
		List<ByteString[]> antecedent = new ArrayList<ByteString[]>();
		antecedent.addAll(candidate.getAntecedent());
		double denominator = 0.0;
		double confidence = 0.0;
		ByteString[] head = candidate.getHead();
		
		if(antecedent.isEmpty()){
			candidate.setStdConfidence(1.0);
		}else{
			//Confidence
			try{
				if(FactDatabase.numVariables(head) == 2){
					ByteString var1, var2;
					var1 = head[FactDatabase.firstVariablePos(head)];
					var2 = head[FactDatabase.secondVariablePos(head)];
					denominator = (double)computeAntecedentCount(var1, var2, candidate);
				} else {					
					denominator = (double)this.source.countDistinct(candidate.getFunctionalVariable(), antecedent);
				}				
				confidence = (double)candidate.getSupport() / denominator;
				candidate.setStdConfidence(confidence);
				candidate.setBodySize((long)denominator);
			}catch(UnsupportedOperationException e){
				
			}
		}
		
		return candidate.getStdConfidence();
	}
	
	@Override
	public void calculateConfidenceMetrics(Query candidate) {
		computeStandardConfidence(candidate);
		computePCAConfidence(candidate);
	}
	
	public static void main(String[] args) throws IOException {
		FactDatabase db = new FactDatabase();
		//db.load(new File("/home/galarrag/workspace/AMIE/Data/yago2s/yagoFacts.decoded.compressed.ttl"));
		db.load(new File("/home/galarrag/workspace/AMIE/Data/yago2/yago2core.decoded.compressed.notypes.nolanguagecode.tsv"));
		List<ByteString[]> pcaDenom = FactDatabase.triples(
				FactDatabase.triple("?x", "<isMarriedTo>", "?w"),
				FactDatabase.triple("?x", "<directed>", "?z"),
				FactDatabase.triple("?y", "<actedIn>", "?z")
				);
		long timeStamp1 = System.currentTimeMillis();
		System.out.println("Results : " + db.countPairs(ByteString.of("?x"), ByteString.of("?y"), pcaDenom));
		System.out.println("PCA denom: " + ((System.currentTimeMillis() - timeStamp1) / 1000.0) + " seconds");
		
		List<ByteString[]> stdDenom = FactDatabase.triples(
				FactDatabase.triple("?x", "<directed>", "?z"),
				FactDatabase.triple("?y", "<actedIn>", "?z")
				);
		timeStamp1 = System.currentTimeMillis();
		System.out.println("Results : " + db.countPairs(ByteString.of("?x"), ByteString.of("?y"), stdDenom));
		System.out.println("Std-conf denom: " + ((System.currentTimeMillis() - timeStamp1) / 1000.0) + " seconds");
		
		Query q = new Query(FactDatabase.triple("?x", "<isMarriedTo>", "?y"), stdDenom);
		if (db.functionality(ByteString.of("<isMarriedTo>")) 
				> db.inverseFunctionality(ByteString.of("<isMarriedTo>"))) {
			q.setFunctionalVariable(ByteString.of("?x"));
		} else {
			q.setFunctionalVariable(ByteString.of("?y"));			
		}
		q.setSupport(db.countPairs(ByteString.of("?x"), ByteString.of("?y"), q.getTriples()));	
		HeadVariablesMiningAssistant assistant = new HeadVariablesMiningAssistant(db);
		assistant.setEnabledFunctionalityHeuristic(true);
		timeStamp1 = System.currentTimeMillis();
		assistant.calculateConfidenceBoundsAndApproximations(q);
		System.out.println("Calculating approximation: " + ((System.currentTimeMillis() - timeStamp1) / 1000.0) + " seconds");
	}
}
