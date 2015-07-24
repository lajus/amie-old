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
public class DefaultMiningAssistant extends MiningAssistant{
	/**
	 * Store counts for hard queries
	 */
	protected Map<Pair<ByteString, Boolean>, Long> hardQueries;
	
	
	public DefaultMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
		this.hardQueries = Collections.synchronizedMap(new HashMap<Pair<ByteString, Boolean>, Long>());
		// TODO Auto-generated constructor stub
	}
	
	public long getTotalCount(Query query){
		return kb.size();
	}
	
	@Override
	public void getInitialAtomsFromSeeds(Collection<ByteString> relations, double minCardinality, Collection<Query> output) {
		Query query = new Query();
		//The query must be empty
		if (!query.isEmpty()){
			throw new IllegalArgumentException("Expected an empty query");
		}
		
		ByteString[] newEdge = query.fullyUnboundTriplePattern();		
		query.getTriples().add(newEdge);
		
		for(ByteString relation: relations){
			newEdge[1] = relation;
			
			int countVarPos = this.countAlwaysOnSubject? 0 : findCountingVariable(newEdge);
			List<ByteString[]> emptyList = Collections.emptyList();
			long cardinality = this.kb.countProjection(query.getHead(), emptyList);
			
			ByteString[] succedent = newEdge.clone();
			Query candidate = new Query(succedent, cardinality);
			candidate.setFunctionalVariablePosition(countVarPos);
			registerHeadRelation(candidate);
			ArrayList<Query> tmpOutput = new ArrayList<>();
			if(canAddInstantiatedAtoms() && !relation.equals(FactDatabase.EQUALSbs)) {
				getInstantiatedAtoms(candidate, null, 0, countVarPos == 0 ? 2 : 0, minCardinality, tmpOutput);			
				output.addAll(tmpOutput);
			}
			
			if (!this.enforceConstants) {
				output.add(candidate);
			}
		}
		
		query.getTriples().remove(0);
	}
	
	@Override
	public void getInitialAtoms(double minSupportThreshold, Collection<Query> output) {
		List<ByteString[]> newEdgeList = new ArrayList<ByteString[]>(1);
		ByteString[] newEdge = new ByteString[]{ByteString.of("?x"), ByteString.of("?y"), ByteString.of("?z")};
		newEdgeList.add(newEdge);
		List<ByteString[]> emptyList = Collections.emptyList();
		IntHashMap<ByteString> relations = this.kb.countProjectionBindings(newEdge, emptyList, newEdge[1]);
		buildInitialQueries(relations, minSupportThreshold, output);		
	}

	
	/**
	 * Returns all candidates obtained by adding a closing edge (an edge with two existing variables).
	 * @param currentNode
	 * @param minSupportThreshold
	 * @param omittedVariables
	 * @return
	 */
	public void getClosingAtoms(Query query, double minSupportThreshold, Collection<Query> output) {
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
								promisingRelations = kb.countProjectionBindings(query.getHead(), query.getAntecedent(), newEdge[1]);
								long t2 = System.currentTimeMillis();
								if((t2 - t1) > 20000 && !silent)
									System.err.println("countProjectionBindings var=" + newEdge[1] + " "  + query + " has taken " + (t2 - t1) + " ms");
							}else{
								long t1 = System.currentTimeMillis();
								promisingRelations = kb.countProjectionBindings(rewrittenQuery.getHead(), rewrittenQuery.getAntecedent(), newEdge[1]);
								long t2 = System.currentTimeMillis();
								if((t2 - t1) > 20000 && !silent)
									System.err.println("countProjectionBindings on rewritten query var=" + newEdge[1] + " "  + rewrittenQuery + " has taken " + (t2 - t1) + " ms");						
							}
						} else {
							promisingRelations = this.kb.countProjectionBindings(query.getHead(), query.getAntecedent(), newEdge[1]);
						}
						query.getTriples().remove(nPatterns);
						List<ByteString> listOfPromisingRelations = promisingRelations.decreasingKeys();
						for(ByteString relation: listOfPromisingRelations){
							int cardinality = promisingRelations.get(relation);
							if (cardinality < minSupportThreshold) {
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
							Query candidate = query.addAtom(newEdge, cardinality);
							if(!candidate.isRedundantRecursive()){
								candidate.setHeadCoverage((double)cardinality / (double)this.headCardinalities.get(candidate.getHeadRelation()));
								candidate.setSupportRatio((double)cardinality / (double)this.kb.size());
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
	public void getDanglingAtoms(Query query, double minCardinality, Collection<Query> output) {		
		ByteString[] newEdge = query.fullyUnboundTriplePattern();
		
		if (query.isEmpty()) {
			throw new IllegalArgumentException("This method expects a non-empty query");
		}
	
	
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
		
		getDanglingAtoms(query, newEdge, minCardinality, output);
	}
	
	/**
	 * It adds to the output all the rules resulting from adding dangling atom instantiation of "edge"
	 * to the query.
	 * @param query
	 * @param edge
	 * @param minSupportThreshold Minimum support threshold.
	 * @param output
	 */
	protected void getDanglingAtoms(Query query, ByteString[] edge, double minSupportThreshold, Collection<Query> output) {
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
					promisingRelations = this.kb.countProjectionBindings(query.getHead(), query.getAntecedent(), newEdge[1]);
					long t2 = System.currentTimeMillis();
					if((t2 - t1) > 20000 && !silent) {
						System.err.println("countProjectionBindings var=" + newEdge[1] + " "  + query + " has taken " + (t2 - t1) + " ms");
					}
				}else{
					long t1 = System.currentTimeMillis();
					promisingRelations = this.kb.countProjectionBindings(rewrittenQuery.getHead(), rewrittenQuery.getAntecedent(), newEdge[1]);
					long t2 = System.currentTimeMillis();
					if((t2 - t1) > 20000 && !silent)
					System.err.println("countProjectionBindings on rewritten query var=" + newEdge[1] + " "  + rewrittenQuery + " has taken " + (t2 - t1) + " ms");						
				}
				
				query.getTriples().remove(nPatterns);					
				int danglingPosition = (joinPosition == 0 ? 2 : 0);
				boolean boundHead = !FactDatabase.isVariable(query.getTriples().get(0)[danglingPosition]);
				List<ByteString> listOfPromisingRelations = promisingRelations.decreasingKeys();				
				// The relations are sorted by support, therefore we can stop once we have reached
				// the minimum support.
				for(ByteString relation: listOfPromisingRelations){
					int cardinality = promisingRelations.get(relation);
					
					if (cardinality < minSupportThreshold) {
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
					
					Query candidate = query.addAtom(newEdge, cardinality);
					List<ByteString[]> recursiveAtoms = candidate.getRedundantAtoms();
					if(!recursiveAtoms.isEmpty()){
						if(canAddInstantiatedAtoms()){
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
								finalCardinality = this.kb.countDistinct(candidate.getFunctionalVariable(), candidate.getTriples());
							}else{
								//Still pending
								finalCardinality = this.kb.countProjection(candidate.getHead(), candidate.getAntecedent());
							}
							
							if(finalCardinality < minSupportThreshold)
								continue;
							
							candidate.setSupport(finalCardinality);
						}
					}
					
					candidate.setHeadCoverage(candidate.getSupport() / this.headCardinalities.get(candidate.getHeadRelation()));
					candidate.setSupportRatio(candidate.getSupport() / this.kb.size());
					candidate.setParent(query);	
					output.add(candidate);
					/*if (canAddInstantiatedAtoms()) {
						// Pruning by maximum length for the \mathcal{O}_E operator.
						if (this.exploitMaxLengthOption) {
							if (query.getRealLength() < this.maxDepth - 1 
									|| openVariables.size() < 2) {
								getInstantiatedAtoms(candidate, candidate, nPatterns, danglingPosition, minSupportThreshold, output);	
							}
						} else {
							getInstantiatedAtoms(candidate, candidate, nPatterns, danglingPosition, minSupportThreshold, output);							
						}
					}*/
					
/*					if (!this.enforceConstants) {
						// If this rule will not be refined anyway.
						if (candidate.getRealLength() == this.maxDepth 
								&& !candidate.isClosed() ) {
							continue;
						}
						// If the assistant has been told to avoid atoms of the form type(x, y)
						if (relation.equals(this.typeString) 
								&& this.avoidUnboundTypeAtoms) {
							continue;
						}
						output.add(candidate);
					}*/
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
		int[] hardnessInfo = this.kb.identifyHardQueryTypeI(query.getTriples());
		if(hardnessInfo == null) return false;
		ByteString[] hardAtom1 = query.getTriples().get(hardnessInfo[2]);
		ByteString[] hardAtom2 = query.getTriples().get(hardnessInfo[3]);
		List<ByteString[]> subquery = new ArrayList<ByteString[]>(2);
		subquery.add(newEdge);
		subquery.add(hardAtom1);
		if (this.kb.identifyHardQueryTypeI(subquery) != null) return true;
		subquery.set(1, hardAtom2);
		if (this.kb.identifyHardQueryTypeI(subquery) != null) return true;		
		return false;
	}

	/**
	 * Application of the "Add instantiated atom" operator. It takes a rule of the form
	 * r(x, w) ^ ..... => rh(x, y), where r(x, w) is recently added atom and adds to the
	 * output all the derived rules where "w" is bound to a constant that keeps the whole
	 * pattern above the minCardinality threshold.
	 * @param query
	 * @param parentQuery
	 * @param bindingTriplePos
	 * @param danglingPosition
	 * @param minSupportThreshold
	 * @param output
	 */
	protected void getInstantiatedAtoms(Query query, Query parentQuery, 
			int bindingTriplePos, int danglingPosition, double minSupportThreshold, Collection<Query> output) {
		ByteString[] danglingEdge = query.getTriples().get(bindingTriplePos);
		Query rewrittenQuery = null;
		if (!query.isEmpty() && this.enableQueryRewriting) {
			rewrittenQuery = rewriteProjectionQuery(query, bindingTriplePos, danglingPosition == 0 ? 2 : 0);
		}
		
		IntHashMap<ByteString> constants = null;
		if(rewrittenQuery != null){
			long t1 = System.currentTimeMillis();		
			constants = this.kb.countProjectionBindings(rewrittenQuery.getHead(), rewrittenQuery.getAntecedent(), danglingEdge[danglingPosition]);
			long t2 = System.currentTimeMillis();
			if((t2 - t1) > 20000 && !silent)
				System.err.println("countProjectionBindings var=" + danglingEdge[danglingPosition] + " in " + query + " (rewritten to " + rewrittenQuery + ") has taken " + (t2 - t1) + " ms");						
		}else{
			long t1 = System.currentTimeMillis();		
			constants = this.kb.countProjectionBindings(query.getHead(), query.getAntecedent(), danglingEdge[danglingPosition]);
			long t2 = System.currentTimeMillis();
			if((t2 - t1) > 20000 && !silent)
				System.err.println("countProjectionBindings var=" + danglingEdge[danglingPosition] + " in " + query + " has taken " + (t2 - t1) + " ms");			
		}
		
		int joinPosition = (danglingPosition == 0 ? 2 : 0);
		for(ByteString constant: constants){
			int cardinality = constants.get(constant);
			if(cardinality >= minSupportThreshold){
				ByteString[] targetEdge = danglingEdge.clone();
				targetEdge[danglingPosition] = constant;
				assert(FactDatabase.isVariable(targetEdge[joinPosition]));
				
				Query candidate = query.instantiateConstant(bindingTriplePos, danglingPosition, constant, cardinality);				
				// Do this checking only for non-empty queries
				//If the new edge does not contribute with anything
				if (!query.isEmpty()) {
					long cardLastEdge = this.kb.countDistinct(targetEdge[joinPosition], candidate.getTriples());
					if(cardLastEdge < 2)
						continue;
				}
				
				if(candidate.getRedundantAtoms().isEmpty()){
					candidate.setHeadCoverage((double)cardinality / headCardinalities.get(candidate.getHeadRelation()));
					candidate.setSupportRatio((double)cardinality / (double)kb.size());
					candidate.setParent(parentQuery);
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
		int hardnessInfo[] = this.kb.identifyHardQueryTypeI(query.getTriples());
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

	/**
	 * Returns the number of distinct bindings of the given variables in the body of the rule.
	 * @param var1
	 * @param var2
	 * @param query
	 * @return
	 */
	protected long computeBodySize(ByteString var1, ByteString var2, Query query){
		long t1 = System.currentTimeMillis();		
		long result = this.kb.countDistinctPairs(var1, var2, query.getAntecedent());
		long t2 = System.currentTimeMillis();	
		query.setPcaConfidenceRunningTime(t2 - t1);
		if((t2 - t1) > 20000 && !this.silent)
			System.out.println("countPairs vars " + var1 + ", " + var2 + " in " + FactDatabase.toString(query.getAntecedent()) + " has taken " + (t2 - t1) + " ms");		
		
		return result;
	}
	
	/**
	 * Returns the denominator of the PCA confidence expression for the antecedent of a rule.
	 * @param var1
	 * @param var2
	 * @param query
	 * @param antecedent
	 * @param existentialTriple
	 * @param nonExistentialPosition
	 * @return
	 */
	protected double computePcaBodySize(ByteString var1, ByteString var2, Query query, List<ByteString[]> antecedent, ByteString[] existentialTriple, int nonExistentialPosition) {		
		antecedent.add(existentialTriple);
		long t1 = System.currentTimeMillis();
		long result = this.kb.countDistinctPairs(var1, var2, antecedent);
		long t2 = System.currentTimeMillis();
		query.setConfidenceRunningTime(t2 - t1);
		if((t2 - t1) > 20000 && !this.silent)
			System.out.println("countPairs vars " + var1 + ", " + var2 + " in " + FactDatabase.toString(antecedent) + " has taken " + (t2 - t1) + " ms");		
		
		return result;		
	}

	@Override
	public double computeCardinality(Query rule) {
		if (rule.isEmpty()) {
			rule.setSupport(0l);
			rule.setHeadCoverage(0.0);
			rule.setSupportRatio(0.0);
		} else {
			ByteString[] head = rule.getHead();
			if (FactDatabase.numVariables(head) == 2) {
				rule.setSupport(this.kb.countDistinctPairs(head[0], head[2], rule.getTriples()));
			} else {
				rule.setSupport(this.kb.countDistinct(rule.getFunctionalVariable(), rule.getTriples()));
			}
			rule.setSupportRatio((double) rule.getSupport() / this.kb.size());
			Double relationSize = this.headCardinalities.get(head[1].toString());
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

		existentialTriple[freeVarPos] = ByteString.of("?xw");
		if (!antecedent.isEmpty()) {
			antecedent.add(existentialTriple);
			try{
				if (noOfHeadVars == 1) {
					pcaDenominator = (double) this.kb.countDistinct(rule.getFunctionalVariable(), antecedent);
				} else {
					pcaDenominator = (double) this.kb.countDistinctPairs(succedent[0], succedent[2], antecedent);					
				}
				rule.setPcaBodySize(pcaDenominator);
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
		ByteString[] head = candidate.getHead();
		
		if (!antecedent.isEmpty()){
			//Confidence
			try{
				if(FactDatabase.numVariables(head) == 2){
					ByteString var1, var2;
					var1 = head[FactDatabase.firstVariablePos(head)];
					var2 = head[FactDatabase.secondVariablePos(head)];
					denominator = (double) computeBodySize(var1, var2, candidate);
				} else {					
					denominator = (double) this.kb.countDistinct(candidate.getFunctionalVariable(), antecedent);
				}				
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
				FactDatabase.triple("?a", "<hasChild>", "?x"),
				FactDatabase.triple("?e", "<hasChild>", "?b"),
				FactDatabase.triple("?e", "<isMarriedTo>", "?a"));
		//?e  <hasChild>  ?b  ?e  <isMarriedTo>  ?a   => ?a  <hasChild>  ?b
		long timeStamp1 = System.currentTimeMillis();
		System.out.println("Results Std: " + db.countDistinctPairs(ByteString.of("?a"), ByteString.of("?b"), pcaDenom.subList(1,  pcaDenom.size() - 1)));
		System.out.println("Results PCA: " + db.countDistinctPairs(ByteString.of("?a"), ByteString.of("?b"), pcaDenom));
		System.out.println("PCA denom: " + ((System.currentTimeMillis() - timeStamp1) / 1000.0) + " seconds");
	}
}
