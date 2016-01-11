package amie.mining.assistant.experimental;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import amie.data.KB;
import amie.mining.assistant.MiningAssistant;
import amie.rules.QueryEquivalenceChecker;
import amie.rules.Rule;
import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.MultiMap;
import javatools.datatypes.Pair;

public class CompletenessMiningAssistant extends MiningAssistant {

	public static final String isComplete = "isComplete";
	
	public static final String isIncomplete = "isIncomplete";
	
	public static final String isRelevant = "<isRelevant>";
	
	public static final ByteString isCompleteBS = ByteString.of(isComplete);
	
	public static final ByteString isIncompleteBS = ByteString.of(isIncomplete);
	
	public static final ByteString isRelevantBS = ByteString.of(isRelevant);
	
	public CompletenessMiningAssistant(KB dataSource) {
		super(dataSource);
		this.allowConstants = false;
		this.bodyExcludedRelations = Arrays.asList(amie.data.U.typeRelationBS, 
				amie.data.U.subClassRelationBS, amie.data.U.domainRelationBS, 
				amie.data.U.rangeRelationBS, isCompleteBS, isIncompleteBS);
	}

	@Override
	public String getDescription() {
        return "Mining completeness rules of the form "
        		+ "B => isComplete(x, relation) or B => isIncomplete(x, relation) "
        		+ "[EXPERIMENTAL]";
	}
	
	@Override
	public void setHeadExcludedRelations(java.util.Collection<ByteString> headExcludedRelations) {};
	
	public void setBodyExcludedRelations(java.util.Collection<ByteString> bodyExcludedRelations) {};
	
	@Override
	protected void buildInitialQueries(IntHashMap<ByteString> relations, 
			double minSupportThreshold, Collection<Rule> output) {
		Rule query = new Rule();
		ByteString[] newEdge = query.fullyUnboundTriplePattern();
		
		for (ByteString relation: relations) {		
			if (relation.equals(isCompleteBS) ||
					relation.equals(isIncompleteBS)) {
				registerHeadRelation(relation, (double) relations.get(relation));
				continue;
			}
			ByteString[] succedent = newEdge.clone();
			ByteString domain = null;
			if (this.kbSchema != null) {
				if (this.kb.isFunctional(relation)) {
					domain = amie.data.U.getRelationDomain(this.kbSchema, relation);
				} else {
					domain = amie.data.U.getRelationRange(this.kbSchema, relation);
				}
			}
			
			List<ByteString[]> queryArray = new ArrayList<>();
			if (domain != null)
				queryArray.add(KB.triple(succedent[0], amie.data.U.typeRelationBS, domain));
			
			succedent[1]  = isCompleteBS;
			succedent[2] = relation;
			queryArray.add(succedent);
			int countVarPos = 0;
			long cardinalityComplete = kb.countDistinct(succedent[0], queryArray);
			
			if (cardinalityComplete >= minSupportThreshold) {
				Rule candidateComplete = null;
				if (domain == null) {
					candidateComplete = new Rule(succedent, cardinalityComplete);				
				} else {
					queryArray.remove(1);
					candidateComplete = new Rule(succedent, queryArray, cardinalityComplete);
				}
				candidateComplete.setFunctionalVariablePosition(countVarPos);
				output.add(candidateComplete);
			}
						
			succedent[1] = isIncompleteBS;
			queryArray = new ArrayList<>();
			if (domain != null)
				queryArray.add(KB.triple(succedent[0], amie.data.U.typeRelationBS, domain));
			queryArray.add(succedent);
			
			long cardinalityIncomplete = kb.countDistinct(succedent[0], queryArray);
			if (cardinalityIncomplete >= minSupportThreshold) {
				Rule candidateIncomplete = null;
				if (domain == null) {
					candidateIncomplete = new Rule(succedent, cardinalityIncomplete);				
				} else {
					queryArray.remove(1);
					candidateIncomplete = new Rule(succedent, queryArray, cardinalityIncomplete);
				}
				candidateIncomplete.setFunctionalVariablePosition(countVarPos);
				output.add(candidateIncomplete);
			}
		}
	}
	
	/**@Override
	public void getDanglingAtoms(Rule rule, double minSupportThreshold, java.util.Collection<Rule> output) {
		// Proceed with the standard addition of dangling atoms once the compulsory atoms
		// have been added.
		if (containsCardinalityAtom(rule) || containsRelevanceAtom(rule)) {
			super.getDanglingAtoms(rule, minSupportThreshold, output);
		}
	}**/
	

	@Override
	public void getTypeSpecializedAtoms(Rule rule, double minSupportThreshold, Collection<Rule> output) {
		ByteString[] lastAtom = rule.getLastRealTriplePattern();
		Pair<ByteString, Integer> compositeRelation = KB.parseCardinalityRelation(lastAtom[1]);
		if (compositeRelation == null) {
			super.getTypeSpecializedAtoms(rule, minSupportThreshold, output);
		} else {
			ByteString oldRelation = lastAtom[1];
			// Do not specialize the equals relation
			if (oldRelation.toString().startsWith(KB.hasNumberOfValuesEquals)) 
				return;
			
			ByteString targetRelation = lastAtom[2];
			int newCard = -1;
			if (rule.getHead()[1].equals(isCompleteBS)) {
				int maximalCardinality = kb.maximalRightCumulativeCardinality(targetRelation, 
						(long)minSupportThreshold);
				newCard = compositeRelation.second.intValue() + 1;
				if (newCard > maximalCardinality)
					return;
			} else {
				newCard = compositeRelation.second.intValue() - 1;
				if (newCard == 0)
					return;
			}
								
			ByteString newRelation = ByteString.of(compositeRelation.first.toString() + newCard);
			lastAtom[1] = newRelation;
			long cardinality = kb.countDistinct(rule.getFunctionalVariable(), rule.getTriples());
			lastAtom[1] = oldRelation;
			if (cardinality >= minSupportThreshold) {
				ByteString[] newAtom = lastAtom.clone();
				newAtom[1] = newRelation;				
				Rule candidate = rule.replaceLastAtom(newAtom, cardinality);
				candidate.addParent(rule);
				output.add(candidate);
			}
		}
	}
	
	private void addCardinalityAtom(Rule rule, double minSupportThreshold, Collection<Rule> output) {
		// We'll force a cardinality atom at the end
		ByteString[] head = rule.getHead();
		ByteString targetRelation = head[2];
		int startCardinality = -1;
		String equalityRelation = null;
		String inequalityRelation = null;				
		if (this.kb.isFunctional(targetRelation)) {
			equalityRelation = KB.hasNumberOfValuesEquals;
			inequalityRelation = head[1].equals(isCompleteBS) ? 
					KB.hasNumberOfValuesGreaterThan : KB.hasNumberOfValuesSmallerThan;
		} else {
			equalityRelation = KB.hasNumberOfValuesEqualsInv;
			inequalityRelation =  head[1].equals(isCompleteBS) ? 
					KB.hasNumberOfValuesGreaterThanInv : KB.hasNumberOfValuesSmallerThanInv;
		}
		
		if (head[1].equals(isIncompleteBS)) {
			startCardinality = kb.maximalCardinality(targetRelation);
		} else {
			startCardinality = 0;
		}
		
		inequalityRelation = inequalityRelation + startCardinality;
		equalityRelation = equalityRelation + startCardinality;

		ByteString[] newAtom = head.clone();
		long cardinality = 0;
		// First rule
		newAtom[1] = ByteString.of(inequalityRelation);
		rule.getTriples().add(newAtom);
		cardinality = kb.countDistinct(rule.getFunctionalVariable(), rule.getTriples());
		rule.getTriples().remove(rule.getTriples().size() - 1);			
		if (cardinality >= minSupportThreshold) {
			Rule candidate = rule.addAtom(newAtom, cardinality);
			output.add(candidate);
		}
		
		// Second rule
		if (head[1].equals(isCompleteBS)) {
			newAtom[1] = ByteString.of(equalityRelation);
			rule.getTriples().add(newAtom);
			cardinality = kb.countDistinct(rule.getFunctionalVariable(), rule.getTriples());
			rule.getTriples().remove(rule.getTriples().size() - 1);			
			if (cardinality >= minSupportThreshold) {
				Rule candidate = rule.addAtom(newAtom, cardinality);
				output.add(candidate);
			}
		}
	}
	
	@Override
	public void getInstantiatedAtoms(Rule parentRule, double minSupportThreshold, 
		Collection<Rule> danglingEdges, Collection<Rule> output) {
		
		if (!containsAtomWithRelation(parentRule)) {
			ByteString[] relevanceAtom = new ByteString[]{parentRule.getFunctionalVariable(), 
					isRelevantBS, ByteString.of("TRUE")};
			
			parentRule.getTriples().add(relevanceAtom);
			long support = kb.countDistinct(relevanceAtom[0], parentRule.getTriples());
			parentRule.getTriples().remove(parentRule.getTriples().size() - 1);
			if (support > minSupportThreshold) {
				Rule candidate = parentRule.addAtom(relevanceAtom, support);
				candidate.addParent(parentRule);
				output.add(candidate);
			}
		}
		
		if (!containsCardinalityAtom(parentRule)) {
			addCardinalityAtom(parentRule, minSupportThreshold, output);
		}
	}
	
	@Override
	protected boolean testLength(Rule candidate) {
		int length = maxDepth;
		if (candidate.containsRelation(amie.data.U.typeRelationBS)) ++length;
		if (candidate.containsRelation(isRelevantBS)) ++length;
		if (containsCardinalityAtom(candidate)) ++length;
		
		return candidate.getRealLength() <= length;
	}
	
	
	private boolean containsAtomWithRelation(Rule parentQuery) {
		for (ByteString[] atom : parentQuery.getTriples()) {
			if (atom[1].equals(isRelevantBS)) {
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Returns true if the rule contains a cardinality constraint 
	 * atom.
	 * @param rule
	 * @return
	 */
	private boolean containsCardinalityAtom(Rule rule) {
		return indexOfCardinalityAtom(rule) != -1;
	}
	
	@Override
	public void setAdditionalParents(Rule currentRule, MultiMap<Integer, Rule> indexedOutputSet) {
		int idxOfRelationAtom = currentRule.firstIndexOfRelation(amie.data.U.typeRelationBS);
		int idxOfCardinalityRelation = indexOfCardinalityAtom(currentRule);
		if (idxOfRelationAtom == -1 && idxOfCardinalityRelation == -1)
			super.setAdditionalParents(currentRule, indexedOutputSet);
		
        int parentHashCode = currentRule.alternativeParentHashCode();
        Set<Rule> candidateParents = indexedOutputSet.get(parentHashCode);
        
        // First check if there are no a parents of the same size caused by
        // specialization operators. For example if the current rule is
        // A: livesIn(x, Paris), type(x, Architect) => isFamous(x, true) derived from 
        // B: type(x, Architect) => isFamous(x, true) but in other thread we have mined the rule
        // C: livesIn(x, Paris) (x, Person) => isFamous(x, true), then we need to make the bridge between
        // A and C (namely C is also a father of A)
       
        
    }
	
	/**private List<Rule> findSpecializationParentsOfSameSize(Rule currentRule, Set<Rule> candidateParents) {
		int idxOfRelationAtom = currentRule.firstIndexOfRelation(amie.data.U.typeRelationBS);
		int idxOfCardinalityRelation = indexOfCardinalityAtom(currentRule);
        List<Rule> parents = new ArrayList<>();
		List<ByteString[]> currentRuleTriples = new ArrayList<>();
        Collections.copy(currentRuleTriples, currentRule.getTriples());
        currentRuleTriples.remove(idxOfRelationAtom);
        ByteString[] typeAtomInCurrentRule = currentRuleTriples.get(idxOfRelationAtom);
        ByteString typeInCurrentRule = typeAtomInCurrentRule[2];
        
        for (Rule candidateParent : candidateParents) {
        	if (candidateParent.getLength() != currentRule.getLength())
        		continue;
        	
    		List<ByteString[]> candidateParentTriples = new ArrayList<>(); 
    		Collections.copy(candidateParentTriples, 
    				candidateParent.getTriples());
    		int indexOfTypeAtomInCandidate = candidateParent.firstIndexOfRelation(amie.data.U.typeRelationBS);
    		if (indexOfTypeAtomInCandidate == -1)
    			continue;
    		
    		ByteString[] typeAtomInCandidate = candidateParentTriples.get(indexOfTypeAtomInCandidate);
    		ByteString typeInCandidate = typeAtomInCandidate[2];
    		candidateParentTriples.remove(indexOfTypeAtomInCandidate);
    		if (QueryEquivalenceChecker.areEquivalent(candidateParentTriples, currentRuleTriples) &&
    				(amie.data.U.isSuperType(kb, typeInCandidate, typeInCurrentRule) && )) {
    			parents.add(candidateParent);
    		}
    	}
        
        return parents;
	}**/

	private int indexOfCardinalityAtom(Rule rule) {
		List<ByteString[]> triples = rule.getTriples();
		for (int i = triples.size() - 1; i >= 0; --i) {
			if (KB.parseCardinalityRelation(triples.get(i)[1]) != null)
				return i;
		}
		
		return -1;
	}

	@Override
	public double computePCAConfidence(Rule rule) {
		List<ByteString[]> antecedent = new ArrayList<ByteString[]>();
		antecedent.addAll(rule.getTriples().subList(1, rule.getTriples().size()));
		ByteString[] succedent = rule.getTriples().get(0);
		ByteString[] negativeTriple = succedent.clone();
		negativeTriple[1] = succedent[1].equals(isCompleteBS) ? isIncompleteBS : isCompleteBS;
		antecedent.add(negativeTriple);
		long counterEvidence = kb.countDistinct(rule.getFunctionalVariable(), antecedent);
		double support = rule.getSupport();
		rule.setPcaBodySize(support + counterEvidence);
		return rule.getPcaConfidence();
	}
}