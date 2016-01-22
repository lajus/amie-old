package amie.mining.assistant.experimental;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
	
	public static final String isRelevanthasWikiLength = "<isRelevanthasWikiLength>";
	
	public static final String isRelevanthasIngoingLinks = "<isRelevanthasIngoingLinks>";
	
	public static final String hasChanged = "<hasChanged>";
	
	public static final String hasNotChanged = "<hasNotChanged>";
	
	public static final ByteString isRelevanthasWikiLengthBS = ByteString.of(isRelevanthasWikiLength); 
	
	public static final ByteString isRelevanthasIngoingLinksBS = ByteString.of(isRelevanthasIngoingLinks); 
	
	public static final ByteString hasChangedBS = ByteString.of(hasChanged);
	
	public static final ByteString hasNotChangedBS = ByteString.of(hasNotChanged);
	
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
			succedent[1]  = isCompleteBS;
			succedent[2] = relation;
			int countVarPos = 0;
			long cardinalityComplete = kb.count(succedent);
			
			if (cardinalityComplete >= minSupportThreshold) {
				Rule candidateComplete =  new Rule(succedent, cardinalityComplete);
				candidateComplete.setFunctionalVariablePosition(countVarPos);
				output.add(candidateComplete);
			}
						
			succedent[1] = isIncompleteBS;
			long cardinalityIncomplete = kb.count(succedent);
			if (cardinalityIncomplete >= minSupportThreshold) {
				Rule candidateIncomplete = new Rule(succedent, cardinalityIncomplete);
				candidateIncomplete.setFunctionalVariablePosition(countVarPos);
				output.add(candidateIncomplete);
			}
		}
	}
	

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
				int maximalCardinality = -1;
				if (kb.isFunctional(targetRelation)) {
					maximalCardinality = kb.maximalRightCumulativeCardinality(targetRelation, 
						(long)minSupportThreshold);
				} else {
					maximalCardinality = kb.maximalRightCumulativeCardinalityInv(targetRelation, 
							(long)minSupportThreshold);					
				}
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
			if (kb.isFunctional(targetRelation)) {
				startCardinality = kb.maximalCardinality(targetRelation);
			} else {
				startCardinality = kb.maximalCardinalityInv(targetRelation);
			}
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
		
		if (!parentRule.containsRelation(amie.data.U.typeRelationBS)) {
			addTypeAtom(parentRule, minSupportThreshold, output);
		}
		
		if (!parentRule.containsRelation(isRelevantBS)) {
			addRelevanceAtom(parentRule, isRelevantBS, minSupportThreshold, output);
		}
		
		if (!parentRule.containsRelation(isRelevanthasWikiLengthBS)) {
			addRelevanceAtom(parentRule, isRelevanthasWikiLengthBS, minSupportThreshold, output);
		}
		
		if (!parentRule.containsRelation(isRelevanthasIngoingLinksBS)) {
			addRelevanceAtom(parentRule, isRelevanthasIngoingLinksBS, minSupportThreshold, output);
		}
		
		if (!parentRule.containsRelation(hasNotChangedBS)) {
			addChangedAtom(parentRule, hasNotChangedBS, minSupportThreshold, output);
		}
		
		if (!containsCardinalityAtom(parentRule)) {
			addCardinalityAtom(parentRule, minSupportThreshold, output);
		}
	}
	
	private void addTypeAtom(Rule parentRule, double minSupportThreshold, Collection<Rule> output) {
		ByteString[] head = parentRule.getHead();
		ByteString relation = head[2];
		ByteString[] newEdge = head.clone();
		newEdge[1] = amie.data.U.typeRelationBS;
		ByteString domain = null;
		if (this.kbSchema != null) {
			if (this.kb.isFunctional(relation)) {
				domain = amie.data.U.getRelationDomain(this.kbSchema, relation);
			} else {
				domain = amie.data.U.getRelationRange(this.kbSchema, relation);
			}
		}
		
		if (domain == null)
			return;
		
		newEdge[2] = domain;
		
		parentRule.getTriples().add(newEdge);
		long cardinalityComplete = kb.countDistinct(head[0], parentRule.getTriples());
		parentRule.getTriples().remove(parentRule.getTriples().size() - 1);
		
		if (cardinalityComplete >= minSupportThreshold) {
			Rule candidateComplete = parentRule.addAtom(newEdge, cardinalityComplete);
			candidateComplete.addParent(parentRule);
			output.add(candidateComplete);
		}
	}

	private void addChangedAtom(Rule parentRule, ByteString changeRelation, double minSupportThreshold,
			Collection<Rule> output) {
		ByteString[] head = parentRule.getHead();
		ByteString targetRelation = head[2];
		
		ByteString[] changedAtom = new ByteString[]{parentRule.getFunctionalVariable(), 
				changeRelation, targetRelation};
		
		parentRule.getTriples().add(changedAtom);
		long support = kb.countDistinct(changedAtom[0], parentRule.getTriples());
		parentRule.getTriples().remove(parentRule.getTriples().size() - 1);
		if (support > minSupportThreshold) {
			Rule candidate = parentRule.addAtom(changedAtom, support);
			candidate.addParent(parentRule);
			output.add(candidate);
		}
		
	}

	private void addRelevanceAtom(Rule parentRule, ByteString relevanceRelation, double minSupportThreshold, Collection<Rule> output) {
		ByteString[] relevanceAtom = new ByteString[]{parentRule.getFunctionalVariable(), 
				relevanceRelation, ByteString.of("TRUE")};
		
		parentRule.getTriples().add(relevanceAtom);
		long support = kb.countDistinct(relevanceAtom[0], parentRule.getTriples());
		parentRule.getTriples().remove(parentRule.getTriples().size() - 1);
		if (support > minSupportThreshold) {
			Rule candidate = parentRule.addAtom(relevanceAtom, support);
			candidate.addParent(parentRule);
			output.add(candidate);
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
        
        // First check if there are no a parents of the same size caused by
        // specialization operators. For example if the current rule is
        // A: livesIn(x, Paris), type(x, Architect) => isFamous(x, true) derived from 
        // B: type(x, Architect) => isFamous(x, true) but in other thread we have mined the rule
        // C: livesIn(x, Paris) (x, Person) => isFamous(x, true), then we need to make the bridge between
        // A and C (namely C is also a father of A)
        int offset = 0;
        List<ByteString[]> queryPattern = currentRule.getTriples();
        // Go up until you find a parent that was output
        while (queryPattern.size() - offset > 1) {
        	int currentLength = queryPattern.size() - offset;
            int parentHashCode = Rule.headAndLengthHashCode(currentRule.getHeadKey(), currentLength);
            // Give all the rules of size 'currentLength' and the same head atom (potential parents)
            Set<Rule> candidateParentsOfCurrentLength = indexedOutputSet.get(parentHashCode);
            
            if (candidateParentsOfCurrentLength != null) {
	            for (Rule parent : candidateParentsOfCurrentLength) {
	                if (parent.subsumes(currentRule) 
	                		|| subsumesWithSpecialAtoms(parent, currentRule)) {
	                	currentRule.addParent(parent);	                	
	                }
	            }
        	}
            ++offset;
        }  
    }
	
	private boolean subsumesWithSpecialAtoms(Rule parent, Rule currentRule) {
		int idxOfTypeAtomRule = currentRule.firstIndexOfRelation(amie.data.U.typeRelationBS);
		int idxOfCardinalityAtomRule = indexOfCardinalityAtom(currentRule);
		int idxOfTypeAtomParent = parent.firstIndexOfRelation(amie.data.U.typeRelationBS);
		int idxOfCardinalityAtomParent = indexOfCardinalityAtom(parent);
				
		if (idxOfTypeAtomRule != -1 && idxOfTypeAtomParent != -1) {
			ByteString[] typeAtomInRule = currentRule.getTriples().get(idxOfTypeAtomRule);
			ByteString[] typeAtomInParent = currentRule.getTriples().get(idxOfTypeAtomParent);
			List<ByteString[]> triplesParent = parent.getTriplesCopy();
			List<ByteString[]> triplesRule = currentRule.getTriplesCopy();
			triplesParent.remove(idxOfTypeAtomParent);
			triplesRule.remove(idxOfTypeAtomRule);
			Rule newRule = new Rule(currentRule.getHead(), triplesRule.subList(1, triplesRule.size()), 0);
			Rule newParent = new Rule(parent.getHead(), triplesParent.subList(1, triplesParent.size()), 0);
			if (typeAtomInParent[2].equals(typeAtomInRule[2]) || 
					amie.data.U.isSuperType(this.kbSchema, typeAtomInParent[2], typeAtomInRule[2])) {
				return subsumesWithSpecialAtoms(newParent, newRule);
			}
		}
		
		if (idxOfCardinalityAtomRule != -1 && idxOfCardinalityAtomParent != -1) {
			ByteString[] cardinalityAtomInRule = currentRule.getTriples().get(idxOfCardinalityAtomRule);
			ByteString[] cardinalityAtomInParent = parent.getTriples().get(idxOfCardinalityAtomParent);
			List<ByteString[]> triplesParent = parent.getTriplesCopy();
			List<ByteString[]> triplesRule = currentRule.getTriplesCopy();
			triplesParent.remove(idxOfCardinalityAtomParent);
			triplesRule.remove(idxOfCardinalityAtomRule);
			Rule newRule = new Rule(currentRule.getHead(), triplesRule.subList(1, triplesRule.size()), 0);
			Rule newParent = new Rule(parent.getHead(), triplesParent.subList(1, triplesParent.size()), 0);
			if (Arrays.equals(cardinalityAtomInParent, cardinalityAtomInRule) || 
					subsumesCardinalityAtom(cardinalityAtomInParent, cardinalityAtomInRule)) {
				return subsumesWithSpecialAtoms(newParent, newRule);
			}
		}
		
		return QueryEquivalenceChecker.areEquivalent(parent.getTriples(), currentRule.getTriples())
				|| parent.subsumes(currentRule);
	}

	private boolean subsumesCardinalityAtom(ByteString[] triplesParent, ByteString[] triplesRule) {
		Pair<ByteString, Integer> relationPairParent = KB.parseCardinalityRelation(triplesParent[1]);
		Pair<ByteString, Integer> relationPairRule = KB.parseCardinalityRelation(triplesRule[1]);
		
		List<ByteString> gtList = Arrays.asList(KB.hasNumberOfValuesGreaterThanBS, 
				KB.hasNumberOfValuesGreaterThanInvBS);
		
		List<ByteString> stList = Arrays.asList(KB.hasNumberOfValuesSmallerThanBS, 
				KB.hasNumberOfValuesSmallerThanInvBS);
		
		if (relationPairParent != null && relationPairRule != null) {
			if (relationPairParent.first.equals(relationPairRule.first)) {
				if (gtList.contains(relationPairParent.first)) {
					return relationPairParent.second < relationPairRule.second;
				} else if (stList.contains(relationPairParent.first)) {
					return relationPairParent.second > relationPairRule.second;
				}					
			} else {
				if (relationPairRule.first.equals(KB.hasNumberOfValuesGreaterThanBS) 
						&& relationPairRule.second == 0) {
					return relationPairParent.second == 0 && 
							relationPairParent.equals(KB.hasNumberOfValuesEqualsBS);
				} else if (relationPairRule.first.equals(KB.hasNumberOfValuesGreaterThanInvBS) 
						&& relationPairRule.second == 0) {
					return relationPairParent.second == 0 && 
							relationPairParent.equals(KB.hasNumberOfValuesEqualsInvBS);
				}
			}
		}
		
		return false;
	}

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
	
	public static void main(String args[]) {
		CompletenessMiningAssistant assistant = new CompletenessMiningAssistant(new KB());
		List<ByteString[]> ruleTriples = KB.triples(
				KB.triple("?a", KB.hasNumberOfValuesSmallerThanInv + "1", "hasChild"), 
				KB.triple("?l", "marriedTo", "?a"), KB.triple("?a", "marriedTo", "?l"));
		ByteString[] head = KB.triple("?a", isIncomplete, "hasChild");
		List<ByteString[]> parentTriples = KB.triples(
				KB.triple("?a", KB.hasNumberOfValuesSmallerThanInv + "2", "hasChild"));
		Rule parent = new Rule(head, parentTriples, 1);
		Rule currentRule = new Rule(head, ruleTriples, 1);
		System.out.println(parent.subsumes(currentRule));
		System.out.println(assistant.subsumesWithSpecialAtoms(parent, currentRule));
	}
}