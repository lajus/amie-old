package amie.mining.assistant.experimental;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import amie.data.KB;
import amie.mining.assistant.MiningAssistant;
import amie.rules.Rule;
import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Pair;

public class CompletenessMiningAssistant extends MiningAssistant {

	public static final String isComplete = "isComplete";
	
	public static final String isIncomplete = "isIncomplete";
	
	public static final ByteString isCompleteBS = ByteString.of(isComplete);
	
	public static final ByteString isIncompleteBS = ByteString.of(isIncomplete);

	
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
	protected void buildInitialQueries(IntHashMap<ByteString> relations, double minSupportThreshold, Collection<Rule> output) {
		Rule query = new Rule();
		ByteString[] newEdge = query.fullyUnboundTriplePattern();
		
		for (ByteString relation: relations) {		
			if (relation.equals(isCompleteBS) ||
					relation.equals(isIncompleteBS)) {
				registerHeadRelation(relation, (double) relations.get(relation));
				continue;
			}
			ByteString[] succedentComplete = newEdge.clone();
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
				queryArray.add(KB.triple(succedentComplete[0], amie.data.U.typeRelationBS, domain));
						
			succedentComplete[1]  = isCompleteBS;
			succedentComplete[2] = relation;
			queryArray.add(succedentComplete);
			int countVarPos = 0;
			long cardinalityComplete = kb.countDistinct(succedentComplete[0], queryArray);
			
			if (cardinalityComplete >= minSupportThreshold) {
				Rule candidateComplete = null;
				if (domain == null) {
					candidateComplete = new Rule(succedentComplete, cardinalityComplete);				
				} else {
					queryArray.remove(1);
					candidateComplete = new Rule(succedentComplete, queryArray, cardinalityComplete);
				}
				candidateComplete.setFunctionalVariablePosition(countVarPos);
				output.add(candidateComplete);
			}
		}
	}
	
	@Override
	public void getDanglingAtoms(Rule rule, double minSupportThreshold, java.util.Collection<Rule> output) {
		if (rule.getLength() < 3 && !containsCardinalityAtom(rule)) {
			// We'll force a cardinality atom at the end
			ByteString[] head = rule.getHead();
			ByteString targetRelation = head[2];
			int maxCardinality;
			String cardinalityRelation = null;
			String gtRelation = null;
			if (this.kb.isFunctional(head[2])) {
				cardinalityRelation = KB.hasNumberOfValuesEquals;
				gtRelation = KB.hasNumberOfValuesGreaterThan;
			} else {
				maxCardinality = kb.maximalCardinalityInv(targetRelation, 
						(int)minSupportThreshold);
				cardinalityRelation = KB.hasNumberOfValuesEqualsInv;
				gtRelation = KB.hasNumberOfValuesGreaterThanInv;
			}
			
			maxCardinality = kb.maximalCardinality(targetRelation, 
					(int)minSupportThreshold);
			if (maxCardinality == -1) {
				// Proceed as usual
				super.getDanglingAtoms(rule, minSupportThreshold, output);
			} else {
				gtRelation = gtRelation + maxCardinality;
			}

			ByteString[] newAtom = head.clone();
			
			// First rule
			newAtom[1] = ByteString.of(gtRelation);
			rule.getTriples().add(newAtom);
			long cardinality = kb.countDistinct(rule.getFunctionalVariable(), rule.getTriples());
			rule.getTriples().remove(rule.getTriples().size() - 1);			
			if (cardinality >= minSupportThreshold) {
				Rule candidate = rule.addAtom(newAtom, cardinality);
				output.add(candidate);
			}
			
			// Second rule
			cardinalityRelation = cardinalityRelation + "0";
			newAtom[1] = ByteString.of(cardinalityRelation);
			rule.getTriples().add(newAtom);
			cardinality = kb.countDistinct(rule.getFunctionalVariable(), rule.getTriples());
			rule.getTriples().remove(rule.getTriples().size() - 1);			
			if (cardinality >= minSupportThreshold) {
				Rule candidate = rule.addAtom(newAtom, cardinality);
				output.add(candidate);
			}
		} else {
			super.getDanglingAtoms(rule, minSupportThreshold, output);
		}
	}
	
	/**
	 * Returns true if the rule contains a cardinality constraint 
	 * atom.
	 * @param rule
	 * @return
	 */
	private boolean containsCardinalityAtom(Rule rule) {
		List<ByteString[]> triples = rule.getTriples();
		for (int i = triples.size() - 1; i >= 0; --i) {
			if (KB.parseCardinalityRelation(triples.get(i)[1]) != null)
				return true;
		}
		
		return false;
	}

	@Override
	public void getTypeSpecializedAtoms(Rule rule, double minSupportThreshold, Collection<Rule> output) {
		if (rule.getRealLength() >= maxDepth) {
			return;
		}
			
		ByteString[] lastAtom = rule.getLastRealTriplePattern();
		Pair<ByteString, Integer> compositeRelation = KB.parseCardinalityRelation(lastAtom[1]);
		if (compositeRelation == null) {
			super.getTypeSpecializedAtoms(rule, minSupportThreshold, output);
		} else {
			ByteString oldRelation = lastAtom[1];
			int newCard = compositeRelation.second.intValue() - 1;
			ByteString newRelation = null;
			if (oldRelation.toString().startsWith(KB.hasNumberOfValuesEquals)) {
				return;
			} else {
				if (newCard >= 0) {
					newRelation = ByteString.of(compositeRelation.first.toString() + newCard);
				} else {
					return;
				}
			}
			lastAtom[1] = newRelation;
			long cardinality = kb.countDistinct(rule.getFunctionalVariable(), rule.getTriples());
			lastAtom[1] = oldRelation;
			if (cardinality >= minSupportThreshold) {
				ByteString[] newAtom = lastAtom.clone();
				newAtom[1] = ByteString.of(compositeRelation.first.toString() 
						+ (compositeRelation.second.intValue() + 1));				
				Rule candidate = rule.replaceLastAtom(newAtom, cardinality);
				candidate.setParent(rule);
				output.add(candidate);
			}
		}
	}
	
	
	@Override
	protected void getInstantiatedAtoms(Rule queryWithDanglingEdge, Rule parentQuery, 
			int danglingAtomPosition, int danglingPositionInEdge, double minSupportThreshold, Collection<Rule> output) {
		ByteString lastAtom[] = queryWithDanglingEdge.getLastRealTriplePattern();
		// We leave the specialization of the type relation to the type instantiation atom
		if (lastAtom[1].equals(amie.data.U.typeRelationBS)) 
			return;
		
		super.getInstantiatedAtoms(queryWithDanglingEdge, 
				parentQuery, danglingAtomPosition, danglingPositionInEdge, 
				minSupportThreshold, output);
	}
	
	@Override
	public double computePCAConfidence(Rule rule) {
		List<ByteString[]> antecedent = new ArrayList<ByteString[]>();
		antecedent.addAll(rule.getTriples().subList(1, rule.getTriples().size()));
		ByteString[] succedent = rule.getTriples().get(0);
		ByteString[] negativeTriple = succedent.clone();
		negativeTriple[1] = isIncompleteBS;
		antecedent.add(negativeTriple);
		long counterEvidence = kb.countDistinct(rule.getFunctionalVariable(), antecedent);
		double support = rule.getSupport();
		rule.setPcaBodySize(support + counterEvidence);
		return rule.getPcaConfidence();
	}
}