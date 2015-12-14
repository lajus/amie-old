package amie.mining.assistant.experimental;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import amie.data.KB;
import amie.mining.assistant.MiningAssistant;
import amie.rules.Rule;

public class CompletenessMiningAssistant extends MiningAssistant {

	public static final String isComplete = "isComplete";
	
	public static final String isIncomplete = "isIncomplete";
	
	public static final ByteString isCompleteBS = ByteString.of(isComplete);
	
	public static final ByteString isIncompleteBS = ByteString.of(isCompleteBS);

	
	public CompletenessMiningAssistant(KB dataSource) {
		super(dataSource);
		this.allowConstants = false;
	}

	@Override
	public String getDescription() {
        return "Mining completeness rules of the form "
        		+ "B => isComplete(x, relation) or B => isIncomplete(x, relation) "
        		+ "[EXPERIMENTAL]";
	}
	
	@Override
	public void setHeadExcludedRelations(java.util.Collection<ByteString> headExcludedRelations) {};

	
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
			
			succedentComplete[1]  = isCompleteBS;
			succedentComplete[2] = relation;
			int countVarPos = 0;
			long cardinalityComplete = kb.count(succedentComplete);
			
			if (cardinalityComplete >= minSupportThreshold) {			
				Rule candidateComplete = new Rule(succedentComplete, cardinalityComplete);				
				candidateComplete.setFunctionalVariablePosition(countVarPos);
				output.add(candidateComplete);
			}
		}
	}
	

	public void getTypeSpecializedAtoms(Rule rule, double minSupportThreshold, Collection<Rule> output) {
		ByteString[] lastAtom = rule.getLastRealTriplePattern();
		ByteString[] head = rule.getHead();
		if (!lastAtom[1].equals(amie.data.U.typeRelationBS))
			return;
		
		Set<ByteString> subtypes = null;
		if (KB.isVariable(lastAtom[2])) {
			ByteString targetRelation = head[2];
			subtypes = new LinkedHashSet<>();
			ByteString domain = null;
			if (kb.isFunctional(targetRelation)) {
				domain = amie.data.U.getRelationDomain(kbSchema, head[2]);
			} else {
				domain = amie.data.U.getRelationRange(kbSchema, head[2]);
			}
			if (domain != null) {
				subtypes.add(domain);
			}
		} else {
			ByteString typeToSpecialize = lastAtom[2];			
			subtypes = amie.data.U.getSubtypes(this.kbSchema, typeToSpecialize);
		}		

		ByteString originalValue = lastAtom[2];
		for (ByteString subtype : subtypes) {
			lastAtom[2] = subtype;
			long support = kb.countDistinct(rule.getFunctionalVariable(), rule.getTriples());
			lastAtom[2] = originalValue;
			if (support >= minSupportThreshold) {
				Rule newRule = rule.specializeTypeAtom(subtype, support);
				newRule.setParent(rule);
				output.add(newRule);
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
		
		super.getInstantiatedAtoms(queryWithDanglingEdge, parentQuery, danglingAtomPosition, danglingPositionInEdge, minSupportThreshold, output);
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