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
	public void getInstantiatedAtoms(Rule rule, double minSupportThreshold, 
			Collection<Rule> danglingEdges, Collection<Rule> output) {
		List<Rule> rulestoAlwaysSpecialize = new ArrayList<>();
		for (Rule r : danglingEdges) {
			ByteString[] head = rule.getHead();
			ByteString completenessRel = head[2];
			ByteString[] lastAtom = r.getLastRealTriplePattern();
			String relationStr = lastAtom[1].toString();
			if (relationStr.startsWith("hasNumberOfValues")) {
				rulestoAlwaysSpecialize.add(r);
			}
			
			ByteString originalVar = lastAtom[2];
			lastAtom[2] = completenessRel;
			long cardinality = kb.countDistinct(r.getFunctionalVariable(), r.getTriples());
			lastAtom[2] = originalVar;
			if (cardinality < minSupportThreshold)
				continue;
			
			Rule candidate = r.instantiateConstant(2, completenessRel, cardinality);
			output.add(candidate);			
		}		
		danglingEdges.removeAll(rulestoAlwaysSpecialize);
		super.getInstantiatedAtoms(rule, minSupportThreshold, danglingEdges, output);

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