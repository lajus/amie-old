package amie.mining.assistant.experimental;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import amie.data.KB;
import amie.mining.ConfidenceMetric;
import amie.mining.assistant.MiningAssistant;
import amie.rules.Rule;
import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;

public class StarShapedMiningAssistant extends MiningAssistant {

	private CompletenessMiningAssistant wrapped;
	
	public StarShapedMiningAssistant(KB dataSource) {
		super(dataSource);
		wrapped = new CompletenessMiningAssistant(dataSource);
		this.maxDepth = 1;
		this.recursivityLimit = 1;
		this.confidenceMetric = ConfidenceMetric.StandardConfidence;
	}
	
	@Override
	public String getDescription() {
        return "Mining star completeness rules of the form "
        		+ "B => isComplete(x, relation) or B => isIncomplete(x, relation) "
        		+ "where B  = r1(x, y1) ^ r2(x, y2) ... [EXPERIMENTAL]";
	}
	
	@Override
	protected void buildInitialQueries(IntHashMap<ByteString> relations, 
			double minSupportThreshold, Collection<Rule> output) {
		wrapped.buildInitialQueries(relations, minSupportThreshold, output);
	}
	
	@Override
	public void setRecursivityLimit(int recursitivityLimit) {}
	
	@Override
	public void getClosingAtoms(Rule rule, double minSupportThreshold, Collection<Rule> output) {}
	
	@Override
	public void getDanglingAtoms(Rule rule, double minSupportThreshold, Collection<Rule> output) {
		ByteString[] newEdge = rule.fullyUnboundTriplePattern();
		ByteString[] head = rule.getHead();
		//General case
		if(!isNotTooLong(rule))
			return;

		int nPatterns = rule.getTriples().size();		
		newEdge[0] = head[0];
		rule.getTriples().add(newEdge);
		IntHashMap<ByteString> promisingRelations = kb.frequentBindingsOf(newEdge[1], 
				rule.getFunctionalVariable(), rule.getTriples());
		rule.getTriples().remove(nPatterns);
		
		for(ByteString relation: promisingRelations){
			if (this.bodyExcludedRelations != null && 
					this.bodyExcludedRelations.contains(relation))
				continue;
			//Here we still have to make a redundancy check						
			int cardinality = promisingRelations.get(relation);
			if(cardinality >= minSupportThreshold) {
				newEdge[1] = relation;
				Rule candidate = rule.addAtom(newEdge, cardinality);
				if(rule.containsRelation(relation) 
						|| relation.equals(head[2])) {
					continue;
				}
				
				candidate.setHeadCoverage((double)candidate.getSupport() 
						/ headCardinalities.get(candidate.getHeadRelation()));
				candidate.setSupportRatio((double)candidate.getSupport() 
						/ (double)getTotalCount(candidate));
				candidate.addParent(rule);
				output.add(candidate);
			}
		}
	}
	
	@Override
	public void getTypeSpecializedAtoms(Rule rule, double minSupportThreshold, Collection<Rule> output) {}
	
	@Override
	public boolean shouldBeOutput(Rule candidate) {
		return candidate.getRealLength() > 1;
	}
	@Override
	public double computePCAConfidence(Rule rule) {
		return 0.0;
	}

}
