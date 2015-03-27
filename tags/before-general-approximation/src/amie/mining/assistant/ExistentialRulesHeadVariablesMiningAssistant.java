package amie.mining.assistant;

import java.util.ArrayList;
import java.util.List;

import javatools.datatypes.ByteString;
import amie.data.FactDatabase;
import amie.query.Query;

public class ExistentialRulesHeadVariablesMiningAssistant extends
		HeadVariablesMiningAssistant {

	public ExistentialRulesHeadVariablesMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
		// TODO Auto-generated constructor stub
	}
	
	public void calculateConfidenceMetrics(Query candidate) {
		// TODO Auto-generated method stub
		List<ByteString[]> antecedent = new ArrayList<ByteString[]>();
		antecedent.addAll(candidate.getAntecedent());
		List<ByteString[]> succedent = new ArrayList<ByteString[]>();
		succedent.addAll(candidate.getTriples().subList(0, 1));
		double improvedDenominator = 0.0;
		double denominator = 0.0;
		double confidence = 0.0;
		double improvedConfidence = 0.0;
		ByteString[] head = candidate.getHead();
		ByteString[] existentialTriple = head.clone();
		int freeVarPos, countVarPos;
				
		if(antecedent.isEmpty()){
			candidate.setStdConfidence(1.0);
			candidate.setPcaConfidence(1.0);
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
				confidence = (double)candidate.getSupport() / denominator;
				candidate.setStdConfidence(confidence);
				candidate.setBodySize((long)denominator);
			}catch(UnsupportedOperationException e){
				
			}
			
			// In this case, still report the PCA.
			if (candidate.isClosed()) {				
				countVarPos = candidate.getFunctionalVariablePosition();
				if(FactDatabase.numVariables(existentialTriple) == 1){
					freeVarPos = FactDatabase.firstVariablePos(existentialTriple) == 0 ? 2 : 0;
				}else{
					freeVarPos = existentialTriple[0].equals(candidate.getFunctionalVariable()) ? 2 : 0;
				}
				existentialTriple[freeVarPos] = ByteString.of("?x");
				
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
						improvedDenominator = denominator;
					}else{
						if(FactDatabase.numVariables(head) == 2){
							ByteString var1, var2;
							var1 = head[FactDatabase.firstVariablePos(head)];
							var2 = head[FactDatabase.secondVariablePos(head)];
							improvedDenominator = (double)computePCAAntecedentCount(var1, var2, candidate, antecedent, existentialTriple, candidate.getFunctionalVariablePosition());
						}else{
							antecedent.add(existentialTriple);
							improvedDenominator = (double)source.countDistinct(candidate.getFunctionalVariable(), antecedent);
						}
						
						improvedConfidence = (double)candidate.getSupport() / improvedDenominator;					
					}
					
					candidate.setPcaConfidence(improvedConfidence);	
					candidate.setBodyStarSize((long)improvedDenominator);
				}catch(UnsupportedOperationException e){
					
				}
			}
		}
	}
	
	public boolean testConfidenceThresholds(Query candidate) {
		boolean addIt = true;
		
		if (candidate.getLength() == 1) {
			return false;
		}
		
		if (candidate.containsLevel2RedundantSubgraphs()) {
			return false;
		}
		
		calculateConfidenceMetrics(candidate);
		
		if (candidate.getStdConfidence() >= minStdConfidence && candidate.getPcaConfidence() >= minPcaConfidence) {
			//Now check the confidence with respect to its ancestors
			List<Query> ancestors = candidate.getAncestors();			
			for (int i = ancestors.size() - 2; i >= 0; --i) {
				if (ancestors.get(i).isClosed() 
						&& 
						(candidate.getStdConfidence() <= ancestors.get(i).getStdConfidence() || candidate.getPcaConfidence() <= ancestors.get(i).getPcaConfidence())) {
					addIt = false;
					break;
				}
			}
		}else{
			return false;
		}
		
		return addIt;
	}
}
