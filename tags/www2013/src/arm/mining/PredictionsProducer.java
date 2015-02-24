package arm.mining;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Triple;
import arm.data.FactDatabase;
import arm.query.Query;

public class PredictionsProducer {
	
	private int numberOfPredictions;
	
	private FactDatabase training;
	
	public PredictionsProducer(FactDatabase training) {
		super();
		this.training = training;
		numberOfPredictions = 30;
	}
	
	public PredictionsProducer(FactDatabase training, int sampleSize) {
		super();
		this.training = training;
		numberOfPredictions = sampleSize;
	}
	
	/**
	 * @return the numberOfPredictions
	 */
	public int getNumberOfPredictions() {
		return numberOfPredictions;
	}

	/**
	 * @param numberOfPredictions the numberOfPredictions to set
	 */
	public void setNumberOfPredictions(int numberOfPredictions) {
		this.numberOfPredictions = numberOfPredictions;
	}
	
	public Object generatePredictions(Query rule, FactDatabase source){		
		if(FactDatabase.numVariables(rule.getHead()) == 1)
			return predictBindingsForSingleVariable(rule, source);
		else
			return predictBindingsForTwoVariables(rule, source);
	}
	
	public Object generateBodyBindings(Query rule, FactDatabase source){
		if(FactDatabase.numVariables(rule.getHead()) == 1)
			return generateBindingsForSingleVariable(rule, source);
		else
			return generateBindingsForTwoVariables(rule, source);		
	}

	private Object generateBindingsForTwoVariables(Query rule, FactDatabase source) {
		//First get the bindings for the projection variable in the antecedent
		ByteString[] head = rule.getHead();		
		ByteString[] vars = new ByteString[2];
		vars[0] = rule.getProjectionVariable(); 
		vars[1] = head[FactDatabase.secondVariablePos(head)];
		if(vars[1].equals(vars[0])){
			vars[1] = head[FactDatabase.firstVariablePos(head)];
		}
			
		return source.selectDistinct(vars[0], vars[1], rule.getAntecedent());

	}

	private Object generateBindingsForSingleVariable(Query rule, FactDatabase source) {
		return source.selectDistinct(rule.getProjectionVariable(), rule.getAntecedent());
	}

	private Map<ByteString, IntHashMap<ByteString>> predictBindingsForTwoVariables(Query rule, FactDatabase source) {
		//First get the bindings for the projection variable in the antecedent
		ByteString[] head = rule.getHead();		
		ByteString[] vars = new ByteString[2];
		vars[0] = rule.getProjectionVariable(); 
		vars[1] = head[FactDatabase.secondVariablePos(head)];
		if(vars[1].equals(vars[0])){
			vars[1] = head[FactDatabase.firstVariablePos(head)];
		}
			
		return source.difference(vars[0], vars[1], rule.getAntecedent(), rule.getTriples());
	}
	
	
	private Set<ByteString> predictBindingsForSingleVariable(Query rule, FactDatabase source) {
		//First get the bindings for the projection variable in the antecedent
		return source.difference(rule.getProjectionVariable(), rule.getAntecedent(), rule.getTriples());
	}
	
	public void runMode2(Collection<Query> rules){
		for(Query rule: rules){
			Object predictions = generatePredictions(rule, training);
			Collection<Triple<ByteString, ByteString, ByteString>> newPredictions = samplePredictions(predictions, rule);
			printPredictions(rule, newPredictions);
		}		
	}

	private Collection<Triple<ByteString, ByteString, ByteString>> samplePredictions(Object predictions, Query rule) {
		// TODO Auto-generated method stub
		int nVars = FactDatabase.numVariables(rule.getHead());
		if(nVars == 2){
			return samplePredictionsTwoVariables((Map<ByteString, IntHashMap<ByteString>>)predictions, rule);
		}else if(nVars == 1){
			return samplePredictionsOneVariable((Set<ByteString>)predictions, rule);			
		}
		
		return null;
	}

	private Collection<Triple<ByteString, ByteString, ByteString>> samplePredictionsOneVariable(Set<ByteString> predictions, Query rule) {
		// TODO Auto-generated method stub
		return null;
	}

	private Collection<Triple<ByteString, ByteString, ByteString>> samplePredictionsTwoVariables(Map<ByteString, IntHashMap<ByteString>> predictions, Query rule) {
		Set<ByteString> keySet = predictions.keySet();
		ByteString relation = rule.getHead()[1];
		//Depending on the counting variable the order is different
		int countingVarPos = rule.getProjectionVariablePosition();
		Set<Triple<ByteString, ByteString, ByteString>> samplingCandidates = new LinkedHashSet<Triple<ByteString, ByteString, ByteString>>();
		List<Triple<ByteString, ByteString, ByteString>> result = new ArrayList<>(numberOfPredictions);
		
		for(ByteString value1: keySet){
			for(ByteString value2: predictions.get(value1)){
				Triple<ByteString, ByteString, ByteString> triple = new Triple<ByteString, ByteString, ByteString>(null, null, null);
				
				if(value1.equals(value2)) continue;
				
				if(countingVarPos == 0){
					triple.first = value1;
					triple.third = value2;
				}else{
					triple.first = value2;
					triple.third = value1;					
				}
				
				triple.second = relation;
				samplingCandidates.add(triple);
			}			
		}
		
		//Now sample them
		if(samplingCandidates.size() <= numberOfPredictions){
			return samplingCandidates;
		}else{
			Object[] candidates = samplingCandidates.toArray();
			int i;
			Random r = new Random();
			for(i = 0; i < numberOfPredictions; ++i){				
				result.add((Triple<ByteString, ByteString, ByteString>)candidates[i]);
			}
			
			while(i < candidates.length){
			    int rand = r.nextInt(i);
			    if(rand < numberOfPredictions){
			    	//Pick a random number in the reserviour
			    	result.set(r.nextInt(numberOfPredictions), (Triple<ByteString, ByteString, ByteString>)candidates[i]);
			    }
			    ++i;
			}
		}
		
		return result;
	}

	public void runMode1(Collection<Query> rules) {
		List<Triple<ByteString, ByteString, ByteString>> allPredictions = new ArrayList<>();
		
		for(Query rule: rules){
			Object predictions = generatePredictions(rule, training);
			Collection<Triple<ByteString, ByteString, ByteString>> newPredictions = samplePredictions(predictions, rule, allPredictions);
			printPredictions(rule, newPredictions);
		}
	}

	private void printPredictions(Query rule,
			Collection<Triple<ByteString, ByteString, ByteString>> newPredictions) {

		for(Triple<ByteString, ByteString, ByteString> triple: newPredictions){
			System.out.println(rule.getRuleString() + "\t" + triple.first + "\t" + triple.second + "\t" + triple.third);
		}
	}

	/**
	 * 
	 * @param predictions
	 * @param rule
	 */
	private Collection<Triple<ByteString, ByteString, ByteString>> samplePredictions(Object predictions, Query rule, Collection<Triple<ByteString, ByteString, ByteString>> allPredictions) {
		// TODO Auto-generated method stub
		int nVars = FactDatabase.numVariables(rule.getHead());
		if(nVars == 2){
			return samplePredictionsTwoVariables((Map<ByteString, IntHashMap<ByteString>>)predictions, rule, allPredictions);
		}else if(nVars == 1){
			return samplePredictionsOneVariable((Set<ByteString>)predictions, rule, allPredictions);			
		}
		
		return null;
	}

	private Collection<Triple<ByteString, ByteString, ByteString>> samplePredictionsOneVariable(
			Set<ByteString> predictions,
			Query rule,
			Collection<Triple<ByteString, ByteString, ByteString>> allPredictions) {
		// TODO Auto-generated method stub
		return null;
		
	}

	private Collection<Triple<ByteString, ByteString, ByteString>> samplePredictionsTwoVariables(Map<ByteString, IntHashMap<ByteString>> predictions, Query rule, Collection<Triple<ByteString, ByteString, ByteString>> allPredictions){
		Set<ByteString> keySet = predictions.keySet();
		ByteString relation = rule.getHead()[1];
		//Depending on the counting variable the order is different
		int countingVarPos = rule.getProjectionVariablePosition();
		Set<Triple<ByteString, ByteString, ByteString>> samplingCandidates = new LinkedHashSet<Triple<ByteString, ByteString, ByteString>>();
		List<Triple<ByteString, ByteString, ByteString>> result = new ArrayList<>(numberOfPredictions);
		
		for(ByteString value1: keySet){
			for(ByteString value2: predictions.get(value1)){
				Triple<ByteString, ByteString, ByteString> triple = new Triple<ByteString, ByteString, ByteString>(null, null, null);
				
				if(value1.equals(value2)) continue;
				
				if(countingVarPos == 0){
					triple.first = value1;
					triple.third = value2;
				}else{
					triple.first = value2;
					triple.third = value1;					
				}
				
				triple.second = relation;
				
				if(!allPredictions.contains(triple)){
					samplingCandidates.add(triple);
				}
			}			
		}
		
		//Now sample them
		if(samplingCandidates.size() <= numberOfPredictions){
			allPredictions.addAll(samplingCandidates);
			return samplingCandidates;
		}else{
			Object[] candidates = samplingCandidates.toArray();
			int i;
			Random r = new Random();
			for(i = 0; i < numberOfPredictions; ++i){				
				result.add((Triple<ByteString, ByteString, ByteString>)candidates[i]);
			}
			
			while(i < candidates.length){
			    int rand = r.nextInt(i);
			    if(rand < numberOfPredictions){
			    	//Pick a random number in the reserviour
			    	result.set(r.nextInt(numberOfPredictions), (Triple<ByteString, ByteString, ByteString>)candidates[i]);
			    }
			    ++i;
			}
			
			allPredictions.addAll(result);
		}
		
		return result;
	}
}
