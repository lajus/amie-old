package arm.mining.assistant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Pair;
import arm.data.FactDatabase;
import arm.mining.Metric;
import arm.query.Query;

/**
 * Simpler miner assistant which implements all the logic required 
 * to mine conjunctive rules from a RDF datastore.
 * 
 * @author lgalarra
 *
 */
public class MiningAssistant{
	
	public static int RECURSITIVITY_LIMIT = 3;
	
	/**
	 * Factory object to instantiate query components
	 */
	protected FactDatabase source;
	
	/**
	 * Exclusively used for schema information
	 */
	protected FactDatabase schemaSource;
	
	/**
	 * Number of different objects in the underlying dataset
	 */
	protected int totalObjectCount;

	/**
	 * Number of different subjects in the underlying dataset
	 */
	protected int totalSubjectCount;
	
	/**
	 * Type keyword
	 */
	protected ByteString typeString;
	
	/**
	 * Subproperty keyword
	 */
	protected ByteString subPropertyString;
		
	/**
	 * Minimum confidence
	 */
	protected double minConfidence;
	
	/**
	 * Minimum confidence
	 */
	protected double minImprovedConfidence;
	
	/**
	 * Maximum number of atoms allowed in the antecedent
	 */
	
	private Query subclassQuery;
	
	/**
	 * Maximum number of atoms in a query
	 */
	protected int maxDepth;
	
	/**
	 * Contains the number of triples per relation in the database
	 */
	protected IntHashMap<String> headCardinalities;

	/**
	 * Allow constants for refinements
	 */
	protected boolean allowConstants;
	
	/**
	 * List of excluded relations;
	 */
	protected Collection<ByteString> excludedRelations;

	/**
	 * Min predictiveness threshold
	 */
	private double minPredictiveness;

	/**
	 * Max predictiveness threshold
	 */
	private double maxPredictiveness;

	/**
	 * Count directly on subject or use functional information
	 */
	protected boolean countAlwaysOnSubject;

	/**
	 * Use to rank type edges
	 */
	protected Metric rankingMetric;
	
	/**
	 * Flag to enable query rewriting and approximations
	 */
	protected boolean enableOptimizations;
	
	
	/**
	 * @param dataSource
	 */
	public MiningAssistant(FactDatabase dataSource) {
		source = dataSource;
		this.minConfidence = 0.25;
		this.maxDepth = 3;
		allowConstants = false;
		ByteString[] rootPattern = Query.fullyUnboundTriplePattern1();
		List<ByteString[]> triples = new ArrayList<ByteString[]>();
		triples.add(rootPattern);
		totalSubjectCount = source.countDistinct(rootPattern[0], triples);
		totalObjectCount = source.countDistinct(rootPattern[2], triples);
		typeString = ByteString.of("rdf:type");
		subPropertyString = ByteString.of("rdfs:subPropertyOf");
		headCardinalities = new IntHashMap<String>();
		ByteString[] subclassPattern = Query.fullyUnboundTriplePattern1();
		subclassPattern[1] = subPropertyString;
		subclassQuery = new Query(subclassPattern, 0);
		countAlwaysOnSubject = false;
	}	
	
	public int getTotalCount(Query candidate){
		if(countAlwaysOnSubject){
			return totalSubjectCount;
		}else{
			return getTotalCount(candidate.getProjectionVariablePosition());
		}
	}
	
	/**
	 * Returns the total number of subjects in the database.
	 * @return
	 */
	public int getTotalSubjectCount(){
		return totalSubjectCount;
	}
		
	public int getTotalObjectCount() {
		return totalObjectCount;
	}

	/**
	 * @return the maxDepth
	 */
	public int getMaxDepth() {
		return maxDepth;
	}

	/**
	 * @param maxDepth the maxDepth to set
	 */
	public void setMaxDepth(int maxAntecedentDepth) {
		this.maxDepth = maxAntecedentDepth;
	}
	
	/**
	 * @return the minConfidence
	 */
	public double getMinConfidence() {
		return minConfidence;
	}

	/**
	 * @return the minImprovedConfidence
	 */
	public double getMinImprovedConfidence() {
		return minImprovedConfidence;
	}

	/**
	 * @param minImprovedConfidence the minImprovedConfidence to set
	 */
	public void setMinImprovedConfidence(double minImprovedConfidence) {
		this.minImprovedConfidence = minImprovedConfidence;
	}

	/**
	 * @param minConfidence the minConfidence to set
	 */
	public void setMinConfidence(double minConfidence) {
		this.minConfidence = minConfidence;
	}
	
	public FactDatabase getSchemaSource(){
		return schemaSource;
	}
	
	public void setSchemaSource(FactDatabase schemaSource) {
		// TODO Auto-generated method stub
		this.schemaSource = schemaSource;
	}

	public boolean registerHeadRelation(Query query){		
		return headCardinalities.put(query.getHeadKey(), query.getCardinality());		
	}
	
	public int getHeadCardinality(Query query){
		return headCardinalities.get(query.getHeadKey());
	}

	protected Set<ByteString> getSubClasses(ByteString className){
		ByteString[] lastPattern = subclassQuery.getTriples().get(0);
		ByteString tmpVar = lastPattern[2];
		lastPattern[2] = className;		
		Set<ByteString> result = source.selectDistinct(lastPattern[0], subclassQuery.getTriples());
		lastPattern[2] = tmpVar;
		return result;
	}
	
	
	public void getDanglingEdges(Query query, Collection<ByteString> relations, int minCardinality, Collection<Query> output) {
		//The query must be empty
		if(!query.isEmpty()){
			throw new IllegalArgumentException("Expected an empty query");
		}
		
		ByteString[] newEdge = query.fullyUnboundTriplePattern();		
		query.getTriples().add(newEdge);
		
		for(ByteString relation: relations){
			newEdge[1] = relation;
			
			int countVarPos = countAlwaysOnSubject? 0 : findCountingVariable(newEdge);
			ByteString countingVariable = newEdge[countVarPos];
			int cardinality = source.countDistinct(countingVariable, query.getTriples());
			
			ByteString[] succedent = newEdge.clone();
			Query candidate = new Query(succedent, cardinality);
			candidate.setProjectionVariable(countingVariable);
			
			if(allowConstants)
				getInstantiatedEdges(candidate, null, candidate.getLastTriplePattern(), countVarPos == 0 ? 2 : 0, minCardinality, output);
			
			output.add(candidate);		
		}
		
		query.getTriples().remove(0);
	}

	/**
	 * Returns all candidates obtained by adding a new triple pattern to the query
	 * @param query
	 * @param minCardinality
	 * @return
	 */
	public void getDanglingEdges(Query query, int minCardinality, Collection<Query> output){		
		ByteString[] newEdge = query.fullyUnboundTriplePattern();
		
		if(query.isEmpty()){
			//Initial case
			query.getTriples().add(newEdge);
			IntHashMap<ByteString> relations = source.frequentBindingsOf(newEdge[1], newEdge[0], query.getTriples());
			for(ByteString relation: relations){
				if(excludedRelations != null && excludedRelations.contains(newEdge[1]))
					continue;
				
				
				int cardinality = relations.get(relation);
				if(cardinality >= minCardinality){
					ByteString[] succedent = newEdge.clone();
					succedent[1] = relation;
					int countVarPos = countAlwaysOnSubject? 0 : findCountingVariable(succedent);
					
					if(!succedent[countVarPos].equals(succedent[0])){
						//Recalculate the cardinality
						cardinality = source.countDistinct(succedent[countVarPos], FactDatabase.triples(succedent));
						if(cardinality < minCardinality)
							continue;
					}
						
					Query candidate = new Query(succedent, cardinality);
					candidate.setProjectionVariable(succedent[countVarPos]);
					registerHeadRelation(candidate);
					if(allowConstants)
						getInstantiatedEdges(candidate, null, candidate.getLastTriplePattern(), countVarPos == 0 ? 2 : 0, minCardinality, output);
					
					output.add(candidate);
				}
			}			
			query.getTriples().remove(0);
		}else{
			//General case
			if(!testLength(query))
				return;
			
			List<ByteString> joinVariables = null;
			
			//Then do it for all values
			if(query.isSafe()){
				joinVariables = query.getVariables();
			}else{
				joinVariables = query.getMustBindVariables();
			}

			int nPatterns = query.getTriples().size();
			ByteString originalRelationVariable = newEdge[1];		
			
			for(int joinPosition = 0; joinPosition <= 2; joinPosition += 2){
				ByteString originalFreshVariable = newEdge[joinPosition];
				
				for(ByteString joinVariable: joinVariables){					
					newEdge[joinPosition] = joinVariable;
					query.getTriples().add(newEdge);
					IntHashMap<ByteString> promisingRelations = source.frequentBindingsOf(newEdge[1], query.getProjectionVariable(), query.getTriples());
					query.getTriples().remove(nPatterns);
					
					int danglingPosition = (joinPosition == 0 ? 2 : 0);
					boolean boundHead = !FactDatabase.isVariable(query.getTriples().get(0)[danglingPosition]);
					for(ByteString relation: promisingRelations){
						if(excludedRelations != null && excludedRelations.contains(relation))
							continue;
						//Here we still have to make a redundancy check						
						int cardinality = promisingRelations.get(relation);
						if(cardinality >= minCardinality){
							newEdge[1] = relation;
							Query candidate = query.addEdge(newEdge, cardinality, newEdge[joinPosition], newEdge[danglingPosition]);
							if(candidate.containsUnifiablePatterns()){
								//Verify whether dangling variable unifies to a single value (I do not like this hack)
								if(boundHead && source.countDistinct(newEdge[danglingPosition], candidate.getTriples()) < 2)
									continue;
							}
							
							candidate.setHeadCoverage((double)candidate.getCardinality() / headCardinalities.get(candidate.getHeadKey()));
							candidate.setSupport((double)candidate.getCardinality() / (double)getTotalCount(candidate));
							candidate.setParent(query);							
							if(allowConstants){
								getInstantiatedEdges(candidate, candidate, candidate.getLastTriplePattern(), danglingPosition, minCardinality, output);
							}
							
							output.add(candidate);
						}
					}
					
					newEdge[1] = originalRelationVariable;
				}
				newEdge[joinPosition] = originalFreshVariable;
			}
		}
	}

	/**
	 * Based on the functionality constraints
	 * @param candidate
	 */
	protected int findCountingVariable(ByteString[] head) {
		int nVars = FactDatabase.numVariables(head);
		if(nVars == 1){
			return FactDatabase.firstVariablePos(head);
		}else{
			double functionality = source.functionality(head[1]);
			double inverseFunctionality = source.inverseFunctionality(head[1]);
			if(functionality >= inverseFunctionality){
				return 0;
			}else{
				return 2;
			}
		}
	}
	
	protected void calculateMetrics(Query candidate) {		
		// TODO Auto-generated method stub
		List<ByteString[]> antecedent = new ArrayList<ByteString[]>();
		antecedent.addAll(candidate.getTriples().subList(1, candidate.getTriples().size()));
		List<ByteString[]> succedent = new ArrayList<ByteString[]>();
		succedent.addAll(candidate.getTriples().subList(0, 1));
		double improvedDenominator = 0.0;
		double denominator = 0.0;
		double confidence = 0.0;
		double improvedConfidence = 0.0;
		double predictiveness = 0.0;
		ByteString[] existentialTriple = succedent.get(0).clone();
		int freeVarPos = 0;
		
		if(FactDatabase.numVariables(existentialTriple) == 1){
			freeVarPos = FactDatabase.firstVariablePos(existentialTriple);
		}else{
			if(existentialTriple[0].equals(candidate.getProjectionVariable()))
				freeVarPos = 2;
			else
				freeVarPos = 0;
		}

		existentialTriple[freeVarPos] = ByteString.of("?x");
				
		if(antecedent.isEmpty()){
			candidate.setConfidence(1.0);
			candidate.setImprovedConfidence(1.0);
		}else{
			//Confidence
			try{
				denominator = (double)source.countDistinct(candidate.getProjectionVariable(), antecedent);
				confidence = (double)candidate.getCardinality() / denominator;
				candidate.setConfidence(confidence);
				candidate.setBodySize((int)denominator);
			}catch(UnsupportedOperationException e){
				
			}
			
			//Improved confidence: Add an existential version of the head
			antecedent.add(existentialTriple);
			try{
				improvedDenominator = (double)source.countDistinct(candidate.getProjectionVariable(), antecedent);
				improvedConfidence = (double)candidate.getCardinality() / improvedDenominator;
				candidate.setImprovedConfidence(improvedConfidence);
			}catch(UnsupportedOperationException e){
				
			}
			antecedent.remove(antecedent.size() - 1);
			predictiveness = 1 - (improvedDenominator / denominator);
			candidate.setPredictiveness(predictiveness);
			candidate.setImprovedPredictiveness(predictiveness * improvedConfidence);
			candidate.setImprovedStdPredictiveness(predictiveness * confidence);
		}
	}

	/**
	 * Returns all candidates obtained by binding two values
	 * @param currentNode
	 * @param minCardinality
	 * @param omittedVariables
	 * @return
	 */
	public void getCloseCircleEdges(Query query, int minCardinality, Collection<Query> output){		
		int nPatterns = query.getTriples().size();

		if(query.isEmpty())
			return;
		
		if(!testLength(query))
			return;
		
		List<ByteString> sourceVariables = null;
		List<ByteString> allVariables = query.getVariables();		
		
		if(query.isSafe()){
			sourceVariables = query.getVariables();
		}else{
			sourceVariables = query.getMustBindVariables(); 
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
				
				for(ByteString variable: allVariables){
					if(!variable.equals(sourceVariable)){
						newEdge[closeCirclePosition] = variable;
						
						query.getTriples().add(newEdge);
						IntHashMap<ByteString> promisingRelations = source.frequentBindingsOf(newEdge[1], query.getProjectionVariable(), query.getTriples());
						query.getTriples().remove(nPatterns);
						
						for(ByteString relation: promisingRelations){
							if(excludedRelations != null && excludedRelations.contains(relation))
								continue;
							
							//Here we still have to make a redundancy check
							int cardinality = promisingRelations.get(relation);
							newEdge[1] = relation;
							if(cardinality >= minCardinality){										
								Query candidate = query.closeCircle(newEdge, cardinality);
								if(!candidate.isRedundantRecursive()){
									candidate.setHeadCoverage((double)cardinality / (double)headCardinalities.get(candidate.getHeadKey()));
									candidate.setSupport((double)cardinality / (double)getTotalCount(candidate));
									candidate.setParent(query);
									output.add(candidate);
								}
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
	
	protected boolean testLength(Query candidate){
		return candidate.getLength() < maxDepth;
	}

	public boolean testRule(Query candidate) {
		boolean addIt = true;
		
		if(!candidate.isSafe()){
			return false;
		}
		calculateMetrics(candidate);
		if(candidate.getConfidence() >= minConfidence && candidate.getImprovedConfidence() >= minImprovedConfidence && candidate.getImprovedStdPredictiveness() >= minPredictiveness && candidate.getImprovedStdPredictiveness() <= maxPredictiveness){
			//Now check the confidence with respect to its ancestors
			List<Query> ancestors = candidate.getAncestors();			
			for(int i = ancestors.size() - 2; i >= 0; --i){
				if(ancestors.get(i).isSafe() && (candidate.getConfidence() <= ancestors.get(i).getConfidence() || candidate.getImprovedConfidence() <= ancestors.get(i).getImprovedConfidence())){
					addIt = false;
					break;
				}
			}
		}else{
			return false;
		}
		
		return addIt;
	}

	protected void getInstantiatedEdges(Query query, Query originalQuery, ByteString[] danglingEdge, int danglingPosition, int minCardinality, Collection<Query> output) {
		IntHashMap<ByteString> constants = source.frequentBindingsOf(danglingEdge[danglingPosition], query.getProjectionVariable(), query.getTriples());
		for(ByteString constant: constants){
			int cardinality = constants.get(constant);
			if(cardinality >= minCardinality){
				ByteString[] lastPatternCopy = query.getLastTriplePattern().clone();
				lastPatternCopy[danglingPosition] = constant;
				int cardLastEdge = source.count(lastPatternCopy);
				if(cardLastEdge < 2)
					continue;
				
				Query candidate = query.unify(danglingPosition, constant, cardinality);
				if(candidate.getRedundantAtoms().isEmpty()){
					candidate.setHeadCoverage((double)cardinality / headCardinalities.get(candidate.getHeadKey()));
					candidate.setSupport((double)cardinality / (double)getTotalCount(candidate));
					candidate.setParent(originalQuery);					
					output.add(candidate);
				}
			}
		}
	}

	public void setAllowConstants(boolean allowConstants) {
		// TODO Auto-generated method stub
		this.allowConstants = allowConstants;
	}

	public Collection<ByteString> getExcludedRelations() {
		return excludedRelations;
	}

	public void setExcludedRelations(Collection<ByteString> excludedRelations) {
		this.excludedRelations = excludedRelations;
	}

	public int getTotalCount(int projVarPosition) {
		if(projVarPosition == 0)
			return totalSubjectCount;
		else if(projVarPosition == 2)
			return totalObjectCount;
		else
			throw new IllegalArgumentException("Only 0 and 2 are valid variable positions");
	}

	public void setMinPredictiveness(double minPredictiveness) {
		// TODO Auto-generated method stub
		this.minPredictiveness = minPredictiveness;
		
	}

	public void setMaxPredictiveness(double maxPredictiveness) {
		// TODO Auto-generated method stub
		this.maxPredictiveness = maxPredictiveness;
	}

	public void setCountAlwaysOnSubject(boolean countAlwaysOnSubject) {
		// TODO Auto-generated method stub
		this.countAlwaysOnSubject = countAlwaysOnSubject;
	}

	public int getFactsCount() {
		// TODO Auto-generated method stub
		return source.size();
	}

	public void setRankingMetric(Metric rankingMetric) {
		// TODO Auto-generated method stub
		this.rankingMetric = rankingMetric;		
	}	
	
	public Metric getRankingMetric(){
		return rankingMetric;
	}

	/**
	 * @return the enableOptimizations
	 */
	public boolean isEnableOptimizations() {
		return enableOptimizations;
	}

	/**
	 * @param enableOptimizations the enableOptimizations to set
	 */
	public void setEnableOptimizations(boolean enableOptimizations) {
		this.enableOptimizations = enableOptimizations;
	}
}