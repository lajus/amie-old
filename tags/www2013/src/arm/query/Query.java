/**
 * @author lgalarra
 * @date Aug 8, 2012
 */
package arm.query;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javatools.datatypes.ByteString;
import arm.data.EquivalenceChecker2;
import arm.data.FactDatabase;
import arm.mining.Metric;

/**
 * @author lgalarra
 *
 */
public class Query implements Comparable<Query>{


	/**
	 * The triple patterns
	 */
	List<ByteString[]> triples;
	
	/**
	 * List of variables that require to be bound
	 */
	List<ByteString> mustBindVariables;
	
	/**
	 * List of all variables occurring in the query
	 */
	List<ByteString> variables;
	

	/**
	 * Support w.r.t some set of entities from a relation
	 */
	double headCoverage;
	
	/**
	 * Support w.r.t the set of all subjects in the database
	 */
	double support;
	
	/**
	 * Standard definition of confidence
	 */
	double confidence;
	
	/**
	 * Improved definition of confidence which includes an existential version of the head
	 * for the antecedent (body) size estimation.
	 */
	double improvedConfidence;
		
	/**
	 * Absolute number of bindings for the projection variable of the query
	 */
	int cardinality;
	
	/**
	 * String unique key for the head of the query
	 */
	private String headKey;

	/**
	 * Predictiveness power
	 */
	private double predictiveness;

	/**
	 * Improved predictiveness
	 */
	private double improvedPredictiveness;
	
	/**
	 * Predictiveness * standard confidence
	 */
	private double improvedStdPredictiveness;
	
	/**
	 * Parent query
	 */
	private Query parent;
	
	/**
	 * The variable used for counting
	 */
	private ByteString projectionVariable;

	/**
	 * The position in the head used for counting
	 */
	private int projectionVariablePosition;
	
	/**
	 * The number of instances of the counting variable in the antecedent
	 */
	private int bodySize;
	
	/**
	 * predictiveness * improved confidence * body size
	 */
	private double predImprConfBodySize;
	
	/**
	 * predictiveness * confidence * body size
	 */
	private double predConfBodySize;

	/**
	 * Integer counter used to guarantee unique variable names
	 */
	private static int varsCount = 0;	
	
	/**
	 * Information about the precision of the rule in some evaluation context
	 */
	int[] evaluationResult;

	/**
	 * Body - Head (whatever is false or unknown in the database)
	 */
	private int bodyMinusHeadSize;

	/**
	 * Body - Head* (existential version of the head)
	 */
	private int bodyStarSize;
	
	
	private static Metric rankingMetric = Metric.Confidence;
	
	private char highestVariable;
			
	
	public static ByteString[] triple(ByteString sub, ByteString pred, ByteString obj){
		ByteString[] newTriple = new ByteString[3];
		newTriple[0] = sub;
		newTriple[1] = pred; 
		newTriple[2] = obj;
		return newTriple;
	}
	
	public static boolean equal(ByteString[] pattern1, ByteString pattern2[]){
		return pattern1[0] == pattern2[0] && pattern1[1] == pattern2[1] && pattern1[2] == pattern2[2];
	}
	
	public ByteString[] fullyUnboundTriplePattern(){
		ByteString[] result = new ByteString[3];
		result[0] = ByteString.of("?" + highestVariable);		
		result[1] = ByteString.of("?p");
		++highestVariable;
		result[2] = ByteString.of("?" + highestVariable);
		++highestVariable;		
		return result;		
	}
	
	public static synchronized ByteString[] fullyUnboundTriplePattern1(){
		ByteString[] result = new ByteString[3];
		++varsCount;
		result[0] = ByteString.of("?s" + varsCount);
		result[1] = ByteString.of("?p" + varsCount);
		result[2] = ByteString.of("?o" + varsCount);
		return result;
	}
	
	public static boolean equals(ByteString[] atom1, ByteString[] atom2){
		return atom1[0].equals(atom2[0]) && atom1[1].equals(atom2[1]) && atom1[2].equals(atom2[2]);
	}
	
	public Query(){
		triples = new ArrayList<ByteString[]>();
		headKey = null;
		cardinality = 0;
		confidence = -2.0;
		improvedConfidence = -2.0;
		parent = null;
		projectionVariable = null; 
		bodySize = 0;
		mustBindVariables = new ArrayList<ByteString>();
		variables = new ArrayList<ByteString>();
		highestVariable = 'a';
	}
			

	public Query(ByteString[] pattern, int cardinality){
		triples = new ArrayList<ByteString[]>();
		variables = new ArrayList<ByteString>();
		mustBindVariables = new ArrayList<ByteString>();
		confidence = -2.0;
		improvedConfidence = -2.0;
		predictiveness = -2.0;
		improvedPredictiveness = -2.0;
		this.cardinality = cardinality;
		parent = null;
		triples.add(pattern);
		projectionVariable = pattern[0];
		projectionVariablePosition = 0;
		bodySize = 0;
		computeHeadKey();
		for(int i = 0; i < pattern.length; ++i){
			if(FactDatabase.isVariable(pattern[i])){
				mustBindVariables.add(pattern[i]);
				variables.add(pattern[i]);
			}
		}
		highestVariable = (char) (pattern[2].charAt(1) + 1);
	}
	
	public Query(Query otherQuery, int cardinality){
		triples = new ArrayList<ByteString[]>();
		for(ByteString[] sequence: otherQuery.triples){
			triples.add(sequence.clone());
		}
		variables = new ArrayList<ByteString>();
		variables.addAll(otherQuery.variables);
		mustBindVariables = new ArrayList<ByteString>();
		mustBindVariables.addAll(otherQuery.mustBindVariables);
		this.cardinality = cardinality;
		this.setProjectionVariable(otherQuery.getProjectionVariable());
		computeHeadKey();
		confidence = -2.0;
		improvedConfidence = -2.0;
		predictiveness = -2.0;
		improvedPredictiveness = -2.0;
		parent = null;
		bodySize = 0;
		highestVariable = otherQuery.highestVariable;		
	}
	
	private void computeHeadKey() {
		// TODO Auto-generated method stub
		headKey = "";
		headKey += triples.get(0)[1].toString(); 
	}
	
	public List<ByteString[]> getTriples() {
		return triples;
	}
	
	public ByteString[] getHead(){
		return triples.get(0);
	}
	
	public ByteString[] getSuccedent(){
		return triples.get(0);
	}
	
	public List<ByteString[]> getBody(){
		return getAntecedent();
	}
	
	public List<ByteString[]> getAntecedent(){
		return triples.subList(1, triples.size());
	}
 
	protected void setTriples(ArrayList<ByteString[]> triples) {
		this.triples = triples;
	}
	
	/**
	 * @param variables the variables to set
	 */
	public void setVariables(List<ByteString> variables) {
		this.variables = variables;
	}
	
	/**
	 * @return the mustBindVariables
	 */
	public List<ByteString> getMustBindVariables() {
		return mustBindVariables;
	}

	public double getHeadCoverage() {
		return headCoverage;
	}

	public void setHeadCoverage(double headCoverage) {
		this.headCoverage = headCoverage;
	}
	
	/**
	 * @return the support
	 */
	public double getSupport() {
		return support;
	}

	/**
	 * @param support the support to set
	 */
	public void setSupport(double support) {
		this.support = support;
	}

	/**
	 * @return the headBodyCount
	 */
	public int getCardinality() {
		return cardinality;
	}

	/**
	 * @param headBodyCount the headBodyCount to set
	 */
	public void setCardinality(int cardinality) {
		this.cardinality = cardinality;
	}

	public int getBodySize() {
		return bodySize;
	}

	public void setBodySize(int bodySize) {
		this.bodySize = bodySize;
	}

	/**
	 * @return the confidence
	 */
	public double getConfidence() {
		return confidence;
	}

	/**
	 * @return the evaluationResult
	 */
	public int[] getEvaluationResult() {
		return evaluationResult;
	}

	/**
	 * @param evaluationResult the evaluationResult to set
	 */
	public void setEvaluationResult(int[] evaluationResult) {
		this.evaluationResult = evaluationResult;
	}

	/**
	 * @param confidence the confidence to set
	 */
	public void setConfidence(double confidence) {
		this.confidence = confidence;
	}

	/**
	 * @return the improvedConfidence
	 */
	public double getImprovedConfidence() {
		return improvedConfidence;
	}
	
	public double getImprovedStdPredictiveness() {
		return improvedStdPredictiveness;
	}

	public void setImprovedStdPredictiveness(double improvedStdPredictiveness) {
		this.improvedStdPredictiveness = improvedStdPredictiveness;
	}

	/**
	 * 
	 * @return
	 */
	public double getPredictiveness(){
		return predictiveness;
	}
	
	/**
	 * 
	 * @param predictiveness
	 */
	public void setPredictiveness(double predictiveness) {
		// TODO Auto-generated method stub
		this.predictiveness = predictiveness;
	}
	
	/**
	 * 
	 */
	public double getImprovedPredictiveness(){
		return improvedPredictiveness;
	}
	
	/**
	 * 
	 * @param d
	 */
	public void setImprovedPredictiveness(double improvedPredictiveness) {
		this.improvedPredictiveness = improvedPredictiveness;
		
	}

	
	/**
	 * @param improvedConfidence the improvedConfidence to set
	 */
	public void setImprovedConfidence(double improvedConfidence) {
		this.improvedConfidence = improvedConfidence;
	}

	public ByteString[] getLastTriplePattern(){
		return triples.get(triples.size() - 1);
	}
		
	public double getPredImprConfBodySize() {
		return predImprConfBodySize;
	}

	public void setPredImprConfBodySize(double predImprConfBodySize) {
		this.predImprConfBodySize = predImprConfBodySize;
	}

	public double getPredConfBodySize() {
		return predConfBodySize;
	}

	public void setPredConfBodySize(double predConfBodySize) {
		this.predConfBodySize = predConfBodySize;
	}

	/**
	 * Returns true if the triple pattern contains constants in all its positions
	 * @param pattern
	 * @return
	 */
	public static boolean isGroundAtom(ByteString[] pattern) {
		// TODO Auto-generated method stub
		return !FactDatabase.isVariable(pattern[0]) && !FactDatabase.isVariable(pattern[1]) && !FactDatabase.isVariable(pattern[2]);
	}

	/**
	 * Look for the redundant atoms with respect to the given
	 * atom
	 * @param newAtom
	 * @return
	 */
	public List<ByteString[]> getRedundantAtoms(int withRespectToIdx){
		ByteString[] newAtom = triples.get(withRespectToIdx);
		List<ByteString[]> redundantAtoms = new ArrayList<ByteString[]>();
		for(ByteString[] pattern: triples){
			if(pattern != newAtom){
				if(isUnifiable(pattern, newAtom) || isUnifiable(newAtom, pattern)){
					redundantAtoms.add(pattern);
				}
			}
		}
		
		return redundantAtoms;
	}
	
	/**
	 * Checks whether the last atom in the query is redundant
	 * @param newAtom
	 * @return
	 */
	public List<ByteString[]> getRedundantAtoms(){
		ByteString[] newAtom = getLastTriplePattern();
		List<ByteString[]> redundantAtoms = new ArrayList<ByteString[]>();
		for(ByteString[] pattern: triples){
			if(pattern != newAtom){
				if(isUnifiable(pattern, newAtom) || isUnifiable(newAtom, pattern)){
					redundantAtoms.add(pattern);
				}
			}
		}
		
		return redundantAtoms;
	}
	
	public ByteString getProjectionVariable(){
		return projectionVariable;
	}
	
	public int getProjectionVariablePosition(){
		return projectionVariablePosition;
	}
	
	/**
	 * @param projectionVariable the projectionVariable to set
	 */
	public void setProjectionVariable(ByteString projectionVariable) {
		this.projectionVariable = projectionVariable;
		if(this.projectionVariable.equals(triples.get(0)[0]))
			projectionVariablePosition = 0;
		else
			projectionVariablePosition = 2;
	}

	/**
	 * Determines if the second argument is unifiable to the first one. Unifiable means there is a
	 * valid unification mapping (variable -> variable, variable -> constant) between the components  
	 * of the triple pattern
	 * @param pattern
	 * @param newAtom
	 * @return boolean
	 */
	public static boolean isUnifiable(ByteString[] pattern, ByteString[] newAtom) {
		// TODO Auto-generated method stub
		boolean unifiesSubject = pattern[0].equals(newAtom[0]) || FactDatabase.isVariable(pattern[0]);
		if(!unifiesSubject) return false;
		
		boolean unifiesPredicate = pattern[1].equals(newAtom[1]) || FactDatabase.isVariable(pattern[1]);
		if(!unifiesPredicate) return false;
		
		boolean unifiesObject = pattern[2].equals(newAtom[2]) || FactDatabase.isVariable(pattern[2]);
		if(!unifiesObject) return false;
		
		return true;
	}
	
	public static boolean areEquivalent(ByteString[] pattern, ByteString[] newAtom) {
		boolean unifiesSubject = pattern[0].equals(newAtom[0]) || 
				(FactDatabase.isVariable(pattern[0]) && FactDatabase.isVariable(newAtom[0]));
		if(!unifiesSubject) return false;
		
		boolean unifiesPredicate = pattern[1].equals(newAtom[1]) 
				|| (FactDatabase.isVariable(pattern[1]) && FactDatabase.isVariable(newAtom[1]));
		if(!unifiesPredicate) return false;
		
		boolean unifiesObject = pattern[2].equals(newAtom[2]) || 
				(FactDatabase.isVariable(pattern[2]) && FactDatabase.isVariable(newAtom[2]));
		if(!unifiesObject) return false;
		
		return true;
	}
	
	public static boolean unifies(ByteString[] test, List<ByteString[]> query){
		for(ByteString[] pattern: query){
			if(isUnifiable(pattern, test))
				return true;
			
		}
		
		return false;
	}
	
	public static List<ByteString[]> redundantAtoms(ByteString[] test, List<ByteString[]> query){
		List<ByteString[]> redundantAtoms = new ArrayList<ByteString[]>();
		for(ByteString[] pattern: query){
			if(isUnifiable(pattern, test))
				redundantAtoms.add(pattern);
			
		}
		
		return redundantAtoms;
	}
	
	/**
	 * Determines whether the last atom of the query.
	 * @return boolean
	 */
	public boolean containsUnifiablePatterns() {
		int nPatterns = triples.size();
		for(int i = 0; i < nPatterns; ++i){
			for(int j = i + 1; j < nPatterns; ++j){
				if(isUnifiable(triples.get(j), triples.get(i)) || isUnifiable(triples.get(i), triples.get(j)))
					return true;
			}
		}
		
		return false;
	}

	
	public String toString(){
		StringBuilder stringBuilder = new StringBuilder();
		for(ByteString[] pattern: triples){
			stringBuilder.append(pattern[0]);
			stringBuilder.append(" ");
			stringBuilder.append(pattern[1]);
			stringBuilder.append(" ");			
			stringBuilder.append(pattern[2]);
			stringBuilder.append("  ");
		}
		
		return stringBuilder.toString();
	}
	
	/**
	 * Returns a list with all the different variables in the query.
	 * @return List<ByteString>
	 */
	public List<ByteString> getVariables(){
		return variables;
	}

	/**
	 * Determines if a pattern contains repeated components, which are considered hard to satisfy (i.e., ?x somePredicate ?x)
	 * @return boolean
	 */
	public boolean containsRepeatedVariablesInLastPattern() {
		// TODO Auto-generated method stub
		ByteString[] triple = getLastTriplePattern();
		return triple[0].equals(triple[1]) || triple[0].equals(triple[2]) || triple[1].equals(triple[2]);
	}

	public boolean isRedundantRecursive() {
		List<ByteString[]> redundantAtoms = getRedundantAtoms();
		ByteString[] lastPattern = getLastTriplePattern();
		for(ByteString[] redundantAtom: redundantAtoms){
			if(equals(lastPattern, redundantAtom)){
				return true;
			}
		}
		
		return false;
 	}

	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return triples.isEmpty();
	}

	public boolean isSafe() {
		// TODO Auto-generated method stub
		return !triples.isEmpty() && mustBindVariables.isEmpty();
	}
	
	public String getHeadKey(){		
		if(headKey == null){
			computeHeadKey();
		}
		
		return headKey;
	}
	
	public int getLength(){
		return triples.size();
	}
	
	public int getLengthWithoutTypes(){
		int size = 0;
		for(ByteString[] triple: triples){
			if(!triple[1].equals(ByteString.of("rdf:type")) || FactDatabase.isVariable(triple[2]))
				++size;
		}
		
		return size;
	}


	public Query getParent() {
		return parent;
	}

	public void setParent(Query parent) {
		this.parent = parent;
	}

	public Query addEdge(ByteString[] newEdge, int cardinality, ByteString joinedVariable, ByteString danglingVariable) {
		Query newQuery = new Query(this, cardinality);
		ByteString[] copyNewEdge = newEdge.clone();
		newQuery.triples.add(copyNewEdge);
		newQuery.mustBindVariables.remove(joinedVariable);
		newQuery.mustBindVariables.add(danglingVariable);
		newQuery.variables.add(danglingVariable);		
		return newQuery;		
	}
	
	public Query addEdge(ByteString[] newEdge, int cardinality) {
		Query newQuery = new Query(this, cardinality);
		ByteString[] copyNewEdge = newEdge.clone();
		newQuery.triples.add(copyNewEdge);
		return newQuery;		
	}

	public Query closeCircle(ByteString[] newEdge, int cardinality) {
		Query newQuery = new Query(this, cardinality);
		ByteString[] copyNewEdge = newEdge.clone();
		newQuery.triples.add(copyNewEdge);
		
		for(ByteString variable: copyNewEdge){
			if(FactDatabase.isVariable(variable)){
				newQuery.mustBindVariables.remove(variable);
			}
		}
		return newQuery;
	}
	


	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + cardinality;
		result = prime * result + ((headKey == null) ? 0 : headKey.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Query other = (Query) obj;
		if (headKey == null) {
			if (other.headKey != null)
				return false;
		} else if (!headKey.equals(other.headKey))
			return false;
		
		return EquivalenceChecker2.equal(triples, other.triples);
	}
	

	public String getRuleString(){
		StringBuilder strBuilder = new StringBuilder();
		for(int i = 1; i < triples.size(); ++i){
			ByteString[] pattern = triples.get(i);
			strBuilder.append(pattern[0]);
			strBuilder.append("  ");
			strBuilder.append(pattern[1]);
			strBuilder.append("  ");			
			strBuilder.append(pattern[2]);
			strBuilder.append("  ");
		}
		
		strBuilder.append(" => ");
		ByteString[] head = triples.get(0);
		strBuilder.append(head[0]);
		strBuilder.append("  ");
		strBuilder.append(head[1]);
		strBuilder.append("  ");			
		strBuilder.append(head[2]);
		
		return strBuilder.toString();
	}

	public String getRuleFullString() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(getRuleString());
		
		strBuilder.append("  Support: " + support);
		strBuilder.append(" Head coverage: " + headCoverage);
		strBuilder.append(" Confidence: " + confidence);
		strBuilder.append(" Improved confidence: " + improvedConfidence);		
		strBuilder.append(" Predictiveness: " + predictiveness);
		strBuilder.append(" Improved Predictiveness: " + improvedPredictiveness);
		strBuilder.append(" Std. Improved Predictiveness: " + improvedStdPredictiveness);
		strBuilder.append(" Cardinality: " + cardinality);
		strBuilder.append(" Pred*Conf*Body: " + predConfBodySize);
		strBuilder.append(" Pred*ImprConf*Body: " + predImprConfBodySize);
		strBuilder.append(" Body size: " + bodySize);
		strBuilder.append(" Key variable: " + projectionVariable);

		return strBuilder.toString();
	}
	
	public String getRuleFullString2() {
		DecimalFormat df = new DecimalFormat("#.########");
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(getRuleString());
		
		strBuilder.append("\t" + df.format(support) );
		strBuilder.append("\t" + df.format(headCoverage));
		strBuilder.append("\t" + df.format(confidence));
		strBuilder.append("\t" + df.format(improvedConfidence));
		strBuilder.append("\t" + df.format(predictiveness));
		strBuilder.append("\t" + df.format(improvedPredictiveness));
		strBuilder.append("\t" + df.format(improvedStdPredictiveness));
		strBuilder.append("\t" + cardinality);		
		strBuilder.append("\t" + bodySize);
		strBuilder.append("\t" + predConfBodySize);
		strBuilder.append("\t" + predImprConfBodySize);
		strBuilder.append("\t" + projectionVariable);

		return strBuilder.toString();
	}

	public Query unify(int danglingPosition, ByteString constant, int cardinality) {
		Query newQuery = new Query(this, cardinality);
		ByteString[] lastNewPattern = newQuery.getLastTriplePattern();
		newQuery.mustBindVariables.remove(lastNewPattern[danglingPosition]);
		newQuery.variables.remove(lastNewPattern[danglingPosition]);
		lastNewPattern[danglingPosition] = constant;

		if(newQuery.getLength() == 1)
			newQuery.computeHeadKey();
		
		return newQuery;
	}
	
	public Query unify(int triplePos, int danglingPosition, ByteString constant, int cardinality) {
		Query newQuery = new Query(this, cardinality);
		ByteString[] targetEdge = newQuery.getTriples().get(triplePos);
		newQuery.mustBindVariables.remove(targetEdge[danglingPosition]);
		newQuery.variables.remove(targetEdge[danglingPosition]);
		targetEdge[danglingPosition] = constant;
		newQuery.cleanInequalityConstraints();

		if(newQuery.getLength() == 1)
			newQuery.computeHeadKey();
		
		return newQuery;
	}


	private void cleanInequalityConstraints() {
		List<ByteString[]> toRemove = new ArrayList<ByteString[]>();
		for(ByteString[] triple: triples){
			if(triple[1].equals(FactDatabase.DIFFERENTFROMbs)){
				int varPos = FactDatabase.firstVariablePos(triple);
				if(!variables.contains(triple[varPos])){
					//Remove the triple
					toRemove.add(triple);
				}
			}
		}
		
		triples.removeAll(toRemove);
	}

	public List<Query> getAncestors(){
		List<Query> ancestors = new ArrayList<Query>();
		Query current = this.parent;
		
		while(current != null){
			ancestors.add(current);
			current = current.parent;
		}
		
		return ancestors;
	}
	
	public static void setRankingMetric(Metric metric){
		Query.rankingMetric = metric;
	}

	@Override
	public int compareTo(Query o) {
		// TODO Auto-generated method stub
		double value1, value2;
		
		switch(rankingMetric){
		case Support:
			value1 = support;
			value2 = o.support;
			break;
		case HeadCoverage:
			value1 = headCoverage;
			value2 = o.headCoverage;
			break;
		case Confidence:
			value1 = confidence;
			value2 = o.confidence;
			break;
		case ImprovedConfidence:
			value1 = improvedConfidence;
			value2 = o.improvedConfidence;
			break;
		case ConfidenceTimesPredictivenessTimesBodySize:
			value1 = predConfBodySize;
			value2 = o.predConfBodySize;
			break;
		case ImprovedConfidenceTimesPredictivenessTimesBodySize:
			value1 = predImprConfBodySize;
			value2 = o.predImprConfBodySize;
			break;
		default:
			value1 = support;
			value2 = o.support;
			break;			
		}
		
		//If they have the same value return whatever 1
		if(value1 < value2){
			return 1;
		}else{
			return -1;
		}
	}

	public void setBodyMinusHeadSize(int size) {
		bodyMinusHeadSize = size;
	}
	
	public int getBodyMinusHeadSize(){
		return bodyMinusHeadSize;
	}

	public void setBodyStarSize(int size) {
		// TODO Auto-generated method stub
		bodyStarSize = size;
	}
	
	public int getBodyStarSize(){
		return bodyStarSize;
	}

	public boolean metricsCalculated() {
		// TODO Auto-generated method stub
		return confidence != -2.0 && improvedConfidence != -2.0;
	}

	public Query rewriteQuery(ByteString[] remove, ByteString[] target, ByteString victimVar, ByteString targetVar) {
		List<ByteString[]> newTriples = new ArrayList<ByteString[]>();
		for(ByteString[] t: triples){
			if(t != remove){
				ByteString[] clone = t.clone();
				for(int i = 0; i < clone.length; ++i){
					if(clone[i].equals(victimVar))
						clone[i] = targetVar;
				}
				
				newTriples.add(clone);
			}
		}
		
		Query result = new Query();
		//If the removal triple is the head, make sure the target is the new head
		if(remove == triples.get(0)){
			for(int i = 0; i < newTriples.size(); ++i){
				if(Arrays.equals(target, newTriples.get(i))){
					ByteString tmp[] = newTriples.get(0);					
					newTriples.set(0, newTriples.get(i));
					newTriples.set(i, tmp);
				}
			}
		}
		
		result.triples.addAll(newTriples);
		
		return result;
	}

	public boolean variableCanBeDeleted(int triplePos, int varPos) {
		ByteString variable = triples.get(triplePos)[varPos];
		for(int i = 0; i < triples.size(); ++i){
			if(i != triplePos){
				if(FactDatabase.varpos(variable, triples.get(i)) != -1)
					return false;
			}
		}
		//The variable does not appear anywhere else (no constraints)
		return true;
	}
}