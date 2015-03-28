/**
 * @author lgalarra
 * @date Aug 8, 2012
 */
package amie.query;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.MultiMap;
import amie.data.EquivalenceChecker2;
import amie.data.FactDatabase;

/**
 * @author lgalarra
 *
 */
public class Query{

	/**
	 * The triple patterns
	 */
	List<ByteString[]> triples;	

	/**
	 * Support w.r.t some set of entities from a relation
	 */
	double headCoverage;
	
	/**
	 * Support w.r.t the set of all subjects in the database
	 */
	double supportRatio;
	
	/**
	 * Standard definition of confidence
	 */
	double stdConfidence;
	
	/**
	 * Improved definition of confidence which includes an existential version of the head
	 * for the antecedent (body) size estimation.
	 */
	double pcaConfidence;
		
	/**
	 * Absolute number of bindings for the projection variable of the query
	 */
	long support;
	
	/**
	 * In AMIE the cardinality may change when the rule is enhanced with "special" atoms
	 * such as the DIFFERENTFROMbs database command. Since the cardinality is used in the
	 * hashCode function (good to garantee balanced hash tables), we store the first
	 * computed cardinality of the query. Unlike the real cardinality, this values remains
	 * constant since the creation of the object.
	 */
	long hashSupport;
	
	/**
	 * String unique key for the head of the query
	 */
	private String headKey;

	/**
	 * Parent query
	 */
	private Query parent;	
	
	/**
	 * The variable used for counting
	 */
	private ByteString functionalVariable;

	/**
	 * The position in the head used for counting
	 */
	private int functionalVariablePosition;
	
	/**
	 * The number of instances of the counting variable in the antecedent
	 */
	private long bodySize;
	
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
	private long bodyMinusHeadSize;

	/**
	 * Body - Head* (existential version of the head)
	 */
	private long bodyStarSize;
	
	/**
	 * Highest letter used for variable names
	 */
	private char highestVariable;
	/*
	 * Standard confidence theorethical upper bound for standard confidence
	 *  
	 */
	private double stdConfUpperBound;

	/*
	 * PCA confidence theorethical upper bound for PCA confidence
	 */
	private double pcaConfUpperBound;
	
	/**
	 * PCA confidence rough estimation for the hard cases
	 */
	private double pcaEstimation;
	
	/**
	 * Optimistic version of the PCA estimation
	 */
	private double pcaEstimationOptimistic;
	
	/**
	 * Time to run the denominator for the expression of the PCA confidence
	 */
	private double pcaConfidenceRunningTime;
	
	/**
	 * Time to run the denominator for the expression of the standard confidence
	 */	
	private double confidenceRunningTime;
	
	/**
	 * The rule is below an established support threshold
	 */
	private boolean belowMinimumSupport;

	/**
	 * True is the last atom of the rule contains a variable
	 * that binds to a single instance (a case where the AMIE algorithm would prune
	 * the rule).
	 */
	private boolean containsQuasiBindings;
	
	/**
	 * List of parents: queries that are equivalent to the current query except 
	 * by one body atom.
	 */
	private List<Query> ancestors;

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
		triples = new ArrayList<>();
		headKey = null;
		support = -1;
		hashSupport = 0;
		stdConfidence = -2.0;
		pcaConfidence = -2.0;
		parent = null;
		functionalVariable = null; 
		bodySize = -1;
		highestVariable = 'a';
		stdConfUpperBound = 0.0;
		pcaConfUpperBound = 0.0;
		pcaEstimation = 0.0;
		pcaEstimationOptimistic = 0.0;
		belowMinimumSupport = false;
		containsQuasiBindings = false;
		ancestors = new ArrayList<>();
	}
			

	public Query(ByteString[] pattern, long cardinality){
		triples = new ArrayList<>();
		stdConfidence = -2.0;
		pcaConfidence = -2.0;
		support = cardinality;
		hashSupport = cardinality;
		parent = null;
		triples.add(pattern);
		functionalVariable = pattern[0];
		functionalVariablePosition = 0;
		bodySize = 0;
		computeHeadKey();
		highestVariable = (char) (pattern[2].charAt(1) + 1);
		stdConfUpperBound = 0.0;
		pcaConfUpperBound = 0.0;
		pcaEstimation = 0.0;
		pcaEstimationOptimistic = 0.0;	
		belowMinimumSupport = false;
		containsQuasiBindings = false;
		ancestors = new ArrayList<>();
	}
	
	public Query(Query otherQuery, int cardinality){
		triples = new ArrayList<>();
		for(ByteString[] sequence: otherQuery.triples){
			triples.add(sequence.clone());
		}
		this.support = cardinality;
		this.hashSupport = cardinality;
		this.setFunctionalVariable(otherQuery.getFunctionalVariable());
		computeHeadKey();
		stdConfidence = -2.0;
		pcaConfidence = -2.0;
		parent = null;
		bodySize = -1;
		highestVariable = otherQuery.highestVariable;		
		stdConfUpperBound = 0.0;
		pcaConfUpperBound = 0.0;
		pcaEstimation = 0.0;
		pcaEstimationOptimistic = 0.0;
		containsQuasiBindings = false;
		ancestors = new ArrayList<>();
	}
	
	public Query(ByteString[] head, List<ByteString[]> body) {
		triples = new ArrayList<ByteString[]>();
		triples.add(head.clone());
		IntHashMap<ByteString> varsHistogram = new IntHashMap<>();
		highestVariable = 96; // The ascii code before lowercase 'a'
		if (FactDatabase.isVariable(head[0])) {
			varsHistogram.increase(head[0]);
			if (head[0].charAt(1) > highestVariable) {
				highestVariable = head[0].charAt(1);				
			}
		}
			
		
		if (FactDatabase.isVariable(head[2])) {
			varsHistogram.increase(head[2]);	
			if (head[2].charAt(1) > highestVariable) {
				highestVariable = head[2].charAt(1);
			}
		}
				
		for(ByteString[] atom : body) {
			triples.add(atom.clone());
			if (FactDatabase.isVariable(atom[0])) {
				varsHistogram.increase(atom[0]);
				if(atom[0].charAt(1) > highestVariable) {
					highestVariable = atom[0].charAt(1);
				}
			}
			if (FactDatabase.isVariable(atom[2])) {
				varsHistogram.increase(atom[2]);
				if (atom[2].charAt(1) > highestVariable) {
					highestVariable = atom[2].charAt(1);
				}	
			}			
		}
		
		computeHeadKey();
		functionalVariablePosition = 0;
		functionalVariable = triples.get(0)[functionalVariablePosition];
		stdConfidence = -2.0;
		pcaConfidence = -2.0;
		parent = null;
		bodySize = -1;
		stdConfUpperBound = 0.0;
		pcaConfUpperBound = 0.0;
		pcaEstimation = 0.0;
		pcaEstimationOptimistic = 0.0;
		++highestVariable;
		containsQuasiBindings = false;
		ancestors = new ArrayList<>();
	}

	/**
	 * Calculate a simple hash key based on the constant arguments of the
	 * head variables.
	 */
	private void computeHeadKey() {
		headKey = triples.get(0)[1].toString();
		if(!FactDatabase.isVariable(triples.get(0)[2]))
			headKey += triples.get(0)[2].toString();
		else if(!FactDatabase.isVariable(triples.get(0)[0]))
			headKey += triples.get(0)[0].toString();
	}
	
	public List<ByteString[]> getTriples() {
		return triples;
	}
	
	public List<ByteString[]> getRealTriples() {
		List<ByteString[]> resultList = new ArrayList<>();
		for (ByteString[] triple : triples) {
			if (!triple[1].equals(FactDatabase.DIFFERENTFROMbs)) {
				resultList.add(triple);
			}
		}
		
		return resultList;
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
	
	/**
	 * Returns a list with copies of the triples of the rule.
	 * @return
	 */
	public List<ByteString[]> getAntecedentClone() {
		List<ByteString[]> cloneList = new ArrayList<>();		
		for (ByteString[] triple : getAntecedent()) {
			cloneList.add(triple.clone());
		}
		
		return cloneList;
	}
 
	protected void setTriples(ArrayList<ByteString[]> triples) {
		this.triples = triples;
	}
	
	/**
	 * @return the mustBindVariables
	 */
	public List<ByteString> getOpenVariables() {
		IntHashMap<ByteString> histogram = variablesHistogram();
		List<ByteString> variables = new ArrayList<ByteString>();
		for (ByteString var : histogram) {
			if (histogram.get(var) < 2) {
				variables.add(var);
			}
		}
		
		return variables;
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
	public double getSupportRatio() {
		return supportRatio;
	}

	/**
	 * @param support the support to set
	 */
	public void setSupportRatio(double support) {
		this.supportRatio = support;
	}

	/**
	 * @return the headBodyCount
	 */
	public long getSupport() {
		return support;
	}
	
	/**
	 * The cardinality number used to hash the rule.
	 * @return
	 */
	public long getHashCardinality() {
		return hashSupport;
	}

	/**
	 * @param headBodyCount the headBodyCount to set
	 */
	public void setSupport(long cardinality) {
		this.support = cardinality;
	}

	/**
	 * The support of the body of the rule. If the rule has the 
	 * form B => r(x, y) then the body size is support(B).
	 * @return
	 */
	public long getBodySize() {
		return bodySize;
	}

	/**
	 * 
	 * @param bodySize
	 */
	public void setBodySize(long bodySize) {
		this.bodySize = bodySize;
	}

	/**
	 * @return the confidence
	 */
	public double getStdConfidence() {
		return stdConfidence;
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
	public void setStdConfidence(double confidence) {
		this.stdConfidence = confidence;
	}

	/**
	 * @return the pcaConfidence
	 */
	public double getPcaConfidence() {
		return pcaConfidence;
	}
	
	/**
	 * @param pcaConfidence the pcaConfidence to set
	 */
	public void setPcaConfidence(double improvedConfidence) {
		this.pcaConfidence = improvedConfidence;
	}

	public double getConfidenceRunningTime() {
		return confidenceRunningTime;
	}

	public void setConfidenceRunningTime(double confidenceRunningTime) {
		this.confidenceRunningTime = confidenceRunningTime;
	}

	public double getPcaConfidenceRunningTime() {
		return pcaConfidenceRunningTime;
	}

	public void setPcaConfidenceRunningTime(double pcaConfidenceRunningTime) {
		this.pcaConfidenceRunningTime = pcaConfidenceRunningTime;
	}

	public double getPcaEstimationOptimistic() {
		return pcaEstimationOptimistic;
	}

	public void setPcaEstimationOptimistic(double pcaEstimationOptimistic) {
		this.pcaEstimationOptimistic = pcaEstimationOptimistic;
	}

	/**
	 * Returns the last triple pattern added to this rule.
	 * @return
	 */
	public ByteString[] getLastTriplePattern(){
		if (triples.isEmpty()) {
			return null;
		} else {
			return triples.get(triples.size() - 1);
		}
	}
	
	/**
	 * Return the last triple pattern which is not the a pseudo-atom.
	 * @return
	 */
	public ByteString[] getLastRealTriplePattern() {
		if (triples.isEmpty()) {
			return null;
		} else {
			int i = triples.size() - 1;
			ByteString[] last = null;
			while (i >= 0){
				last = triples.get(i);
				if(!last[1].equals(FactDatabase.DIFFERENTFROMbs)) {
					break;
				}
				--i;
			}

			return last;
		}
	}
	
	/**
	 * Returns the number of times the relation occurs in the atoms
	 * of the query
	 * @return
	 */
	public int cardinalityForRelation(ByteString relation) {
		int count = 0;
		for (ByteString[] triple : triples) {
			if (triple[1].equals(relation))
				++count;
		}
		return count;
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
	
	public ByteString getFunctionalVariable(){
		return functionalVariable;
	}
	
	public int getFunctionalVariablePosition(){
		return functionalVariablePosition;
	}
	
	public int getNonFunctionalVariablePosition(){
		return functionalVariablePosition == 0 ? 2 : 0;
	}
	
	public ByteString getNonFunctionalVariable(){
		return triples.get(0)[getNonFunctionalVariablePosition()];
	}
	
	/**
	 * @param functionalVariable the functionalVariable to set
	 */
	public void setFunctionalVariable(ByteString projectionVariable) {
		this.functionalVariable = projectionVariable;
		if(this.functionalVariable.equals(triples.get(0)[0]))
			functionalVariablePosition = 0;
		else
			functionalVariablePosition = 2;
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
	
	/**
	 * It returns a list with all the redundant atoms contained in the first list, i.e., 
	 * atoms whose removal does not affect the results of the query defined in the second list. 
	 * @param test
	 * @param query
	 * @return
	 */
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
				if(isUnifiable(triples.get(j), triples.get(i)) 
						|| isUnifiable(triples.get(i), triples.get(j)))
					return true;
			}
		}
		
		return false;
	}

	/**
	 * Simple string representation of the rule. Check the methods getRuleString, getFullRuleString
	 * and getBasicRuleString.
	 */
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
		List<ByteString> variables = new ArrayList<ByteString>();
		for (ByteString[] triple : triples) {
			if (FactDatabase.isVariable(triple[0])) {
				if (!variables.contains(triple[0])) {
					variables.add(triple[0]);
				}
			}
			
			if (FactDatabase.isVariable(triple[2])) {
				if (!variables.contains(triple[2])) {
					variables.add(triple[2]);
				}
			}
		}
		
		return variables;
	}

	/**
	 * Determines if a pattern contains repeated components, which are considered hard to satisfy 
	 * (i.e., ?x somePredicate ?x)
	 * @return boolean
	 */
	public boolean containsRepeatedVariablesInLastPattern() {
		// TODO Auto-generated method stub
		ByteString[] triple = getLastTriplePattern();
		return triple[0].equals(triple[1]) || triple[0].equals(triple[2]) || triple[1].equals(triple[2]);
	}

	/**
	 * Returns true if the rule contains redudant recursive atoms, i.e., atoms with a relation that
	 * occurs more than once AND that do not have any effect on the query result.
	 * @return
	 */
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

	/**
	 * @return boolean True if the rule has atoms.
	 */
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return triples.isEmpty();
	}
	
	/**
	 * Returns a histogram with the number of different atoms variables 
	 * occur in. It discards pseudo-atoms containing the keywords DIFFERENTFROM
	 * and EQUALS.
	 * @return
	 */
	private IntHashMap<ByteString> variablesHistogram() {
		IntHashMap<ByteString> varsHistogram = new IntHashMap<>();
		for (ByteString triple[] : triples) {
			if (triple[1].equals(FactDatabase.DIFFERENTFROMbs))
				continue;
			
			if (FactDatabase.isVariable(triple[0])) {
				varsHistogram.increase(triple[0]);	
			}
			// Do not count twice if a variable occurs twice in the atom, e.g., r(x, x)
			if (!triple[0].equals(triple[2])) {
				if (FactDatabase.isVariable(triple[2])) {
					varsHistogram.increase(triple[2]);	
				}
			}
		}
		
		return varsHistogram;
	}
	
	/**
	 * @return boolean True if the rule is closed, i.e., each variable in
	 * the rule occurs at least in two atoms.
	 */
	public boolean isClosed() {
		if (triples.isEmpty()) {
			return false;
		}
		
		IntHashMap<ByteString> varsHistogram = variablesHistogram();
		
		for (ByteString variable : varsHistogram) {
			if (varsHistogram.get(variable) < 2) {
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * @return boolean. True if the rule has PCA confidence 1.0
	 */
	public boolean isPerfect() {
		return pcaConfidence == 1.0;
	}
	
	/**
	 * Return a key for the rule based on the constant arguments
	 * of the head atom. It can be used as a hash key.
	 * @return
	 */
	public String getHeadKey(){		
		if(headKey == null){
			computeHeadKey();
		}
		
		return headKey;
	}
	
	/**
	 * Returns the rule's head relation.
	 * @return
	 */
	public String getHeadRelation() {
		return triples.get(0)[1].toString();
	}
	
	/**
	 * Returns the number of atoms of the rule.
	 * @return
	 */
	public int getLength(){
		return triples.size();
	}
	
	/**
	 * Returns the number of atoms of the rule that are not pseudo-atoms
	 * Pseudo-atoms contain the Database keywords "DIFFERENTFROM" and "EQUALS"
	 * @return
	 */
	public int getRealLength() {
		int length = 0;
		for (ByteString[] triple : triples) {
			if (!triple[1].equals(FactDatabase.DIFFERENTFROMbs)) {
				++length;
			}
		}
		
		return length;
	}
	/**
	 * Returns the number of atoms of the rule that are not type constraints
	 * of the form rdf:type(?x, C) where C is a class, i.e., Person.
	 * @param typeString
	 * @return
	 */
	public int getLengthWithoutTypes(ByteString typeString){
		int size = 0;
		for(ByteString[] triple: triples){
			if(!triple[1].equals(typeString) 
					|| FactDatabase.isVariable(triple[2]))
				++size;
		}
		
		return size;
	}
	
	/**
	 * Returns the number of atoms of the rule that are neither type constraints
	 * of the form rdf:type(?x, C) or linksTo atoms.
	 * @param typeString
	 * @return
	 */
	public int getLengthWithoutTypesAndLinksTo(ByteString typeString, ByteString linksString){
		int size = 0;
		for(ByteString[] triple: triples){
			if((!triple[1].equals(typeString) || FactDatabase.isVariable(triple[2])) 
					&& !triple[1].equals(linksString))
				++size;
		}
		
		return size;
	}

	/**
	 * Sets the rule's parent rule.
	 * @param parent
	 */
	public void setParent(Query parent) {
		if (parent != null) {
			ancestors.add(parent);
		}
	}

	/**
	 * Returns a new rule that contains all the atoms of the current rule plus
	 * the atom provided as argument.
	 * @param newEdge The new atom.
	 * @param cardinality The support of the new rule.
	 * @param joinedVariable The position of the common variable w.r.t to the rule in the new atom, 
	 * i.e., 0 if the new atoms joins on the subject or 2 if it joins on the object. 
	 * @param danglingVariable The position of the fresh variable in the new atom.
	 * @return
	 */
	public Query addEdge(ByteString[] newEdge, 
			int cardinality, ByteString joinedVariable, ByteString danglingVariable) {
		Query newQuery = new Query(this, cardinality);
		ByteString[] copyNewEdge = newEdge.clone();
		newQuery.triples.add(copyNewEdge);
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
		return newQuery;
	}
	
	/**
	 * The alternative hash code for the parents of the rule. The alternative hash code
	 * if a small variante of the hashCode method that disregards the support of the rule.
	 * @return
	 */
	public int alternativeParentHashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((getHeadKey() == null) ? 0 : getHeadKey().hashCode());
		return result;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int)hashSupport;
		result = prime * result + (int)getRealLength();
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
		if (getHeadKey() == null) {
			if (other.getHeadKey() != null) {
				return false;
			}
		} else if (!getHeadKey().equals(other.getHeadKey())) {
			return false;
		}
		if (support != other.support) {
			return false;
		}
		
		return EquivalenceChecker2.equal(triples, other.triples);
	}
	
	public String getRuleString(){
		//Guarantee that atoms in rules are output in the same order across runs of the program
		class TripleComparator implements Comparator<ByteString[]>{
			public int compare(ByteString[] t1, ByteString[] t2){
				int predicateCompare = t1[1].toString().compareTo(t2[1].toString());
				if(predicateCompare == 0){
					int objectCompare = t1[2].toString().compareTo(t2[2].toString());
					if(objectCompare == 0){
						return t1[0].toString().compareTo(t2[0].toString());
					}					
					return objectCompare;
				}
				
				return predicateCompare;
			}
		}
		
		TreeSet<ByteString[]> sortedBody = new TreeSet<ByteString[]>(new TripleComparator());
		sortedBody.addAll(getAntecedent());						
		StringBuilder strBuilder = new StringBuilder();
		for(ByteString[] pattern: sortedBody){
			if (pattern[1].equals(FactDatabase.DIFFERENTFROMbs)) {
				continue;
			}
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
	
	public String getFullRuleString() {
		DecimalFormat df = new DecimalFormat("#.#########");
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(getRuleString());
		
		strBuilder.append("\t" + df.format(getSupportRatio()) );
		strBuilder.append("\t" + df.format(getHeadCoverage()));
		strBuilder.append("\t" + df.format(getStdConfidence()));
		strBuilder.append("\t" + df.format(getPcaConfidence()));
		strBuilder.append("\t" + getSupport());		
		strBuilder.append("\t" + getBodySize());
		strBuilder.append("\t" + getBodyStarSize());
		strBuilder.append("\t" + getFunctionalVariable());
		strBuilder.append("\t" + stdConfUpperBound);
		strBuilder.append("\t" + pcaConfUpperBound);
		strBuilder.append("\t" + pcaEstimation);
		strBuilder.append("\t" + pcaEstimationOptimistic);		
		strBuilder.append("\t" + confidenceRunningTime);
		strBuilder.append("\t" + pcaConfidenceRunningTime);

		return strBuilder.toString();
	}
	
	public String getBasicRuleString() {
		DecimalFormat df = new DecimalFormat("#.#########");
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(getRuleString());
		
		strBuilder.append("\t" + df.format(getSupportRatio()) );
		strBuilder.append("\t" + df.format(getHeadCoverage()));
		strBuilder.append("\t" + df.format(getStdConfidence()));
		strBuilder.append("\t" + df.format(getPcaConfidence()));
		strBuilder.append("\t" + getSupport());		
		strBuilder.append("\t" + getBodySize());
		strBuilder.append("\t" + getBodyStarSize());
		strBuilder.append("\t" + getFunctionalVariable());

		return strBuilder.toString();
	}
	
	public static String toDatalog(ByteString[] atom) {
		return atom[1].toString().replace("<", "").replace(">", "") + "(" + atom[0] + ", " + atom[2] + ")";
	}
	
	public String getDatalogString() {
		StringBuilder builder = new StringBuilder();
		
		builder.append(Query.toDatalog(getHead()));
		builder.append(" <=");
		for (ByteString[] atom : getBody()) {
			builder.append(" ");
			builder.append(Query.toDatalog(atom));
			builder.append(",");
		}
		
		if (builder.charAt(builder.length() - 1) == ',')
			builder.deleteCharAt(builder.length() - 1);
		
		return builder.toString();
	}

	public Query unify(int danglingPosition, ByteString constant, int cardinality) {
		Query newQuery = new Query(this, cardinality);
		ByteString[] lastNewPattern = newQuery.getLastTriplePattern();
		lastNewPattern[danglingPosition] = constant;
		newQuery.computeHeadKey();
		return newQuery;
	}
	
	/**
	 * Return a new query where the variable at position danglingPosition in triple at position triplePos 
	 * is bound to constant.
	 * @param triplePos
	 * @param danglingPosition
	 * @param constant
	 * @param cardinality
	 * @return
	 */
	public Query unify(int triplePos, int danglingPosition, ByteString constant, int cardinality) {
		Query newQuery = new Query(this, cardinality);
		ByteString[] targetEdge = newQuery.getTriples().get(triplePos);
		targetEdge[danglingPosition] = constant;
		newQuery.cleanInequalityConstraints();		
		return newQuery;
	}


	private void cleanInequalityConstraints() {
		List<ByteString[]> toRemove = new ArrayList<>();
		IntHashMap<ByteString> varHistogram = variablesHistogram();
		for(ByteString[] triple: triples){
			if(triple[1].equals(FactDatabase.DIFFERENTFROMbs)){
				int varPos = FactDatabase.firstVariablePos(triple);
				// Check if the variable became orphan
				if (!varHistogram.containsKey(triple[varPos])) {
					toRemove.add(triple);					
				}
			}
		}
		
		triples.removeAll(toRemove);
	}

	public List<Query> getAncestors(){
		return ancestors;
	}
	
	private void gatherAncestors(Query q, Set<Query> output) {		
		if (q.ancestors == null 
				|| q.ancestors.isEmpty()) {
			return;
		} else {
			// Let's do depth search
			for (Query ancestor : q.ancestors) {
				output.add(ancestor);
				gatherAncestors(ancestor, output);
			}
		}
	}
	
	public List<Query> getAllAncestors(){
		Set<Query> output = new LinkedHashSet<>();
		for (Query ancestor : ancestors) {
			output.add(ancestor);
			gatherAncestors(ancestor, output);
		}
		return new ArrayList<>(output);
	}

	public void setBodyMinusHeadSize(int size) {
		bodyMinusHeadSize = size;
	}
	
	public long getBodyMinusHeadSize(){
		return bodyMinusHeadSize;
	}

	public void setBodyStarSize(long size) {
		// TODO Auto-generated method stub
		bodyStarSize = size;
	}
	
	public long getBodyStarSize(){
		return bodyStarSize;
	}

	public boolean metricsCalculated() {
		return stdConfidence != -2.0 && pcaConfidence != -2.0;
	}

	public boolean isBelowMinimumSupport() {
		return belowMinimumSupport;
	}

	public void setBelowMinimumSupport(boolean belowMinimumSupport) {
		this.belowMinimumSupport = belowMinimumSupport;
	}
	
	public boolean containsQuasiBindings() {
		return this.containsQuasiBindings;
	}
	
	public void setContainsQuasibinding(boolean containsQuasiBindings) {
		this.containsQuasiBindings = containsQuasiBindings;
		
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
	
	public static int findFunctionalVariable(Query q, FactDatabase d){
		ByteString[] head = q.getHead();
		if(FactDatabase.numVariables(head) == 1) return FactDatabase.firstVariablePos(head); 
		return d.functionality(head[1]) > d.inverseFunctionality(head[1]) ? 0 : 2;
	}

	public static void printRuleHeaders() {
		System.out.println("Rule\tSupport\tHead Coverage\tConfidence\tPCA Confidence\tPositive Examples\tBody size\tPCA Body size\tPrediction variable\tStd. Lower Bound\tPCA Lower Bound");
	}

	public void setConfidenceUpperBound(double stdConfUpperBound) {
		this.stdConfUpperBound = stdConfUpperBound;
	}

	public void setPcaConfidenceUpperBound(double pcaConfUpperBound) {
		// TODO Auto-generated method stub
		this.pcaConfUpperBound = pcaConfUpperBound;
	}
	
	/**
	 * @return the pcaEstimation
	 */
	public double getPcaEstimation() {
		return pcaEstimation;
	}

	/**
	 * @param pcaEstimation the pcaEstimation to set
	 */
	public void setPcaEstimation(double pcaEstimation) {
		this.pcaEstimation = pcaEstimation;
	}
	
	/**
	 * For rules with an even number of atoms (n > 2), it checks if it contains
	 * level 2 redundant subgraphs, that is, each relation occurs exactly twice
	 * in the rule.
	 * @return
	 */
	public boolean containsLevel2RedundantSubgraphs() {
		if(!isClosed() || triples.size() < 4 || triples.size() % 2 == 1) {
			return false;
		}
		
		IntHashMap<ByteString> relationCardinalities = new IntHashMap<>();
		for (ByteString[] pattern : triples) {
			relationCardinalities.increase(pattern[1]);
		}
		
		for (ByteString relation : relationCardinalities) {
			if (relationCardinalities.get(relation) != 2) {
				return false;
			}
		}
		
		return true;
	}

	public boolean containsDisallowedDiamond(){
		if(!isClosed() || triples.size() < 4 || triples.size() % 2 == 1) 
			return false;		
		
		// Calculate the relation count
		HashMap<ByteString, List<ByteString[]>> subgraphs = new HashMap<ByteString, List<ByteString[]>>();		
		for(ByteString[] pattern : triples){
			List<ByteString[]> subgraph = subgraphs.get(pattern[1]);
			if(subgraph == null){
				subgraph = new ArrayList<ByteString[]>();
				subgraphs.put(pattern[1], subgraph);
				if(subgraphs.size() > 2)
					return false;
			}			
			subgraph.add(pattern);
			
		}
		
		if(subgraphs.size() != 2)
			return false;
		
		Object[] relations = subgraphs.keySet().toArray();
		List<int[]> joinInfoList = new ArrayList<int[]>();
		for(ByteString[] p1: subgraphs.get(relations[0])){
			int[] bestJoinInfo = null;
			int bestCount = -1;
			ByteString[] bestMatch = null;
			for(ByteString[] p2: subgraphs.get(relations[1])){
				int[] joinInfo = Query.doTheyJoin(p1, p2);
				if(joinInfo != null){
					int joinCount = joinCount(joinInfo);
					if(joinCount > bestCount){
						bestCount = joinCount;
						bestJoinInfo = joinInfo;
						bestMatch = p2;
					}
				}
			}
			subgraphs.get(relations[1]).remove(bestMatch);			
			joinInfoList.add(bestJoinInfo);
		}
		
		int[] last = joinInfoList.get(0);
		for(int[] joinInfo: joinInfoList.subList(1, joinInfoList.size())){
			if(!Arrays.equals(last, joinInfo) || (last[1] == 1 && joinInfo[1] == last[1]))
				return false;
		}
		
		return true;
	}
	
	private int joinCount(int[] vector){
		int count = 0;
		for(int v: vector)
			count += v;
		
		return count;
	}

	private static int[] doTheyJoin(ByteString[] p1, ByteString[] p2) {
		int subPos = FactDatabase.varpos(p1[0], p2);
		int objPos = FactDatabase.varpos(p1[2], p2);
		
		if(subPos != -1 || objPos != -1){
			int[] result = new int[3];
			result[0] =  (subPos == 0 ? 1 : 0); //subject-subject
			result[1] =  (subPos == 2 ? 1 : 0); 
			result[1] += (objPos == 0 ? 1 : 0); //subject-object
			result[2] =  (objPos == 2 ? 1 : 0);
			return result;
		}else{
			return null;
		}
	}
	
	/**
	 * Applies the mappings provided as first argument to the subject and object
	 * positions of the query included in the second argument.
	 * @param query
	 * @param mappings
	 */
	public static void bind(Map<ByteString, ByteString> mappings, 
			List<ByteString[]> inputTriples) {
		for (ByteString[] triple : inputTriples) {
			ByteString binding = mappings.get(triple[0]);
			if (binding != null) {
				triple[0] = binding;
			}
			binding = mappings.get(triple[2]);
			if (binding != null) {
				triple[2] = binding;
			}
		}
	}

	/**
	 * Replaces all occurrences of oldVal with newVal in the subject and object
	 * positions of the input query.
	 * @param oldVal
	 * @param newVal
	 * @param query
	 */
	public static void bind(ByteString oldVal, 
			ByteString newVal, List<ByteString[]> query) {
		for (ByteString[] triple : query) {
			if (triple[0].equals(oldVal)) {
				triple[0] = newVal;
			}

			if (triple[2].equals(oldVal)) {
				triple[2] = newVal;
			}
		}
	}

	/**
	 * Verifies if the given rule has higher confidence that its parent rules. The parent
	 * rules are those rules that were refined in previous stages of the AMIE algorithm
	 * and led to the construction of the current rule.
	 * @return true if the rule has better confidence that its parent rules.
	 */
	public boolean hasConfidenceGain() {
		if (isClosed()){
			if (parent != null && parent.isClosed()) {
				return getPcaConfidence() > parent.getPcaConfidence();
			} else {
				return true;
			}
		} else {
			return false;
		}
	}
	
	/**
	 * It returns the query expression corresponding to the normalization value
	 * used to calculate the PCA confidence.
	 * @return
	 */
	public List<ByteString[]> getPCAQuery() {
		if (isEmpty()) {
			return Collections.emptyList();
		}
		
		List<ByteString[]> newTriples = new ArrayList<>();
		for (ByteString[] triple : triples) {
			newTriples.add(triple.clone());
		}
		ByteString[] existentialTriple = newTriples.get(0);
		existentialTriple[getNonFunctionalVariablePosition()] = ByteString.of("?x");
		return newTriples;
	}

	/**
	 * Given a list of rules A1 => X1, ... An => Xn, having the same head relation, it returns the combined rule
	 * A1,..., An => X', where X' is the most specific atom. For example given the rules A1 => livesIn(x, y) and 
	 * A2 => livesIn(x, USA), the method returns A1, A2 => livesIn(x, USA). 
	 * @param rules
	 * @return
	 */
	public static Query combineRules(List<Query> rules) {
		// Look for the most specific head
		Query canonicalHeadRule = rules.get(0);
		for (int i = 0; i < rules.size(); ++i) {
			int nVariables = FactDatabase.numVariables(rules.get(i).getHead());
			if (nVariables == 1) {
				canonicalHeadRule = rules.get(i);
			}
		}
		
		// We need to rewrite the rules	
		ByteString[] canonicalHead = canonicalHeadRule.getHead().clone();
		ByteString canonicalSubjectExp = canonicalHead[0];
		ByteString canonicalObjectExp = canonicalHead[2];
		Set<ByteString> nonHeadVariables = new LinkedHashSet<>();
		int varCount = 0;
		List<ByteString[]> commonAntecendent = new ArrayList<>();
		
		for (Query rule : rules) {
			List<ByteString[]> antecedentClone = rule.getAntecedentClone();
			
			Set<ByteString> otherVariables = rule.getNonHeadVariables();
			for (ByteString var : otherVariables) {
				Query.bind(var, ByteString.of("?v" + varCount), antecedentClone);
				++varCount;
				nonHeadVariables.add(var);
			}
			
			ByteString[] head = rule.getHead();
			Map<ByteString, ByteString> mappings = new HashMap<>();
			mappings.put(head[0], canonicalSubjectExp);
			mappings.put(head[2], canonicalObjectExp);
			Query.bind(mappings, antecedentClone);
			
			commonAntecendent.addAll(antecedentClone);
		}
		
		Query resultRule = new Query(canonicalHead, commonAntecendent);
		return resultRule;
	}

	/**
	 * The set of variables that are not in the conclusion of the rule.
	 */
	private Set<ByteString> getNonHeadVariables() {
		ByteString[] head = getHead();
		Set<ByteString> nonHeadVars = new LinkedHashSet<>();
		for (ByteString[] triple : getAntecedent()) {
			if (FactDatabase.isVariable(triple[0]) 
					&& FactDatabase.varpos(triple[0], head) == -1) {
				nonHeadVars.add(triple[0]);
			}
			
			if (FactDatabase.isVariable(triple[2]) 
					&& FactDatabase.varpos(triple[2], head) == -1) {
				nonHeadVars.add(triple[2]);
			}
		}
		
		return nonHeadVars;
	}

	public boolean containsRelation(ByteString relation) {
		for (ByteString[] triple : triples) {
			if (triple[1].equals(relation)) {
				return true;
			}
		}
		
		return false;
	}
	
	public int containsRelationTimes(ByteString relation) {
		int count = 0;
		for (ByteString[] triple : triples) {
			if (triple[1].equals(relation)) {
				++count;
			}
		}	
		return count;
	}
	
	/**
	 * Given the antecedent and the succedent of a rule as sets of atoms, it
	 * returns the combinations of atoms of size 'i' that are "parents" of the current
	 * rule, i.e., subsets of atoms of the original rule.
	 * @param queryPattern
	 * @param i
	 */
	public static void getParentsOfSize(List<ByteString[]> antecedent, 
			ByteString[] head,
			int windowSize, List<List<ByteString[]>> parentsOfSizeI) {
		int fixedSubsetSize = windowSize - 1;
		if (antecedent.size() < windowSize) {
			return;
		}
		List<ByteString[]> fixedPrefix = antecedent.subList(0, fixedSubsetSize);
		for (int i = fixedSubsetSize; i < antecedent.size(); ++i) {
			List<ByteString[]> combination = new ArrayList<>();
			// Add the head atom.
			combination.add(head);
			combination.addAll(fixedPrefix);
			combination.add(antecedent.get(i));
			parentsOfSizeI.add(combination);
		}
		if (windowSize > 1) {
			getParentsOfSize(antecedent.subList(1, antecedent.size()), head, windowSize, parentsOfSizeI);
		}
	}
	
	public static void main(String args[]) {
/*		List<ByteString[]> query = FactDatabase.triples(
				FactDatabase.triple("?a", "<soc_sec_id>", "?l"),
				FactDatabase.triple("?b", "<soc_sec_id>", "?l"),
				FactDatabase.triple("?a", "<surname>", "?f"), 
				FactDatabase.triple("?b", "<surname>", "?f")
				);
		
		List<List<ByteString[]>> result = new ArrayList<List<ByteString[]>>();

		for (int i = 1; i <= 3; ++i) {
			System.out.println("i = " + i);
			getParentsOfSize(query, FactDatabase.triple("?a", "equals", "?b"), i, result);
			for (List<ByteString[]> parent : result) {
				System.out.print(FactDatabase.toString(parent) + ", ");
			}
			System.out.println();
			result.clear();
		}*/
		
		List<ByteString[]> antecedent = FactDatabase.triples(
				FactDatabase.triple("?z3", "<surname>", "?b"),
				FactDatabase.triple("?z1", "<rp>", "?a"),
				FactDatabase.triple("?z1", "<rp>", "?z2"),
				FactDatabase.triple("?z2", "<rpp>", "?z3")
				);
		
		ByteString[] head = FactDatabase.triple("?a", "<rh>", "?b");
		Query query = new Query(head, antecedent);
		System.out.println(query.containsSinglePath());
		List<ByteString[]> path = query.getCanonicalPath();
		System.out.println(FactDatabase.toString(path));
		
		List<ByteString[]> antecedent2 = FactDatabase.triples(
				FactDatabase.triple("?z3", "<surname>", "?b"),
				FactDatabase.triple("?b", "<rp>", "?a"),
				FactDatabase.triple("?z1", "<rp>", "?z2"),
				FactDatabase.triple("?z2", "<rpp>", "?z3")
				);
		
		ByteString[] head2 = FactDatabase.triple("?a", "<rh>", "?b");
		Query query2 = new Query(head2, antecedent2);
		System.out.println(query2.containsSinglePath());
	}

	/**
	 * It determines whether the rule contains a single path that connects the head variables
	 * in the body.
	 * For example, the rule:
	 * - worksAt(x, w), locatedInCity(w, z), locatedInCountry(z, y) =>  livesIn(x, y)
	 * meets such criteria because there is a single path of variables that connects the head variables
	 * in the body: x -> w -> z -> y. 
	 * 
	 * @return
	 */
	public boolean containsSinglePath() {
		if (getHeadVariables().size() < 2) {
			// We are not interested in rules with a constant in the head.
			return false;
		}
		IntHashMap<ByteString> histogram = variablesHistogram();
		for (ByteString variable : getHeadVariables()) {
			if (histogram.get(variable) != 2) {
				return false;
			}
		}
		
		return true;
	}

	/**
	 * Returns an array with the variables that occur in the body but not in the head.
	 * @return
	 */
	private Set<ByteString> getBodyVariables() {
		List<ByteString> headVariables = getHeadVariables();
		Set<ByteString> result = new LinkedHashSet<>();
		for (ByteString[] triple : getBody()) {
			if (FactDatabase.isVariable(triple[2]) 
					&& !headVariables.contains(triple[2])) {
				result.add(triple[2]);
			}
			
			if (FactDatabase.isVariable(triple[2]) 
					&& !headVariables.contains(triple[2])) {
				result.add(triple[2]);
			}
		}
		return result;
	}

	/**
	 * Returns the head variables of the rule.
	 * @return
	 */
	private List<ByteString> getHeadVariables() {
		List<ByteString> headVariables = new ArrayList<>();
		ByteString[] head = getHead();
		if (FactDatabase.isVariable(head[0])) {
			headVariables.add(head[0]);
		}
		if (FactDatabase.isVariable(head[2])) {
			headVariables.add(head[2]);
		}
		return headVariables;
	}

	/**
	 * Given a rule that contains a single variables path for the head variables in the body 
	 * (the method containsSinglePath returns true), it returns the atoms sorted so that 
	 * the path can be reproduced.
	 * @return
	 */
	public List<ByteString[]> getCanonicalPath() {
		// First check the most functional variable
		ByteString funcVar = getFunctionalVariable();
		ByteString nonFuncVar = getNonFunctionalVariable();
		List<ByteString[]> body = getBody();
		MultiMap<ByteString, ByteString[]> variablesToAtom= new MultiMap<>();
		List<ByteString[]> path = new ArrayList<>();
		// Build a multimap, variable -> {atoms where the variable occurs}
		for (ByteString[] bodyAtom : body) {
			if (FactDatabase.isVariable(bodyAtom[0])) {
				variablesToAtom.put(bodyAtom[0], bodyAtom);
			}
			
			if (FactDatabase.isVariable(bodyAtom[2])) {
				variablesToAtom.put(bodyAtom[2], bodyAtom);
			}
		}
		
		// Start with the functional variable.
		ByteString joinVariable = funcVar;
		ByteString[] lastAtom = null;
		while (true) {
			List<ByteString[]> atomsList = variablesToAtom.getAsList(joinVariable);
			// This can be only the head variable
			if (atomsList.size() == 1) {
				lastAtom = atomsList.get(0);				
			} else {
				for (ByteString[] atom : atomsList) {
					if (atom != lastAtom) {
						// Bingo
						lastAtom = atom;
						break;
					}
				}
			}
			path.add(lastAtom);
			joinVariable = 
					lastAtom[0].equals(joinVariable) ? lastAtom[2] : lastAtom[0];
			if (joinVariable.equals(nonFuncVar)) {
				break;
			}				
		}
		
		return path;
	}
}