package amie.mining.assistant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Pair;
import amie.data.FactDatabase;
import amie.mining.ConfidenceMetric;
import amie.query.Query;

/**
 * Simpler miner assistant which implements all the logic required 
 * to mine conjunctive rules from a RDF datastore.
 * 
 * @author lgalarra
 *
 */
public class MiningAssistant{
	
	/**
	 * Maximum number of times a relation can appear in 
	 * a rule.
	 */
	protected int recursivityLimit = 3;
	
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
	protected long totalObjectCount;

	/**
	 * Number of different subjects in the underlying dataset
	 */
	protected long totalSubjectCount;
	
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
	protected double minStdConfidence;
	
	/**
	 * Minimum confidence
	 */
	protected double minPcaConfidence;
	
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
	protected HashMap<String, Long> headCardinalities;

	/**
	 * Allow constants for refinements
	 */
	protected boolean allowConstants;
	
	/**
	 * Enforce constants in all atoms of rules
	 */
	protected boolean enforceConstants;
	
	/**
	 * List of excluded relations for the body of rules;
	 */
	protected Collection<ByteString> bodyExcludedRelations;
	
	/**
	 * List of excluded relations for the head of rules;
	 */
	protected Collection<ByteString> headExcludedRelations;
	
	/**
	 * List of target relations for the body of rules;
	 */
	protected Collection<ByteString> bodyTargetRelations;

	/**
	 * Count directly on subject or use functional information
	 */
	protected boolean countAlwaysOnSubject;
	
	/**
	 * Use a functionality vs suggested functionality heuristic to prune low confident rule upfront.
	 */
	protected boolean enabledFunctionalityHeuristic;
	
	/**
	 * Enable confidence and PCA confidence upper bounds for pruning when given a confidence threshold
	 */
	protected boolean enabledConfidenceUpperBounds;
	
	/**
	 * If true, the assistant will output minimal debug information
	 */
	protected boolean silent;
	
	/**
	 * If true, use an optimistic approach to estimate PCA confidence
	 */
	protected boolean pcaOptimistic;
	
	/**
	 * If true, the assistant will never add atoms of the form type(x, y), i.e., it will always bind 
	 * the second argument to a type.
	 */
	protected boolean avoidUnboundTypeAtoms;
	
	/**
	 * If false, the assistant will not exploit the maximum length restriction to improve
	 * runtime. 
	 */
	protected boolean exploitMaxLengthOption;
	
	/**
	 * Enable query rewriting to optimize runtime.
	 */
	protected boolean enableQueryRewriting;
	
	/**
	 * Enable perfect rule pruning, i.e., do not further specialize rules with PCA confidence
	 * 1.0.
	 */
	protected boolean enablePerfectRules;
	
	/**
	 * Confidence metric used to assess the quality of rules.
	 */
	protected ConfidenceMetric confidenceMetric;
	
	
	/**
	 * @param dataSource
	 */
	public MiningAssistant(FactDatabase dataSource) {
		this.source = dataSource;
		this.minStdConfidence = 0.0;
		this.minPcaConfidence = 0.0;
		this.maxDepth = 3;
		this.allowConstants = false;
		ByteString[] rootPattern = Query.fullyUnboundTriplePattern1();
		List<ByteString[]> triples = new ArrayList<ByteString[]>();
		triples.add(rootPattern);
		this.totalSubjectCount = this.source.countDistinct(rootPattern[0], triples);
		this.totalObjectCount = this.source.countDistinct(rootPattern[2], triples);
		this.typeString = ByteString.of("rdf:type");
		this.subPropertyString = ByteString.of("rdfs:subPropertyOf");
		this.headCardinalities = new HashMap<String, Long>();
		ByteString[] subclassPattern = Query.fullyUnboundTriplePattern1();
		subclassPattern[1] = subPropertyString;
		this.subclassQuery = new Query(subclassPattern, 0);
		this.countAlwaysOnSubject = false;
		this.silent = false;
		this.pcaOptimistic = false;
		this.exploitMaxLengthOption = true;
		this.enableQueryRewriting = true;
		this.confidenceMetric = ConfidenceMetric.PCAConfidence;
		buildRelationsDictionary();
		
	}	
	
	private void buildRelationsDictionary() {
		Collection<ByteString> relations = source.getRelations();
		for (ByteString relation : relations) {
			ByteString[] query = FactDatabase.triple(ByteString.of("?x"), relation, ByteString.of("?y"));
			long relationSize = source.count(query);
			headCardinalities.put(relation.toString(), relationSize);
		}
	}

	public int getRecursivityLimit() {
		return recursivityLimit;
	}

	public void setRecursivityLimit(int recursivityLimit) {
		this.recursivityLimit = recursivityLimit;
	}

	public long getTotalCount(Query candidate){
		if(countAlwaysOnSubject){
			return totalSubjectCount;
		}else{
			return getTotalCount(candidate.getFunctionalVariablePosition());
		}
	}
	
	/**
	 * Returns the total number of subjects in the database.
	 * @return
	 */
	public long getTotalSubjectCount(){
		return totalSubjectCount;
	}
		
	public long getTotalObjectCount() {
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
	 * @return the minStdConfidence
	 */
	public double getMinConfidence() {
		return minStdConfidence;
	}

	/**
	 * @return the pcaOptimistic
	 */
	public boolean isPcaOptimistic() {
		return pcaOptimistic;
	}

	/**
	 * @param pcaOptimistic the pcaOptimistic to set
	 */
	public void setPcaOptimistic(boolean pcaOptimistic) {
		this.pcaOptimistic = pcaOptimistic;
	}

	/**
	 * @return the minPcaConfidence
	 */
	public double getMinImprovedConfidence() {
		return minPcaConfidence;
	}

	/**
	 * @param minPcaConfidence the minPcaConfidence to set
	 */
	public void setMinPcaConfidence(double minImprovedConfidence) {
		this.minPcaConfidence = minImprovedConfidence;
	}

	/**
	 * @param minStdConfidence the minStdConfidence to set
	 */
	public void setMinStdConfidence(double minConfidence) {
		this.minStdConfidence = minConfidence;
	}
	
	public FactDatabase getSchemaSource(){
		return schemaSource;
	}
	
	public void setSchemaSource(FactDatabase schemaSource) {
		// TODO Auto-generated method stub
		this.schemaSource = schemaSource;
	}

	public boolean registerHeadRelation(Query query){		
		return headCardinalities.put(query.getHeadRelation(), new Long(query.getSupport())) == null;		
	}
	
	public long getHeadCardinality(Query query){
		return headCardinalities.get(query.getHeadRelation()).longValue();
	}
	
	public long getRelationCardinality(String relation) {
		return headCardinalities.get(relation);
	}
	
	public long getRelationCardinality(ByteString relation) {
		return headCardinalities.get(relation.toString());
	}

	protected Set<ByteString> getSubClasses(ByteString className){
		ByteString[] lastPattern = subclassQuery.getTriples().get(0);
		ByteString tmpVar = lastPattern[2];
		lastPattern[2] = className;		
		Set<ByteString> result = source.selectDistinct(lastPattern[0], subclassQuery.getTriples());
		lastPattern[2] = tmpVar;
		return result;
	}
	
	protected boolean canAddInstantiatedAtom() {
		return allowConstants || enforceConstants;
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
			long cardinality = source.countDistinct(countingVariable, query.getTriples());
			
			ByteString[] succedent = newEdge.clone();
			Query candidate = new Query(succedent, cardinality);
			candidate.setFunctionalVariable(countingVariable);
			registerHeadRelation(candidate);			

			if(canAddInstantiatedAtom()) {
				getInstantiatedEdges(candidate, null, candidate.getLastTriplePattern(), countVarPos == 0 ? 2 : 0, minCardinality, output);
			}
			
			if (!enforceConstants) {
				output.add(candidate);
			}
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
				if(headExcludedRelations != null && headExcludedRelations.contains(newEdge[1]))
					continue;
				
				
				long cardinality = relations.get(relation);
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
					candidate.setFunctionalVariable(succedent[countVarPos]);
					registerHeadRelation(candidate);					
					if(canAddInstantiatedAtom()) {
						getInstantiatedEdges(candidate, null, candidate.getLastTriplePattern(), countVarPos == 0 ? 2 : 0, minCardinality, output);
					}
					
					if (!enforceConstants) {
						output.add(candidate);
					}
				}
			}			
			query.getTriples().remove(0);
		}else{
			//General case
			if(!testLength(query))
				return;
			
			if (exploitMaxLengthOption) {
				if(query.getRealLength() == maxDepth - 1){
					if(!query.getOpenVariables().isEmpty() && !allowConstants){
						return;
					}
				}
			}
			
			List<ByteString> joinVariables = null;
			
			//Then do it for all values
			if(query.isClosed()){
				joinVariables = query.getVariables();
			}else{
				joinVariables = query.getOpenVariables();
			}

			int nPatterns = query.getTriples().size();
			ByteString originalRelationVariable = newEdge[1];		
			
			for(int joinPosition = 0; joinPosition <= 2; joinPosition += 2){
				ByteString originalFreshVariable = newEdge[joinPosition];
				
				for(ByteString joinVariable: joinVariables){					
					newEdge[joinPosition] = joinVariable;
					query.getTriples().add(newEdge);
					IntHashMap<ByteString> promisingRelations = source.frequentBindingsOf(newEdge[1], query.getFunctionalVariable(), query.getTriples());
					query.getTriples().remove(nPatterns);
					
					int danglingPosition = (joinPosition == 0 ? 2 : 0);
					boolean boundHead = !FactDatabase.isVariable(query.getTriples().get(0)[danglingPosition]);
					for(ByteString relation: promisingRelations){
						if(bodyExcludedRelations != null && bodyExcludedRelations.contains(relation))
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
							
							candidate.setHeadCoverage((double)candidate.getSupport() / headCardinalities.get(candidate.getHeadRelation()));
							candidate.setSupportRatio((double)candidate.getSupport() / (double)getTotalCount(candidate));
							candidate.setParent(query);							
							if(canAddInstantiatedAtom()) {
								getInstantiatedEdges(candidate, candidate, candidate.getLastTriplePattern(), danglingPosition, minCardinality, output);
							}
							
							if (!enforceConstants) {
								output.add(candidate);
							}
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
	
	/**
	 * It computes the standard and the PCA confidence of a given rule. It assumes
	 * the rule's cardinality (absolute support) is known.
	 * @param candidate
	 */
	public void calculateConfidenceMetrics(Query candidate) {		
		computeStandardConfidence(candidate);
		computePCAConfidence(candidate);
	}

	/**
	 * Returns all candidates obtained by binding two values
	 * @param currentNode
	 * @param minCardinality
	 * @param omittedVariables
	 * @return
	 */
	public void getCloseCircleEdges(Query query, int minCardinality, Collection<Query> output){
		if (enforceConstants) {
			return;
		}
		
		int nPatterns = query.getTriples().size();

		if(query.isEmpty())
			return;
		
		if(!testLength(query))
			return;
		
		List<ByteString> sourceVariables = null;
		List<ByteString> allVariables = query.getVariables();
		List<ByteString> openVariables = query.getOpenVariables();
		
		if(query.isClosed()){
			sourceVariables = query.getVariables();
		}else{
			sourceVariables = openVariables; 
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
						IntHashMap<ByteString> promisingRelations = source.frequentBindingsOf(newEdge[1], query.getFunctionalVariable(), query.getTriples());
						query.getTriples().remove(nPatterns);
						
						for(ByteString relation: promisingRelations){
							if(bodyExcludedRelations != null && bodyExcludedRelations.contains(relation))
								continue;
							
							//Here we still have to make a redundancy check
							int cardinality = promisingRelations.get(relation);
							newEdge[1] = relation;
							if(cardinality >= minCardinality){										
								Query candidate = query.closeCircle(newEdge, cardinality);
								if(!candidate.isRedundantRecursive()){
									candidate.setHeadCoverage((double)cardinality / (double)headCardinalities.get(candidate.getHeadRelation()));
									candidate.setSupportRatio((double)cardinality / (double)getTotalCount(candidate));
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
	
	/**
	 * Check whether the rule meets the length criteria stored in the assiatant object
	 * @param candidate
	 * @return
	 */
	protected boolean testLength(Query candidate){
		return candidate.getRealLength() < maxDepth;
	}
	
	/**
	 * It computes the confidence upper bounds and approximations for the rule sent as argument.
	 * @param candidate
	 * @return True If neither the confidence bounds nor the approximations are aplicable or if they
	 * did not find enough evidence to discard the rule.
	 */
	public boolean calculateConfidenceBoundsAndApproximations(Query candidate) {		
		if(enabledConfidenceUpperBounds){
			if (!calculateConfidenceBounds(candidate)) {
				return false;
			}
		}

		if (enabledFunctionalityHeuristic) {
			int realLength = candidate.getRealLength();
			if(realLength == 3) {
				return calculateConfidenceApproximationFor3Atoms(candidate);
			} else if (realLength > 3) {
				return calculateConfidenceApproximationForGeneralCase(candidate);
			}
				
		}
		
		return true;
	}
	
	/**
	 * It computes the confidence bounds for rules
	 * @param candidate
	 * @return boolean True if the confidence bounds are not applicable or they cannot
	 * find enough evidence to discard the rule.
	 */
	private boolean calculateConfidenceBounds(Query candidate) {
		if (candidate.getRealLength() != 3) {
			return true;
		}
		
		int[] hardQueryInfo = null;
		hardQueryInfo = source.identifyHardQueryTypeI(candidate.getAntecedent());
		if(hardQueryInfo != null){
			double pcaConfUpperBound = getPcaConfidenceUpperBound(candidate);			
			if(pcaConfUpperBound < this.minPcaConfidence){
				if (!silent) {
					System.err.println("Query " + candidate + " discarded by PCA confidence upper bound " + pcaConfUpperBound);			
				}
				return false;
			}
			
			double stdConfUpperBound = getConfidenceUpperBound(candidate);			
			
			if(stdConfUpperBound < this.minStdConfidence){
				if (!silent) {
					System.err.println("Query " + candidate + " discarded by standard confidence upper bound " + stdConfUpperBound);
				}
				return false;
			}

			candidate.setConfidenceUpperBound(stdConfUpperBound);
			candidate.setPcaConfidenceUpperBound(pcaConfUpperBound);
		}
		
		return true;
	}

	/**
	 * Given a rule with more than 3 atoms and a single path connecting the head variables, 
	 * it computes a confidence approximation.
	 * @param candidate
	 * @return boolean True if the approximation is not applicable or produces a value 
	 * above the confidence thresholds, i.e., there is not enough evidence to drop the rule.
	 */
	protected boolean calculateConfidenceApproximationForGeneralCase(
			Query candidate) {
		// First identify whether the rule is a single path rule
		if (!candidate.containsSinglePath()) {
			// The approximation is not applicable.
			return true;
		}
		double denominator = 1.0;
		// If the approximation is applicable, let's reorder the atoms in the canonical way
		List<ByteString[]> path = candidate.getCanonicalPath();
		//System.out.println("======================");
		//System.out.println("Rule " + candidate.getRuleString());
		// Let's calculate the first term.
		ByteString r1 = path.get(0)[1];
		ByteString rh = candidate.getHead()[1];
		int[] joinInformation = Query.joinPositions(path.get(0), candidate.getHead());
		// If r1 is not functional or it is not joining from the subject, we replace it with the corresponding inverse relation.
		boolean relationRewritten = joinInformation[0] != 0;		
		double funr1 = this.source.functionality(r1, relationRewritten);
		double overlap = 0.0;
		overlap = computeOverlap(joinInformation, r1, rh);
		// The first part of the formula
		denominator = denominator * (overlap / funr1);
		//System.out.println("overlap(" + r1 + ", " + rh + ") = " + overlap);
		//System.out.println("fun(" + r1 + " (" + relationRewritten + ")" + ") = " + funr1);
		
		// Now iterate
		for (int i = 1; i < path.size(); ++i) {
			ByteString ri = path.get(i)[1];
			ByteString ri_1 = path.get(i - 1)[1];
			joinInformation = Query.joinPositions(path.get(i - 1), path.get(i));
			// Inverse r_{i-1} if it is not functional or it joins from the subject.
			boolean rewriteRi = joinInformation[1] != 0;
			double rng = 0.0;
			double funri = this.source.functionality(ri, rewriteRi);
			double ifunri = this.source.inverseFunctionality(ri, rewriteRi);
			
			rng = this.source.relationColumnSize(ri_1, joinInformation[0]);
			//System.out.println("|range(" + ri_1 + " (" + rewriteRi_1 + ")" + ")| = " + rng);
			
			overlap = computeOverlap(joinInformation, ri_1, ri);
			//System.out.println(Arrays.toString(joinInformation) + " overlap(" + ri_1 + ", " + ri + ") = " + overlap);
			//System.out.println("fun(" + ri + "("+ rewriteRi +")" + ") = " + funri);
			//System.out.println("ifun(" + ri + "("+ rewriteRi +")" + ") = " + ifunri);			
			double term = (overlap * ifunri) / (rng * funri); 
			denominator = denominator * term;
		}
		//System.out.println("denom=" + denominator);
		//System.out.println("======================");
		
		double estimatedPCA = (double)candidate.getSupport() / denominator;
		candidate.setPcaEstimation(estimatedPCA);
		if (estimatedPCA < this.minPcaConfidence) {
			if (!this.silent) {
				System.err.println("Query " + candidate + " discarded by functionality heuristic with ratio " + estimatedPCA);
			}							
			return false;
		}
		
		return true;
	}

	/**
	 * Given two relations and the positions at which they join, it returns the number 
	 * of entities in the overlap of such positions.
	 * @param joinInformation
	 * @param r1
	 * @param rh
	 * @return
	 */
	private double computeOverlap(int[] jinfo, ByteString r1, ByteString r2) {
		if (jinfo[0] == 0 && jinfo[1] == 0) {
			return this.source.overlap(r1, r2, FactDatabase.SUBJECT2SUBJECT);
		} else if (jinfo[0] == 2 && jinfo[1] == 2) {
			return this.source.overlap(r1, r2, FactDatabase.OBJECT2OBJECT);
		} else if (jinfo[0] == 0 && jinfo[1] == 2) {
			return this.source.overlap(r1, r2, FactDatabase.SUBJECT2OBJECT);
		} else if (jinfo[0] == 2 && jinfo[1] == 0) {
			return this.source.overlap(r2, r1, FactDatabase.SUBJECT2OBJECT);
		} else {
			return 0.0;
		}
	}

	/**
	 * Calculate the confidence approximation of the query for the case when 
	 * the rule has exactly 3 atoms.
	 * @param candidate
	 * @return boolean True if the approximation is not applicable or produces a value above the confidence thresholds, i.e.,
	 * there is not enough evidence to drop the rule.
	 */
	protected boolean calculateConfidenceApproximationFor3Atoms(Query candidate) {
		int[] hardQueryInfo = null;
		double headFunctionality = source.x_functionality(candidate.getHead()[1], candidate.getFunctionalVariablePosition());		
		hardQueryInfo = source.identifyHardQueryTypeIII(candidate.getAntecedent());
		if(hardQueryInfo != null){
			ByteString[] targetPatternOutput = null;
			ByteString[] targetPatternInput = null; //Atom with the projection variable
			ByteString[] p1, p2;
			p1 = candidate.getAntecedent().get(hardQueryInfo[2]);
			p2 = candidate.getAntecedent().get(hardQueryInfo[3]);
			int posCommonInput = hardQueryInfo[0];
			int posCommonOutput = hardQueryInfo[1];					
			if (FactDatabase.varpos(candidate.getFunctionalVariable(), p1) == -1) {
				targetPatternOutput = p1;
				targetPatternInput = p2;
				posCommonInput = hardQueryInfo[0];
				posCommonOutput = hardQueryInfo[1];
			} else if (FactDatabase.varpos(candidate.getFunctionalVariable(), p2) == -1) {
				targetPatternOutput = p2;
				targetPatternInput = p1;
				posCommonInput = hardQueryInfo[1];
				posCommonOutput = hardQueryInfo[0];							
			}
			
			//Many to many case
			if (targetPatternOutput != null) {						
				double f1 = source.x_functionality(targetPatternInput[1], posCommonInput == 0 ? 2 : 0);
				double f2 = source.x_functionality(targetPatternOutput[1], posCommonOutput);
				double f3 = source.x_functionality(targetPatternOutput[1], posCommonOutput == 0 ? 2 : 0); //Duplicate elimination term
				double nentities = source.relationColumnSize(targetPatternInput[1], posCommonInput);
				double overlap;
				if(posCommonInput == posCommonOutput)
					overlap = source.overlap(targetPatternInput[1], targetPatternOutput[1], posCommonInput + posCommonOutput);
				else if(posCommonInput < posCommonOutput)
					overlap = source.overlap(targetPatternInput[1], targetPatternOutput[1], posCommonOutput);
				else
					overlap = source.overlap(targetPatternOutput[1], targetPatternInput[1], posCommonInput);
				
				double overlapHead;
				int posInput = posCommonInput == 0 ? 2 : 0;
				if (pcaOptimistic) {
					//Run the body query on a single variable
					List<ByteString[]> existentialAntecedent = new ArrayList<ByteString[]>(candidate.getAntecedent());
					ByteString[] newHead = candidate.getHead().clone();
					newHead[candidate.getNonFunctionalVariablePosition()] = ByteString.of("?s");
					existentialAntecedent.add(newHead);
					overlapHead = source.countDistinct(candidate.getFunctionalVariable(), existentialAntecedent);							
				} else {
					if(posInput == candidate.getFunctionalVariablePosition()){
						overlapHead = source.overlap(targetPatternInput[1], candidate.getHead()[1], posInput + candidate.getFunctionalVariablePosition());
					}else if(posInput < candidate.getFunctionalVariablePosition()){
						overlapHead = source.overlap(targetPatternInput[1], candidate.getHead()[1], candidate.getFunctionalVariablePosition());							
					}else{
						overlapHead = source.overlap(candidate.getHead()[1], targetPatternInput[1], posInput);							
					}
				}
				
				double f4 = (1 / f1) * (overlap / nentities);
				double ratio = overlapHead * f4 * (f3 / f2);
				ratio = (double)candidate.getSupport() / ratio;
				candidate.setPcaEstimation(ratio);
				candidate.setPcaEstimationOptimistic(source.x_functionality(targetPatternOutput[1], posCommonOutput) / headFunctionality);
				if(ratio < minPcaConfidence) { 
					if (!silent) {
						System.err.println("Query " + candidate + " discarded by functionality heuristic with ratio " + ratio);
					}							
					return false;
				}
			}
		}
		
		return true;
	}

	/**
	 * It checks whether a rule satisfies the confidence thresholds and the
	 * skyline heuristic: the strategy that avoids outputing rules that do not
	 * improve the confidence w.r.t their parents.
	 * @param candidate
	 * @return
	 */
	public boolean testConfidenceThresholds(Query candidate) {
		boolean addIt = true;
		
		if(candidate.containsLevel2RedundantSubgraphs()){
			return false;
		}
		
		if(candidate.getStdConfidence() >= minStdConfidence 
				&& candidate.getPcaConfidence() >= minPcaConfidence){
			//Now check the confidence with respect to its ancestors
			List<Query> ancestors = candidate.getAncestors();			
			for(int i = 0; i < ancestors.size(); ++i){
				double ancestorConfidence = 0.0;
				double ruleConfidence = 0.0;
				if (this.confidenceMetric == ConfidenceMetric.PCAConfidence) {
					ancestorConfidence = ancestors.get(i).getPcaConfidence();
					ruleConfidence = candidate.getPcaConfidence();
				} else {
					ancestorConfidence = ancestors.get(i).getStdConfidence();
					ruleConfidence = candidate.getStdConfidence();
				}
				// Skyline technique on PCA confidence					
				if (ancestors.get(i).isClosed() && 
						ruleConfidence <= ancestorConfidence){
					addIt = false;
					break;
				}		
			}
		}else{
			return false;
		}
		
		return addIt;
	}

	private double getPcaConfidenceUpperBound(Query query) {
		int[] hardCaseInfo = source.identifyHardQueryTypeI(query.getAntecedent());
		ByteString projVariable = query.getFunctionalVariable();
		//ByteString commonVariable = query.getAntecedent().get(hardCaseInfo[2])[hardCaseInfo[0]];
		int freeVarPosition = query.getFunctionalVariablePosition() == 0 ? 2 : 0;
		List<ByteString[]> easyQuery = new ArrayList<ByteString[]>(query.getAntecedent());
		
		//Remove the pattern that does not have the projection variable
		ByteString[] pattern1 = easyQuery.get(hardCaseInfo[2]);
		ByteString[] pattern2 = easyQuery.get(hardCaseInfo[3]);
		ByteString[] remained = null;
		
		if(!pattern1[0].equals(projVariable) && !pattern1[2].equals(projVariable)){
			easyQuery.remove(hardCaseInfo[2]);
			remained = pattern2;
		}else if(!pattern2[0].equals(projVariable) && !pattern2[2].equals(projVariable)){
			easyQuery.remove(hardCaseInfo[3]);
			remained = pattern1;
		}
		
		//Add the existential triple only if it is not redundant
		if(remained != null){
			if(!remained[1].equals(query.getHead()[1]) || hardCaseInfo[1] != query.getFunctionalVariablePosition()){
				ByteString[] existentialTriple = query.getHead().clone();
				existentialTriple[freeVarPosition] = ByteString.of("?z");
				easyQuery.add(existentialTriple);
			}
		}
		
		double denominator = source.countDistinct(projVariable, easyQuery);
		return query.getSupport() / denominator;
	}

	private double getConfidenceUpperBound(Query query) {
		int[] hardCaseInfo = source.identifyHardQueryTypeI(query.getAntecedent());
		double denominator = 0.0;
		ByteString[] triple = new ByteString[3];
		triple[0] = ByteString.of("?x");
		triple[1] = query.getAntecedent().get(0)[1];
		triple[2] = ByteString.of("?y");
		
		if(hardCaseInfo[0] == 2){
			// Case r(y, z) r(x, z)
			denominator = source.countDistinct(ByteString.of("?x"), FactDatabase.triples(triple));
		}else{
			// Case r(z, y) r(z, x)
			denominator = source.countDistinct(ByteString.of("?y"), FactDatabase.triples(triple));
		}
		
		return query.getSupport() / denominator;
	}

	protected void getInstantiatedEdges(Query query, Query originalQuery, 
			ByteString[] danglingEdge, int danglingPosition, int minCardinality, Collection<Query> output) {
		IntHashMap<ByteString> constants = source.frequentBindingsOf(danglingEdge[danglingPosition], query.getFunctionalVariable(), query.getTriples());
		for(ByteString constant: constants){
			int cardinality = constants.get(constant);
			if(cardinality >= minCardinality){
				ByteString[] lastPatternCopy = query.getLastTriplePattern().clone();
				lastPatternCopy[danglingPosition] = constant;
				long cardLastEdge = source.count(lastPatternCopy);
				if(cardLastEdge < 2)
					continue;
				
				Query candidate = query.unify(danglingPosition, constant, cardinality);

				if(candidate.getRedundantAtoms().isEmpty()){
					candidate.setHeadCoverage((double)cardinality / headCardinalities.get(candidate.getHeadRelation()));
					candidate.setSupportRatio((double)cardinality / (double)getTotalCount(candidate));
					candidate.setParent(originalQuery);					
					output.add(candidate);
				}
			}
		}
	}
	
	/**
	 * It computes the number of positive examples (cardinality) of the given rule 
	 * based on the evidence in the database.
	 * @param rule
	 * @return
	 */
	public long computeCardinality(Query rule) {
		ByteString[] head = rule.getHead();
		ByteString countVariable = null;
		if (countAlwaysOnSubject) {
			countVariable = head[0];
		} else {
			countVariable = rule.getFunctionalVariable();
		}
		rule.setSupport(source.countDistinct(countVariable, rule.getTriples()));
		rule.setSupportRatio((double) rule.getSupport() / source.size());
		return rule.getSupport();
	}
	
	/**
	 * It computes the PCA confidence of the given rule based on the evidence in database.
	 * @param rule
	 * @return
	 */
	public double computePCAConfidence(Query rule) {
		// TODO Auto-generated method stub
		List<ByteString[]> antecedent = new ArrayList<ByteString[]>();
		antecedent.addAll(rule.getTriples().subList(1, rule.getTriples().size()));
		ByteString[] succedent = rule.getTriples().get(0);
		double pcaDenominator = 0.0;
		double pcaConfidence = 0.0;
		ByteString[] existentialTriple = succedent.clone();
		int freeVarPos = 0;
		
		if(FactDatabase.numVariables(existentialTriple) == 1){
			freeVarPos = FactDatabase.firstVariablePos(existentialTriple);
		}else{
			if(existentialTriple[0].equals(rule.getFunctionalVariable()))
				freeVarPos = 2;
			else
				freeVarPos = 0;
		}

		existentialTriple[freeVarPos] = ByteString.of("?x");
		if(antecedent.isEmpty()){
			rule.setPcaConfidence(1.0);
		}else{
			//Improved confidence: Add an existential version of the head
			antecedent.add(existentialTriple);
			try{
				pcaDenominator = (double)source.countDistinct(rule.getFunctionalVariable(), antecedent);
				pcaConfidence = (double)rule.getSupport() / pcaDenominator;
				rule.setPcaConfidence(pcaConfidence);
			}catch(UnsupportedOperationException e){
				
			}
		}
		
		return rule.getPcaConfidence();
	}
	
	public double computeStandardConfidence(Query candidate) {
		// Calculate confidence
		double denominator = 0.0;
		double confidence = 0.0;		
		List<ByteString[]> antecedent = new ArrayList<ByteString[]>();
		antecedent.addAll(candidate.getTriples().subList(1, candidate.getTriples().size()));
				
		if(antecedent.isEmpty()){
			candidate.setStdConfidence(1.0);
		}else{
			//Confidence
			try{
				denominator = (double) source.countDistinct(candidate.getFunctionalVariable(), antecedent);
				confidence = (double)candidate.getSupport() / denominator;
				candidate.setStdConfidence(confidence);
				candidate.setBodySize((int)denominator);
			}catch(UnsupportedOperationException e){
				
			}
		}		
		
		return candidate.getStdConfidence();
	}

	public void setAllowConstants(boolean allowConstants) {
		// TODO Auto-generated method stub
		this.allowConstants = allowConstants;
	}

	public boolean isEnforceConstants() {
		return enforceConstants;
	}

	public void setEnforceConstants(boolean enforceConstants) {
		this.enforceConstants = enforceConstants;
	}

	public Collection<ByteString> getBodyExcludedRelations() {
		return bodyExcludedRelations;
	}
	
	public void setBodyExcludedRelations(Collection<ByteString> excludedRelations) {
		this.bodyExcludedRelations = excludedRelations;
	}
	
	/**
	 * @return the headExcludedRelations
	 */
	public Collection<ByteString> getHeadExcludedRelations() {
		return headExcludedRelations;
	}

	/**
	 * @param headExcludedRelations the headExcludedRelations to set
	 */
	public void setHeadExcludedRelations(
			Collection<ByteString> headExcludedRelations) {
		this.headExcludedRelations = headExcludedRelations;
	}

	public Collection<ByteString> getBodyTargetRelations() {
		return bodyTargetRelations;
	}
	
	public boolean isAvoidUnboundTypeAtoms() {
		return avoidUnboundTypeAtoms;
	}

	public void setAvoidUnboundTypeAtoms(boolean avoidUnboundTypeAtoms) {
		this.avoidUnboundTypeAtoms = avoidUnboundTypeAtoms;
	}

	public void setTargetBodyRelations(
			Collection<ByteString> bodyTargetRelations) {
		this.bodyTargetRelations = bodyTargetRelations;
	}	

	public long getTotalCount(int projVarPosition) {
		if(projVarPosition == 0)
			return totalSubjectCount;
		else if(projVarPosition == 2)
			return totalObjectCount;
		else
			throw new IllegalArgumentException("Only 0 and 2 are valid variable positions");
	}

	public void setCountAlwaysOnSubject(boolean countAlwaysOnSubject) {
		// TODO Auto-generated method stub
		this.countAlwaysOnSubject = countAlwaysOnSubject;
	}

	public long getFactsCount() {
		// TODO Auto-generated method stub
		return source.size();
	}

	/**
	 * @return the enabledFunctionalityHeuristic
	 */
	public boolean isEnabledFunctionalityHeuristic() {
		return enabledFunctionalityHeuristic;
	}

	/**
	 * @param enabledFunctionalityHeuristic the enabledFunctionalityHeuristic to set
	 */
	public void setEnabledFunctionalityHeuristic(boolean enableOptimizations) {
		this.enabledFunctionalityHeuristic = enableOptimizations;
	}

	/**
	 * @return the enabledConfidenceUpperBounds
	 */
	public boolean isEnabledConfidenceUpperBounds() {
		return enabledConfidenceUpperBounds;
	}

	/**
	 * @param enabledConfidenceUpperBounds the enabledConfidenceUpperBounds to set
	 */
	public void setEnabledConfidenceUpperBounds(boolean enabledConfidenceUpperBounds) {
		this.enabledConfidenceUpperBounds = enabledConfidenceUpperBounds;
	}

	/**
	 * @return the silent
	 */
	public boolean isSilent() {
		return silent;
	}

	/**
	 * @param silent the silent to set
	 */
	public void setSilent(boolean silent) {
		this.silent = silent;
	}

	public boolean isExploitMaxLengthOption() {
		return exploitMaxLengthOption;
	}

	public void setExploitMaxLengthOption(boolean exploitMaxLengthOption) {
		this.exploitMaxLengthOption = exploitMaxLengthOption;
	}

	public boolean isEnableQueryRewriting() {
		return enableQueryRewriting;
	}

	public void setEnableQueryRewriting(boolean enableQueryRewriting) {
		this.enableQueryRewriting = enableQueryRewriting;
	}

	public boolean isEnablePerfectRules() {
		return enablePerfectRules;
	}

	public void setEnablePerfectRules(boolean enablePerfectRules) {
		this.enablePerfectRules = enablePerfectRules;
	}

	public ConfidenceMetric getConfidenceMetric() {
		return confidenceMetric;
	}

	public void setConfidenceMetric(ConfidenceMetric confidenceMetric) {
		this.confidenceMetric = confidenceMetric;
	}
}