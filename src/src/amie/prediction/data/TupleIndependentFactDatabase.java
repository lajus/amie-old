package amie.prediction.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javatools.administrative.Announce;
import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Triple;
import javatools.filehandlers.FileLines;
import javatools.parsers.Char17;
import javatools.parsers.NumberFormatter;
import amie.data.FactDatabase;

public class TupleIndependentFactDatabase extends FactDatabase {
	

	Map<Triple<ByteString, ByteString, ByteString>, Double> probabilities = 
			new HashMap<Triple<ByteString, ByteString, ByteString>, Double>();
	
	// Triples can have probability 1.0 because they were deduced from rules
	// with 100% confidence. We want to keep track of those predictions that were
	// certain from the beginning because they are part of the original KB
	Map<Triple<ByteString, ByteString, ByteString>, Boolean> certaintyMap = 
			new HashMap<Triple<ByteString, ByteString, ByteString>, Boolean>();
	
	/**
	 * Adds a certain tuple to the tuple-independent in-memory database.
	 */
	protected boolean add(ByteString subject, ByteString predicate, 
			ByteString object) {
		return add(subject, predicate, object, 1.0, true);
	}
	
	/**
	 * Adds a tuple to the tuple-independent in-memory database.
	 */
	protected boolean add(ByteString subject, ByteString predicate, 
			ByteString object, double probability, boolean certainty) {
		boolean returnVal = super.add(subject, predicate, object);
		Triple<ByteString, ByteString, ByteString> triple = 
				new Triple<ByteString, ByteString, ByteString>(subject, predicate, object);	
		synchronized (probabilities) {
			probabilities.put(triple, probability);	
		}
		
		synchronized (certaintyMap) {
			certaintyMap.put(triple, certainty);
		}
		
		return returnVal;
	}
	
	/**
	 * Determines whether the triple was marked as certain in the KB.
	 * @param subject
	 * @param predicate
	 * @param object
	 * @return
	 */
	public boolean isCertain(Triple<ByteString, ByteString, ByteString> triple) {
		Boolean returnVal = certaintyMap.get(triple);
		if (returnVal == null)
			return false;
		
		return returnVal;
	}
	
	/**
	 * Loads a TSV file (one triple per line) into the in-memory database.
	 *  
	 */
	protected void load(File f, String message, boolean buildJoinTable)
			throws IOException {
		long size = size();
		if (f.isDirectory()) {
			long time = System.currentTimeMillis();
			Announce.doing("Loading files in " + f.getName());
			for (File file : f.listFiles())
				load(file);
			Announce.done("Loaded "
					+ (size() - size)
					+ " facts in "
					+ NumberFormatter.formatMS(System.currentTimeMillis()
							- time));
		}
		for (String line : new FileLines(f, "UTF-8", message)) {
			if (line.endsWith("."))
				line = Char17.cutLast(line);
			String[] split = line.split("\t");
			if (split.length == 3)
				add(split[0].trim(), split[1].trim(), split[2].trim());
			else if (split.length == 4)
				add(split[1].trim(), split[2].trim(), split[3].trim());
			else if (split.length == 5)
				add(split[1].trim(), split[2].trim(), split[3].trim(), Double.parseDouble(split[4]));
		}

		if (message != null)
			Announce.message("     Loaded", (size() - size), "facts");
	}

	/** Adds a fact to the in-memory database **/
	public boolean add(CharSequence subject, CharSequence predicate, 
			CharSequence object, double probability) {
		return add(compress(subject), compress(predicate), compress(object), probability, false);
	}
	
	/**
	 * Returns the probability of a given fact. It assumes the fact already exists.
	 * @param atom
	 * @return
	 */
	public double probabilityOfFact(ByteString[] atom) {
		Triple<ByteString, ByteString, ByteString> triple = array2Triple(atom);
		return probabilityOfFact(triple);
	}
	
	/**
	 * Returns the probability of a given fact. It assumes the fact already exists.
	 * @param triple
	 * @return
	 */
	public double probabilityOfFact(Triple<ByteString, ByteString, ByteString> triple) {
		Double probability = probabilities.get(triple);
		if (probability != null)
			return probability.doubleValue();
		
		return 0.0;
	}
	
	/**
	 * Returns the probability of a given sequence of atoms where it is known
	 * that exists only one variable.
	 * @param query
	 * @param var
	 * @return Probability for each binding of the select variable
	 */
	protected double[] probabibilitiesOfQuery(List<ByteString[]> query, ByteString var) {
		Set<ByteString> bindings = selectDistinct(var, query);
		if (bindings == null) {
			System.out.println(FactDatabase.toString(query));
		}
		double[] results = new double[bindings.size()];
		try (Instantiator insty = new Instantiator(query, var)) {
			int k = 0;
			for (ByteString inst : bindings) {
				double result = 1.0;
				for (ByteString[] atom : insty.instantiate(inst)) {
					result *= probabilityOfFact(atom);
				}
				results[k] = result;
				++k;
			}
		}
		
		return results;
	}
	
	/**
	 * Returns the probabilities for all the bindings of the given query. 
	 * @param query
	 * @return Probability for each possible combination of bindings that satisfies the query.
	 */
	protected double[] probabibilitiesOfQuery(List<ByteString[]> query) {
		// Count the number of variables
		double[] result = new double[]{0.0};
		if (!containsVariables(query)) {
			result = new double[query.size()];
			int k = 0;
			for (ByteString[] atom : query) {
				result[k] = probabilityOfFact(atom);
				++k;
			}
			return result;
		}
		
		if (containsSingleVariable(query)) {
			return probabibilitiesOfQuery(query, getFirstVariable(query));
		}
		// Pick a variable to start
		ByteString pivotVariable = getFirstVariable(query);
		Set<ByteString> bindings = selectDistinct(pivotVariable, query);
		
		try (Instantiator insty = new Instantiator(query, pivotVariable)) {
			double tmp[][] = new double[bindings.size()][];
			int effectiveSize = 0;
			int totalSize = 0;
			for (ByteString inst : bindings) {
				double[] tmpAtI = probabibilitiesOfQuery(insty.instantiate(inst));
				if (tmpAtI.length > 1 || tmpAtI[0] > 0.0) {
					totalSize += tmpAtI.length;
					tmp[effectiveSize] = tmpAtI;
					++effectiveSize;
				}
			}
			if (totalSize > 0) {
				if (totalSize > 1000000) {
					System.out.println(FactDatabase.toString(query));
					System.out.println("Warning: Memory issue " + totalSize);
				}
				result = new double[totalSize];
				int k = 0;
				for (int i = 0; i < effectiveSize; ++i) {
					for (int j = 0; j < tmp[i].length; ++j, ++k) {
						result[k] = tmp[i][j];
					}
				}
			}
			
		}
	
		return result;
	}

	/**
	 * It returns the only variable in the query pattern.
	 * @param query
	 * @return
	 */
	private ByteString getFirstVariable(List<ByteString[]> query) {
		for (ByteString[] atom : query) {
			for (int i = 0; i < atom.length; ++i) {
				if (isVariable(atom[i])) {
					return atom[i];
				}
			}
		}
		
		return null;
	}

	/**
	 * Determines whether a query contains a single variable.
	 * @param query
	 * @return
	 */
	public static boolean containsSingleVariable(List<ByteString[]> query) {
		ByteString variable = null;
		for (ByteString[] atom : query) {
			for (int i = 0; i < atom.length; ++i) {
				if (isVariable(atom[i])) {
					if (variable != null && !atom[i].equals(variable)) {
						return false;
					} else if (variable == null) {
						variable = atom[i];
					}
				}
			}
		}
		
		return true;
	}
	
	/**
	 * It calculates the probabilistic size of a relation, by adding up the probabilities of all 
	 * the triples of the relation.
	 * @param relation
	 * @return
	 */
	public double probabilisticSize(ByteString relation) {
		Map<ByteString, IntHashMap<ByteString>> subjects = predicate2subject2object.get(relation);
		double size = 0.0;
		ByteString fact[] = new ByteString[]{null, relation, null};
		for (ByteString subject : subjects.keySet()) {
			fact[0] = subject;
			for (ByteString object : subjects.get(subject)) {
				fact[2] = object;
				size += probabilityOfFact(fact);
			}
		}
		return size;
	}
	
	/**
	 * It calculates the probabilistic support of a query with respect to the variables in projection atom, 
	 * as well as the support of the "existentialized" version when one of the variables in the
	 * projection atom has been replaced by an existential variable.
	 * @param query
	 * @param projection
	 * @param valueToReplace the value (variable or constant) in the projection atom that will be replaced by a fresh variable
	 * for the calculation of the existentialized support. 
	 * @return An array of two doubles. The first one corresponds to the support of the query w.r.t the projection atom.
	 * The second value is the existentialized support, that is, the count of positives and negative examples.
	 */
	public double[] probabilitiesOf(List<ByteString[]> query, ByteString[] projection, ByteString valueToReplace) {
		double result[] = new double[2];
		result[0] = result[1] = 0.0;
		ByteString[] existentializedProjection = projection.clone();
		int replacePosition = varpos(valueToReplace, existentializedProjection);
		existentializedProjection[replacePosition] = ByteString.of("?vx"); 
		
		ByteString var1 = existentializedProjection[replacePosition == 0 ? 2 : 0];
		
		List<ByteString[]> listExistential = new ArrayList<>(1);
		listExistential.add(existentializedProjection);
		List<ByteString[]> listProjection = new ArrayList<>(1);
		listProjection.add(projection);
		
		List<ByteString[]> fullQuery = new ArrayList<>();
		fullQuery.addAll(query);
		fullQuery.add(projection);
		
		List<ByteString[]> fullExistQuery = new ArrayList<>();
		fullExistQuery.addAll(query);
		fullExistQuery.add(existentializedProjection);
		
		if (numVariables(projection) == 1) {
			// Iterate over the bindings of one variable in the relation		
			Set<ByteString> entities = selectDistinct(var1, fullExistQuery);		
			try (Instantiator insty1 = new Instantiator(query, var1); // Instantiator of x on the body B of the rule B => r(x, y)
					Instantiator headExistInsty1 = new Instantiator(listExistential, var1); // Instantiator of x on the existentialized head r(x, y')
					Instantiator headInst1 = new Instantiator(listProjection, var1); ) { // Instantiator of x on the original head atom r(x, y)  
				for (ByteString val1 : entities) {
					insty1.instantiate(val1);
					headExistInsty1.instantiate(val1);
					headInst1.instantiate(val1);
					double[] bodyProbabilities = probabibilitiesOfQuery(query); // Body probability
					if (bodyProbabilities.length > 1 || bodyProbabilities[0] != 0.0) {
						double[] headProbabilities = probabibilitiesOfQuery(listExistential);
						// For the denominator
						double headProbability = aggregateProbabilities(headProbabilities);
						result[1] += probability(headProbability, bodyProbabilities);					
						// For the numerator
						headProbability = probabilityOfFact(projection);
						result[0] += probability(headProbability, bodyProbabilities);
					}
				}
			}
		} else {
			Map<ByteString, IntHashMap<ByteString>> entities = selectDistinct(var1, valueToReplace, fullExistQuery);
			try (Instantiator insty1 = new Instantiator(query, var1); // Instantiator of x on the body B of the rule B => r(x, y)
					Instantiator insty2 = new Instantiator(query, valueToReplace); // Instantiator of y on the body B of the rule B => r(x, y)
					Instantiator headExistInsty1 = new Instantiator(listExistential, var1); // Instantiator of x on the existentialized head r(x, y')
					Instantiator headInst1 = new Instantiator(listProjection, var1); // Instantiator of x on the original head atom r(x, y)
					Instantiator headInst2 = new Instantiator(listProjection, valueToReplace)) { // Instantiator of y on the original head atom r(x, y)  
				for (ByteString val1 : entities.keySet()) {
					insty1.instantiate(val1);
					headExistInsty1.instantiate(val1);
					headInst1.instantiate(val1);
					IntHashMap<ByteString> objects = entities.get(val1);
					for (ByteString val2 : objects) {
						headInst2.instantiate(val2);
						insty2.instantiate(val2);
						double[] bodyProbabilities = probabibilitiesOfQuery(query); // Body probability
						if (bodyProbabilities.length > 1 || bodyProbabilities[0] != 0.0) {
							double[] headProbabilities = probabibilitiesOfQuery(listExistential);						
							// For the denominator
							double headProbability = aggregateProbabilities(headProbabilities);
							result[1] += probability(headProbability, bodyProbabilities);					
							headProbability = probabilityOfFact(projection);
							result[0] += probability(headProbability, bodyProbabilities);
						}
					}
				}
			}
		}
		
		return result;
	}
	
	/**
	 * Given a list of probabilities p1, ..., pn, it applies the formula
	 * 1 - (1 - p1) ... (1 - pn)
	 * @param headProbabilities
	 * @return
	 */
	private double aggregateProbabilities(double[] probabilities) {
		double total = 1.0;
		for (double prob : probabilities) {
			total *= (1.0 - prob);
		}
		
		return 1.0 - total;
	}

	/**
	 * It calculates the probabilistic version of support for a given query. The projection
	 * variables are the ones occurring in the argument projection.
	 * @param query
	 * @param projection
	 * @return
	 */
	public double probabilityOf(List<ByteString[]> query, ByteString[] projection) {
		// First check the number of variables of the projection triple
		int numVariables = numVariables(projection);
		double sum = 0.0;
		if (numVariables == 1) {
			int varPos = firstVariablePos(projection);
			ByteString var = projection[varPos];
			try (Instantiator insty = new Instantiator(query, var)) {
				for (ByteString inst : resultsOneVariable(projection)) {
					double projProbability = probabilityOfFact(projection);
					double[] bodyProbabilities = probabibilitiesOfQuery(insty.instantiate(inst));
					if (bodyProbabilities != null) {
						sum += probability(projProbability, bodyProbabilities);
					}
				}
			}
			
		} else if (numVariables == 2) {
			int varPos1 = firstVariablePos(projection);
			int varPos2 = secondVariablePos(projection);
			ByteString var1 = projection[varPos1];
			ByteString var2 = projection[varPos2];
			
			Map<ByteString, IntHashMap<ByteString>> instantiations = 
					resultsTwoVariables(var1, var2, projection);
			Triple<ByteString, ByteString, ByteString> head = 
					new Triple<ByteString, ByteString, ByteString>(projection[0], projection[1], projection[2]);			
			try (Instantiator insty1 = new Instantiator(query, var1);
					Instantiator insty2 = new Instantiator(query, var2)) {
				for (ByteString val1 : instantiations.keySet()) {
					insty1.instantiate(val1);
					for (ByteString val2 : instantiations.get(val1)) {
						insty2.instantiate(val2);
						double[] bodyProbabilities = 
								probabibilitiesOfQuery(query);
						if (bodyProbabilities != null) {
							head.first = val1;
							head.third = val2;
							double headProbability = probabilityOfFact(head);
							sum += probability(headProbability, bodyProbabilities);
						}
					}
				}
			}
		}
		
		
		return sum;
	}
	
	/**
	 * It calculates the probabilistic version of the number of distinct bindings for
	 * variables var1 and var2 in the query.
	 * @param query
	 * @param projection
	 * @param var1
	 * @param var2
	 * @return
	 */
	public double pcaProbabilityOf(List<ByteString[]> query, 
			ByteString[] projection, ByteString var1, ByteString var2) {
		ByteString existentialVariable = null;
		ByteString commonVariable = null;
		ByteString existentializedVariable = null;
		double sum = 0.0;
		if (projection[0].equals(var1) || projection[0].equals(var2)) {
			existentialVariable = projection[2];
			commonVariable = projection[0];
			existentializedVariable = projection[0].equals(var1) ? var2 : var1; 
		} else if (projection[2].equals(var1) || projection[2].equals(var2)) {
			existentialVariable = projection[0];
			commonVariable = projection[2];
			existentializedVariable = projection[2].equals(var1) ? var2 : var1; 
		}
		
		Map<ByteString, IntHashMap<ByteString>> bindings = selectDistinct(commonVariable, existentializedVariable, query);		
		List<ByteString[]> projectionList = new ArrayList<ByteString[]>();
		projectionList.add(projection);
		
		try(Instantiator h1inst = new Instantiator(projectionList, commonVariable);
				Instantiator b1inst = new Instantiator(query, commonVariable);
				Instantiator b2inst = new Instantiator(query, existentializedVariable)) {
			for (ByteString valCommon : bindings.keySet()) {
				b1inst.instantiate(valCommon);
				h1inst.instantiate(valCommon);
				for (ByteString valExistentialized : bindings.get(valCommon)) {
					b2inst.instantiate(valExistentialized);			
					double[] bodyProbabilities = probabibilitiesOfQuery(query);
					double headProbability = 1.0;
					Set<ByteString> examples = selectDistinct(existentialVariable, projectionList);
					try (Instantiator h2inst = new Instantiator(projectionList, existentialVariable)) {
						if (examples.contains(valExistentialized)) {
							continue;
						}
						
						for (ByteString example : examples) {
							h2inst.instantiate(example);							
							headProbability *= (1.0 - probabilityOfFact(projection));
						}
						headProbability = 1.0 - headProbability;
					}
					sum += probability(headProbability, bodyProbabilities);
				}
			}
		}
		
		return sum;
	}
	
	/**
	 * Implementation of the formula for the probabilistic support of a fact.
	 * @param projProbability
	 * @param bodyProbabilities
	 * @return
	 */
	private double probability(double projProbability, double[] bodyProbabilities) {
		double bodyProbability = 1.0;
		for (double val : bodyProbabilities) {
			bodyProbability *= (1.0 - val);
		}
		bodyProbability = 1.0 - bodyProbability;
		return projProbability * bodyProbability;
	}

	/**
	 * 
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String args[]) throws IOException {
		TupleIndependentFactDatabase db = new TupleIndependentFactDatabase();
		
		db.load(new File(args[0]));

		//?a  <isMarriedTo>  ?f  ?f  <livesIn>  ?b   => ?a  <livesIn>  ?b[8.0, 0.6153846153846154]	
		//?a  <hasChild>  ?f  ?f  <livesIn>  ?b   => ?a  <livesIn>  ?b[6.0, 0.8571428571428571]	
		//		?e  <isMarriedTo>  ?a  ?e  <livesIn>  ?b   => ?a  <livesIn>  ?b[8.0, 0.6153846153846154]	
		//		?e  <hasChild>  ?a  ?e  <livesIn>  ?b   => ?a  <livesIn>  ?b[6.0, 0.75]	
		
		//?a <livesIn> ?b  ?a <isMarriedTo> ?v0  ?v0 <livesIn> ?b  ?a <hasChild> ?v1  ?v1 <livesIn> ?b  ?v2 <hasChild> ?a  ?v2 <livesIn> ?b  ?v3 <isMarriedTo> ?a  ?v3 <livesIn> ?b
		List<ByteString[]> query = FactDatabase.triples(
				FactDatabase.triple("?a",  "<livesIn>",  "?b"),
				FactDatabase.triple("?a",  "<isMarriedTo>",  "?v0"),
				FactDatabase.triple("?v0", "<livesIn>", "?b"),
				FactDatabase.triple("?a",  "<hasChild>",  "?v1"),
				FactDatabase.triple("?v1", "<livesIn>", "?b"),				
				FactDatabase.triple("?v3",  "<isMarriedTo>",  "?a"),
				FactDatabase.triple("?v3",  "<livesIn>",  "?b"),
				FactDatabase.triple("?v2", "<hasChild>", "?a"),
				FactDatabase.triple("?v2", "<livesIn>", "?b")
				);
		System.out.println(db.countPairs("?a", "?b", query));
	}
}
