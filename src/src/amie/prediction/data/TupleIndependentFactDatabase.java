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
	
	/**
	 * Adds a certain tuple to the tuple-independent in-memory database.
	 */
	protected boolean add(ByteString subject, ByteString predicate, 
			ByteString object) {
		return add(subject, predicate, object, 1.0);
	}
	
	/**
	 * Adds a tuple to the tuple-independent in-memory database.
	 */
	protected boolean add(ByteString subject, ByteString predicate, 
			ByteString object, double probability) {
		boolean returnVal = super.add(subject, predicate, object);
		Triple<ByteString, ByteString, ByteString> triple = 
				new Triple<ByteString, ByteString, ByteString>(subject, predicate, object);	
		probabilities.put(triple, probability);
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
	public boolean add(CharSequence subject, 
			CharSequence predicate, 
			CharSequence object, 
			double probability) {
		return add(compress(subject), compress(predicate), compress(object), probability);
	}
	
	/**
	 * Returns the probability of a given fact. It assumes the fact already exists.
	 * @param atom
	 * @return
	 */
	public double probabilityOf(ByteString[] atom) {
		Triple<ByteString, ByteString, ByteString> triple = array2Triple(atom);
		return probabilityOf(triple);
	}
	
	/**
	 * Returns the probability of a given fact. It assumes the fact already exists.
	 * @param triple
	 * @return
	 */
	public double probabilityOf(Triple<ByteString, ByteString, ByteString> triple) {
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
	protected List<Double> probabibilityOfQuery(List<ByteString[]> query, ByteString var) {
		Set<ByteString> bindings = selectDistinct(var, query);
		List<Double> results = new ArrayList<Double>();
		try (Instantiator insty = new Instantiator(query, var)) {
			for (ByteString inst : bindings) {
				double result = 1.0;
				for (ByteString[] atom : insty.instantiate(inst)) {
					result *= probabilityOf(atom);
				}
				results.add(result);
			}
		}
		
		return results;
	}
	
	/**
	 * Returns the probabilities for all the bindings of the given query.
	 * @param query
	 * @param var
	 * @return Probability for each binding of the select variable
	 */
	protected List<Double> probabibilityOfQuery(List<ByteString[]> query) {
		// Count the number of variables
		List<Double> result = new ArrayList<Double>();
		if (!containsVariables(query)) {
			for (ByteString[] atom : query) {
				result.add(probabilityOf(atom));
			}
			return result;
		}
		
		if (containsSingleVariable(query)) {
			return probabibilityOfQuery(query, getFirstVariable(query));
		}
		// Pick a variable to start
		ByteString pivotVariable = getFirstVariable(query);
		Set<ByteString> bindings = selectDistinct(pivotVariable, query);
		
		try (Instantiator insty = new Instantiator(query, pivotVariable)) {
			for (ByteString inst : bindings) {
				result.addAll(probabibilityOfQuery(insty.instantiate(inst)));
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
	
	/** returns the number of distinct pairs (var1,var2) for the query */
	public double probabilityOf(List<ByteString[]> query, ByteString[] projection) {
		// First check the number of variables of the projection triple
		int numVariables = numVariables(projection);
		double sum = 0.0;
		if (numVariables == 1) {
			int varPos = firstVariablePos(projection);
			ByteString var = projection[varPos];
			try (Instantiator insty = new Instantiator(query, var)) {
				for (ByteString inst : resultsOneVariable(projection)) {
					double projProbability = probabilityOf(projection);
					List<Double> bodyProbabilities = probabibilityOfQuery(insty.instantiate(inst));
					if (!bodyProbabilities.isEmpty()) {
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
						List<Double> bodyProbabilities = 
								probabibilityOfQuery(query);
						if (!bodyProbabilities.isEmpty()) {
							head.first = val1;
							head.third = val2;
							double headProbability = probabilityOf(head);
							sum += probability(headProbability, bodyProbabilities);
						}
					}
				}
			}
		}
		
		
		return sum;
	}
	
	
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
					List<Double> bodyProbabilities = probabibilityOfQuery(query);
					double headProbability = 1.0;
					Set<ByteString> examples = selectDistinct(existentialVariable, projectionList);
					try (Instantiator h2inst = new Instantiator(projectionList, existentialVariable)) {
						if (examples.contains(valExistentialized)) {
							continue;
						}
						
						for (ByteString example : examples) {
							h2inst.instantiate(example);							
							headProbability *= (1.0 - probabilityOf(projection));
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
	 * Implementation of the formula for the probabilistic support of a 
	 * fact.
	 * @param projProbability
	 * @param bodyProbabilities
	 * @return
	 */
	private double probability(double projProbability, List<Double> bodyProbabilities) {
		double bodyProbability = 1.0;
		for (Double val : bodyProbabilities) {
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
		//db.load(new File("/home/galarrag/workspace/AMIE/Data/yago2/sample-final/yago2core.10kseedsSample.decoded.compressed.notypes.tsv"));
		db.add("<Francois>", "<livesIn>", "<Paris>", 0.9);
		db.add("<Francois>", "<livesIn>", "<Nantes>", 0.9);
		db.add("<Paris>", "<locatedIn>", "<France>", 1.0);
		db.add("<Nantes>", "<locatedIn>", "<France>", 0.8);
		db.add("<Francois>", "<livesIn>", "<France>", 0.9);
		db.add("<Ana>", "<livesIn>", "<Gye>", 0.9);
		db.add("<Gye>", "<locatedIn>", "<Ecuador>", 0.5);
		db.add("<Ana>", "<livesIn>", "<Ecuador>", 1.0);	
		db.add("<Luis>", "<created>", "<Titanic>", 0.5);	
		db.add("<Luis>", "<directed>", "<Titanic>", 0.9);
		db.add("<Ana>", "<created>", "<Blah>", 0.77);	
		db.add("<Ana>", "<directed>", "<Transformers>", 0.75);	
		db.add("<Francois>", "<directed>", "<Amelie>", 0.8);	
		//List<ByteString[]> query = triples(triple(ByteString.of("<Francois>"), ByteString.of("<livesIn>"), ByteString.of("?x")),
		//		triple(ByteString.of("?x"), ByteString.of("<locatedIn>"), ByteString.of("<France>")));
		//List<ByteString[]> query2 = triples(triple(ByteString.of("<Francois>"), ByteString.of("<livesIn>"), ByteString.of("?x")),
		//		triple(ByteString.of("?x"), ByteString.of("<locatedIn>"), ByteString.of("?y")));
		List<ByteString[]> query3 = triples(triple(ByteString.of("?x"), ByteString.of("<livesIn>"), ByteString.of("?y")),
				triple(ByteString.of("?y"), ByteString.of("<locatedIn>"), ByteString.of("?z")));
		List<ByteString[]> query5 = triples(triple(ByteString.of("?a"), ByteString.of("<created>"), ByteString.of("?b")));
		//System.out.println(db.probabibilityOfQuery(query));
		//System.out.println(db.probabibilityOfQuery(query2));
		System.out.println(db.probabilityOf(query3, triple(ByteString.of("?x"), ByteString.of("<livesIn>"), ByteString.of("?z"))));
		System.out.println(db.pcaProbabilityOf(query3, triple(ByteString.of("?x"), ByteString.of("<livesIn>"), ByteString.of("?w")), 
				ByteString.of("?x"), ByteString.of("?z")));
		System.out.println(db.probabilityOf(query5, triple(ByteString.of("?a"), ByteString.of("<directed>"), ByteString.of("?b"))));
		System.out.println(db.pcaProbabilityOf(query5, triple(ByteString.of("?a"), ByteString.of("<directed>"), ByteString.of("?x")), 
				ByteString.of("?a"), ByteString.of("?b")));
		
		db.add("<Francois>", "<livesIn>", "<USA>");
		System.out.println(db.pcaProbabilityOf(query3, triple(ByteString.of("?x"), ByteString.of("<livesIn>"), ByteString.of("?w")), 
				ByteString.of("?x"), ByteString.of("?z")));
		
		db.add("<Diana>", "<livesIn>", "<Lyon>");
		db.add("<Lyon>", "<isLocatedIn>", "<France>");
		db.add("<Diana>", "<livesIn>", "<UK>");
		System.out.println(db.pcaProbabilityOf(query3, triple(ByteString.of("?x"), ByteString.of("<livesIn>"), ByteString.of("?w")), 
				ByteString.of("?x"), ByteString.of("?z")));
	}
}