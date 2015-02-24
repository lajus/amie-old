package arm.data;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javatools.administrative.Announce;
import javatools.administrative.D;
import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.datatypes.Pair;
import javatools.filehandlers.FileLines;
import javatools.parsers.Char;
import javatools.parsers.NumberFormatter;

/**
 * Class FactDatabase
 * 
 * This class implements an in-memory database for facts without identifiers,
 * tuned for Luis' project
 * 
 * @author Fabian M. Suchanek
 * 
 */
public class FactDatabase {

  //---------------------------------------------------------------------------
  //              Indexes
  //---------------------------------------------------------------------------

  /** Index */
  protected final Map<ByteString, Map<ByteString, IntHashMap<ByteString>>> subject2predicate2object = new IdentityHashMap<ByteString, Map<ByteString, IntHashMap<ByteString>>>();

  /** Index */
  protected final Map<ByteString, Map<ByteString, IntHashMap<ByteString>>> predicate2object2subject = new IdentityHashMap<ByteString, Map<ByteString, IntHashMap<ByteString>>>();

  /** Index */
  protected final Map<ByteString, Map<ByteString, IntHashMap<ByteString>>> object2subject2predicate = new IdentityHashMap<ByteString, Map<ByteString, IntHashMap<ByteString>>>();

  /** Index */
  protected final Map<ByteString, Map<ByteString, IntHashMap<ByteString>>> predicate2subject2object = new IdentityHashMap<ByteString, Map<ByteString, IntHashMap<ByteString>>>();

  /** Index */
  protected final Map<ByteString, Map<ByteString, IntHashMap<ByteString>>> object2predicate2subject = new IdentityHashMap<ByteString, Map<ByteString, IntHashMap<ByteString>>>();

  /** Index */
  protected final Map<ByteString, Map<ByteString, IntHashMap<ByteString>>> subject2object2predicate = new IdentityHashMap<ByteString, Map<ByteString, IntHashMap<ByteString>>>();

  /** Number of facts per subject */
  protected final IntHashMap<ByteString> subjectSize = new IntHashMap<ByteString>();

  /** Number of facts per object */
  protected final IntHashMap<ByteString> objectSize = new IntHashMap<ByteString>();

  /** Number of facts per relation */
  protected final IntHashMap<ByteString> predicateSize = new IntHashMap<ByteString>();

  /** Number of facts */
  protected int size;

  /** (X differentFrom Y Z ...) predicate*/
  public static final String DIFFERENTFROMstr="differentFrom";
  
  /** (X differentFrom Y Z ...) predicate*/
  public static final ByteString DIFFERENTFROMbs=ByteString.of(DIFFERENTFROMstr);
  
  //---------------------------------------------------------------------------
  //              Loading
  //---------------------------------------------------------------------------

  /** Adds a fact */
  protected boolean add(ByteString subject, ByteString relation, ByteString object, Map<ByteString, Map<ByteString, IntHashMap<ByteString>>> map) {
    synchronized (map) {
      Map<ByteString, IntHashMap<ByteString>> relation2object = map.get(subject);
      if (relation2object == null) map.put(subject, relation2object = new IdentityHashMap<ByteString, IntHashMap<ByteString>>());
      IntHashMap<ByteString> objects = relation2object.get(relation);
      if (objects == null) relation2object.put(relation, objects = new IntHashMap<ByteString>());
      return (objects.add(object));
    }
  }

  /** Adds a fact */
  public boolean add(CharSequence... fact) {
    return (add(compress(fact[0]), compress(fact[1]), compress(fact[2])));
  }

  /** Adds a fact */
  protected boolean add(ByteString subject, ByteString predicate, ByteString object) {
    if (!add(subject, predicate, object, subject2predicate2object)) return (false);
    add(predicate, object, subject, predicate2object2subject);
    add(object, subject, predicate, object2subject2predicate);
    add(predicate, subject, object, predicate2subject2object);
    add(object, predicate, subject, object2predicate2subject);
    add(subject, object, predicate, subject2object2predicate);
    synchronized (subjectSize) {
      subjectSize.increase(subject);
    }
    synchronized (predicateSize) {
      predicateSize.increase(predicate);
    }
    synchronized (objectSize) {
      objectSize.increase(object);
    }
    size++;
    return (true);
  }

  /** Returns the number of facts */
  public int size() {
    return (size);
  }

  public int size(int var) {
    if (var < 0 || var > 2) throw new IllegalArgumentException("Variable position must be between 0 and 2");
    switch (var) {
      case 0:
        return subjectSize.size();
      case 1:
        return predicateSize.size();
      case 2:
        return objectSize.size();
      default:
        throw new IllegalArgumentException("Variable position must be between 0 and 2");
    }
  }

  /** TRUE if the ByteString is a SPARQL variable */
  public static boolean isVariable(CharSequence s) {
    return (s.length() > 0 && s.charAt(0) == '?');
  }

  /** Loads a file or all files in the folder */
  public void load(File f) throws IOException {
    load(f, "Loading " + f.getName());
  }

  /** Loads a file or all files in the folder */
  protected void load(File f, String message) throws IOException {
    int size = size();
    if (f.isDirectory()) {
      long time = System.currentTimeMillis();
      Announce.doing("Loading files in " + f.getName());
      for (File file : f.listFiles())
        load(file);
      Announce.done("Loaded " + (size() - size) + " facts in " + NumberFormatter.formatMS(System.currentTimeMillis() - time));
    }
    for (String line : new FileLines(f, "UTF-8", message)) {
      if (line.endsWith(".")) line = Char.cutLast(line);
      String[] split = line.split("\t");
      if (split.length == 3) add(split[0].trim(), split[1].trim(), split[2].trim());
      else if (split.length == 4) add(split[1].trim(), split[2].trim(), split[3].trim());
      //else Announce.warning("Unsupported number of items in line:",split.length, line);
    }
    if (message != null) Announce.message("     Loaded", (size() - size), "facts");
  }

  /** Loads a files in the folder that match the regex pattern */
  public void load(File folder, Pattern namePattern) throws IOException {
    List<File> files = new ArrayList<File>();
    for (File file : folder.listFiles())
      if (namePattern.matcher(file.getName()).matches()) files.add(file);
    load(files);
  }

  /** Loads the files */
  public void load(File... files) throws IOException {
    load(Arrays.asList(files));
  }

  /** Loads the files */
  public void load(List<File> files) throws IOException {
    int size = size();
    long time = System.currentTimeMillis();
    long memory = Runtime.getRuntime().freeMemory();
    Announce.doing("Loading files");
    final int[] running = new int[1];
    for (final File file : files) {
      running[0]++;
      new Thread() {

        public void run() {
          try {
            synchronized (Announce.blanks) {
              Announce.message("Starting " + file.getName());
            }
            load(file, (String) null);
          } catch (Exception e) {
            e.printStackTrace();
          }
          synchronized (Announce.blanks) {
            Announce.message("Finished " + file.getName() + ", still running: " + (running[0] - 1));
            synchronized (running) {
              if (--running[0] == 0) running.notify();
            }
          }
        }
      }.start();
    }
    try {
      synchronized (running) {
        running.wait();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Announce.done("Loaded " + (size() - size) + " facts in " + NumberFormatter.formatMS(System.currentTimeMillis() - time) + " using "
        + ((Runtime.getRuntime().freeMemory() - memory) / 1000000) + " MB");
  }

  /** Loads the files */
  public void loadSequential(List<File> files) throws IOException {
    int size = size();
    long time = System.currentTimeMillis();
    long memory = Runtime.getRuntime().freeMemory();
    Announce.doing("Loading files");
    for (File file : files)
      load(file);
    Announce.done("Loaded " + (size() - size) + " facts in " + NumberFormatter.formatMS(System.currentTimeMillis() - time) + " using "
        + ((Runtime.getRuntime().freeMemory() - memory) / 1000000) + " MB");
  }

  //---------------------------------------------------------------------------
  //              Functionality
  //---------------------------------------------------------------------------

  /** Returns the harmonic functionality, as defined in the PARIS paper*/
  public double functionality(ByteString relation) {
    return ((double) predicate2subject2object.get(relation).size() / predicateSize.get(relation));
  }

  /** Returns the harmonic functionality, as defined in the PARIS paper*/
  public double functionality(CharSequence relation) {
    return (functionality(compress(relation)));
  }

  /** Returns the harmonic inverse functionality, as defined in the
  PARIS paper*/
  public double inverseFunctionality(ByteString relation) {
    return ((double) predicate2object2subject.get(relation).size() / predicateSize.get(relation));
  }

  /** Returns the harmonic inverse functionality, as defined in the
  PARIS paper*/
  public double inverseFunctionality(CharSequence relation) {
    return (inverseFunctionality(compress(relation)));
  }

  //---------------------------------------------------------------------------
  //              Single triple selections
  //---------------------------------------------------------------------------

  /** TRUE if the 0th component is different from the 2n, 3rd, 4th, etc. */
  public static boolean differentFrom(CharSequence... triple) {
    return(differentFrom(triple(triple)));
  }

  /** TRUE if the 0th component is different from the 2n, 3rd, 4th, etc. */
  public static boolean differentFrom(ByteString... triple) {
    if(!triple[1].equals(DIFFERENTFROMbs)) throw new IllegalArgumentException("DifferentFrom can only be called with a differentFrom predicate: "+toString(triple));
    for(int i=2;i<triple.length;i++) {
      if(triple[0].equals(triple[i])) return(false);
    }
    return(true);
  }

  /** Returns the result of the map for key1 and key2 */
  protected IntHashMap<ByteString> get(Map<ByteString, Map<ByteString, IntHashMap<ByteString>>> map, ByteString key1, ByteString key2) {
    Map<ByteString, IntHashMap<ByteString>> m = map.get(key1);
    if (m == null) return (new IntHashMap<>());
    IntHashMap<ByteString> r = m.get(key2);
    if (r == null) return (new IntHashMap<>());
    return (r);
  }

  /** Returns the results of the triple pattern query, if it contains exactly 1
   * variable */
  public Set<ByteString> resultsOneVariable(CharSequence... triple) {
    if (numVariables(triple) != 1) throw new IllegalArgumentException("Triple should contain exactly one variable: " + toString(triple));    
    return (resultsOneVariable(triple(triple)));
  }

  /** Returns the results of the triple pattern query, if it contains exactly 1
   * variable */
  protected IntHashMap<ByteString> resultsOneVariable(ByteString... triple) {
    if(triple[1].equals(DIFFERENTFROMbs)) throw new IllegalArgumentException("Cannot query differentFrom: " + toString(triple));
    if (isVariable(triple[0])) return (get(predicate2object2subject, triple[1], triple[2]));
    if (isVariable(triple[1])) return (get(object2subject2predicate, triple[2], triple[0]));
    return (get(subject2predicate2object, triple[0], triple[1]));
  }

  /** TRUE if the database contains this fact (no variables) */
  public boolean contains(CharSequence... fact) {
    if (numVariables(fact) != 0) throw new IllegalArgumentException("Triple should not contain a variable: " + toString(fact));
    return (contains(triple(fact)));
  }

  /** TRUE if the database contains this fact (no variables) */
  protected boolean contains(ByteString... fact) {
    if(fact[1]==DIFFERENTFROMbs) return(differentFrom(fact));
    return (get(subject2predicate2object, fact[0], fact[1]).contains(fact[2]));
  }

  /** Returns map results for key */
  protected Map<ByteString, IntHashMap<ByteString>> get(Map<ByteString, Map<ByteString, IntHashMap<ByteString>>> map, ByteString key1) {
    Map<ByteString, IntHashMap<ByteString>> m = map.get(key1);
    if (m == null) return (Collections.emptyMap());
    else return (m);
  }

  /**
   * Returns the results of a triple query pattern with two variables as a map
   * of first value to set of second values.  
   */
  public Map<ByteString, IntHashMap<ByteString>> resultsTwoVariables(int pos1, int pos2, CharSequence[] triple) {
    if (!isVariable(triple[pos1]) || !isVariable(triple[pos2]) || numVariables(triple) != 2 || pos1 == pos2) throw new IllegalArgumentException(
        "Triple should contain 2 variables, one at " + pos1 + " and one at " + pos2 + ": " + toString(triple));
    return (resultsTwoVariables(pos1, pos2, triple(triple)));
  }

  /**
   * Returns the results of a triple query pattern with two variables as a map
   * of first value to set of second values.  
   */
  public Map<ByteString, IntHashMap<ByteString>> resultsTwoVariables(CharSequence var1, CharSequence var2, CharSequence[] triple) {
    if (varpos(var1, triple) == -1 || varpos(var2, triple) == -1 || var1.equals(var2) || numVariables(triple) != 2) throw new IllegalArgumentException(
        "Triple should contain the two variables " + var1 + ", " + var2 + ": " + toString(triple));
    return (resultsTwoVariables(compress(var1), compress(var2), triple(triple)));
  }

  /**
   * Returns the results of a triple query pattern with two variables as a map
   * of first value to set of second values
   */
  public Map<ByteString, IntHashMap<ByteString>> resultsTwoVariables(int pos1, int pos2, ByteString[] triple) {
    if(triple[1].equals(DIFFERENTFROMbs)) throw new IllegalArgumentException("Cannot query with differentFrom: " + toString(triple));    switch (pos1) {
      case 0:
        switch (pos2) {
          case 1:
            return (get(object2subject2predicate, triple[2]));
          case 2:
            return (get(predicate2subject2object, triple[1]));
        }
        break;
      case 1:
        switch (pos2) {
          case 0:
            return get(object2predicate2subject, triple[2]);
          case 2:
            return get(subject2predicate2object, triple[0]);
        }
        break;
      case 2:
        switch (pos2) {
          case 0:
            return get(predicate2object2subject, triple[1]);
          case 1:
            return get(subject2object2predicate, triple[0]);
        }
        break;
    }
    throw new IllegalArgumentException("Invalid combination of variables in " + toString(triple) + " pos1 = " + pos1 + " pos2=" + pos2);
  }

  /**
   * Returns the results of a triple query pattern with two variables as a map
   * of first value to set of second values
   */
  public Map<ByteString, IntHashMap<ByteString>> resultsTwoVariables(ByteString var1, ByteString var2, ByteString[] triple) {
    int varPos1 = varpos(var1, triple);
    int varPos2 = varpos(var2, triple);
    return resultsTwoVariables(varPos1, varPos2, triple);
  }

  /** Returns number of results of the triple pattern query with 1 variable */
  protected int countOneVariable(ByteString... triple) {
    if(triple[1].equals(DIFFERENTFROMbs)) return(Integer.MAX_VALUE);
    return (resultsOneVariable(triple).size());
  }

  /** Returns number of results of the triple pattern query with 2 variables */
  protected int countTwoVariables(ByteString... triple) {
    // Safety check
    // if(numVariables(triple)!=2) throw new
    // InvalidParameterException("Triple must contain exactly 2 variables: "+Arrays.toString(triple));
    if(triple[1].equals(DIFFERENTFROMbs)) return(Integer.MAX_VALUE);
    if (!isVariable(triple[0])) return (subjectSize.get(triple[0], 0));
    if (!isVariable(triple[1])) {
      // To be correct, we should guard against the same variable...
      // if(triple[0].equals(triple[2])) throw new
      // UnsupportedOperationException("Reflexive triple pattern");
      return (predicateSize.get(triple[1], 0));
    }
    return (objectSize.get(triple[2], 0));
  }

  /** Returns number of variable occurrences in a triple */
  public static int numVariables(CharSequence... fact) {
    int counter = 0;
    for (int i = 0; i < fact.length; i++)
      if (isVariable(fact[i])) counter++;
    return (counter);
  }

  /** returns number of instances of this triple */
  public int count(CharSequence... triple) {
    return (count(triple(triple)));
  }

  /** returns number of instances of this triple */
  protected int count(ByteString... triple) {
    switch (numVariables(triple)) {
      case 0:
        return (contains(triple) ? 1 : 0);
      case 1:
        return (countOneVariable(triple));
      case 2:
        return (countTwoVariables(triple));
      case 3:
        if(triple[1]==DIFFERENTFROMbs) return(Integer.MAX_VALUE);
        return (size());
    }
    return (-1);
  }

  //---------------------------------------------------------------------------
  //              Existence
  //---------------------------------------------------------------------------

  /** Remove triple*/
  public static List<ByteString[]> remove(int pos, List<ByteString[]> triples) {
    if (pos == 0) return (triples.subList(1, triples.size()));
    if (pos == triples.size() - 1) return (triples.subList(0, triples.size() - 1));
    List<ByteString[]> result = new ArrayList<>(triples);
    result.remove(pos);
    return (result);
  }

  /** Returns most restrictive triple, -1 if most restrictive has count 0*/
  protected int mostRestrictiveTriple(List<ByteString[]> triples) {
    int bestPos = -1;
    int count = Integer.MAX_VALUE;
    for (int i = 0; i < triples.size(); i++) {
      int myCount = count(triples.get(i));
      if (myCount >= count) continue;
      if (myCount == 0) return (-1);
      bestPos = i;
      count = myCount;
    }
    return (bestPos);
  }

  /** Returns most restrictive triple, -1 if most restrictive has count 0*/
  protected int mostRestrictiveTriple(List<ByteString[]> triples, ByteString variable) {
    int bestPos = -1;
    int count = Integer.MAX_VALUE;
    for (int i = 0; i < triples.size(); i++) {
      if (varpos(variable, triples.get(i)) != -1) {
        int myCount = count(triples.get(i));
        if (myCount >= count) continue;
        if (myCount == 0) return (-1);
        bestPos = i;
        count = myCount;
      }
    }
    return (bestPos);
  }

  /** Returns most restrictive triple that contains either the proj or the variable, -1 if most restrictive has count 0*/
  protected int mostRestrictiveTriple(List<ByteString[]> triples, ByteString var1, ByteString var2) {
    int bestPos = -1;
    int count = Integer.MAX_VALUE;
    for (int i = 0; i < triples.size(); i++) {
      ByteString[] triple = triples.get(i);
      if (contains(triple, var1) || contains(triple, var2)) {
        int myCount = count(triple);
        if (myCount >= count) continue;
        if (myCount == 0) return (-1);
        bestPos = i;
        count = myCount;
      }
    }
    return (bestPos);
  }

  private boolean contains(ByteString[] triple, ByteString variable) {
    return triple[0].equals(variable) || triple[1].equals(variable) || triple[2].equals(variable);
  }

  /** Returns the position of a variable in a triple*/
  public static int varpos(ByteString var, ByteString[] triple) {
    for (int i = 0; i < triple.length; i++) {
      if (var.equals(triple[i])) return (i);
    }
    return (-1);
  }

  /** Returns the position of a variable in a triple*/
  public static int varpos(CharSequence var, CharSequence[] triple) {
    for (int i = 0; i < triple.length; i++) {
      if (var.equals(triple[i])) return (i);
    }
    return (-1);
  }

  /** Returns the position of the first variable in the pattern */
  public static int firstVariablePos(ByteString... fact) {
    for (int i = 0; i < fact.length; i++)
      if (isVariable(fact[i])) return (i);
    return (-1);
  }

  /** Returns the position of the second variable in the pattern */
  public static int secondVariablePos(ByteString... fact) {
    for (int i = firstVariablePos(fact) + 1; i < fact.length; i++)
      if (isVariable(fact[i])) return (i);
    return (-1);
  }

  /** TRUE if the query result exists */
  protected boolean exists(List<CharSequence[]> triples) {
    return (existsBS(triples(triples)));
  }

  /** TRUE if the query result exists */
  protected boolean existsBS(List<ByteString[]> triples) {
    if (triples.isEmpty()) return (false);
    if (triples.size() == 1) return (count(triples.get(0)) != 0);
    int bestPos = mostRestrictiveTriple(triples);
    if (bestPos == -1) return (false);
    ByteString[] best = triples.get(bestPos);

    switch (numVariables(best)) {
      case 0:
        if (!contains(best)) return (false);
        return (existsBS(remove(bestPos, triples)));
      case 1:
        try (Instantiator insty = new Instantiator(remove(bestPos, triples), best[firstVariablePos(best)])) {
          for (ByteString inst : resultsOneVariable(best)) {
            if (existsBS(insty.instantiate(inst))) return (true);
          }
        }
        return (false);
      case 2:
        int firstVar = firstVariablePos(best);
        int secondVar = secondVariablePos(best);
        Map<ByteString, IntHashMap<ByteString>> instantiations = resultsTwoVariables(firstVar, secondVar, best);
        List<ByteString[]> otherTriples = remove(bestPos, triples);
        try (Instantiator insty1 = new Instantiator(otherTriples, best[firstVar]);
            Instantiator insty2 = new Instantiator(otherTriples, best[secondVar])) {
          for (ByteString val1 : instantiations.keySet()) {
            insty1.instantiate(val1);
            for (ByteString val2 : instantiations.get(val1)) {
              if (existsBS(insty2.instantiate(val2))) return (true);
            }
          }
        }
        return (false);
      case 3:
      default:
        return (size() != 0);
    }
  }

  //---------------------------------------------------------------------------
  //              Count Distinct
  //---------------------------------------------------------------------------

  /** returns the number of instances that fulfill a certain condition*/
  public int countDistinct(CharSequence variable, List<CharSequence[]> query) {
    return (selectDistinct(variable, query).size());
  }

  /** returns the number of instances that fulfill a certain condition*/
  public int countDistinct(ByteString variable, List<ByteString[]> query) {
    return (selectDistinct(variable, query).size());
  }

  //---------------------------------------------------------------------------
  //              Selection
  //---------------------------------------------------------------------------

  /** returns the instances that fulfill a certain condition*/
  public Set<ByteString> selectDistinct(CharSequence variable, List<CharSequence[]> query) {
    return (selectDistinct(compress(variable), triples(query)));
  }

  /** returns the instances that fulfill a certain condition*/
  public Set<ByteString> selectDistinct(ByteString variable, List<ByteString[]> query) {

    // Only one triple

    if (query.size() == 1) {
      ByteString[] triple = query.get(0);
      switch (numVariables(triple)) {
        case 0:
          return (Collections.emptySet());
        case 1:
          return (resultsOneVariable(triple));
        case 2:
          int firstVar = firstVariablePos(triple);
          int secondVar = secondVariablePos(triple);
          if (triple[firstVar].equals(variable)) return (resultsTwoVariables(firstVar, secondVar, triple).keySet());
          else return (resultsTwoVariables(secondVar, firstVar, triple).keySet());
        default:
          switch (Arrays.asList(query.get(0)).indexOf(variable)) {
            case 0:
              return (subjectSize);
            case 1:
              return (predicateSize);
            case 2:
              return (objectSize);
          }
      }
      throw new RuntimeException("Very weird: SELECT " + variable + " WHERE " + toString(query.get(0)));
    }

    int bestPos = mostRestrictiveTriple(query);
    IntHashMap<ByteString> result = new IntHashMap<>();
    if (bestPos == -1) return (result);
    ByteString[] best = query.get(bestPos);

    // If the variable is in the most restrictive triple
    if (Arrays.asList(best).indexOf(variable) != -1) {
      switch (numVariables(best)) {
        case 1:
          try (Instantiator insty = new Instantiator(remove(bestPos, query), variable)) {
            for (ByteString inst : resultsOneVariable(best)) {
              if (existsBS(insty.instantiate(inst))) result.add(inst);
            }
          }
          break;
        case 2:
          int firstVar = firstVariablePos(best);
          int secondVar = secondVariablePos(best);
          Map<ByteString, IntHashMap<ByteString>> instantiations = best[firstVar].equals(variable) ? resultsTwoVariables(firstVar, secondVar, best)
              : resultsTwoVariables(secondVar, firstVar, best);
          try (Instantiator insty = new Instantiator(query, variable)) {
            for (ByteString val : instantiations.keySet()) {
              if (existsBS(insty.instantiate(val))) result.add(val);
            }
          }
          break;
        case 3:
        default:
          throw new UnsupportedOperationException("3 variables in the projection triple are not yet supported: SELECT " + variable + " WHERE "
              + toString(query));
      }
      return (result);
    }
    

    // If the variable is not in the most restrictive triple...
    List<ByteString[]> others = remove(bestPos, query);
    switch (numVariables(best)) {
      case 0:
        return (selectDistinct(variable, others));
      case 1:
        ByteString var = best[firstVariablePos(best)];
        try (Instantiator insty = new Instantiator(others, var)) {
          for (ByteString inst : resultsOneVariable(best)) {
            result.addAll(selectDistinct(variable, insty.instantiate(inst)));
          }
        }
        break;
      case 2:
        int firstVar = firstVariablePos(best);
        int secondVar = secondVariablePos(best);
        Map<ByteString, IntHashMap<ByteString>> instantiations = resultsTwoVariables(firstVar, secondVar, best);
        try (Instantiator insty1 = new Instantiator(others, best[firstVar]); Instantiator insty2 = new Instantiator(others, best[secondVar])) {
          for (ByteString val1 : instantiations.keySet()) {
            insty1.instantiate(val1);
            for (ByteString val2 : instantiations.get(val1)) {
              result.addAll(selectDistinct(variable, insty2.instantiate(val2)));
            }
          }
        }
        break;
      case 3:
      default:
        throw new UnsupportedOperationException("3 variables in the projection triple are not yet supported: SELECT " + variable + " WHERE "
            + toString(query));
    }
    return (result);

  }

  //---------------------------------------------------------------------------
  //              Select distinct, two variables
  //---------------------------------------------------------------------------

  /** Returns all (distinct) pairs of values that make the query true*/
  public Map<ByteString, IntHashMap<ByteString>> selectDistinct(CharSequence var1, CharSequence var2, List<CharSequence[]> query) {
    return (selectDistinct(compress(var1), compress(var2), triples(query)));
  }

  /** Returns all (distinct) pairs of values that make the query true*/
  public Map<ByteString, IntHashMap<ByteString>> selectDistinct(ByteString var1, ByteString var2, List<ByteString[]> query) {
    if (query.isEmpty()) return (Collections.emptyMap());
    if (query.size() == 1) {
      return (resultsTwoVariables(var1, var2, query.get(0)));
    }
    Map<ByteString, IntHashMap<ByteString>> result = new HashMap<>();
    try (Instantiator insty1 = new Instantiator(query, var1)) {
      for (ByteString val1 : selectDistinct(var1, query)) {
        Set<ByteString> val2s = selectDistinct(var2, insty1.instantiate(val1));
        IntHashMap<ByteString> ihm = val2s instanceof IntHashMap<?> ? (IntHashMap<ByteString>) val2s : new IntHashMap<>(val2s);
        if (!val2s.isEmpty()) result.put(val1, ihm);
      }
    }
    return (result);
  }

  //---------------------------------------------------------------------------
  //              Count single projection bindings
  //---------------------------------------------------------------------------

  /** Maps each value of the variable to the number of distinct values of the projection variable*/
  public IntHashMap<ByteString> frequentBindingsOf(CharSequence variable, CharSequence projectionVariable, List<CharSequence[]> query) {
    return (frequentBindingsOf(compress(variable), compress(projectionVariable), triples(query)));
  }

  /** Maps each value of the variable to the number of distinct values of the projection variable*/
  public IntHashMap<ByteString> frequentBindingsOf(ByteString variable, ByteString projectionVariable, List<ByteString[]> query) {
    // If only one triple
    if (query.size() == 1) {
      ByteString[] triple = query.get(0);
      int varPos = varpos(variable, triple);
      int projPos = varpos(projectionVariable, triple);
      if (varPos == -1 || projPos == -1) throw new IllegalArgumentException("Query should contain at least two variables: " + toString(triple));
      if (numVariables(triple) == 2) return (new IntHashMap<ByteString>(resultsTwoVariables(varPos, projPos, triple)));
      // Three variables (only supported if varpos==2 and projPos==0)
      if (projPos != 0) throw new UnsupportedOperationException(
          "frequentBindingsOf on most general triple is only possible with projection variable in position 1: " + toString(query));

      // Two variables
      IntHashMap<ByteString> res = new IntHashMap<>();
      if (varPos == projPos) {
        try (Instantiator insty = new Instantiator(query, triple[projPos])) {
          for (ByteString inst : resultsOneVariable(triple)) {
            res.add(selectDistinct(variable, insty.instantiate(inst)));
          }
        }
        return res;
      }

      for (ByteString predicate : predicateSize.keys()) {
        triple[1] = predicate;
        res.add(predicate, resultsTwoVariables(0, 2, triple).size());
      }
      triple[1] = variable;
      return (res);
    }

    // Find most restrictive triple
    int bestPos = mostRestrictiveTriple(query, projectionVariable, variable);
    IntHashMap<ByteString> result = new IntHashMap<>();
    if (bestPos == -1) return (result);
    ByteString[] best = query.get(bestPos);
    int varPos = varpos(variable, best);
    int projPos = varpos(projectionVariable, best);
    List<ByteString[]> other = remove(bestPos, query);

    // If the variable and the projection variable are in the most restrictive triple
    if (varPos != -1 && projPos != -1) {
      switch (numVariables(best)) {
        case 2:
          int firstVar = firstVariablePos(best);
          int secondVar = secondVariablePos(best);
          Map<ByteString, IntHashMap<ByteString>> instantiations = best[firstVar].equals(variable) ? resultsTwoVariables(firstVar, secondVar, best)
              : resultsTwoVariables(secondVar, firstVar, best);
          try (Instantiator insty1 = new Instantiator(other, variable); Instantiator insty2 = new Instantiator(other, projectionVariable)) {
            for (ByteString val1 : instantiations.keySet()) {
              insty1.instantiate(val1);
              for (ByteString val2 : instantiations.get(val1)) {
                if (existsBS(insty2.instantiate(val2))) result.increase(val1);
              }
            }
          }
          break;
        case 3:
        default:
          throw new UnsupportedOperationException("3 variables in the variable triple are not yet supported: FREQBINDINGS " + variable + " WHERE "
              + toString(query));
      }
      return (result);
    }

    // If the variable is in the most restrictive triple
    if (varPos != -1) {
      switch (numVariables(best)) {
        case 1:
          try (Instantiator insty = new Instantiator(other, variable)) {
            for (ByteString inst : resultsOneVariable(best)) {
              result.add(inst, selectDistinct(projectionVariable, insty.instantiate(inst)).size());
            }
          }
          break;
        case 2:
          int firstVar = firstVariablePos(best);
          int secondVar = secondVariablePos(best);
          Map<ByteString, IntHashMap<ByteString>> instantiations = best[firstVar].equals(variable) ? resultsTwoVariables(firstVar, secondVar, best)
              : resultsTwoVariables(secondVar, firstVar, best);
          try (Instantiator insty1 = new Instantiator(query, variable)) {
            for (ByteString val1 : instantiations.keySet()) {
              result.add(val1, selectDistinct(projectionVariable, insty1.instantiate(val1)).size());
            }
          }
          break;
        case 3:
        default:
          throw new UnsupportedOperationException("3 variables in the variable triple are not yet supported: FREQBINDINGS " + variable + " WHERE "
              + toString(query));
      }
      return (result);
    }
    
    //Default case
    if(projPos != -1){
        switch (numVariables(best)) {
        case 1:
          try (Instantiator insty = new Instantiator(other, projectionVariable)) {
            for (ByteString inst : resultsOneVariable(best)) {
            	result.add(selectDistinct(variable, insty.instantiate(inst)));
            }
          }
          break;
        case 2:
          int firstVar = firstVariablePos(best);
          int secondVar = secondVariablePos(best);
          Map<ByteString, IntHashMap<ByteString>> instantiations = best[firstVar].equals(projectionVariable) ? resultsTwoVariables(firstVar,
              secondVar, best) : resultsTwoVariables(secondVar, firstVar, best);
          try (Instantiator insty1 = new Instantiator(query, projectionVariable)) {
            for (ByteString val1 : instantiations.keySet()) {
              result.add(selectDistinct(variable, insty1.instantiate(val1)));
            }
          }
          break;
        case 3:
        default:
          throw new UnsupportedOperationException("3 variables in the projection triple are not yet supported: FREQBINDINGS " + variable + " WHERE "
              + toString(query));
      }
      return (result);
    }
    
	return result;
  }

  //---------------------------------------------------------------------------
  //              Count Projection Bindings
  //---------------------------------------------------------------------------

  /** Counts, for each binding of the variable at position pos, the number of instantiations of the triple*/
  protected IntHashMap<ByteString> countBindings(int pos, ByteString... triple) {
    switch (numVariables(triple)) {      
      case 1:
        return (new IntHashMap<ByteString>(resultsOneVariable(triple)));
      case 2:
        switch (pos) {
          case 0: // We want the most frequent subjects
            // ?x loves ?y
            if (isVariable(triple[2])) return (new IntHashMap<ByteString>(get(predicate2subject2object, triple[1])));
            // ?x ?r Elvis
            else return (new IntHashMap<ByteString>(get(object2subject2predicate, triple[2])));
          case 1: // We want the most frequent predicates
            // Elvis ?r ?y
            if (isVariable(triple[2])) return (new IntHashMap<ByteString>(get(subject2predicate2object, triple[0])));
            // ?x ?r Elvis
            else return new IntHashMap<ByteString>(get(object2predicate2subject, triple[2]));
          case 2: // we want the most frequent objects
            // Elvis ?r ?y
            if (isVariable(triple[1])) return new IntHashMap<ByteString>(get(subject2object2predicate, triple[0]));
            // ?x loves ?y
            return (new IntHashMap<ByteString>(get(predicate2object2subject, triple[1])));
        }
      case 3:
        return (pos == 0 ? subjectSize : pos == 1 ? predicateSize : objectSize);
      default:
        throw new InvalidParameterException("Triple should contain at least 1 variable: " + toString(triple));
    }
  }

  /** Counts for each binding of the variable at pos how many instances of the projection triple exist in the query*/
  protected IntHashMap<ByteString> countProjectionBindings(int pos, ByteString[] projectionTriple, List<ByteString[]> otherTriples) {
    if (!isVariable(projectionTriple[pos])) throw new IllegalArgumentException("Position " + pos + " in " + toString(projectionTriple)
        + " must be a variable");
    IntHashMap<ByteString> result = new IntHashMap<>();
    switch (numVariables(projectionTriple)) {
      case 1:
        try (Instantiator insty = new Instantiator(otherTriples, projectionTriple[pos])) {
          for (ByteString inst : resultsOneVariable(projectionTriple)) {
            if (existsBS(insty.instantiate(inst))) result.increase(inst);
          }
        }
        break;
      case 2:
        int firstVar = firstVariablePos(projectionTriple);
        int secondVar = secondVariablePos(projectionTriple);
        Map<ByteString, IntHashMap<ByteString>> instantiations = resultsTwoVariables(firstVar, secondVar, projectionTriple);
        try (Instantiator insty1 = new Instantiator(otherTriples, projectionTriple[firstVar]);
            Instantiator insty2 = new Instantiator(otherTriples, projectionTriple[secondVar])) {
          for (ByteString val1 : instantiations.keySet()) {
            insty1.instantiate(val1);
            for (ByteString val2 : instantiations.get(val1)) {
              if (existsBS(insty2.instantiate(val2))) result.increase(firstVar == pos ? val1 : val2);
            }
          }
        }
        break;
      case 3:
      default:
        throw new UnsupportedOperationException("3 variables in the projection triple are not yet supported: " + toString(projectionTriple) + ", "
            + toString(otherTriples));
    }
    return (result);
  }

  /** Counts for each binding of variable how many instances of the projection triple exist in the query*/
  public IntHashMap<ByteString> countProjectionBindings(ByteString[] projectionTriple, List<ByteString[]> otherTriples, ByteString variable) {
    int pos = Arrays.asList(projectionTriple).indexOf(variable);

    // If the other triples are empty, count all bindings
    if (otherTriples.isEmpty()) {
      return (countBindings(pos, projectionTriple));
    }

    // If the variable appears in the projection triple,
    // use the other method
    if (pos != -1) {
      return (countProjectionBindings(pos, projectionTriple, otherTriples));
    }    

    // Now let's iterate through all instantiations of the projectionTriple
    List<ByteString[]> wholeQuery = new ArrayList<ByteString[]>();
    wholeQuery.add(projectionTriple);
    wholeQuery.addAll(otherTriples);
    
    ByteString instVar = null;
    int posRestrictive = mostRestrictiveTriple(wholeQuery);
    ByteString[] mostRestrictive = posRestrictive != -1 ? wholeQuery.get(posRestrictive) : projectionTriple;
    IntHashMap<ByteString> result = new IntHashMap<>();
    int posInCommon = (mostRestrictive != projectionTriple) ? firstVariableInCommon(mostRestrictive, projectionTriple) : -1;
    int nHeadVars = numVariables(projectionTriple);
    
    //Avoid ground facts in the projection triple
    if(mostRestrictive == projectionTriple || posInCommon == -1 || nHeadVars == 1){
      switch (numVariables(projectionTriple)) {
        case 1:
          instVar = projectionTriple[firstVariablePos(projectionTriple)];
          try (Instantiator insty = new Instantiator(otherTriples, instVar)) {
            for (ByteString inst : resultsOneVariable(projectionTriple)) {
              result.add(selectDistinct(variable, insty.instantiate(inst)));
            }
          }
          break;
        case 2:
          int firstVar = firstVariablePos(projectionTriple);
          int secondVar = secondVariablePos(projectionTriple);
          Map<ByteString, IntHashMap<ByteString>> instantiations = resultsTwoVariables(firstVar, secondVar, projectionTriple);
          try (Instantiator insty1 = new Instantiator(otherTriples, projectionTriple[firstVar]);
              Instantiator insty2 = new Instantiator(otherTriples, projectionTriple[secondVar])) {
            for (ByteString val1 : instantiations.keySet()) {
              insty1.instantiate(val1);
              for (ByteString val2 : instantiations.get(val1)) {
                result.add(selectDistinct(variable, insty2.instantiate(val2)));
              }
            }
          }
          break;
        case 3:
        default:
          throw new UnsupportedOperationException("3 variables in the projection triple are not yet supported: " + toString(projectionTriple) + ", "
              + toString(otherTriples));
      }	
    }else{
    	List<ByteString[]> otherTriples2 = new ArrayList<ByteString[]>(wholeQuery);
        List<ByteString[]> projectionTripleList = new ArrayList<ByteString[]>(1);
        projectionTripleList.add(projectionTriple);
        otherTriples2.remove(projectionTriple);
    	//Iterate over the most restrictive triple
    	switch (numVariables(mostRestrictive)) {
        case 1:
    	   //Go for an improved plan, but remove the bound triple
           otherTriples2.remove(mostRestrictive);
	       instVar = mostRestrictive[firstVariablePos(mostRestrictive)];
	       try (Instantiator insty1 = new Instantiator(otherTriples2, instVar); 
		        Instantiator insty2 = new Instantiator(projectionTripleList, instVar)) {
	          for (ByteString inst : resultsOneVariable(mostRestrictive)) {
	            result.add(countProjectionBindings(insty2.instantiate(inst).get(0), insty1.instantiate(inst), variable));
	          }
	       }    	 
         break;
        case 2:
          int projectionPosition = FactDatabase.varpos(mostRestrictive[posInCommon], projectionTriple);
          //If the projection triple has two variables, bind the common variable without problems
          if(nHeadVars == 2){	          
	        try(Instantiator insty1 = new Instantiator(otherTriples2, mostRestrictive[posInCommon]);		           
	        	Instantiator insty3 = new Instantiator(projectionTripleList, projectionTriple[projectionPosition])){
	        	IntHashMap<ByteString> instantiations = countBindings(posInCommon, mostRestrictive);
		        for(ByteString b1: instantiations){
		          result.add(countProjectionBindings(insty3.instantiate(b1).get(0), insty1.instantiate(b1), variable));
		        }
	          }
          }else if(nHeadVars == 1){
              instVar = projectionTriple[firstVariablePos(projectionTriple)];
              try (Instantiator insty = new Instantiator(otherTriples, instVar)) {
                for (ByteString inst : resultsOneVariable(projectionTriple)) {
                  result.add(selectDistinct(variable, insty.instantiate(inst)));
                }
              }
          }
          break;          
        case 3:
        default:
          throw new UnsupportedOperationException("3 variables in the most restrictive triple are not yet supported: " + toString(mostRestrictive) + ", "
              + toString(wholeQuery));

      }	
    	
    }
    
    return (result);
  }

  public int firstVariableInCommon(ByteString[] t1, ByteString[] t2) {
	  for(int i = 0; i < t1.length; ++i){
		  if(FactDatabase.isVariable(t1[i]) && varpos(t1[i], t2) != -1)
			  return i;
	  }
	  
	  return -1;
  }

  public int numVarsInCommon(ByteString[] a, ByteString[] b) {
	int count = 0;
	for(int i = 0; i < a.length; ++i){
		if(FactDatabase.isVariable(a[i]) && varpos(a[i], b) != -1)
			++count;
	}
	
	return count;
  }

  /** Counts, for each binding of the variable the number of instantiations of the projection triple */
  public IntHashMap<ByteString> countProjectionBindings(CharSequence[] projectionTriple, List<CharSequence[]> query, CharSequence variable) {
    ByteString[] projection = triple(projectionTriple);
    List<ByteString[]> otherTriples = new ArrayList<>();
    for (CharSequence[] t : query) {
      ByteString[] triple = triple(t);
      if (!Arrays.equals(triple, projection)) otherTriples.add(triple);
    }
    return (countProjectionBindings(projection, otherTriples, compress(variable)));
  }

  //---------------------------------------------------------------------------
  //              Count Projection
  //---------------------------------------------------------------------------

  /**
   * Counts the number of instances of the projection triple that exist in
   * joins with the query
   */
  public int countProjection(CharSequence[] projectionTriple, List<CharSequence[]> query) {
    ByteString[] projection = triple(projectionTriple);
    //Create "otherTriples"
    List<ByteString[]> otherTriples = new ArrayList<>();
    for (CharSequence[] t : query) {
      ByteString[] triple = triple(t);
      if (!Arrays.equals(triple, projection)) otherTriples.add(triple);
    }
    return (countProjection(projection, otherTriples));
  }

  /**
   * Counts the number of instances of the projection triple that exist in
   * joins with the other triples
   */
  public int countProjection(ByteString[] projectionTriple, List<ByteString[]> otherTriples) {
    if (otherTriples.isEmpty()) return (count(projectionTriple));
    switch (numVariables(projectionTriple)) {
      case 0:
        return (count(projectionTriple));
      case 1:
        int counter = 0;
        ByteString variable = projectionTriple[firstVariablePos(projectionTriple)];
        try (Instantiator insty = new Instantiator(otherTriples, variable)) {
          for (ByteString inst : resultsOneVariable(projectionTriple)) {
            if (existsBS(insty.instantiate(inst))) counter++;
          }
        }
        return (counter);
      case 2:
        counter = 0;
        int firstVar = firstVariablePos(projectionTriple);
        int secondVar = secondVariablePos(projectionTriple);
        Map<ByteString, IntHashMap<ByteString>> instantiations = resultsTwoVariables(firstVar, secondVar, projectionTriple);
        try (Instantiator insty1 = new Instantiator(otherTriples, projectionTriple[firstVar])) {
          for (ByteString val1 : instantiations.keySet()) {
            try (Instantiator insty2 = new Instantiator(insty1.instantiate(val1), projectionTriple[secondVar])) {
              for (ByteString val2 : instantiations.get(val1)) {
                if (existsBS(insty2.instantiate(val2))) counter++;
              }
            }
          }
        }
        return (counter);
      case 3:
      default:
        throw new UnsupportedOperationException("3 variables in the projection triple are not yet supported: " + toString(projectionTriple) + ", "
            + toString(otherTriples));
    }
  }

  //---------------------------------------------------------------------------
  //              Counting pairs
  //---------------------------------------------------------------------------

  /** returns the number of distinct pairs (var1,var2) for the query*/
  public long countPairs(CharSequence var1, CharSequence var2, List<ByteString[]> query) {
    return (countPairs(compress(var1), compress(var2), triples(query)));
  }
  
  public int[] identifyHardQuery(List<ByteString[]> query){
	  if(query.size() < 2)
		  return null;
	  
	  int lastIdx = query.size() - 1;
	  for(int idx1 = 0; idx1 < lastIdx; ++idx1){
		  for(int idx2 = idx1 + 1; idx2 < query.size(); ++idx2){
			  ByteString[] t1, t2;
			  t1 = query.get(idx1);
			  t2 = query.get(idx2);
			  
			  //Not the same relation
			  if(!t1[1].equals(t2[1]) || numVariables(t1) != 2 || numVariables(t2) != 2)
				  return null;
			  
			  if(!t1[0].equals(t2[0]) && t1[2].equals(t2[2])){
				  return new int[]{2, 0, idx1, idx2};
			  }else if(t1[0].equals(t2[0]) && !t1[2].equals(t2[2])){
				  return new int[]{0, 2, idx1, idx2};
			  }
		  }
	  }
	  
	  return null;
  }  
  
  public long countPairs(ByteString var1, ByteString var2, List<ByteString[]> query, int[] queryInfo) {
	long result = 0;
  	//Approximate count
  	ByteString joinVariable = query.get(0)[queryInfo[0]];
  	ByteString targetVariable = query.get(0)[queryInfo[1]];
  	ByteString targetRelation = query.get(0)[1];  	
	//Heuristic
	if(predicateSize.get(targetRelation) < 50000)
		return countPairs(var1, var2, query);

  	int duplicatesEstimate, duplicatesCard;
	double duplicatesFactor;
	
  	duplicatesCard = countDistinct(targetVariable, query.subList(0, 1));
  	if(queryInfo[0] == 2){
  	  duplicatesFactor = (1.0 / functionality(targetRelation)) - 1.0;
  	}else{
	  duplicatesFactor = (1.0 / inverseFunctionality(targetRelation)) - 1.0;
  	}
  	duplicatesEstimate = (int)Math.ceil(duplicatesCard * duplicatesFactor);
	    
  	try (Instantiator insty1 = new Instantiator(query.subList(0, 1), joinVariable)) {
  	  for(ByteString value: selectDistinct(joinVariable, query.subList(0, 1))){
  		  result += (long)Math.ceil(Math.pow(countDistinct(targetVariable, insty1.instantiate(value)), 2));
	  }
	}
  	
  	result -= duplicatesEstimate;
  	
  	return result;
  }
  
  public long countPairs(ByteString var1, ByteString var2, List<ByteString[]> query, int[] queryInfo, ByteString[] existentialTriple, int nonExistentialPosition) {
	long result = 0;
  	//Approximate count
  	ByteString joinVariable = query.get(queryInfo[2])[queryInfo[0]];
  	ByteString targetVariable = null;
  	ByteString targetRelation = query.get(queryInfo[2])[1];  	
	//Heuristic
	if(predicateSize.get(targetRelation) < 20000){
		query.add(existentialTriple);
		result = countPairs(var1, var2, query);
		return result;
	}
	
	List<ByteString[]> subquery = new ArrayList<ByteString[]>();
	if(varpos(existentialTriple[nonExistentialPosition], query.get(queryInfo[2])) != -1){
		subquery.add(query.get(0));
		targetVariable = query.get(queryInfo[2])[queryInfo[1]];
	}
		
	if(varpos(existentialTriple[nonExistentialPosition], query.get(queryInfo[3])) != -1){
		subquery.add(query.get(queryInfo[3]));
		targetVariable = query.get(queryInfo[3])[queryInfo[1]];		
	}
	
	subquery.add(existentialTriple);
		
  	try (Instantiator insty1 = new Instantiator(subquery, joinVariable)) {
  	  for(ByteString value: selectDistinct(joinVariable, query.subList(0, 1))){
  	    result += (long)countDistinct(targetVariable, insty1.instantiate(value));
	  }
	}
  	  	
  	query.remove(query.size() - 1);
  	
  	return result;
  }

  /** returns the number of distinct pairs (var1,var2) for the query*/
  public long countPairs(ByteString var1, ByteString var2, List<ByteString[]> query) {
	//Go for the standard plan
	long result = 0;
	
    try (Instantiator insty1 = new Instantiator(query, var1)) {
      Set<ByteString> bindings = selectDistinct(var1, query);	
      for (ByteString val1 : bindings) {
        result += countDistinct(var2, insty1.instantiate(val1));
      }
    }
    
    return (result);
  }

/** Can instantiate a variable in a query with a value*/
  protected static class Instantiator implements Closeable {

    List<ByteString[]> query;

    int[] positions;

    ByteString variable;

    public Instantiator(List<ByteString[]> q, ByteString var) {
      positions = new int[q.size() * 3];
      int numPos = 0;
      query = q;
      variable = var;
      for (int i = 0; i < query.size(); i++) {
        for (int j = 0; j < query.get(i).length; j++) {
          if (query.get(i)[j].equals(variable)) positions[numPos++] = i * 3 + j;
        }
      }

      if (numPos < positions.length) positions[numPos] = -1;
    }

    public List<ByteString[]> instantiate(ByteString value) {
      for (int i = 0; i < positions.length; i++) {
        if (positions[i] == -1) break;
        query.get(positions[i] / 3)[positions[i] % 3] = value;
      }
      return (query);
    }

    @Override
    public void close() {
      for (int i = 0; i < positions.length; i++) {
        if (positions[i] == -1) break;
        query.get(positions[i] / 3)[positions[i] % 3] = variable;
      }
    }
  }

  //---------------------------------------------------------------------------
  //              Creating Triples
  //---------------------------------------------------------------------------

  /** ToString for a triple*/
  public static <T> String toString(T[] s) {
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < s.length; i++)
      b.append(s[i]).append(" ");
    return (b.toString());
  }

  /** ToString for a query*/
  public static String toString(List<ByteString[]> s) {
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < s.size(); i++)
      b.append(toString(s.get(i))).append(" ");
    return (b.toString());
  }

  /** Compresses a string to an internal string*/
  public static ByteString compress(CharSequence s) {
    if (s instanceof ByteString) return ((ByteString) s);
    String str = s.toString();
    int pos = str.indexOf("\"^^");
    if (pos != -1) str = str.substring(0, pos + 1);
    return (ByteString.of(str));
  }

  /** Makes a list of triples*/
  public static List<ByteString[]> triples(ByteString[]... triples) {
    return (Arrays.asList(triples));
  }

  /** makes triples*/
  @SuppressWarnings("unchecked")
  public static List<ByteString[]> triples(List<? extends CharSequence[]> triples) {
    if (iscompressed(triples)) return ((List<ByteString[]>) triples);
    List<ByteString[]> t = new ArrayList<>();
    for (CharSequence[] c : triples)
      t.add(triple(c));
    return (t);
  }

  /** TRUE if this query is compressed*/
  public static boolean iscompressed(List<? extends CharSequence[]> triples) {
    for (int i = 0; i < triples.size(); i++) {
      CharSequence[] t = triples.get(i);
      if (!(t instanceof ByteString[])) return (false);
    }
    return true;
  }

  /** Makes a triple */
  public static ByteString[] triple(ByteString... triple) {
    return (triple);
  }

  /** Makes a triple */
  public static ByteString[] triple(CharSequence... triple) {
    ByteString[] result = new ByteString[triple.length];
    for (int i = 0; i < triple.length; i++)
      result[i] = compress(triple[i]);
    return (result);
  }

  /** Pattern of a triple*/
  public static final Pattern triplePattern = Pattern.compile("(\\w+)\\((\\??\\w+)\\s*,\\s*(\\??\\w+)\\)");

  /** Pattern of a triple*/
//  public static final Pattern amieTriplePattern = Pattern.compile("(\\??\\w+)\\s+(<[\\w:]+>)\\s+(\\??\\w+)");
//  public static final Pattern amieTriplePattern = Pattern.compile("(\\??\\w+|<[\\w:'.\\(\\),]+>)\\s+(<[\\w:]+>)\\s+(\\??\\w+|<[\\w:'.\\(\\),]+>)");
  public static final Pattern amieTriplePattern = Pattern.compile("(\\??\\w+|<[-\\w\\p{L}/:–'.\\(\\),]+>)\\s+(<[\\w:]+>)\\s+(\\??\\w+|<[-\\w\\p{L}/:–'.\\(\\),]+>)");
  
  //public static final Pattern amieTriplePattern=Pattern.compile("(<?\\??\\w+>?)\\s+(<\\w+>)\\s+(<?\\??\\w+>?)");

  /** Parses a triple of the form r(x,y)*/
  public static ByteString[] triple(String s) {
    Matcher m = triplePattern.matcher(s);
    if (m.find()) return (triple(m.group(2), m.group(1), m.group(3)));
    m = amieTriplePattern.matcher(s);
    if (!m.find()) return (triple(m.group(1), m.group(2), m.group(3)));
    return (null);
  }

  /** Parses a query */
  public static ArrayList<ByteString[]> triples(String s) {
    Matcher m = triplePattern.matcher(s);
    ArrayList<ByteString[]> result = new ArrayList<>();
    while (m.find())
      result.add(triple(m.group(2), m.group(1), m.group(3)));
    if (result.isEmpty()) {
      m = amieTriplePattern.matcher(s);
      while (m.find())
        result.add(triple(m.group(1), m.group(2), m.group(3)));
    }
    return (result);
  }

  /** Parses a rule of the form triple & triple & ... => triple
   * or triple :- triple & triple & ... */
  public static Pair<List<ByteString[]>, ByteString[]> rule(String s) {
    List<ByteString[]> triples = triples(s);
    if(triples.isEmpty()) return null;
    if (s.contains(":-")) return (new Pair<>(triples.subList(1, triples.size()), triples.get(0)));
    if (s.contains("=>")) return (new Pair<>(triples.subList(0, triples.size() - 1), triples.get(triples.size() - 1)));
    return (null);
  }


  public Set<ByteString> difference(ByteString projectionVariable, List<ByteString[]> antecedent, List<ByteString[]> head) {
    // TODO Auto-generated method stub
    Set<ByteString> bodyBindings = new HashSet<ByteString>(selectDistinct(projectionVariable, antecedent));
    Set<ByteString> headBindings = selectDistinct(projectionVariable, head);

    bodyBindings.removeAll(headBindings);
    return bodyBindings;
  }


  public Map<ByteString, IntHashMap<ByteString>> difference(ByteString var1, ByteString var2, List<ByteString[]> antecedent, List<ByteString[]> head) {
    // Look for all bindings for the variables that appear on the antecedent but not in the head
    Map<ByteString, IntHashMap<ByteString>> bodyBindings = selectDistinct(var1, var2, antecedent);
    Map<ByteString, IntHashMap<ByteString>> headBindings = selectDistinct(var1, var2, head);
    Map<ByteString, IntHashMap<ByteString>> result = new HashMap<ByteString, IntHashMap<ByteString>>();

    Set<ByteString> keySet = bodyBindings.keySet();
    for (ByteString key : keySet) {
      if (!headBindings.containsKey(key)) result.put(key, bodyBindings.get(key));
    }

    return result;
  }
  
  public static int aggregate(Map<ByteString, IntHashMap<ByteString>> bindings){
	  int result = 0;
	  
	  for(ByteString binding: bindings.keySet()){
		  result += bindings.get(binding).size();
	  }
	  
	  return result;
  }
  
  public static void hardQueriesTest(FactDatabase d){
	    long t1 = System.currentTimeMillis();
	    D.p(d.selectDistinct(compress("?s13"), triples("?s13 <isCitizenOf> ?o93 & ?o13 <isLocatedIn> ?o93 & ?s13 <diedIn> ?x")));
	    long t2 = System.currentTimeMillis();
	    System.out.println("Q1 Time: " + ((double)(t2 - t1)) + " ms");
	    
	    t1 = System.currentTimeMillis();    
	    D.p(d.frequentBindingsOf(compress("?p9136"), compress("?s21"), triples(triple("?s21", "<isLocatedIn>", "?o21"), triple("?s697", "<isLocatedIn>", "?o21"), triple("?o21", "?p9136", "?s697"))));
	    t2 = System.currentTimeMillis();
	    System.out.println("Q2 Time: " + ((double)(t2 - t1)) + " ms");
	    
	    t1 = System.currentTimeMillis();
	    D.p(d.countDistinct(compress("?s25"), triples("?s25 <isCitizenOf> ?o72 & ?o25 <isLocatedIn> ?o72")));
	    t2 = System.currentTimeMillis();
	    System.out.println("Q3 Time: " + ((double)(t2 - t1)) + " ms");
	    
	    t1 = System.currentTimeMillis();    
	    D.p(d.frequentBindingsOf(compress("?p18230"), compress("?s21"), triples(triple("?s21", "<isLocatedIn>", "?o21"), triple("?s697", "<isLocatedIn>", "?o21"), triple("?s697", "?p18230", "?o18230"))));
	    t2 = System.currentTimeMillis();
	    System.out.println("Q4 Time: " + ((double)(t2 - t1)) + " ms");
	 
	    t1 = System.currentTimeMillis();        
	    D.p(d.frequentBindingsOf(compress("?p9126"), compress("?s21"), triples(triple("?s21", "<isLocatedIn>", "?o21"), triple("?s697", "<isCitizenOf>", "?o21"), triple("?s697", "?p9126", "?o21"))));    
	    t2 = System.currentTimeMillis();
	    System.out.println("Q5 Time: " + ((double)(t2 - t1)) + " ms");
	    
	    t1 = System.currentTimeMillis();        
	    D.p(d.frequentBindingsOf(compress("?p9126"), compress("?s21"), triples(triple("?s21", "<isLocatedIn>", "?o21"), triple("?s697", "<isCitizenOf>", "?o21"), triple("?o21", "?p9126", "?s697"))));    
	    t2 = System.currentTimeMillis();
	    System.out.println("Q6 Time: " + ((double)(t2 - t1)) + " ms");	  
  }

  /** test */
  public static void main(String[] args) throws Exception {
    FactDatabase d = new FactDatabase();
    ArrayList<File> files = new ArrayList<File>();
    for(String file: args)
    	files.add(new File(file));
    
    d.load(files);
    D.p(d.selectDistinct(ByteString.of("?a"), ByteString.of("?b"), triples(triple("?a","<hasChild>","?b"), triple("?b", "<hasChild>", "?a"), triple("?a", "<isMarriedTo>", "?b"))));
    D.p(d.selectDistinct(ByteString.of("?a"), ByteString.of("?b"), triples(triple("?a","<isLocatedIn>","?b"), triple("?a", "<diedIn>", "?b"))));    
  }
}
