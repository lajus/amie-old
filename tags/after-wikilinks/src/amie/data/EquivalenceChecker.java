package amie.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javatools.administrative.D;
import javatools.datatypes.ByteString;

/**
 * Checks whether two queries are equivalent
 * 
 * @author Fabian M. Suchanek
 *
 */
public class EquivalenceChecker {

  /** TRUE if two queries are equal*/
  public static boolean equal(List<ByteString[]> q1, List<ByteString[]> q2) {
    return (q1.size()==q2.size() && equal(makeAllVariablesPseudoConstants(q1), 0, q2));
  }
  
  /** Makes all variables constants*/
  public static List<ByteString[]> makeAllVariablesPseudoConstants(List<ByteString[]> q) {
    List<ByteString[]> result = new ArrayList<>(q.size());
    for (int i = 0; i < q.size(); i++) {
      ByteString[] t = q.get(i);
      ByteString[] newT = new ByteString[t.length];
      for (int j = 0; j < t.length; j++) {
        newT[j] = FactDatabase.isVariable(t[j])?ByteString.of("@"+t[j]):t[j];
      }
      result.add(newT);
    }
    return (result);
  }
  
  /** TRUE if two queries are equal*/
  protected static boolean equal(List<ByteString[]> q1, int pos, List<ByteString[]> q2) {
    if (pos == q1.size()) return (true);
    ByteString[] t1 = q1.get(pos);
    for (int i2 = 0; i2 < q2.size(); i2++) {
      ByteString[] t2 = q2.get(i2);
      Map<ByteString, ByteString> match = match(t2, t1);
      if (match == null) continue;
      if (equal(q1, pos + 1, instantiateAndRemove(q2, match, t2))) {
        return (true);
      }
    }
    return (false);
  }

  /** returns a mapping of the variables of t1 to the ones of t2 to match t2 -- or null*/
  public static Map<ByteString, ByteString> match(ByteString[] t1, ByteString[] t2) {
    Map<ByteString, ByteString> result = new HashMap<>();
    for (int i = 0; i < t1.length; i++) {
      if (t1[i].equals(t2[i])) continue;
      if (FactDatabase.isVariable(t1[i]) && t2[i].charAt(0)=='@') {
        if (result.containsKey(t1[i]) && !result.get(t1[i]).equals(t2[i])) return (null);
        result.put(t1[i], t2[i]);
        continue;
      }
      return (null);
    }
    return (result);
  }

  /** Instantiates a query*/
  public static List<ByteString[]> instantiateAndRemove(List<ByteString[]> q, Map<ByteString, ByteString> match, ByteString[] removeMe) {
    if (match == null) return (null);
    if (match.isEmpty()) return (q);
    List<ByteString[]> result = new ArrayList<>(q.size());
    for (int i = 0; i < q.size(); i++) {
      ByteString[] t = q.get(i);
      if (t.equals(removeMe)) continue;
      ByteString[] newT = new ByteString[t.length];
      for (int j = 0; j < t.length; j++) {
        newT[j] = D.getOr(match, t[j], t[j]);
      }
      result.add(newT);
    }
    return (result);
  }

  /** amie.tests*/
  public static void main(String[] args) throws Exception {
    D.p(equal(FactDatabase.triples(FactDatabase.triple("bob","loves","?x2"), FactDatabase.triple("?x2","livesIn","?x3")),FactDatabase.triples(FactDatabase.triple("?y2","livesIn","?y1"), FactDatabase.triple("bob","loves","?y2"))));
  }
}
