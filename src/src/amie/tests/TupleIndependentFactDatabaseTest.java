package amie.tests;

import java.util.ArrayList;
import java.util.List;

import javatools.datatypes.ByteString;
import amie.data.FactDatabase;
import amie.prediction.data.TupleIndependentFactDatabase;
import junit.framework.TestCase;

/**
 * Test cases for the probabilistic version of the support and the PCA
 * confidence.
 * @author galarrag
 *
 */
public class TupleIndependentFactDatabaseTest extends TestCase {
	public static TupleIndependentFactDatabase kb1;
	
	public static TupleIndependentFactDatabase kb2;
	
	public static TupleIndependentFactDatabase kb3;
	
	static {
		kb1 = new TupleIndependentFactDatabase();
		kb2 = new TupleIndependentFactDatabase();
		kb3 = new TupleIndependentFactDatabase();
		
		kb1.add("<Francois>", "<livesIn>", "<Paris>");
		kb1.add("<Francois>", "<livesIn>", "<Nantes>");
		kb1.add("<Francois>", "<livesIn>", "<France>");
		
		kb1.add("<Danai>", "<livesIn>", "<Lyon>");
		kb1.add("<Danai>", "<livesIn>", "<UK>");		
		
		kb1.add("<Antoine>", "<livesIn>", "<Strasbourg>");
		kb1.add("<Antoine>", "<livesIn>", "<France>");
		
		kb1.add("<Peter>", "<livesIn>", "<Italy>");
		
		
		kb1.add("<Paris>", "<locatedIn>", "<France>");
		
		kb1.add("<Metz>", "<locatedIn>", "<France>");
		
		kb1.add("<Nantes>", "<locatedIn>", "<France>");
		
		kb1.add("<Lyon>", "<locatedIn>", "<France>");
		
		kb1.add("<Nancy>", "<locatedIn>", "<France>");
		
		
		kb1.add("<Francois>", "<isCitizenOf>", "<France>");
		
		kb1.add("<Antoine>", "<isCitizenOf>", "<France>");

		kb1.add("<Danai>", "<isCitizenOf>", "<France>");		

		kb1.add("<Danai>", "<isCitizenOf>", "<Greece>");
		
		// Probabilistic triples
		kb2.add("<Jean>", "<livesIn>", "<Paris>", 0.5);
		kb2.add("<Jean>", "<livesIn>", "<Metz>", 0.5);
		kb2.add("<Jean>", "<isCitizenOf>", "<France>", 0.5);
		kb2.add("<Louis>", "<isCitizenOf>", "<France>", 0.5);
		kb2.add("<Louis>", "<livesIn>", "<Nancy>", 0.4);		
		kb2.add("<Marion>", "<livesIn>", "<Metz>", 0.5);
		
		kb2.add("<Metz>", "<locatedIn>", "<France>");
		kb2.add("<Paris>", "<locatedIn>", "<France>");
		kb2.add("<Nancy>", "<locatedIn>", "<France>");
		kb2.add("<Lyon>", "<locatedIn>", "<France>");
		
		// Examples for rules with constants
		
		kb3.add("<Louis>", "<speaks>", "<French>");
		kb3.add("<Jean>", "<speaks>", "<French>");
		kb3.add("<Danai>", "<speaks>", "<French>");
		kb3.add("<Romain>", "<speaks>", "<French>");
				
		kb3.add("<Jean>", "<isCitizenOf>", "<France>");
		kb3.add("<Louis>", "<isCitizenOf>", "<France>");
		kb3.add("<Danai>", "<isCitizenOf>", "<Greece>");
		kb3.add("<Romain>", "<isCitizenOf>", "<France>");		
	}
	
	public void test0() {
		List<ByteString[]> body0 = FactDatabase.triples(
				FactDatabase.triple("?a", "<isCitizenOf>", "?b"));
		
		ByteString[] head0 = FactDatabase.triple("?a", "<livesIn>", "?b");
		List<ByteString[]> rule0 = new ArrayList<ByteString[]>();
		rule0.add(head0);
		rule0.addAll(body0);
		double scores[] = kb1.probabilitiesOf(body0, head0, ByteString.of("?b"));
		long support = kb1.countPairs(ByteString.of("?a"), ByteString.of("?b"), rule0);
		head0[2] = ByteString.of("?y");
		long pcaBody = kb1.countPairs(ByteString.of("?a"), ByteString.of("?b"), rule0);
		assertEquals((double)support, scores[0], 0.00000001);
		assertEquals((double)pcaBody, scores[1], 0.00000001);
	}
	
	public void test1() {
		List<ByteString[]> body1 = FactDatabase.triples(
				FactDatabase.triple("?a", "<livesIn>", "?c"),
				FactDatabase.triple("?c", "<locatedIn>", "?b"));
		
		ByteString[] head1 = FactDatabase.triple("?a", "<livesIn>", "?b");
		List<ByteString[]> rule1 = new ArrayList<ByteString[]>();
		rule1.add(head1);
		rule1.addAll(body1);
		double scores[] = kb1.probabilitiesOf(body1, head1, ByteString.of("?b"));
		long support = kb1.countPairs(ByteString.of("?a"), ByteString.of("?b"), rule1);
		head1[2] = ByteString.of("?y");
		long pcaBody = kb1.countPairs(ByteString.of("?a"), ByteString.of("?b"), rule1);
		assertEquals((double)support, scores[0], 0.00000001);
		assertEquals((double)pcaBody, scores[1], 0.00000001);
	}
	
	public void test2() {
		List<ByteString[]> body2 = FactDatabase.triples(
				FactDatabase.triple("?a", "<livesIn>", "?c"),
				FactDatabase.triple("?c", "<locatedIn>", "?b"));
		
		ByteString[] head2 = FactDatabase.triple("?a", "<isCitizenOf>", "?b");
		List<ByteString[]> rule2 = new ArrayList<ByteString[]>();
		rule2.add(head2);
		rule2.addAll(body2);
		double scores[] = kb2.probabilitiesOf(body2, head2, ByteString.of("?b"));
		assertEquals(0.575, scores[0], 0.00000001);
		assertEquals(scores[0], scores[1], 0.00000001);
	}
	
	public void test3() {	
		List<ByteString[]> body3 = FactDatabase.triples(
				FactDatabase.triple("?a", "<livesIn>", "?c"),
				FactDatabase.triple("?c", "<locatedIn>", "?b"));
		
		ByteString[] head3 = FactDatabase.triple("?a", "<isCitizenOf>", "?b");
		List<ByteString[]> rule3 = new ArrayList<ByteString[]>();
		rule3.add(head3);
		rule3.addAll(body3);
		double scores1[] = kb2.probabilitiesOf(body3, head3, ByteString.of("?b"));
		
		kb2.add("<Guillaume>", "<livesIn>", "<Lyon>", 0.5);
		kb2.add("<Guillaume>", "<isCitizenOf>", "<Luxembourg>", 0.7); // This introduces an error of 0.35
		
		double scores2[] = kb2.probabilitiesOf(body3, head3, ByteString.of("?b"));
		
		assertEquals(scores1[0], scores2[0], 0.00000001);
		assertEquals(scores2[1], scores2[0] + 0.35, 0.00000001);
	}
	
	public void test4() {
		List<ByteString[]> body4 = FactDatabase.triples(
				FactDatabase.triple("?a", "<speaks>", "<French>"));
		
		ByteString[] head4 = FactDatabase.triple("?a", "<isCitizenOf>", "<France>");
		List<ByteString[]> rule4 = new ArrayList<ByteString[]>();
		rule4.add(head4);
		rule4.addAll(body4);
		double scores[] = kb3.probabilitiesOf(body4, head4, ByteString.of("<France>"));
		long support = kb3.countDistinct(ByteString.of("?a"), rule4);
		head4[2] = ByteString.of("?y");
		long pcaBody = kb3.countDistinct(ByteString.of("?a"), rule4);
		assertEquals((double)support, scores[0], 0.00000001);
		assertEquals((double)pcaBody, scores[1], 0.00000001);
	}
	
	public void test5() {
		List<ByteString[]> body5 = FactDatabase.triples(
				FactDatabase.triple("?a", "<speaks>", "<French>"));
		
		ByteString[] head5 = FactDatabase.triple("?a", "<isCitizenOf>", "<France>");
		List<ByteString[]> rule5 = new ArrayList<ByteString[]>();
		rule5.add(head5);
		rule5.addAll(body5);
		double scores1[] = kb3.probabilitiesOf(body5, head5, ByteString.of("<France>"));
		
		kb3.add("<Gonzalo>", "<speaks>", "<Spanish>", 0.7);
		kb3.add("<Gonzalo>", "<isCitizenOf>", "<Ecuador>", 0.7); // No effect
		
		kb3.add("<Pierre>", "<speaks>", "<French>", 0.8);
		kb3.add("<Pierre>", "<isCitizenOf>", "<France>", 0.8); // 0.64 to score[0]
		
		kb3.add("<Fabian>", "<isCitizenOf>", "<France>", 0.8); // 0.64 to score[0]
		kb3.add("<Fabian>", "<isCitizenOf>", "<Germany>", 0.8); // Counter-example contributes with 0.96 * 0.8
		kb3.add("<Fabian>", "<speaks>", "<French>", 0.8); // 1.28 to score[1]

		double scores2[] = kb3.probabilitiesOf(body5, head5, ByteString.of("<France>"));
		
		assertEquals(scores1[0] + 0.64 + 0.64, scores2[0], 0.00000001);
		assertEquals(scores1[1] + 0.64 + 0.768, scores2[1], 0.00000001);
	}
}
