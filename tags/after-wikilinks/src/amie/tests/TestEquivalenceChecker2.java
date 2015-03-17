package amie.tests;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import javatools.datatypes.ByteString;
import javatools.datatypes.Pair;
import junit.framework.TestCase;
import amie.data.EquivalenceChecker2;
import amie.data.FactDatabase;
import amie.query.Query;

public class TestEquivalenceChecker2 extends TestCase {
	
	List<Pair<List<ByteString[]>, List<ByteString[]>>> cases;
	
	public TestEquivalenceChecker2(String name) {
		super(name);
	}

	protected void setUp() throws Exception {
		super.setUp();
		cases = new ArrayList<>();
		
		//The same query, 1 triple pattern
		List<ByteString[]> q01 = FactDatabase.triples(FactDatabase.triple("bob","loves","?x2"));
		List<ByteString[]> q02 = FactDatabase.triples(FactDatabase.triple("bob","loves","?x2"));
		Pair<List<ByteString[]>, List<ByteString[]>> p0 = new Pair<>(q01, q02);
		cases.add(p0);
		
		//The same query, 2 triple patterns
		List<ByteString[]> q11 = FactDatabase.triples(FactDatabase.triple("bob","loves","?x2"), FactDatabase.triple("?x2","livesIn","?x3"));
		List<ByteString[]> q12 = FactDatabase.triples(FactDatabase.triple("bob","loves","?x2"), FactDatabase.triple("?x2","livesIn","?x3"));
		Pair<List<ByteString[]>, List<ByteString[]>> p1 = new Pair<>(q11, q12);
		cases.add(p1);
		
		//Two different queries, one triple pattern
		List<ByteString[]> q21 = FactDatabase.triples(FactDatabase.triple("bob","loves","?x2"));
		List<ByteString[]> q22 = FactDatabase.triples(FactDatabase.triple("?z","loves","?x"));
		Pair<List<ByteString[]>, List<ByteString[]>> p2 = new Pair<>(q21, q22);
		cases.add(p2);
		
		//Two different queries, 2 triple patterns
		List<ByteString[]> q31 = FactDatabase.triples(FactDatabase.triple("bob","loves","?x2"), FactDatabase.triple("?x2","livesIn","?x3"));
		List<ByteString[]> q32 = FactDatabase.triples(FactDatabase.triple("bob","loves","?x2"), FactDatabase.triple("?x2","isCitizenOf","?x3"));
		Pair<List<ByteString[]>, List<ByteString[]>> p3 = new Pair<>(q31, q32);
		cases.add(p3);

		//The same path query with items shuffled, 2 triple patterns
		List<ByteString[]> q41 = FactDatabase.triples(FactDatabase.triple("bob","loves","?x2"), FactDatabase.triple("?x2","livesIn","?x3"));
		List<ByteString[]> q42 = FactDatabase.triples(FactDatabase.triple("?y2","livesIn","?y1"), FactDatabase.triple("bob","loves","?y2"));
		Pair<List<ByteString[]>, List<ByteString[]>> p4 = new Pair<>(q41, q42);
		cases.add(p4);
		
		//The same star query with items shuffled, 3 triple patterns
		List<ByteString[]> q51 = FactDatabase.triples(FactDatabase.triple("?s","loves","?x"), FactDatabase.triple("?s","knows","?y"), FactDatabase.triple("?y","livesIn","?x"));
		List<ByteString[]> q52 = FactDatabase.triples(FactDatabase.triple("?s","knows","?y"), FactDatabase.triple("?s","loves","?x"), FactDatabase.triple("?y","livesIn","?x"));
		Pair<List<ByteString[]>, List<ByteString[]>> p5 = new Pair<>(q51, q52);
		cases.add(p5);
		
		//Two different star queries, 3 triple patterns
		List<ByteString[]> q61 = FactDatabase.triples(FactDatabase.triple("?s","loves","?y"), FactDatabase.triple("?s","knows","?y"), FactDatabase.triple("?s","livesIn","?m"));
		List<ByteString[]> q62 = FactDatabase.triples( FactDatabase.triple("?k","livesIn","?m"), FactDatabase.triple("?k","loves","?o"), FactDatabase.triple("?k","knows","?y"));
		Pair<List<ByteString[]>, List<ByteString[]>> p6 = new Pair<>(q61, q62);
		cases.add(p6);
		
		//The same cycle query, 3 triple patterns
		List<ByteString[]> q71 = FactDatabase.triples(FactDatabase.triple("?s","isConnected","?o"), FactDatabase.triple("?x","isConnected","?s"), FactDatabase.triple("?o","isConnected","?x"));
		List<ByteString[]> q72 = FactDatabase.triples(FactDatabase.triple("?m","isConnected","?s"), FactDatabase.triple("?r","isConnected","?m"), FactDatabase.triple("?s","isConnected","?r"));
		Pair<List<ByteString[]>, List<ByteString[]>> p7 = new Pair<>(q71, q72);
		cases.add(p7);

		//Different cycle queries, 3 triple patterns
		List<ByteString[]> q81 = FactDatabase.triples(FactDatabase.triple("?s","isConnected","?o"), FactDatabase.triple("?x","isConnected","?s"), FactDatabase.triple("?o","isConnected","?x"));
		List<ByteString[]> q82 = FactDatabase.triples(FactDatabase.triple("?m","isConnected","?s"), FactDatabase.triple("?r","isConnected","?m"), FactDatabase.triple("?s","isConnected","?r"));
		Pair<List<ByteString[]>, List<ByteString[]>> p8 = new Pair<>(q81, q82);
		cases.add(p8);
		
		//Close star items shuffled, 3 triple patterns
		List<ByteString[]> q91 = FactDatabase.triples(FactDatabase.triple("?s","created","?o"), FactDatabase.triple("?s","directed","?o"), FactDatabase.triple("?s","produced","?o"));
		List<ByteString[]> q92 = FactDatabase.triples(FactDatabase.triple("?x","produced","?y"), FactDatabase.triple("?x","created","?y"), FactDatabase.triple("?x","directed","?y"));
		Pair<List<ByteString[]>, List<ByteString[]>> p9 = new Pair<>(q91, q92);
		cases.add(p9);
		
		//Different queries, close star items shuffled, 3 triple patterns
		List<ByteString[]> q101 = FactDatabase.triples(FactDatabase.triple("?s","created","?o"), FactDatabase.triple("?s","directed","?o"), FactDatabase.triple("?s","produced","?o"));
		List<ByteString[]> q102 = FactDatabase.triples(FactDatabase.triple("?x","produced","?z"), FactDatabase.triple("?x","created","?y"), FactDatabase.triple("?x","directed","?y"));
		Pair<List<ByteString[]>, List<ByteString[]>> p10 = new Pair<>(q101, q102);
		cases.add(p10);
		
		//Reflexive query
		List<ByteString[]> q111 = FactDatabase.triples(FactDatabase.triple("?s","married","?u"), FactDatabase.triple("?u","married","?s"));
		List<ByteString[]> q112 = FactDatabase.triples(FactDatabase.triple("?x","married","?z"), FactDatabase.triple("?z","married","?x"));
		Pair<List<ByteString[]>, List<ByteString[]>> p11 = new Pair<>(q111, q112);
		cases.add(p11);
		
		//Non Reflexive query
		List<ByteString[]> q121 = FactDatabase.triples(FactDatabase.triple("?s","married","?u"), FactDatabase.triple("?u","married","?s"));
		List<ByteString[]> q122 = FactDatabase.triples(FactDatabase.triple("?x","married","?z"), FactDatabase.triple("?z","married","?y"));
		Pair<List<ByteString[]>, List<ByteString[]>> p12 = new Pair<>(q121, q122);
		cases.add(p12);
		
		//Non Reflexive query
		List<ByteString[]> q131 = FactDatabase.triples(FactDatabase.triple("?s16","connected","?o16"), FactDatabase.triple("?s16","connected","?o308"), FactDatabase.triple("?o16","connected","?o318"));
		List<ByteString[]> q132 = FactDatabase.triples(FactDatabase.triple("?s16","connected","?o16"), FactDatabase.triple("?o16","connected","?o318"), FactDatabase.triple("?s16","connected","?o308"));
		Pair<List<ByteString[]>, List<ByteString[]>> p13 = new Pair<>(q131, q132);
		cases.add(p13);
		
		//Problematic case
		List<ByteString[]> q141 = FactDatabase.triples(FactDatabase.triple("?s16","<isLocatedIn>","?o16"), FactDatabase.triple("?s552","<livesIn>","?s16"), FactDatabase.triple("?s552","<isPoliticianOf>","?o16"));
		List<ByteString[]> q142 = FactDatabase.triples(FactDatabase.triple("?s16","<isLocatedIn>","?o16"), FactDatabase.triple("?s552","<isPoliticianOf>","?o16"), FactDatabase.triple("?s552","<livesIn>","?s16"));
		Pair<List<ByteString[]>, List<ByteString[]>> p14 = new Pair<>(q141, q142);
		cases.add(p14);
		
		//Rules with slight changes in topology
		List<ByteString[]> q151 = FactDatabase.triples(FactDatabase.triple("?c","<hasChild>","?b"), FactDatabase.triple("?a","<hasChild>","?b"), FactDatabase.triple("?c","<isMarriedTo>","?a"));
		List<ByteString[]> q152 = FactDatabase.triples(FactDatabase.triple("?c","<hasChild>","?a"), FactDatabase.triple("?b","<hasChild>","?a"), FactDatabase.triple("?b","<isMarriedTo>","?c"));
		Pair<List<ByteString[]>, List<ByteString[]>> p15 = new Pair<>(q151, q152);
		cases.add(p15);
		
		List<ByteString[]> q161 = FactDatabase.triples(FactDatabase.triple("?c","<hasChild>","?b"), FactDatabase.triple("?a","<hasChild>","?b"), FactDatabase.triple("?c","<isMarriedTo>","?a"));
		List<ByteString[]> q162 = FactDatabase.triples(FactDatabase.triple("?c","<hasChild>","?b"), FactDatabase.triple("?a","<hasChild>","?b"), FactDatabase.triple("?a","<isMarriedTo>","?c"));
		Pair<List<ByteString[]>, List<ByteString[]>> p16 = new Pair<>(q161, q162);
		cases.add(p16);

		Pair<List<ByteString[]>, List<ByteString[]>> p17 = new Pair<>(q152, q162);
		cases.add(p17);
		
		List<ByteString[]> q181 = FactDatabase.triples(FactDatabase.triple("?s4","<hasWebsite>","?o4"), FactDatabase.triple("?s4","<isLocatedIn>","?o6"), FactDatabase.triple("?o6","<hasWebsite>","?o4"));
		List<ByteString[]> q182 = FactDatabase.triples(FactDatabase.triple("?s4","<hasWebsite>","?o4"), FactDatabase.triple("?s6","<isLocatedIn>","?s4"), FactDatabase.triple("?s6","<hasWebsite>","?o4"));
		List<ByteString[]> q183 = FactDatabase.triples(FactDatabase.triple("?s4","<hasWebsite>","?o4"), FactDatabase.triple("?s6","<hasWebsite>","?o4"), FactDatabase.triple("?s6","<isLocatedIn>","?s4"));
		List<ByteString[]> q184 = FactDatabase.triples(FactDatabase.triple("?s4","<hasWebsite>","?o4"), FactDatabase.triple("?s6","<hasWebsite>","?o4"), FactDatabase.triple("?s4","<isLocatedIn>","?s6"));
		Pair<List<ByteString[]>, List<ByteString[]>> p18 = new Pair<>(q181, q182);
		Pair<List<ByteString[]>, List<ByteString[]>> p19 = new Pair<>(q181, q183);
		Pair<List<ByteString[]>, List<ByteString[]>> p20 = new Pair<>(q181, q184);
		Pair<List<ByteString[]>, List<ByteString[]>> p21 = new Pair<>(q182, q183);
		Pair<List<ByteString[]>, List<ByteString[]>> p22 = new Pair<>(q182, q184);		
		Pair<List<ByteString[]>, List<ByteString[]>> p23 = new Pair<>(q183, q184);
		
		cases.add(p18);
		cases.add(p19);
		cases.add(p20);
		cases.add(p21);
		cases.add(p22);
		cases.add(p23);

		List<ByteString[]> q241 = FactDatabase.triples(FactDatabase.triple("?s29","<isMarriedTo>","?o29"), FactDatabase.triple("?s29","<hasChild>","?o121"), FactDatabase.triple("?o29","<hasChild>","?o121"));
		List<ByteString[]> q242 = FactDatabase.triples(FactDatabase.triple("?s29","<isMarriedTo>","?o29"), FactDatabase.triple("?o29","<hasChild>","?o121"), FactDatabase.triple("?s29","<hasChild>","?o121"));
		Pair<List<ByteString[]>, List<ByteString[]>> p24 = new Pair<>(q241, q242);
		cases.add(p24);
		
		List<ByteString[]> q251 = FactDatabase.triples(FactDatabase.triple("?s40","<hasOfficialLanguage>","?o40"), FactDatabase.triple("?s4120","<hasOfficialLanguage>","?o29"), FactDatabase.triple("?s40","<dealsWith>","?s4120"));
		List<ByteString[]> q252 = FactDatabase.triples(FactDatabase.triple("?s40","<hasOfficialLanguage>","?o40"), FactDatabase.triple("?s4120","<hasOfficialLanguage>","?o29"), FactDatabase.triple("?s40","<dealsWith>","?s4120"));
		Pair<List<ByteString[]>, List<ByteString[]>> p25 = new Pair<>(q251, q252);
		cases.add(p25);
		
		List<ByteString[]> q261 = FactDatabase.triples(FactDatabase.triple("?a","<livesIn>","?c"), FactDatabase.triple("?a","<livesIn>","?b"), FactDatabase.triple("?b","<hasCapital>","?c"));
		List<ByteString[]> q262 = FactDatabase.triples(FactDatabase.triple("?a","<livesIn>","?b"), FactDatabase.triple("?a","<livesIn>","?c"), FactDatabase.triple("?b","<hasCapital>","?c"));
		Pair<List<ByteString[]>, List<ByteString[]>> p26 = new Pair<>(q261, q262);
		cases.add(p26);
		
		List<ByteString[]> q271 = FactDatabase.triples(FactDatabase.triple("?f","<dealsWith>","?a"), FactDatabase.triple("?b","<dealsWith>","?f"), FactDatabase.triple("?a","<dealsWith>","?b"));
		List<ByteString[]> q272 = FactDatabase.triples(FactDatabase.triple("?e","<dealsWith>","?a"), FactDatabase.triple("?b","<dealsWith>","?e"), FactDatabase.triple("?a","<dealsWith>","?b"));
		Pair<List<ByteString[]>, List<ByteString[]>> p27 = new Pair<>(q271, q272);
		cases.add(p27);
		
		List<ByteString[]> q281 = FactDatabase.triples(FactDatabase.triple("<New_Zealand>","<participatedIn>","?b"), FactDatabase.triple("<United_Kingdom>","<participatedIn>","?b"));
		List<ByteString[]> q282 = FactDatabase.triples(FactDatabase.triple("<United_Kingdom>","<participatedIn>","?b"), FactDatabase.triple("<New_Zealand>","<participatedIn>","?b"));
		Pair<List<ByteString[]>, List<ByteString[]>> p28 = new Pair<>(q281, q282);
		cases.add(p28);

		List<ByteString[]> q291 = FactDatabase.triples(FactDatabase.triple("?a","<holdsPoliticalPosition>","?b"), 
				FactDatabase.triple("?k","<livesIn>","?b"), 
				FactDatabase.triple("?f", "<isLocatedIn>", "?j"), 
				FactDatabase.triple("?j", "<hasCapital>", "?n"));
		List<ByteString[]> q292 = FactDatabase.triples(FactDatabase.triple("?a","<holdsPoliticalPosition>","?c"), 
				FactDatabase.triple("?f", "<isLocatedIn>", "?j"), 
				FactDatabase.triple("?j", "<hasCapital>", "?n"),
				FactDatabase.triple("?z","<livesIn>","?c"));
		Pair<List<ByteString[]>, List<ByteString[]>> p29 = new Pair<>(q291, q292);
		cases.add(p29);
		
		List<ByteString[]> q301 = FactDatabase.triples(FactDatabase.triple("?a","<holdsPoliticalPosition>","?b"), 
				FactDatabase.triple("?f","<livesIn>","?b"), 
				FactDatabase.triple("?f", "<isLocatedIn>", "?j"), 
				FactDatabase.triple("?j", "<hasCapital>", "?n"));
		List<ByteString[]> q302 = FactDatabase.triples(FactDatabase.triple("?a","<holdsPoliticalPosition>","?c"), 
				FactDatabase.triple("?f", "<isLocatedIn>", "?j"), 
				FactDatabase.triple("?j", "<hasCapital>", "?n"),
				FactDatabase.triple("?z","<livesIn>","?c"));
		Pair<List<ByteString[]>, List<ByteString[]>> p30 = new Pair<>(q301, q302);
		cases.add(p30);
		
		List<ByteString[]> q311 = FactDatabase.triples(FactDatabase.triple("?a","<hasAcademicAdvisor>","?b"), 
				FactDatabase.triple("?x","<influences>","?f"), 
				FactDatabase.triple("?f", "<influences>", "?j"), 
				FactDatabase.triple("?j", "<hasAcademicAdvisor>", "?n"));
		List<ByteString[]> q312 = FactDatabase.triples(FactDatabase.triple("?a","<hasAcademicAdvisor>","?b"), 
				FactDatabase.triple("?x","<influences>","?f"), 
				FactDatabase.triple("?j", "<hasAcademicAdvisor>", "?n"),
				FactDatabase.triple("?f", "<influences>", "?j"));
		Pair<List<ByteString[]>, List<ByteString[]>> p31 = new Pair<>(q311, q312);
		cases.add(p31);
	}
	
	public void test0(){
		assertTrue(EquivalenceChecker2.equal(cases.get(0).first, cases.get(0).second));
	}
	
	public void test1(){
		assertTrue(EquivalenceChecker2.equal(cases.get(1).first, cases.get(1).second));
	}
	
	public void test2(){
		assertTrue(!EquivalenceChecker2.equal(cases.get(2).first, cases.get(2).second));
	}
	
	public void test3(){
		assertTrue(!EquivalenceChecker2.equal(cases.get(3).first, cases.get(3).second));
	}
	
	public void test4(){
		assertFalse(EquivalenceChecker2.equal(cases.get(4).first, cases.get(4).second));
	}
	
	public void test5(){
		assertFalse(EquivalenceChecker2.equal(cases.get(5).first, cases.get(5).second));
	}
	
	public void test6(){
		assertTrue(!EquivalenceChecker2.equal(cases.get(6).first, cases.get(6).second));
	}
	
	public void test7(){
		assertTrue(EquivalenceChecker2.equal(cases.get(7).first, cases.get(7).second));
	}
	
	public void test8(){
		assertTrue(EquivalenceChecker2.equal(cases.get(8).first, cases.get(8).second));
	}
	
	public void test9(){
		assertTrue(!EquivalenceChecker2.equal(cases.get(9).first, cases.get(9).second));
	}
	
	public void test10(){
		assertTrue(!EquivalenceChecker2.equal(cases.get(10).first, cases.get(10).second));
	}
	
	public void test11(){
		assertTrue(EquivalenceChecker2.equal(cases.get(11).first, cases.get(11).second));
	}
	
	public void test12(){
		assertFalse(EquivalenceChecker2.equal(cases.get(12).first, cases.get(12).second));
	}
	
	public void test13(){
		assertTrue(EquivalenceChecker2.equal(cases.get(13).first, cases.get(13).second));
	}
	
	public void test14(){
		assertTrue(EquivalenceChecker2.equal(cases.get(14).first, cases.get(14).second));
	}
	
	public void test15(){
		assertFalse(EquivalenceChecker2.equal(cases.get(15).first, cases.get(15).second));
	}
	
	public void test16(){
		assertFalse(EquivalenceChecker2.equal(cases.get(16).first, cases.get(16).second));
	}
	
	public void test17(){
		assertTrue(EquivalenceChecker2.equal(cases.get(17).first, cases.get(17).second));
	}
	
	public void test18(){
		assertFalse(EquivalenceChecker2.equal(cases.get(18).first, cases.get(18).second));
	}
	
	public void test19(){
		assertFalse(EquivalenceChecker2.equal(cases.get(19).first, cases.get(19).second));
	}

	public void test20(){
		assertTrue(EquivalenceChecker2.equal(cases.get(20).first, cases.get(20).second));
	}
	
	public void test21(){
		assertTrue(EquivalenceChecker2.equal(cases.get(21).first, cases.get(21).second));
	}

	public void test22(){
		assertFalse(EquivalenceChecker2.equal(cases.get(22).first, cases.get(22).second));
	}
	
	public void test23(){
		assertFalse(EquivalenceChecker2.equal(cases.get(23).first, cases.get(23).second));
	}

	public void test24(){
		assertTrue(EquivalenceChecker2.equal(cases.get(24).first, cases.get(24).second));
	}
	
	public void test25(){
		assertTrue(EquivalenceChecker2.equal(cases.get(25).first, cases.get(25).second));
	}
	
	public void test26(){
		assertFalse(EquivalenceChecker2.equal(cases.get(26).first, cases.get(26).second));
	}
	
	public void test27(){
		assertTrue(EquivalenceChecker2.equal(cases.get(27).first, cases.get(27).second));
	}
	
	public void test28(){
		assertFalse(EquivalenceChecker2.equal(cases.get(28).first, cases.get(28).second));
	}
	
	public void test29(){
		List<ByteString[]> q1 = FactDatabase.triples(FactDatabase.triple("?a","<hasAcademicAdvisor>","?b"), FactDatabase.triple("?f","<hasAcademicAdvisor>","?b"), FactDatabase.triple("?a","<hasAcademicAdvisor>","?f"));
		List<ByteString[]> q2 = FactDatabase.triples(FactDatabase.triple("?a","<hasAcademicAdvisor>","?b"), FactDatabase.triple("?a","<hasAcademicAdvisor>","?e"), FactDatabase.triple("?e","<hasAcademicAdvisor>","?b"));
		LinkedHashSet<Query> pool = new LinkedHashSet<>();
		Query fq1 = new Query();
		fq1.getTriples().addAll(q1);
		Query fq2 = new Query();
		fq1.setCardinality(84);
		fq1.getHeadKey();
		fq2.getTriples().addAll(q2);
		fq2.setCardinality(84);
		fq2.getHeadKey();
		pool.add(fq1);
		assertTrue(fq2.equals(fq1));
		//assertTrue(pool.contains(fq2));
		pool.add(fq2);
		System.out.println(pool);
	}
	
	public void test30(){
		assertTrue(EquivalenceChecker2.equal(cases.get(29).first, cases.get(29).second));
	}
	
	public void test31(){
		assertFalse(EquivalenceChecker2.equal(cases.get(30).first, cases.get(30).second));
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}
}