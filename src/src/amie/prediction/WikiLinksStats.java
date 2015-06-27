package amie.prediction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javatools.datatypes.ByteString;
import amie.data.FactDatabase;

public class WikiLinksStats {
	
	private static ByteString subjectVar = ByteString.of("?s");
	
	private static ByteString objectVar = ByteString.of("?o");
	
	private static ByteString relationVar = ByteString.of("?p");

	public static List<ByteString[]> getNoLinksQuery(ByteString type1, ByteString type2) {
		List<ByteString[]> query = new ArrayList<>();
		query.addAll(
				FactDatabase.triples(
						FactDatabase.triple(ByteString.of("?s"), ByteString.of("<linksTo>"), ByteString.of("?o")),
						FactDatabase.triple(ByteString.of("?s"), ByteString.of("?p"), ByteString.of("?o")),
						FactDatabase.triple(ByteString.of("?p"), FactDatabase.DIFFERENTFROMbs, ByteString.of("<linksTo>"))
						));
		
		if (type1 != null) {
			query.add(FactDatabase.triple(ByteString.of("?s"), ByteString.of("rdf:type"), type1));
		}
		
		if (type2 != null) {
			query.add(FactDatabase.triple(ByteString.of("?o"), ByteString.of("rdf:type"), type2));
		}
		
		return query;
	}
	
	public static List<ByteString[]> getLinksQuery(ByteString type1, ByteString type2) {
		List<ByteString[]> query = new ArrayList<>();
		query.addAll(
				FactDatabase.triples(
						FactDatabase.triple(ByteString.of("?s"), ByteString.of("<linksTo>"), ByteString.of("?o"))
						)
					);
		
		if (type1 != null) {
			query.add(FactDatabase.triple(ByteString.of("?s"), ByteString.of("rdf:type"), type1));
		}
		
		if (type2 != null) {
			query.add(FactDatabase.triple(ByteString.of("?o"), ByteString.of("rdf:type"), type2));
		}
		
		return query;
	}
	
	public static void main(String args[]) throws IOException {
		FactDatabase db = new FactDatabase();
		List<File> files = new ArrayList<>();
		for (String arg : args) {
			files.add(new File(arg));
		}
		db.load(files);

		List<ByteString[]> noLinksquery = getNoLinksQuery(null, null);
		System.out.println("Semantified links " +  db.countDistinctPairs(subjectVar, objectVar, noLinksquery));
		System.out.println("Total links " +  db.count(FactDatabase.triple(subjectVar, ByteString.of("<linksTo>") , objectVar)));
		
/*		
		List<ByteString[]> noLinksqueryPersonPerson = getNoLinksQuery(ByteString.of("<dbo:Person>"), ByteString.of("<dbo:Person>"));
		List<ByteString[]> linksqueryPersonPerson = getLinksQuery(ByteString.of("<dbo:Person>"), ByteString.of("<dbo:Person>"));
		System.out.println("Links Person -> Person without any other relation: " + db.countPairs(subjectVar, objectVar, noLinksqueryPersonPerson));
		System.out.println("Links Person -> Person: " + db.countPairs(subjectVar, objectVar, linksqueryPersonPerson));
		System.out.println("Top relations: " + 
		telecom.util.collections.Collections.toString(db.countProjectionBindings(noLinksqueryPersonPerson.get(0), noLinksqueryPersonPerson.subList(1, noLinksqueryPersonPerson.size()), relationVar)));
		
		List<ByteString[]> noLinksqueryPersonOrganization = getNoLinksQuery(ByteString.of("<dbo:Person>"), ByteString.of("<dbo:Organisation>"));
		List<ByteString[]> linksqueryPersonOrganization = getLinksQuery(ByteString.of("<dbo:Person>"), ByteString.of("<dbo:Organisation>"));
		System.out.println("Links Person -> Organisation without any other relation: " + db.countPairs(subjectVar, objectVar, noLinksqueryPersonOrganization));
		System.out.println("Links Person -> Organisation: " + db.countPairs(subjectVar, objectVar, linksqueryPersonOrganization));
		System.out.println("Top relations: " + 
		telecom.util.collections.Collections.toString(db.countProjectionBindings(noLinksqueryPersonOrganization.get(0), noLinksqueryPersonOrganization.subList(1, noLinksqueryPersonOrganization.size()), relationVar)));
		
		List<ByteString[]> noLinksqueryPersonPlace = getNoLinksQuery(ByteString.of("<dbo:Person>"), ByteString.of("<dbo:Place>"));
		List<ByteString[]> linksqueryPersonPlace = getLinksQuery(ByteString.of("<dbo:Person>"), ByteString.of("<dbo:Place>"));
		System.out.println("Links Person -> Place without any other relation: " + db.countPairs(subjectVar, objectVar, noLinksqueryPersonPlace));
		System.out.println("Links Person -> Place: " + db.countPairs(subjectVar, objectVar, linksqueryPersonPlace));
		System.out.println("Top relations: " + 
		telecom.util.collections.Collections.toString(db.countProjectionBindings(noLinksqueryPersonPlace.get(0), noLinksqueryPersonPlace.subList(1, noLinksqueryPersonPlace.size()), relationVar)));


		List<ByteString[]> noLinksqueryOrganizationPerson = getNoLinksQuery(ByteString.of("<dbo:Organisation>"), ByteString.of("<dbo:Person>"));
		List<ByteString[]> linksqueryOrganizationPerson = getLinksQuery(ByteString.of("<dbo:Organisation>"), ByteString.of("<dbo:Person>"));
		System.out.println("Links Organisation -> Person without any other relation: " + db.countPairs(subjectVar, objectVar, noLinksqueryOrganizationPerson));
		System.out.println("Links Organisation -> Person: " + db.countPairs(subjectVar, objectVar, linksqueryOrganizationPerson));
		System.out.println("Top relations: " + 
		telecom.util.collections.Collections.toString(db.countProjectionBindings(linksqueryOrganizationPerson.get(0), linksqueryOrganizationPerson.subList(1, linksqueryOrganizationPerson.size()), relationVar)));

		
		List<ByteString[]> noLinksqueryOrganizationOrganization = getNoLinksQuery(ByteString.of("<dbo:Organisation>"), ByteString.of("<dbo:Organisation>"));
		List<ByteString[]> linksqueryOrganizationOrganization = getLinksQuery(ByteString.of("<dbo:Organisation>"), ByteString.of("<dbo:Organisation>"));
		System.out.println("Links Organisation -> Organisation without any other relation: " + db.countPairs(subjectVar, objectVar, noLinksqueryOrganizationOrganization));
		System.out.println("Links Organisation -> Organisation: " + db.countPairs(subjectVar, objectVar, linksqueryOrganizationOrganization));
		System.out.println("Top relations: " + 
		telecom.util.collections.Collections.toString(db.countProjectionBindings(linksqueryOrganizationOrganization.get(0), linksqueryOrganizationOrganization.subList(1, linksqueryOrganizationOrganization.size()), relationVar)));
		
		
		List<ByteString[]> noLinksqueryOrganizationPlace = getNoLinksQuery(ByteString.of("<dbo:Organisation>"), ByteString.of("<dbo:Place>"));
		List<ByteString[]> linksqueryOrganizationPlace = getLinksQuery(ByteString.of("<dbo:Organisation>"), ByteString.of("<dbo:Place>"));
		System.out.println("Links Organisation -> Place without any other relation: " + db.countPairs(subjectVar, objectVar, noLinksqueryOrganizationPlace));
		System.out.println("Links Organisation -> Place: " + db.countPairs(subjectVar, objectVar, linksqueryOrganizationPlace));
		System.out.println("Top relations: " + 
		telecom.util.collections.Collections.toString(db.countProjectionBindings(linksqueryOrganizationPlace.get(0), linksqueryOrganizationPlace.subList(1, linksqueryOrganizationPlace.size()), relationVar)));
				
		List<ByteString[]> noLinksqueryPlacePerson = getNoLinksQuery(ByteString.of("<dbo:Place>"), ByteString.of("<dbo:Person>"));
		List<ByteString[]> linksqueryPlacePerson = getLinksQuery(ByteString.of("<dbo:Place>"), ByteString.of("<dbo:Person>"));
		System.out.println("Links Place -> Person without any other relation: " + db.countPairs(subjectVar, objectVar, noLinksqueryPlacePerson));
		System.out.println("Links Place -> Person: " + db.countPairs(subjectVar, objectVar, linksqueryPlacePerson));
		System.out.println("Top relations: " + 
		telecom.util.collections.Collections.toString(db.countProjectionBindings(linksqueryPlacePerson.get(0), linksqueryPlacePerson.subList(1, linksqueryPlacePerson.size()), relationVar)));
		
		List<ByteString[]> noLinksqueryPlaceOrganization = getNoLinksQuery(ByteString.of("<dbo:Place>"), ByteString.of("<dbo:Organisation>"));
		List<ByteString[]> linksqueryPlaceOrganization = getLinksQuery(ByteString.of("<dbo:Place>"), ByteString.of("<dbo:Organisation>"));
		System.out.println("Links Place -> Organisation without any other relation: " + db.countPairs(subjectVar, objectVar, noLinksqueryPlaceOrganization));
		System.out.println("Links Place -> Organisation: " + db.countPairs(subjectVar, objectVar, linksqueryPlaceOrganization));
		System.out.println("Top relations: " + 
		telecom.util.collections.Collections.toString(db.countProjectionBindings(linksqueryPlaceOrganization.get(0), linksqueryPlaceOrganization.subList(1, linksqueryPlaceOrganization.size()), relationVar)));

		List<ByteString[]> noLinksqueryPlacePlace = getNoLinksQuery(ByteString.of("<dbo:Place>"), ByteString.of("<dbo:Place>"));
		List<ByteString[]> linksqueryPlacePlace = getLinksQuery(ByteString.of("<dbo:Place>"), ByteString.of("<dbo:Place>"));
		System.out.println("Links Place -> Place without any other relation: " + db.countPairs(subjectVar, objectVar, noLinksqueryPlacePlace));
		System.out.println("Links Place -> Place: " + db.countPairs(subjectVar, objectVar, linksqueryPlacePlace));
		System.out.println("Top relations: " + 
		telecom.util.collections.Collections.toString(db.countProjectionBindings(linksqueryPlacePlace.get(0), linksqueryPlacePlace.subList(1, linksqueryPlacePlace.size()), relationVar))); */
	}
}
