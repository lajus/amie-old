package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import javatools.datatypes.ByteString;

public class ColumnOverlapCalculator {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		FactDatabase source = new FactDatabase();
		source.load(new File(args[0]));
		printOverlapTable(source);	
	}

	private static void printOverlapTable(FactDatabase source) {
		//for each pair of relations, print the overlap table
		System.out.println("Relation1\tRelation2\tRelation1-subjects\tRelation1-objects\tRelation2-subjects\tRelation2-objects\tSubject-Subject\tSubject-Object\tObject-Subject\tObject-Object");
		for(ByteString r1: source.predicateSize){
			Set<ByteString> subjects1 = source.predicate2subject2object.get(r1).keySet();
			Set<ByteString> objects1 = source.predicate2object2subject.get(r1).keySet();
			int nSubjectsr1 = subjects1.size();
			int nObjectsr1 = objects1.size();
			for(ByteString r2: source.predicateSize){
				if(r1.equals(r2))
					continue;				
				System.out.print(r1 + "\t");
				System.out.print(r2 + "\t");
				Set<ByteString> subjects2 = source.predicate2subject2object.get(r2).keySet();
				Set<ByteString> objects2 = source.predicate2object2subject.get(r2).keySet();
				int nSubjectr2 = subjects2.size();
				int nObjectsr2 = objects2.size();
				System.out.print(nSubjectsr1 + "\t" + nObjectsr1 + "\t" + nSubjectr2 + "\t" + nObjectsr2 + "\t");
				System.out.print(computeOverlap(subjects1, subjects2) + "\t");
				System.out.print(computeOverlap(subjects1, objects2) + "\t");
				System.out.print(computeOverlap(subjects2, objects1) + "\t");
				System.out.println(computeOverlap(objects1, objects2));
			}
		}		
	}

	private static int computeOverlap(Set<ByteString> subjects1,
			Set<ByteString> subjects2) {
		int overlap = 0; 
		for(ByteString entity1 : subjects1){
			if(subjects2.contains(entity1))
				++overlap;
		}
		
		return overlap;
	}
}
