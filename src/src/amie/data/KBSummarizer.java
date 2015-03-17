package amie.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javatools.datatypes.ByteString;

public class KBSummarizer {

	public static void summarize(FactDatabase db, boolean detailRelations) {
		System.out.println("Number of subjects: " + db.size(0));
		System.out.println("Number of relations: " + db.size(1));
		System.out.println("Number of objects: " + db.size(2));
		System.out.println("Number of facts: " + db.size());
		
		if (detailRelations) {
			System.out.println("Relation\tTriples\tFunctionality\tInverse functionality\tNumber of subjects\tNumber of objects");
			for(ByteString relation: db.predicateSize.keys()){
				System.out.println(relation + "\t" + db.predicateSize.get(relation) + "\t" + db.functionality(relation) + 
						"\t" + db.inverseFunctionality(relation) + "\t" + db.predicate2subject2object.get(relation).size() + "\t" +
						"\t" + db.predicate2object2subject.get(relation).size());
			}
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		FactDatabase db = new FactDatabase();
		List<File> files = new ArrayList<>(); 
		for (String fileName : args) {
			files.add(new File(fileName));
		}
		db.load(files);
		summarize(db, true);
	}
}