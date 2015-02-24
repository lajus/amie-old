package arm.data;

import java.io.File;
import java.io.IOException;

public class KBSummarizer {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		FactDatabase db = new FactDatabase();
		db.load(new File(args[0]));
		
		System.out.println("Number of subjects: " + db.size(0));
		System.out.println("Number of facts: " + db.size());

	}

}
