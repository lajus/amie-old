package amie.data.eval;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import amie.query.Query;
import amie.query.AMIEreader;
import javatools.filehandlers.TSVFile;

public class TSVRuleJoinImproved {

	public static void main(String[] args) throws IOException {
		TSVFile file1 = new TSVFile(new File(args[0]));
		TSVFile file2 = new TSVFile(new File(args[1]));
		
		HashMap<Query, List<String>> rulesIndex = new HashMap<>();
		for (List<String> line : file1) {
			Query rule = AMIEreader.rule(line.get(0));
			rulesIndex.put(rule, line);
		}
		
		for (List<String> line : file2) {
			Query rule = AMIEreader.rule(line.get(0));
			List<String> extraInfo = rulesIndex.get(rule);
			System.out.print(rule.getRuleString() + "\t");
			if (extraInfo != null) {
				for (String field : extraInfo.subList(1, extraInfo.size())) {
					System.out.print(field + "\t");	
				}
			} else {
				System.out.print("-\t-\t-\t");
			}

			
			for (String field : line.subList(1, line.size())) {
				System.out.print(field + "\t");
			}
			
			System.out.println();
		}
		
		file1.close();
		file2.close();

	}

}
