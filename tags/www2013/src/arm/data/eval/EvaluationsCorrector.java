package arm.data.eval;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import javatools.filehandlers.TSVFile;

public class EvaluationsCorrector {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		TSVFile target = new TSVFile(new File(args[0]));
		TSVFile s1 = new TSVFile(new File(args[1]));
		PrintStream pout = null;
		
		if(args.length > 2){
			pout = new PrintStream(new File(args[2]));
		}else{
			pout = System.out;
		}
		
		for(List<String> record: target){
			String source = record.get(4);
			if(source.equals("ManualEvaluation")){
				outputRecord(pout, record);
			}else{
				if(containsEvaluation(s1, record)){
					outputRecord(pout, record);
				}else{
					//Output the prediction and make it manual
					outputManualPrediction(pout, record);
					
				}

			}
		}
		
		target.close();
		s1.close();
		if(args.length > 2) 
			pout.close();
	}

	private static void outputManualPrediction(PrintStream pout, List<String> record) {
		// TODO Auto-generated method stub
		pout.print(record.get(0));
		for(String field: record.subList(1, record.size() - 2)){
			pout.print("\t" + field);
		}
		
		pout.print("\tManualEvaluation");
		pout.println();
	}

	private static boolean containsEvaluation(TSVFile f, List<String> record) {
		for(List<String> r: f){
			if(r.equals(record)){
				return true;
			}
		}
		
		return false;
	}

	private static void outputRecord(PrintStream pout, List<String> record) {
		// TODO Auto-generated method stub
		pout.print(record.get(0));
		for(String field: record.subList(1, record.size())){
			pout.print("\t" + field);
		}
		
		pout.println();
	}

}
