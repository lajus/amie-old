package arm.data.eval;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import javatools.filehandlers.TSVFile;
import arm.data.FactDatabase;
import arm.mining.PredictionsProducer;
import arm.query.AMIEreader;
import arm.query.Query;

public class HitsEvaluator {
	
	public static int findFunctionalVariable(Query q, FactDatabase d){
		ByteString[] head = q.getHead();
		if(FactDatabase.numVariables(head) == 1) return FactDatabase.firstVariablePos(head); 
		return d.functionality(head[1]) > d.inverseFunctionality(head[1]) ? 0 : 2;
	}
	
	public static void main(String args[]) throws IOException{
		if(args.length < 4){
			System.out.println("HitsEvaluator <inputfile> <trainingDb> <targetDb> <outputManual>");
			System.exit(1);
		}
		
		File inputFile = new File(args[0]);		
		FactDatabase trainingDataset = new FactDatabase();
		FactDatabase targetDataset = new FactDatabase();		
		TSVFile tsvFile = new TSVFile(inputFile);
		
		trainingDataset.load(new File(args[1]));
		targetDataset.load(new File(args[2]));
		FileWriter output = new FileWriter(new File(args[3]));
		PrintWriter outputPw = new PrintWriter(output);
		PredictionsProducer predictor = new PredictionsProducer(trainingDataset);	
		int idx = 1;
		for(List<String> record: tsvFile){
			Query q = AMIEreader.rule(record.get(0));
			if(q == null) continue;
			ByteString[] head = q.getHead();
			q.setProjectionVariable(q.getHead()[findFunctionalVariable(q, trainingDataset)]);
			Object bindings = predictor.generateBodyBindings(q, trainingDataset);
			ByteString triple[] = head.clone();
			int hits1 = 0;
			int hits2 = 0;
			if(FactDatabase.numVariables(head) == 1){
				Set<ByteString> oneVarBindings = (Set<ByteString>)bindings;
				for(ByteString binding: oneVarBindings){
					if(q.getProjectionVariablePosition() == 0){
						triple[0] = binding;
					}else{
						triple[2] = binding;
					}
					int eval = Evaluator.evaluate(q, triple, trainingDataset, targetDataset);
					if(eval == 0) ++hits1;					
					if(trainingDataset.count(triple) > 0) ++hits2;
				}
			}else{
				Map<ByteString, IntHashMap<ByteString>> twoVarsBindings = (Map<ByteString, IntHashMap<ByteString>>)bindings;
				for(ByteString value1: twoVarsBindings.keySet()){
					for(ByteString value2: twoVarsBindings.get(value1)){
						if(q.getProjectionVariablePosition() == 0){
							triple[0] = value1;
							triple[2] = value2;
						}else{
							triple[0] = value2;
							triple[2] = value1;					
						}
						int eval = Evaluator.evaluate(q, triple, trainingDataset, targetDataset);
						if(eval == 0) ++hits1;
						if(trainingDataset.count(triple) > 0) ++hits2;  //If predicted in the new version
					}
				}
			}
			
			outputPw.println(q.getRuleString() + "\t" + hits1 + "\t" + hits2);
			++idx;
				
		}
		
		tsvFile.close();
		output.close();	
		outputPw.close();
	}

}
