package amie.data.eval.legacy;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javatools.datatypes.ByteString;
import javatools.datatypes.Triple;
import javatools.filehandlers.FileLines;
import amie.data.FactDatabase;
import amie.data.eval.EvalResult;
import amie.data.eval.EvalSource;
import amie.data.eval.Evaluation;
import amie.data.eval.Evaluator;
import amie.query.Query;

/**
 * Takes a legacy file with the rules and its evaluations and brings it 
 * to TSV formats: 
 * 	rule,subject,predicate,object if no data source is provided
 *  rule,subject,predicate,object,result,source if a the source and target datasources are provided
 * 
 * @author lgalarra
 *
 */
public class PredictionsConvertor {
	public Map<Query, List<Evaluation>> parse(File inputFile) throws IOException{
		Map<Query, List<Evaluation>> resultMap = new HashMap<Query, List<Evaluation> >();
		Query lastRule = null;
		Random randomGenerator = new Random();
		for (String line : new FileLines(inputFile, "UTF-8", "Reading input file with evaluations")) {
			 List<ByteString[]> triplesRaw = FactDatabase.triples(line);
			 List<ByteString[]> triples = new ArrayList<ByteString[]>();			 
			 //Fix the head
			 for(int i = triplesRaw.size() - 1; i >= 0; --i)
				 triples.add(triplesRaw.get(i));
			 
			 
			 if(triples == null || triples.size() < 2){
				 //It must be a fact
				 if(lastRule != null){
					 Evaluation e = parseFact(lastRule, line);
					 if(e != null && resultMap.get(lastRule).size() < 30){
						 resultMap.get(lastRule).add(e);
					 }
				 }
			 }else{
				 //It is a new rule
				 Query newQuery = new Query();
				 newQuery.getTriples().addAll(triples);
				 newQuery.setSupport(randomGenerator.nextInt(Integer.MAX_VALUE));
				 //Parse the key variable
				 int position = line.indexOf("Key variable: ", 0);
				 String keyVariable = line.substring(position + 14).replace("\t", "");
				 newQuery.setFunctionalVariable(ByteString.of(keyVariable));				 
				 resultMap.put(newQuery, new ArrayList<Evaluation>());	
				 lastRule = newQuery;
			 }
		}
		
		return resultMap;
	}
	
	private Evaluation parseFact(Query rule, String line) {
		String[] components = line.split("\t", 2);
		EvalResult result = EvalResult.Unknown;
		
		if(components.length >= 2){
			switch(components[0]){
			case "Y":
				result = EvalResult.True;
				break;
			case "N":
				result = EvalResult.False;
				break;
			case "?":
				result = EvalResult.Unknown;
				break;
			}
			
			String[] triple = components[1].split("/");
			if(triple.length == 3){
				Triple<ByteString, ByteString, ByteString> fact = new Triple<>(ByteString.of(triple[0]), ByteString.of(triple[1]), ByteString.of(triple[2]));
				return new Evaluation(rule, fact, result);
			}
		}
		
		return null;
	}
	
	private void determineSource(Evaluation eval, FactDatabase source, FactDatabase target){
		ByteString[] triple = eval.toTriplePattern();
		int evalResult = Evaluator.evaluate(eval.rule, triple, source, target);
		switch(evalResult){
		case 0: case 2: 
			eval.source = EvalSource.TargetSource;
			break;
		case 1:			
			eval.source = EvalSource.TrainingSource;
			break;
		case 3:
			eval.source = EvalSource.ManualEvaluation;
			break;
		}
	}
	
	private void output(FileWriter outputFile, Map<Query, List<Evaluation>> evalData, FactDatabase source, FactDatabase target) {
		// TODO Auto-generated method stub
		PrintWriter out = new PrintWriter(outputFile, true);
		if(source != null && target != null){
			for(Query rule: evalData.keySet()){
				for(Evaluation eval: evalData.get(rule)){
					out.print(rule.getRuleString());
					out.print("\t");
					out.print(eval.fact.first);
					out.print("\t");
					out.print(eval.fact.second);
					out.print("\t");
					out.print(eval.fact.third);
					out.print("\t");
					determineSource(eval, source, target);
					out.print(eval.source.toString());
					out.print("\t");
					out.println(eval.result.toString());
				}
			}
		}else{
			for(Query rule: evalData.keySet()){
				for(Evaluation eval: evalData.get(rule)){
					out.print(rule.getRuleString());
					out.print("\t");
					out.print(eval.fact.first);
					out.print("\t");
					out.print(eval.fact.second);
					out.print("\t");
					out.println(eval.fact.third);
				}
			}			
		}
		
		out.close();
	}
	
	public static void main(String args[]) throws IOException{
		if(args.length < 2)
			System.err.println("<input> <output> [sourcedb] [targetdb]");
		
		File inputFile = new File(args[0]);
		File outFile = new File(args[1]);
		FactDatabase source = null;
		FactDatabase target = null;
		
		if(!outFile.exists())
			outFile.createNewFile();
		
		if(args.length >= 4){
			source = new FactDatabase();
			target = new FactDatabase();
			source.load(new File(args[2]));
			target.load(new File(args[3]));
		}
		
		FileWriter outputFile = new FileWriter(outFile);
		PredictionsConvertor convertor = new PredictionsConvertor();
		Map<Query, List<Evaluation>> evalData = convertor.parse(inputFile);
		convertor.output(outputFile, evalData, source, target);
	}
}