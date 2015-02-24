package arm.data.eval;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import arm.data.FactDatabase;
import arm.mining.PredictionsProducer;
import arm.query.AMIEreader;
import arm.query.Query;
import javatools.datatypes.Triple;

/**
 * 
 * @author lgalarra
 *
 */
public class AMIERuleEval {

	public static int aggregate(Map<ByteString, IntHashMap<ByteString>> bindings){
		int count = 0;
		for(ByteString value1: bindings.keySet()){
			count += bindings.get(value1).size();
		}
		return count;
	}
	
	public static void main(String args[]) throws IOException{
		FactDatabase db = new FactDatabase();
		db.load(new File(args[1]));
		List<Query> qList = AMIEreader.rules(new File(args[0]));
		Set<Triple<ByteString, ByteString, ByteString>> predictions = new HashSet<Triple<ByteString, ByteString, ByteString>>();	
		
		//Now calculate the body size for the rules
		for(Query q: qList){
			ByteString[] head = q.getHead();
			q.setProjectionVariable(head[HitsEvaluator.findFunctionalVariable(q, db)]);
			long[] result = conditionalBodySize(q, db, predictions);			
			System.out.println(q.getRuleString() + "\t" + result[0] + "\t" + result[1]);
		}
		
		
	}

	private static long[] conditionalBodySize(Query q, FactDatabase db, Set<Triple<ByteString, ByteString, ByteString>> allPredictions) {
		PredictionsProducer pp = new PredictionsProducer(db);
		Object predictionsObj = pp.generatePredictions(q, db);
		Map<ByteString, IntHashMap<ByteString>> predictions = (Map<ByteString, IntHashMap<ByteString>>)predictionsObj;
		int countingVarPos = q.getProjectionVariablePosition();
		long result[] = new long[2];
		
		for(ByteString value1: predictions.keySet()){
			for(ByteString value2: predictions.get(value1)){
				Triple<ByteString, ByteString, ByteString> triple = new Triple<ByteString, ByteString, ByteString>(null, null, null);
				
				if(value1.equals(value2)){ 
					continue;
				}
				
				if(countingVarPos == 0){
					triple.first = value1;
					triple.third = value2;
				}else{
					triple.first = value2;
					triple.third = value1;					
				}
				
				triple.second = q.getHead()[1];
				if(!allPredictions.contains(triple)){
					++result[0];
					allPredictions.add(triple);
				}
				
				++result[1];
			}			
		}
		
		return result;
	}
}
