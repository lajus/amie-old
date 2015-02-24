package arm.mining;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import arm.data.FactDatabase;
import arm.query.AMIEreader;
import arm.query.Query;

public class PredictionsProducerFromFile {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		FactDatabase trainingSource = new FactDatabase();
		trainingSource.load(new File(args[0]));
		
		int sampleSize = Integer.parseInt(args[1]);
		int mode = Integer.parseInt(args[2]);
	
		List<Query> rules = new ArrayList<Query>();
		for(int i = 3; i < args.length; ++i){		
			rules.addAll(AMIEreader.rules(new File(args[i])));
		}
		
		for(Query rule: rules){
			if(trainingSource.functionality(rule.getHead()[1]) >= trainingSource.inverseFunctionality(rule.getHead()[1]))
				rule.setProjectionVariable(rule.getHead()[0]);
			else
				rule.setProjectionVariable(rule.getHead()[2]);
		}
		
		PredictionsProducer pp = new PredictionsProducer(trainingSource, sampleSize);
		if(mode == 1)
			pp.runMode1(rules); //Considered previous samples
		else
			pp.runMode2(rules); //Independent for each rule
	}

}
