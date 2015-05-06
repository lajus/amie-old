package amie.data.eval;

import javatools.datatypes.ByteString;
import javatools.datatypes.Triple;
import amie.query.Query;

public class Evaluation {

	public Query rule;
	
	public Triple<ByteString, ByteString, ByteString> fact;
		
	public EvalResult result;
	
	public EvalSource source;
	
	public Evaluation(Query rule, Triple<ByteString, ByteString, ByteString> fact, EvalResult result){
		this.rule = rule;
		this.fact = fact;
		this.result = result;
		source = EvalSource.Undefined;
	}
	
	public Evaluation(Query rule, Triple<ByteString, ByteString, ByteString> fact, EvalResult result, EvalSource source){
		this.rule = rule;
		this.fact = fact;
		this.result = result;
		this.source = source;
	}

	public ByteString[] toTriplePattern() {
		return new ByteString[]{fact.first, fact.second, fact.third};
	}
}
