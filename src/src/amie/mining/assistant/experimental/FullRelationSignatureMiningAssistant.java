package amie.mining.assistant.experimental;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javatools.datatypes.ByteString;
import amie.data.FactDatabase;
import amie.mining.assistant.DefaultMiningAssistant;
import amie.query.Query;

public class FullRelationSignatureMiningAssistant extends DefaultMiningAssistant {

	public FullRelationSignatureMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
		bodyExcludedRelations = Arrays.asList(ByteString.of("<rdf:type>"));
	}
	
	public void getDanglingEdges(Query query, double minCardinality, Collection<Query> output) {		
		ByteString[] newEdge = query.fullyUnboundTriplePattern();
		ByteString rdfType = ByteString.of("rdf:type");
		
		if(query.isEmpty()){
			//Initial case
			newEdge[1] = rdfType;
			Query candidate = new Query(newEdge, minCardinality);
			candidate.setFunctionalVariablePosition(0);
			registerHeadRelation(candidate);
			getInstantiatedEdges(candidate, null, candidate.getLastTriplePattern(), 2, minCardinality, output);
		} else if (query.getLength() == 1) {
			getDanglingEdges(query, newEdge, minCardinality, output);
		} else if (query.getLength() == 2) {
			List<ByteString> variables = query.getOpenVariables();
			// There must be one
			newEdge[0] = variables.get(0);
			newEdge[1] = rdfType;
			Query candidate = query.addEdge(newEdge, minCardinality);
			getInstantiatedEdges(candidate, candidate, candidate.getLastTriplePattern(), 2, minCardinality, output);
		}
	}
	
	public void getCloseCircleEdges(Query query, double minSupportThreshold, Collection<Query> output) {
		return;
	}
}
