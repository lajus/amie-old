package amie.mining.assistant;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javatools.datatypes.ByteString;
import amie.data.FactDatabase;
import amie.query.Query;

public class FullRelationSignatureMiningAssistant extends HeadVariablesMiningAssistant {

	public FullRelationSignatureMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
		bodyExcludedRelations = Arrays.asList(ByteString.of("<rdf:type>"));
	}
	
	public void getDanglingEdges(Query query, int minCardinality, Collection<Query> output) {		
		ByteString[] newEdge = query.fullyUnboundTriplePattern();
		ByteString rdfType = ByteString.of("<rdf:type>");
		
		if(query.isEmpty()){
			//Initial case
			newEdge[1] = rdfType;
			Query candidate = new Query(newEdge, minCardinality);
			candidate.setFunctionalVariable(newEdge[0]);
			registerHeadRelation(candidate);
			getInstantiatedEdges(candidate, null, candidate.getLastTriplePattern(), 2, minCardinality, output);
		} else if (query.getLength() == 1) {
			addDanglingEdge(query, newEdge, minCardinality, output);
		} else if (query.getLength() == 2) {
			List<ByteString> variables = query.getOpenVariables();
			// There must be one
			newEdge[0] = variables.get(0);
			newEdge[1] = rdfType;
			Query candidate = query.addEdge(newEdge, minCardinality);
			getInstantiatedEdges(candidate, candidate, candidate.getLastTriplePattern(), 2, minCardinality, output);
		}
	}
	
	public void getCloseCircleEdges(Query query, int minCardinality, Collection<Query> output) {
		return;
	}
}
