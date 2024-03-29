package amie.mining.assistant;

import java.util.List;

import javatools.datatypes.ByteString;
import amie.data.FactDatabase;
import amie.query.Query;

public class HeadVariablesImprovedMiningAssistant extends
		HeadVariablesMiningAssistant {

	public HeadVariablesImprovedMiningAssistant(FactDatabase dataSource) {
		super(dataSource);
	}

	protected long computePCAAntecedentCount(ByteString var1, ByteString var2, Query query, List<ByteString[]> antecedent, ByteString[] existentialTriple, int nonExistentialPosition) {
		antecedent.add(existentialTriple);
		ByteString[] typeConstraint1, typeConstraint2;
		typeConstraint1 = new ByteString[3];
		typeConstraint2 = new ByteString[3];
		typeConstraint1[1] = typeConstraint2[1] = ByteString.of("rdf:type");
		typeConstraint1[2] = typeConstraint2[2] = ByteString.of("?w");
		typeConstraint1[0] = existentialTriple[nonExistentialPosition == 0 ? 2 : 0];
		typeConstraint2[0] = existentialTriple[nonExistentialPosition].equals(var1) ? var2 : var1;
		antecedent.add(typeConstraint1);
		antecedent.add(typeConstraint2);
		long result = source.countPairs(var1, var2, antecedent);
		if(result == 0){
			antecedent.remove(antecedent.size() - 1);
			antecedent.remove(antecedent.size() - 1);
			result = source.countPairs(var1, var2, antecedent);
		}
		return result;
	}
}
