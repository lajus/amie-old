/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package amie.mining.assistant.experimental;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import amie.data.KB;
import amie.keys.NonKeysGraph;
import amie.rules.Rule;

/**
 *
 * @author Danai
 */
public class ConditionalKeyMiningAssistant extends KeyMinerMiningAssistant {

    List<List<ByteString>> nonKeys;
    
    NonKeysGraph propertyCombinations;

    public ConditionalKeyMiningAssistant(KB dataSource, String nonKeysFile) throws FileNotFoundException, IOException {
        super(dataSource);
        nonKeys = new ArrayList<List<ByteString>>();
        propertyCombinations = new NonKeysGraph(nonKeysFile);
        System.out.println(propertyCombinations);
    }
    

    @Override
    public void getClosingAtoms(Rule query, double minSupportThreshold, Collection<Rule> output) {
        ByteString[] head = query.getHead();
        List<ByteString> bodyRelations = query.getBodyRelations();
        ByteString[] atom1 = query.fullyUnboundTriplePattern();
        ByteString[] atom2 = query.fullyUnboundTriplePattern();
        ByteString key = NonKeysGraph.StartNode;
        if (!bodyRelations.isEmpty()) {
        	key = bodyRelations.get(bodyRelations.size() - 1);
        }
        
        atom1[0] = head[0];//x
        atom2[0] = head[2];//y
        for (ByteString property : this.propertyCombinations.getEdges(key)) {
            atom1[1] = property;//property
            atom2[1] = property;//property
            atom1[2] = atom2[2];//same fresh variable
            query.getTriples().add(atom1);
            query.getTriples().add(atom2);
            int effectiveSize = query.getTriples().size();
            double support = kb.countDistinctPairs(head[0], head[2], query.getTriples());
            query.getTriples().remove(effectiveSize - 1);
            query.getTriples().remove(effectiveSize - 2);
            if (support >= (double) minSupportThreshold) {
                Rule newQuery = query.addEdges(atom1, atom2);
                newQuery.setSupport(support);
                newQuery.setParent(query);
                output.add(newQuery);
            }
        }
    }

    public void getDanglingAtoms(Rule query, double minCardinality, Collection<Rule> output) {
    }

    @Override
    public void getInstantiatedAtoms(Rule query, double minCardinality, Collection<Rule> temporalSomething, Collection<Rule> output) {
        ByteString[] head = query.getHead();
        List<ByteString> bodyRelations = query.getBodyRelations();
        ByteString[] atom1 = query.fullyUnboundTriplePattern();
        ByteString[] atom2 = query.fullyUnboundTriplePattern();
        ByteString key = NonKeysGraph.StartNode;
        if (!bodyRelations.isEmpty()) {
        	key = bodyRelations.get(bodyRelations.size() - 1);
        }
        
        atom1[0] = head[0];//x
        atom2[0] = head[2];//y
        for (ByteString property : this.propertyCombinations.getEdges(key)) {
            atom1[1] = property;//property
            atom2[1] = property;
            atom2[2] = atom1[2];
            ByteString danglingVariable = atom1[2];
            query.getTriples().add(atom1);
            query.getTriples().add(atom2);
            IntHashMap<ByteString> constants = kb.countProjectionBindings(head, query.getTriples(), atom1[2]);
            int effectiveSize = query.getTriples().size();
            query.getTriples().remove(effectiveSize - 1);
            query.getTriples().remove(effectiveSize - 2);
            for (ByteString constant : constants) {
                int support = constants.get(constant);
                if (support >= minCardinality) {
                    atom1[2] = constant;
                    atom2[2] = constant;
                    Rule newQuery = query.addEdges(atom1, atom2);
                    newQuery.setSupport(support);
                    output.add(newQuery);
                    newQuery.setParent(query);
                }
            }
            
            atom1[2] = danglingVariable;
            atom2[2] = danglingVariable;
        }
    }
}
