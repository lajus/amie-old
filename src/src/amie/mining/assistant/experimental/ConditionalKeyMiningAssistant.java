/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package amie.mining.assistant.experimental;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;
import amie.data.FactDatabase;
import amie.query.Query;

/**
 *
 * @author Danai
 */
public class ConditionalKeyMiningAssistant extends KeyMinerMiningAssistant {

    List<List<ByteString>> nonKeys = new ArrayList<>();

    public ConditionalKeyMiningAssistant(FactDatabase dataSource, File nonKeysFile) throws FileNotFoundException, IOException {
        super(dataSource);
        parseNonKeysFile(nonKeysFile);
    }

    private void parseNonKeysFile(File nonKeysFile) throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new FileReader(nonKeysFile));
        String line = null;
        while ((line = br.readLine()) != null) {
            ArrayList<ByteString> nonKey = new ArrayList<>();
            String instanceTable[] = line.split(", ");
            for (String property : instanceTable) {
                nonKey.add(ByteString.of(property));
            }
            nonKeys.add(nonKey);
        }
        br.close();
    }

    @Override
    public void getClosingAtoms(Query query, double minSupportThreshold, Collection<Query> output) {
        ByteString[] head = query.getHead();
        List<ByteString> bodyRelations = query.getBodyRelations();
        int positionInNonKey = bodyRelations.size();
        ByteString[] atom1 = query.fullyUnboundTriplePattern();
        ByteString[] atom2 = query.fullyUnboundTriplePattern();
        for (List<ByteString> nonKey : nonKeys) {
            if (nonKey.size() > bodyRelations.size()) {
                if (positionInNonKey > 0) {
                    if (!nonKey.subList(0, positionInNonKey).containsAll(bodyRelations)) {
                        continue;
                    }
                }
                ByteString property = nonKey.get(positionInNonKey);
                atom1[0] = head[0];//x
                atom1[1] = property;//property
                atom2[0] = head[2];//y
                atom2[1] = property;//property
                atom1[2] = atom2[2];//same fresh variable
                query.getTriples().add(atom1);
                query.getTriples().add(atom2);
                int effectiveSize = query.getTriples().size();
                double support = kb.countDistinctPairs(head[0], head[2], query.getTriples());
                query.getTriples().remove(effectiveSize - 1);
                query.getTriples().remove(effectiveSize - 2);
                if (support >= (double) minSupportThreshold) {
                    Query newQuery = query.addEdges(atom1, atom2);
                    newQuery.setSupport(support);
                    newQuery.setParent(query);
                    output.add(newQuery);
                }

            }
        }
    }

    public void getDanglingAtoms(Query query, double minCardinality, Collection<Query> output) {
    }

    @Override
    public void getInstantiatedAtoms(Query query, double minCardinality, Collection<Query> temporalSomething, Collection<Query> output) {
        ByteString[] head = query.getHead();
        List<ByteString> bodyRelations = query.getBodyRelations();
        int positionInNonKey = bodyRelations.size();
        ByteString[] atom1 = query.fullyUnboundTriplePattern();
        ByteString[] atom2 = query.fullyUnboundTriplePattern();
        for (List<ByteString> nonKey : nonKeys) {
            if (nonKey.size() > bodyRelations.size()) {
                if (positionInNonKey > 0) {
                    if (!nonKey.subList(0, positionInNonKey).containsAll(bodyRelations)) {
                        continue;
                    }
                }
                ByteString property = nonKey.get(positionInNonKey);
                atom1[0] = head[0];//x
                atom1[1] = property;//property
                atom2[0] = head[2];
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
                        Query newQuery = query.addEdges(atom1, atom2);
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
}
