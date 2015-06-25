/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package amie.mining.assistant.experimental;

import amie.data.FactDatabase;
import amie.query.Query;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import javatools.datatypes.ByteString;
import javatools.datatypes.IntHashMap;

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
    public void getCloseCircleEdges(Query query, int minCardinality, Collection<Query> output) {
        //empty
    }

    @Override
    public void getDanglingEdges(Query query, int minCardinality, Collection<Query> output) {
        ByteString[] head = query.getHead();
        for (List<ByteString> nonKey : nonKeys) {
            List<int[]> subsets = telecom.util.collections.Collections.subsetsUpToSize(5, 2);
            for (int[] subsetIndexes : subsets) {
                for (int i = 0; i < subsetIndexes.length; i++) {
                    List<int[]> conditionalSubsets = telecom.util.collections.Collections.subsetsUpToSize(subsetIndexes.length, subsetIndexes.length - 1);
                    for (int[] conditionalSubset : conditionalSubsets) {
                        for (int instantiatedPropertyIndex : conditionalSubset) {
                            ByteString instantiatedProperty = nonKey.get(instantiatedPropertyIndex);
                            ByteString[] atom = new ByteString[3];
                            atom[0] = head[0];
                            atom[1] = instantiatedProperty;
                            atom[2] = ByteString.of("?www");
                            query.getTriples().add(atom);
                            IntHashMap<ByteString> constants = this.source.countProjectionBindings(head, query.getBody(), atom[2]);
                            query.getTriples().remove(query.getTriples().size() - 1);
                            System.out.println("constants for atom " + Arrays.toString(atom)  + ": "+ constants);
                        }
                    }
                }
            }
        }
    }
}
