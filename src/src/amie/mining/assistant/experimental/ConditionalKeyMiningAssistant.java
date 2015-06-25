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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import javatools.datatypes.ByteString;

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
        for (List<ByteString> nonKey : nonKeys) {
            int nonKeySize = nonKey.size();
            HashSet<HashSet<ByteString>> subsets = new HashSet<>();
            System.out.println("nonKey:"+nonKey);
            for (int size = 2; size <= nonKeySize; size++) {
                System.out.println("size:"+size);
                for (int i = 0; i <= nonKeySize - size; i++) {
                    System.out.println("i:"+i);
                    for (int j = i + 1; j <= nonKeySize-size+1; j++) {
                        System.out.println("j:"+j);
                        HashSet<ByteString> subset = new HashSet<>();
                        subset.add(nonKey.get(i));
                        for (int k = j; k < j+size-1; k++) {
                            subset.add(nonKey.get(k));
                        }
                        System.out.println("subSet:" + subset);
                    }
                }
            }

//            for (int size = 2; size <= nonKeySize; size++) {
//                for (int i = 0; i < nonKey.size(); i++) {
//                    ByteString property = nonKey.get(i);
//                    HashSet<ByteString> subset = new HashSet<>();
//                    subset.add(property);
//                    for (int j = i + 1; j <= Math.min(i + size, nonKeySize); j++) {//
//                        System.out.println("i:" + i + " j:" + j);
//                        subset.add(nonKey.get(j));
//                        System.out.println("subset:" + subset);
//                    }
//                    subsets.add(subset);
//                    System.out.println("subSets:" + subsets);
//                }
//
//            }
//            System.out.println("nonKey:" + nonKey + "  subsets:" + subsets);
        }
    }
}
