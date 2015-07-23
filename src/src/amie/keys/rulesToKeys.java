/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package amie.keys;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

/**
 *
 * @author Danai
 */
public class rulesToKeys {

    public static void main(String[] args) throws IOException {

        /*SET THE FILE AND THE NUMBER OF EXCEPTIONS */
        String triplesFile = "/Users/Danai/NetBeansProjects/AMIE/Datasets/City/fullDescriptionsOfTypeCityNotINV.rdf";
        String rulesFile = "/Users/Danai/NetBeansProjects/AMIE/Datasets/City/rulesFile.rdf";
        rulesToKeys(triplesFile, rulesFile);
    }

    public static void rulesToKeys(String triplesFile, String rulesFile) throws IOException {
        BufferedReader br1 = new BufferedReader(new FileReader(triplesFile));
        String line1 = null;
        HashSet<String> properties = new HashSet<>();
        while ((line1 = br1.readLine()) != null) {
            String tripleTable[] = line1.split("	");
            String property = tripleTable[1].toLowerCase();
            properties.add(property);
        }
        br1.close();
        BufferedReader br2 = new BufferedReader(new FileReader(rulesFile));
        String line2 = null;
        int counter = 0;
        while ((line2 = br2.readLine()) != null) {
         //   System.out.println("counter:"+counter);
                        System.out.println("rule:" + line2);

            HashSet<String> key = new HashSet<>();
            HashMap<String, String> conditions = new HashMap<>();
            // System.out.println("line2:"+line2);
            String[] elements = line2.split("=>")[0].split("  ");
            System.out.println("elements:"+line2.split("=>")[1]);
            String[] measures = line2.split("=>")[1].split("	");
            
            String confidence = measures[0];
            System.out.println("confidence:"+confidence);
                       // String support = measures[4];
            //System.out.println("support:"+support);
            int numberOfTriples = elements.length / 3;
            for (int tripleNumber = 0; tripleNumber < numberOfTriples; tripleNumber++) {
                String object = elements[tripleNumber * 3 + 2];
                String property = elements[tripleNumber * 3 + 1];
                if (object.contains("?")) {
                    key.add(property);
                } else {
                    conditions.put(property, object);
                }

            }counter++;
            //System.out.println("rule:" + line2);
            System.out.println("CONDITION:" + conditions + "	KEY:" + key);
        }br2.close();
       // System.out.println("counter:"+counter);
    }

}
