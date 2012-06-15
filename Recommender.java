package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

public class Recommender {
	
	private HashMap<String, ArrayList<HashMap<String, Integer>>> data = new HashMap<String, ArrayList<HashMap<String,Integer>>>();
	private HashMap<String, ArrayList<HashMap<String, Integer>>> result = new HashMap<String, ArrayList<HashMap<String,Integer>>>();
	private String outputFilePath="output/part-r-00000";
	private String logFilePath="output/log.txt";
	FileWriter logStream;
	BufferedWriter out;
	
	public Recommender() {
		try {
			logStream = new FileWriter(logFilePath);
			out = new BufferedWriter(logStream);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void preprocess() {
		try {
			FileInputStream fstream = new FileInputStream(outputFilePath);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			try {
				while ((strLine = br.readLine()) != null) {
					String[] line = strLine.split(","); // line[0]:word; line[1]:filename; line[2]:count
					if(line.length > 1) {
						HashMap<String, Integer>wordCountMap = new HashMap<String, Integer>();
						wordCountMap.put(line[0], Integer.parseInt(line[2].replaceAll("\t", "")));
						ArrayList<HashMap<String, Integer>> list;
						if(data.containsKey(line[1])){
							// key exist already. get the array and append
							// the wordCountMap into that array
							list = data.get(line[1]);
							
						} else{
							// add a new arraylist with the key and append
							// the wordCountMap into that array
							list = new ArrayList<HashMap<String,Integer>>();
						}
						list.add(wordCountMap);
						data.put(line[1], list);
					}
				}
				System.out.println(data);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void process() {
		for(Entry<String, ArrayList<HashMap<String, Integer>>> entOuter : data.entrySet()) {
			String keyOuter = entOuter.getKey();
			ArrayList<HashMap<String, Integer>> outerList = entOuter.getValue();
			for(Entry<String, ArrayList<HashMap<String, Integer>>> entInner : data.entrySet()) {
				int match = 0;
				double matchPC;
				 String keyInner = entInner.getKey();
				 if(!keyOuter.equals(keyInner)) {
					 //log("Comparing " + keyOuter + " with " + keyInner);
					 // do not compare same 2 files.
					 ArrayList<HashMap<String, Integer>> innerList = entInner.getValue();
					 Iterator<HashMap<String, Integer>> oL = outerList.iterator();
					 while(oL.hasNext()) {
						 Iterator<HashMap<String, Integer>> iL = innerList.iterator();
						 //System.out.println(keyOuter + " vs." + keyInner);
						 HashMap<String, Integer> outerWordCountMap = oL.next();
						 for(Entry<String, Integer> wordCountMap : outerWordCountMap.entrySet()) {
							 String word = wordCountMap.getKey();
							 //log(keyOuter + " has " + word);
							 int outerCount = wordCountMap.getValue();
							 while(iL.hasNext()) {
								 HashMap<String, Integer> innerWordCountMap = iL.next();
								 //log("\tWill search for " + word + " in " + innerWordCountMap);
								 if(innerWordCountMap.get(word) != null){
									 // word 
									 int innerCount = innerWordCountMap.get(word);
									 // DO THE DIFF IN THE COUNT HERE
									 //log("Word match found and diff is " + Math.abs(outerCount - innerCount));
									 match++;
								 }
							 }
						 }
					 }
					 matchPC = match*200/(outerList.size()+innerList.size());
					 //keyOuter = keyOuter.replaceAll("file:/Users/nanand1/Downloads/DocumentMiner/input/", "");
					 //keyInner = keyInner.replaceAll("file:/Users/nanand1/Downloads/DocumentMiner/input/", "");
					 System.out.println(keyOuter + " matches " + keyInner + " by " + matchPC);
				 }
				 
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Recommender r = new Recommender();
		r.preprocess();
		r.process();

	}

}
