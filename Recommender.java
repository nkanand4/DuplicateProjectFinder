package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

public class Recommender {
	
	private HashMap<String, ArrayList<HashMap<String, Integer>>> data = new HashMap<String, ArrayList<HashMap<String,Integer>>>();
	private ArrayList<String> fileMaps = new ArrayList<String>();
	private String dataFileFromHadoop="output/part-r-00000";
	
	
	
	public void setDataFileFromHadoop(String dataFileFromHadoop) {
		this.dataFileFromHadoop = dataFileFromHadoop;
	}
	/**
	 * This method extracts the base name of the file
	 * @param f {@link String} file name
	 * @return {@link String} file's base name
	 */
	
	private String filebasename(String f) {
		String[] fileFrags = f.split("/");
		String fileName = fileFrags[fileFrags.length - 1];		
		return fileName;
	}
	/**
	 * This method pre-processes the output file such that it creates a hash map 
	 * of following format
	 * {
	 * 	filename1 :[{word1: count1}, {word2: count2}, {word3: count3}],
	 * 	filename2 :[{word1: count1}, {word4: count4}, {word3: count3}]
	 * }
	 */
	private void preprocess() {
		try {
			FileInputStream fstream = new FileInputStream(dataFileFromHadoop);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			try {
				while ((strLine = br.readLine()) != null) {
					String[] line = strLine.split(","); // line[0]:filename; line[1]:word; line[2]:count
					if(line.length > 1) {
						HashMap<String, Integer>wordCountMap = new HashMap<String, Integer>();
						wordCountMap.put(line[1], Integer.parseInt(line[2].replaceAll("\t", "")));
						ArrayList<HashMap<String, Integer>> list;
						if(data.containsKey(line[0])){
							// key exist already. get the array and append
							// the wordCountMap into that array
							list = data.get(line[0]);
							
						} else{
							// add a new arraylist with the key and append
							// the wordCountMap into that array
							list = new ArrayList<HashMap<String,Integer>>();
						}
						list.add(wordCountMap);
						data.put(line[0], list);
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
	
	/**
	 * This method determines if the 2 files have
	 * already been compared. 
	 * @param f1 {@link String} file name
	 * @param f2 {@link String} file name
	 * @return {@link Boolean}
	 */
	private boolean isAlreadyCompared(String f1, String f2) {
		boolean alreadyCompared = fileMaps.contains(f1 + f2) || fileMaps.contains(f2 + f1);
		if(!alreadyCompared) {
			fileMaps.add(f1 + f2);
		}
		return alreadyCompared;
	}
	
	/**
	 * This method uses the pre-processed data to
	 * determine the nearly duplicate files.
	 */
	private void process() {
		for(Entry<String, ArrayList<HashMap<String, Integer>>> entOuter : data.entrySet()) {
			String keyOuter = entOuter.getKey();
			keyOuter = filebasename(keyOuter);
			ArrayList<HashMap<String, Integer>> outerList = entOuter.getValue();
			for(Entry<String, ArrayList<HashMap<String, Integer>>> entInner : data.entrySet()) {
				int match = 0, matchPC;
				 String keyInner = entInner.getKey();
				 keyInner = filebasename(keyInner);
				 // do not compare the same files
				 // also not if they are already compared
				 if(!keyOuter.equals(keyInner) && !isAlreadyCompared(keyOuter, keyInner)) {
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
					 if(matchPC > 30) {						 
						 System.out.println(keyOuter + " matches " + keyInner + " by " + matchPC);
					 }
				 }
				 
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Recommender r = new Recommender();
		if(args.length == 0) {
			System.err.println("Please specify the path to output from hadoop.");
			//return;
		} else {
			System.out.println("filename is " + args[0]);
			r.setDataFileFromHadoop(args[0]);
		}
		r.preprocess();
		r.process();
	}

}
