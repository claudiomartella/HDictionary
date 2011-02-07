package org.acaro.semgraph.dictionary;

import java.util.HashMap;

public class RAMDictionary implements Dictionary {

	private HashMap<String, Long> map;
	private long counter = 0;
	
	public RAMDictionary() {
		map = new HashMap<String, Long>();
	}
	
	public long convert(String word) {
		Long value = map.get(word);
		
		if(value == null){
			value = counter++;
			map.put(word, value);
		}
		
		return value;
	}
	
	public int size() {
		return map.size();
	}
}
