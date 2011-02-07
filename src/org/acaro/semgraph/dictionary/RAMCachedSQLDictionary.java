package org.acaro.semgraph.dictionary;

import org.apache.commons.collections.map.LRUMap;

public class RAMCachedSQLDictionary implements Dictionary {
	private SQLDictionary sqldic;
	private LRUMap cache;
	
	public RAMCachedSQLDictionary(int capacity) {
		sqldic = new SQLDictionary();
		cache = new LRUMap(capacity);
	}
	
	public RAMCachedSQLDictionary() {
		this(1000);
	}
	
	public long convert(String word) {
		Long value = (Long) cache.get((Object) word);
		
		if(value == null) {
			value = sqldic.convert(word);
			cache.put(word, value);
		}

		return value;
	}
}