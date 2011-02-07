package org.acaro.semgraph.dictionary;

import java.io.IOException;

public class CounterTest {
	public static void main(String[] args) throws IOException {
		HBaseDictionary dic = new HBaseDictionary();
		
		System.out.println(dic.convert("jesus"));
	}
}
