package org.acaro.semgraph.dictionary;

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

public class HBaseDictionaryTest {
	static Logger logger = Logger.getLogger(HBaseDictionaryTest.class);
	
	public static void main(String[] args) throws IOException {
		BasicConfigurator.configure();
		Logger logg = logger.getRootLogger();
		//logg.setLevel((Level) Level.DEBUG);
		HBaseDictionary dic = new HBaseDictionary();
		
		System.out.println("Starting the test!");
		System.out.println(dic.convert("ONE"));
		System.out.println(dic.convert("TWO"));
		System.out.println(dic.convert("THREE"));
		System.out.println(dic.convert("ONE"));
	}
}