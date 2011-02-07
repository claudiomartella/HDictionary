package org.acaro.semgraph.dictionary;

import java.io.FileReader;
import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.snowball.SnowballAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

public class TotalCorpusToDictionary {

	public static void main(String[] args) throws IOException {
		Dictionary dic = new HBaseDictionary();
		//Dictionary dic = new SQLDictionary();
		//Dictionary dic = new RAMCachedSQLDictionary(50000);
		//Dictionary dic = new MemCachedSQLDictionary();
		//RAMDictionary dic = new RAMDictionary();
		TokenStream ts = new SnowballAnalyzer(org.apache.lucene.util.Version.LUCENE_30, "English").tokenStream("content", new FileReader("./resources/totalCorpus.txt"));
		TermAttribute termAtt = (TermAttribute) ts.addAttribute(TermAttribute.class);
		
		long start = System.currentTimeMillis();
		
		ts.reset(); 
		while (ts.incrementToken()) { 
			dic.convert(termAtt.term());
		} 
		ts.end(); 
		ts.close();
		
		long finish = System.currentTimeMillis();
		System.out.println("Time elapsed (s)="+(finish-start)/1000f);
	}
}