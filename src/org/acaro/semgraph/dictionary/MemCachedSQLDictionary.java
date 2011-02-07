package org.acaro.semgraph.dictionary;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;


public class MemCachedSQLDictionary implements Dictionary {
	private SQLDictionary sqldic;
	private MemcachedClient cache;
	private int EXP_TIME = 3*3600;
	
	public MemCachedSQLDictionary() throws IOException {
		sqldic = new SQLDictionary();
		cache  = new MemcachedClient(new BinaryConnectionFactory(),AddrUtil.getAddresses("localhost:11211"));
	}
	
	public long convert(String word) {
		Long value = null;
		Future<Object> f = cache.asyncGet(word);
		try {
		    value = (Long) f.get(5, TimeUnit.SECONDS);
		} catch(TimeoutException e) {
		    f.cancel(false);
		} catch(InterruptedException e) {
			throw new NotPossibleException("Interrupted get", e);
		} catch(ExecutionException e) {
			throw new NotPossibleException("Execution exception in get", e);
		}
		
		if(value == null) {
			value = sqldic.convert(word);
			cache.add(word, EXP_TIME, value);
		}

		return value;
	}
}