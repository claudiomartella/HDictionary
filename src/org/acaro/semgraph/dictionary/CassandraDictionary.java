package org.acaro.semgraph.dictionary;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.acaro.sempgrah.dictionary.zoo.WriteLock;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.zookeeper.ZooKeeper;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.NumberHelper;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.exceptions.NotFoundException;

public class CassandraDictionary implements Dictionary {
	private ZooKeeper zoo;
	
	public CassandraDictionary(String cassandraAddress, String zookeeperAddress) throws InterruptedException, IOException, ExecutionException {
		Pelops.addPool("SemGraphDictionary", new Cluster(cassandraAddress, 9160), "SemGraphDictionary");
		zoo = new ZooKeeper(zookeeperAddress, 100);
	}
	
	public long convert(String word) {
		long value = getLong(word, ConsistencyLevel.QUORUM);
		
		if(value == 0){
			value = insert(word);
		}
		
		return value;
	}
	
	private long getLong(String word, ConsistencyLevel level) {
		long value;
		Selector sel = Pelops.createSelector("SemGraphDictionary");
		
		try {
			Column col = sel.getColumnFromRow("Words", Bytes.fromUTF8(word), Bytes.fromChar('v'), level);
			value = NumberHelper.toLong(col.getValue());
		} catch (NotFoundException e){
			value = 0;
		}
		
		return value;
	}
	
	private void setLong(String word, long counter, ConsistencyLevel level) {
		Mutator mutator = Pelops.createMutator("SemGraphDictionary");
		Column col = new Column();
		col.setName(Bytes.fromChar('v').getBytes());
		col.setValue(Bytes.fromLong(counter).getBytes());
		mutator.writeColumn("SemGraphDictionary", Bytes.fromUTF8(word), col);
		mutator.execute(level);
	}

	private long insert(String word) throws NotPossibleException {
		long value;
		WriteLock lock = new WriteLock(zoo, "/dictionary");
		
		lock.lock();
		// maybe somebody has inserted our value after last time we checked
		value = getLong(word, ConsistencyLevel.QUORUM);

		if(value == 0){
			// increase the counter
			value = getLong("_counter", ConsistencyLevel.QUORUM);
			value++;
			setLong("_counter", value, ConsistencyLevel.QUORUM)
			// insert new mapping word->counter
			setLong(word, value, ConsistencyLevel.QUORUM);
		}
		lock.unlock();
		
		return value;
	}
}