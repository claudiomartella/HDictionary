package org.acaro.semgraph.dictionary;

import java.sql.*;

import javax.sql.DataSource;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;

public class SQLDictionary implements Dictionary {
	final private String URL = "jdbc:postgresql://localhost/SemGraphDictionary?user=semgraph&password=semgraph";
	final private String SELECT_WORD = "SELECT v FROM dictionary WHERE id = ?";
	final private String INSERT_WORD = "INSERT INTO dictionary VALUES (?)";
	private DataSource ds;
	
	public SQLDictionary() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			throw new NotPossibleException("Can't find SQL Driver", e);
		}
		ds = setupDataSource(URL);
	}
	
	public long convert(String word) {
		long value = getLong(word);
		
		if(value == 0){
			value = insert(word);
		}
		
		return value;
	}

	private long insert(String word) {
		PreparedStatement st = null;
		ResultSet rs  = null;
		Connection cn = null;
		long value = 0;
		
		try {
			cn = ds.getConnection();
			
			st = cn.prepareStatement(INSERT_WORD);
			st.setString(1, word);
			st.executeUpdate();
			rs = st.getGeneratedKeys();
			
			if(rs.next()){
				value = rs.getLong(2);
			} 
			
		}  catch (SQLException e) {
			throw new NotPossibleException("SQLException", e);
		} finally {
            try { if (rs != null) rs.close(); } catch(Exception e) { }
            try { if (st != null) st.close(); } catch(Exception e) { }
            try { if (cn != null) cn.close(); } catch(Exception e) { }
		}
		
		return value;
	}

	private long getLong(String word)  {
		PreparedStatement st = null;
		ResultSet rs  = null;
		Connection cn = null;
		long value = 0;

		try {
			cn = ds.getConnection();
			
			st = cn.prepareStatement(SELECT_WORD);
			st.setString(1, word);
			rs = st.executeQuery();

			if(rs.next()){
				value = rs.getLong(1);
			} 

		} catch (SQLException e) {
			throw new NotPossibleException("SQLException", e);
		} finally {
            try { if (rs != null) rs.close(); } catch(Exception e) { }
            try { if (st != null) st.close(); } catch(Exception e) { }
            try { if (cn != null) cn.close(); } catch(Exception e) { }
		}
		
		return value;
	}
	
    private DataSource setupDataSource(String connectURI) {
    	ObjectPool connectionPool = new GenericObjectPool(null);
    	ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(connectURI,null);
    	PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory,connectionPool,null,null,false,true);
    	PoolingDataSource dataSource = new PoolingDataSource(connectionPool);
    
    	return dataSource;
    }
}