package org.infodancer.persist.sql;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

import org.infodancer.persist.dbapi.ConnectionLockedException;
import org.infodancer.persist.dbapi.DatabaseConnection;
import org.infodancer.persist.dbapi.DatabaseException;

/**
 * An implementation of a PoolConnection for SQL-based JDBC databases.
 * @author matthew
 *
 */
public class SQLConnection extends DatabaseConnection implements Connection
{
	private static final Logger logger = Logger.getLogger(SQLConnection.class.getName());
	Connection con;
	int timeout;
	String schema;
	
	
	public SQLConnection(Connection con)
	{
		this.con = con;
	}

	public void close() throws SQLException 
	{
		logger.warning("[SQLConnection] About to close a connection");
		if (con != null) con.close();
		logger.warning("[SQLConnection] Connection closed successfully");
	}

	public boolean isWrapperFor(Class<?> arg0) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.isWrapperFor(arg0);
	}

	public <T> T unwrap(Class<T> arg0) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.unwrap(arg0);
	}

	public void clearWarnings() throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		con.clearWarnings();
	}

	public void commit() throws DatabaseException
	{
		try
		{
			if (isLocked()) throw new ConnectionLockedException();
			con.commit();
		}
		
		catch (SQLException e)
		{
			throw new DatabaseException(e);
		}
	}

	public Array createArrayOf(String arg0, Object[] arg1) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.createArrayOf(arg0, arg1);
	}

	public Blob createBlob() throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.createBlob();
	}

	public Clob createClob() throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.createClob();
	}

	public NClob createNClob() throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.createNClob();
	}

	public SQLXML createSQLXML() throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.createSQLXML();
	}

	public Statement createStatement() throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.createStatement();
	}

	public Statement createStatement(int arg0, int arg1) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.createStatement(arg0, arg1);
	}

	public Statement createStatement(int arg0, int arg1, int arg2)
			throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.createStatement(arg0, arg1, arg2);
	}

	public Struct createStruct(String arg0, Object[] arg1) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.createStruct(arg0, arg1);
	}

	public boolean getAutoCommit() throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.getAutoCommit();
	}

	public String getCatalog() throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.getCatalog();
	}

	public Properties getClientInfo() throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.getClientInfo();		
	}

	public String getClientInfo(String arg0) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.getClientInfo(arg0);
	}

	public int getHoldability() throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.getHoldability();
	}

	public DatabaseMetaData getMetaData() throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.getMetaData();
	}

	public int getTransactionIsolation() throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.getTransactionIsolation();
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.getTypeMap();
	}

	public SQLWarning getWarnings() throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.getWarnings();
	}

	public boolean isClosed() throws SQLException
	{
		if (isLocked()) return true;
		else return con.isClosed();
	}

	public boolean isReadOnly() throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.isReadOnly();
	}

	public boolean isValid(int arg0) throws SQLException
	{
		return con.isValid(arg0);
	}

	public String nativeSQL(String arg0) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.nativeSQL(arg0);
	}

	public CallableStatement prepareCall(String arg0) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.prepareCall(arg0);
	}

	public CallableStatement prepareCall(String arg0, int arg1, int arg2)
			throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.prepareCall(arg0, arg1, arg2);
	}

	public CallableStatement prepareCall(String arg0, int arg1, int arg2,
			int arg3) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.prepareCall(arg0, arg1, arg2, arg3);
	}

	public PreparedStatement prepareStatement(String arg0) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.prepareStatement(arg0);
	}

	public PreparedStatement prepareStatement(String arg0, int arg1)
			throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.prepareStatement(arg0, arg1);
	}

	public PreparedStatement prepareStatement(String arg0, int[] arg1)
			throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.prepareStatement(arg0, arg1);
	}

	public PreparedStatement prepareStatement(String arg0, String[] arg1)
			throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.prepareStatement(arg0, arg1);
	}

	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2)
			throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.prepareStatement(arg0, arg1, arg2);
	}

	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2,
			int arg3) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.prepareStatement(arg0, arg1, arg2, arg3);
	}

	public void releaseSavepoint(Savepoint arg0) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		con.releaseSavepoint(arg0);
	}

	@Override
	public void rollback() throws DatabaseException
	{
		try
		{
			if (isLocked()) throw new ConnectionLockedException();
			con.rollback();
		}
		
		catch (SQLException e)
		{
			throw new DatabaseException(e);
		}
	}

	public void rollback(Savepoint arg0) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		con.rollback(arg0);
	}

	@Override
	public void setAutoCommit(boolean commit) throws DatabaseException
	{
		try
		{
			if (isLocked()) throw new ConnectionLockedException();
			con.setAutoCommit(commit);
		}
		
		catch (SQLException e)
		{
			throw new DatabaseException(e);
		}
	}

	public void setCatalog(String arg0) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		con.setCatalog(arg0);
	}

	public void setClientInfo(Properties arg0) throws SQLClientInfoException
	{
		if (isLocked()) return;
		con.setClientInfo(arg0);
	}

	public void setClientInfo(String arg0, String arg1)
			throws SQLClientInfoException
	{
		if (isLocked()) return;
		con.setClientInfo(arg0, arg1);
	}

	public void setHoldability(int arg0) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		con.setHoldability(arg0);
	}

	public void setReadOnly(boolean arg0) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		con.setReadOnly(arg0);
	}

	public Savepoint setSavepoint() throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.setSavepoint();
	}

	public Savepoint setSavepoint(String arg0) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		return con.setSavepoint(arg0);
	}

	public void setTransactionIsolation(int arg0) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		con.setTransactionIsolation(arg0);
	}

	public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException
	{
		if (isLocked()) throw new ConnectionLockedException();
		con.setTypeMap(arg0);
	}

	/**
	 * Implemented for compilation on 1.7, but for compatibility with 1.6, doesn't do anything.
	 * @return
	 */
	public int getNetworkTimeout() throws SQLException
	{
		return timeout;
	}
	
	public void setNetworkTimeout(Executor executor, int timeout) throws SQLException
	{
		this.timeout = timeout;
	}
	
	/**
	 * As with the get/setNetworkTimeout() methods, this is included for 1.7 compatibility but not implemented for 1.6 compatibility.
	 * @param executor
	 */
	public void abort(Executor executor)
	{
		
	}
	
	public int getTimeout()
	{
		return timeout;
	}

	public void setTimeout(int timeout)
	{
		this.timeout = timeout;
	}

	public String getSchema()
	{
		return schema;
	}

	public void setSchema(String schema)
	{
		this.schema = schema;
	}

	@Override
	public void destroy()
	{
		try 
		{ 
			logger.warning("[SQLConnection] About to destroy a connection");
			con.close();
			logger.warning("[SQLConnection] Cconnection successfully destroyed");
		}
		catch (SQLException e) { e.printStackTrace(); }
	}

	@Override
	public boolean isValidConnection()
	{
		try
		{
			return con.isValid(3);
		}
		
		catch (SQLException e)
		{
			return false;
		}
	}

	@Override
	public boolean isAutoCommit()
	{
		try
		{
			return con.getAutoCommit();
		}
		
		catch (SQLException e)
		{
			throw new DatabaseException(e);
		}		
	}
}
