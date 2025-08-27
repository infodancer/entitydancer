package org.infodancer.persist.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.infodancer.persist.dbapi.DatabaseConnectionFactory;
import org.infodancer.persist.dbapi.DatabaseException;

public class SQLConnectionFactory implements DatabaseConnectionFactory<SQLConnection>
{
	private static final Logger logger = Logger.getLogger(SQLConnectionFactory.class.getName());
	private static final int TIMEOUT = 30;
	private AtomicLong connectionCount = new AtomicLong(0);
	Properties properties;
	DataSource datasource;
	List<Connection> connections = new ArrayList<Connection>();
		
	public SQLConnectionFactory(javax.sql.DataSource datasource)
	{
		this.datasource = datasource;
	}

	/**
	 * Takes a set of properties (jdbc.url, jdbc.driver, jdbc.user, jdbc.password).
	 * @param properties
	 * @throws ClassNotFoundException
	 */
	public SQLConnectionFactory(Properties properties) 
	throws ClassNotFoundException
	{
		this.properties = properties;
		String driver = properties.getProperty("jdbc.driver");
		Class.forName(driver);
	}
	
	public SQLConnection createConnection()
	{
		// logger.warning("SQLConnectionFactory.createConnection()");
		
		/* Setting the timeout appears to not be supported by tomcat's database pooling
		try
		{
			logger.warning("SQLConnectionFactory.createConnection() trying to set login timeout for " + TIMEOUT + " seconds!");
			if (datasource != null)
			{
				datasource.setLoginTimeout(TIMEOUT);
				logger.warning("SQLConnectionFactory.createConnection() connection attempt will time out in " + TIMEOUT + " seconds!");
			}
			else if (properties != null)
			{
				DriverManager.setLoginTimeout(TIMEOUT);
				logger.warning("SQLConnectionFactory.createConnection() connection attempt will time out in " + TIMEOUT + " seconds!");
			}
		}
		
		catch (SQLException e) 
		{
			logger.warning("Failed to set connection timeout!");
			e.printStackTrace();
		}
		*/
		try
		{
			
			Connection con = null;
			if (datasource != null)
			{
				// logger.warning("SQLConnectionFactory.createConnection() retrieving new connection; " + connectionCount + " are currently open for this instance.");
				con = datasource.getConnection();				
				// logger.warning("SQLConnectionFactory.createConnection() retrieved connection " + connectionCount.addAndGet(1) + " from datasource successfully!");
			}
			else if (properties != null)
			{				
				logger.warning("SQLConnectionFactory.createConnection() retrieving connection from DriverManager!");
				String url = properties.getProperty("jdbc.url");
				String user = properties.getProperty("jdbc.user");
				String password = properties.getProperty("jdbc.password");
				con = DriverManager.getConnection(url, user, password);
				logger.warning("SQLConnectionFactory.createConnection() retrieved connection from DriverManager successfully!");
			}
			else throw new DatabaseException("Neither a Datasource nor a Properties object has been specified!");
			// Add the retrieved connection to the set just in case
			if (!connections.contains(con))
			{
				connections.add(con);
			}
			return new SQLConnection(con);
		}
		
		catch (SQLTimeoutException e)
		{
			logger.warning("SQLConnectionFactory.createConnection() timed out after waiting for " + TIMEOUT + " seconds!");
			// We have possibly become deadlocked; clear all existing connections and try to recover
			for (Connection con : connections)
			{
				logger.warning("SQLConnectionFactory.createConnection() closing open connection due to possible deadlock!");
				try { con.close(); } catch (Exception ee) { e.printStackTrace(); }
			}
			throw new DatabaseException(e);
		}

		catch (SQLException e)
		{
			logger.warning("SQLConnectionFactory.createConnection() encountered an error!");
			throw new DatabaseException(e);
		}
	}

}
