package org.infodancer.persist.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.infodancer.persist.dbapi.DatabaseConnectionFactory;
import org.infodancer.persist.dbapi.DatabaseException;

public class SQLConnectionFactory implements DatabaseConnectionFactory<SQLConnection>
{
	private static final Logger logger = Logger.getLogger(SQLConnectionFactory.class.getName());
	Properties properties;
	DataSource datasource;
	
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
		try
		{
			logger.warning("SQLConnectionFactory.createConnection()");
			Connection con = null;
			if (datasource != null)
			{
				logger.warning("SQLConnectionFactory.createConnection() retrieving connection from datasource!");
				con = datasource.getConnection();
				logger.warning("SQLConnectionFactory.createConnection() retrieved connection from datasource successfully!");
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
			return new SQLConnection(con);
		}
		
		catch (SQLException e)
		{
			throw new DatabaseException(e);
		}
	}

}
