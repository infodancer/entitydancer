package org.infodancer.persist.sql;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.infodancer.persist.dbapi.DatabaseConnectionFactory;
import org.infodancer.persist.dbapi.DatabaseConnectionPool;
import org.infodancer.persist.dbapi.DatabaseException;

public class SQLConnectionFactory implements DatabaseConnectionFactory<SQLConnection>
{
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
	
	@Override
	public SQLConnection createConnection()
	{
		try
		{
			if (datasource != null)
			{
				return new SQLConnection(datasource.getConnection());
			}
			else if (properties != null)
			{				
				String url = properties.getProperty("jdbc.url");
				String user = properties.getProperty("jdbc.user");
				String password = properties.getProperty("jdbc.password");
				return new SQLConnection(DriverManager.getConnection(url, user, password));
			}
			else throw new DatabaseException("Neither a Datasource nor a Properties object has been specified!");
		}
		
		catch (SQLException e)
		{
			throw new DatabaseException(e);
		}
	}

}
