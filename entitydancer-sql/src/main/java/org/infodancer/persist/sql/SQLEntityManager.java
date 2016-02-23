package org.infodancer.persist.sql;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.infodancer.persist.ServiceEntityManager;

public class SQLEntityManager extends ServiceEntityManager implements javax.persistence.EntityManager
{
	public SQLEntityManager()
	{
		super();
	}
	
	/**
	 * Sets the name of the JDBC DataSource to retrieve from the context.
	 * @param name
	 * @throws SQLException 
	 * @throws NamingException 
	 */
	public void setDatasourceName(String name) throws NamingException, SQLException
	{
		SQLDatabase database = new SQLDatabase();
		database.setDataSource(name);
		setDatabase(database);
	}

	/**
	 * Sets the name of the JDBC DataSource to retrieve from the context.
	 * @param name
	 * @throws SQLException 
	 * @throws NamingException 
	 */
	public void setDatabaseName(String name) throws NamingException, SQLException
	{
		InitialContext icontext = new InitialContext();
		Context context = (Context) icontext.lookup("java:/comp/env");
		SQLDatabase database = (SQLDatabase) context.lookup(name);
		setDatabase(database);
	}
	
	public void setPropertiesFile(String name) throws IOException
	{
		SQLDatabase database = new SQLDatabase();
		Properties properties = new Properties();
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(name);
		properties.load(inputStream);
		database.initialize(properties);
		setDatabase(database);
	}
}
