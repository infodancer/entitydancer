package org.infodancer.persist.dbapi;

import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import org.infodancer.persist.sql.SQLDatabase;

public class SQLDatabaseTest
{
	private static final Logger logger = Logger.getLogger("SQLDatabaseTest");
	Properties properties;
	SQLDatabase sqldb;
	
	public void setUp() throws Exception
	{
	}
	
	public void tearDown() throws Exception
	{
	}
	
	public void testGetConnection() throws SQLException
	{
	}
}
