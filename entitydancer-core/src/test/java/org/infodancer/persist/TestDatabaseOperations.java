package org.infodancer.persist;

import junit.framework.TestCase;

public abstract class TestDatabaseOperations extends TestCase
{
	TestDatabase database;
	ServiceEntityManager manager;
	
	public void setUp()
	{
		database = new TestDatabase();
		manager = new ServiceEntityManager();
		manager.setDatabase(database);
	}
	
	public void tearDown()
	{
		if (manager != null) manager.close();
		manager = null;
		database = null;
	}
	

}
