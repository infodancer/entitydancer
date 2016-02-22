package org.infodancer.persist;

import java.beans.IntrospectionException;

import org.infodancer.persist.test.EntityTest;

import junit.framework.TestCase;

public class TestEntityType extends TestCase
{
	ServiceEntityManager manager;
	
	public void setUp()
	{
		manager = new ServiceEntityManager();
		manager.setDatabase(new TestDatabase());
	}
	
	public void tearDown()
	{
		if (manager != null) manager.close();
		manager = null;
	}
	
	/**
	 * This is the most basic, fundamental test that must pass: can the EntityManager 
	 * return an EntityType with the correct basic fields?
	 * @throws IntrospectionException
	 * @throws ClassNotFoundException
	 */
	public void testEntityType() throws IntrospectionException, ClassNotFoundException
	{
		EntityType entity = manager.getEntity(EntityTest.class);
		assertNotNull(entity.getFieldByJavaName("name"));
		assertNotNull(entity.getFieldByJavaName("floatValue"));
		assertNotNull(entity.getFieldByJavaName("doubleValue"));
		assertNotNull(entity.getFieldByJavaName("intValue"));
		assertNotNull(entity.getFieldByJavaName("booleanValue"));
		assertNotNull(entity.getFieldByJavaName("dateValue"));
		assertNotNull(entity.getFieldByJavaName("timestampValue"));		
	}
}
