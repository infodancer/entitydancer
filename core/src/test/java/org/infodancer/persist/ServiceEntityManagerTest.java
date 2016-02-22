package org.infodancer.persist;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import javax.persistence.EntityTransaction;

import org.infodancer.persist.dbapi.DatabaseQuery;
import org.infodancer.persist.dbapi.QueryParameter;
import org.infodancer.persist.dbapi.QueryParameterType;
import org.infodancer.persist.test.EntityTest;

public class ServiceEntityManagerTest extends TestDatabaseOperations
{
	/*
	public void testIsModified() 
	throws IllegalAccessException, IntrospectionException, InvocationTargetException, ClassNotFoundException, InstantiationException
	{
		EntityTest test = new EntityTest();
		test.setBooleanValue(true);
		manager.persist(test);
		Object key = test.getId();
		assertTrue(manager.contains(test));
		test = manager.find(EntityTest.class, key);
		assertFalse(manager.isModified(test));
		test.setBooleanValue(false);
		assertTrue(manager.isModified(test));
	}
	*/ 
	/** 
	 * Tests the ORDER BY portion of the query parser.
	 */
	public void testParseOrderBy()
	{
		String sql = "ORDER BY one,two,three DESC";
		EntityTest test = new EntityTest();
		manager.persist(test);
		DatabaseQuery query = database.createQuery();
		List<String> result = manager.parseOrderBy(new Scanner(sql), query);
		assertNotNull(result);
		assertFalse(result.isEmpty());
		assertTrue(result.contains("one"));
		assertTrue(result.contains("two"));
		assertTrue(result.contains("three"));
		assertTrue(query.isDescending());
	}
	
	/** 
	 * Tests the FROM portion of the query parser.
	 */
	public void testParseFrom()
	{
		String sql = "FROM EntityTest o WHERE";
		EntityTest test = new EntityTest();
		manager.persist(test);
		DatabaseQuery query = database.createQuery();
		Map<String,EntityType> resultTypes = manager.parseFrom(new Scanner(sql), query);
		assertNotNull(resultTypes);
		assertFalse(resultTypes.isEmpty());
		EntityType eType = resultTypes.get("o");
		assertNotNull(eType);
		assertEquals("EntityTest", eType.getTableName());
	}

	
	/** 
	 * Tests the SELECT portion of the query parser.
	 */
	public void testParseSelect()
	{
		String sql = "SELECT DISTINCT o FROM";
		String[] rnames = manager.parseSelect(new Scanner(sql));
		for (String name : rnames)
		{
			if ("DISTINCT".equalsIgnoreCase(name)) fail("DISTINCT is a keyword!");
		}
	}

	/** 
	 * Tests the WHERE portion of the query parser.
	 */
	public void testParseWhereGreater()
	{
		String sql = "WHERE o1.quantity > o2.quantity";
		DatabaseQuery query = database.createQuery();
		List<QueryParameter> params = manager.parseWhereClause(new Scanner(sql), query);
		assertFalse(params.isEmpty());
		for (QueryParameter p : params)
		{
			if (!p.getQueryParameterType().equals(QueryParameterType.GREATER_THAN))
			{
				fail("Query parameter type was incorrectedly parsed!");
			}
		}
	}

	/** 
	 * Tests the FROM portion of the query parser.
	 */
	public void testParseWhereGreaterOrEqual()
	{
		String sql = "WHERE o1.quantity >= o2.quantity";
		DatabaseQuery query = database.createQuery();
		List<QueryParameter> params = manager.parseWhereClause(new Scanner(sql), query);
		assertFalse(params.isEmpty());
		for (QueryParameter p : params)
		{
			if (!p.getQueryParameterType().equals(QueryParameterType.GREATER_THAN_OR_EQUAL))
			{
				fail("Query parameter type was incorrectedly parsed!");
			}
		}
	}

	/** 
	 * Tests the FROM portion of the query parser.
	 */
	public void testParseWhereLess()
	{
		String sql = "WHERE o1.quantity < o2.quantity";
		DatabaseQuery query = database.createQuery();
		List<QueryParameter> params = manager.parseWhereClause(new Scanner(sql), query);
		assertFalse(params.isEmpty());
		for (QueryParameter p : params)
		{
			if (!p.getQueryParameterType().equals(QueryParameterType.LESS_THAN))
			{
				fail("Query parameter type was incorrectedly parsed!");
			}
		}
	}

	/** 
	 * Tests the FROM portion of the query parser.
	 */
	public void testParseWhereLessOrEqual()
	{
		String sql = "WHERE o1.quantity <= o2.quantity";
		DatabaseQuery query = database.createQuery();
		List<QueryParameter> params = manager.parseWhereClause(new Scanner(sql), query);
		assertFalse(params.isEmpty());
		for (QueryParameter p : params)
		{
			if (!p.getQueryParameterType().equals(QueryParameterType.LESS_THAN_OR_EQUAL))
			{
				fail("Query parameter type was incorrectedly parsed!");
			}
		}
	}

	/** 
	 * Tests the FROM portion of the query parser.
	 */
	public void testParseWhereEqual()
	{
		String sql = "WHERE o1.quantity = o2.quantity";
		DatabaseQuery query = database.createQuery();
		List<QueryParameter> params = manager.parseWhereClause(new Scanner(sql), query);
		assertFalse(params.isEmpty());
		for (QueryParameter p : params)
		{
			if (!p.getQueryParameterType().equals(QueryParameterType.EQUAL))
			{
				fail("Query parameter type was incorrectedly parsed!");
			}
		}
	}
	
	public void testSimpleQueryParser()
	{
		
	}
	
	public void testGetTransaction()
	{
		EntityTransaction transaction1 = manager.getTransaction();
		assertFalse(transaction1.isActive());
		transaction1.begin();
		assertTrue(transaction1.isActive());
		EntityTransaction transaction2 = manager.getTransaction();
		assertTrue(transaction2.isActive());
	}

	public void testJoinTransaction()
	{
	}
}
