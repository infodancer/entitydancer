package org.infodancer.persist.dbapi;

import java.util.Map;
import java.util.TreeMap;

import junit.framework.TestCase;

public abstract class DatabaseTest extends TestCase
{
	protected Database database;
		
	public void testIsOpen()
	{
		assertEquals(true, database.isOpen());
		database.close();
		assertEquals(false, database.isOpen());
	}
	
	public void testClose()
	{
		assertEquals(true, database.isOpen());
		database.close();
		assertEquals(false, database.isOpen());
	}

	public DatabaseTable createSimpleTestTable()
	{
		DatabaseTable table = database.createTable("test");
		DatabaseField fieldA = database.createField("A", java.sql.Types.BIGINT);
		fieldA.setGeneratedKey(true);
		fieldA.setPrimaryKey(true);
		table.addField(fieldA);
		DatabaseField fieldB = database.createField("B", java.sql.Types.INTEGER);
		table.addField(fieldB);
		DatabaseField fieldC = database.createField("C", java.sql.Types.BOOLEAN);
		table.addField(fieldC);
		DatabaseField fieldD = database.createField("D", java.sql.Types.VARCHAR);
		table.addField(fieldD);
		DatabaseField fieldE = database.createField("E", java.sql.Types.DATE);
		table.addField(fieldE);
		DatabaseField fieldF = database.createField("F", java.sql.Types.DOUBLE);
		table.addField(fieldF);
		DatabaseField fieldG = database.createField("G", java.sql.Types.FLOAT);
		table.addField(fieldG);
		database.createTable(table);
		return table;
	}
	
	public void testCreateTable()
	{
		assertEquals(true, database.isOpen());
		DatabaseTable table = createSimpleTestTable();
		assertEquals("test", table.getName());
		DatabaseField fieldA = table.getField("A");
		assertNotNull(fieldA);
		assertEquals("A", fieldA.getName());
		assertEquals(java.sql.Types.BIGINT, fieldA.getSqlType());
		DatabaseField fieldB = table.getField("B");
		assertNotNull(fieldB);
		assertEquals("B", fieldB.getName());
		assertEquals(java.sql.Types.INTEGER, fieldB.getSqlType());
		DatabaseField fieldC = table.getField("C");
		assertNotNull(fieldC);
		assertEquals("C", fieldC.getName());
		assertEquals(java.sql.Types.BOOLEAN, fieldC.getSqlType());
		DatabaseField fieldD = table.getField("D");
		assertNotNull(fieldD);
		assertEquals("D", fieldD.getName());
		assertEquals(java.sql.Types.VARCHAR, fieldD.getSqlType());
		DatabaseField fieldE = table.getField("E");
		assertNotNull(fieldE);
		assertEquals("E", fieldE.getName());
		assertEquals(java.sql.Types.DATE, fieldE.getSqlType());
		DatabaseField fieldF = table.getField("F");
		assertNotNull(fieldF);
		assertEquals("F", fieldF.getName());
		assertEquals(java.sql.Types.DOUBLE, fieldF.getSqlType());
		DatabaseField fieldG = table.getField("G");
		assertNotNull(fieldG);
		assertEquals("G", fieldG.getName());
		assertEquals(java.sql.Types.FLOAT, fieldG.getSqlType());
	}

	public void testPersist()
	{
		assertEquals(true, database.isOpen());
		Map<String,Object> pvalues = new TreeMap<String,Object>();
		DatabaseTable table = createSimpleTestTable();
		pvalues.put("B",  new Integer(2));
		pvalues.put("C",  Boolean.TRUE);
		pvalues.put("D",  "TestVarCharField");
		Object key = table.persist(pvalues);
		assertNotNull(key);
		Map<String,Object> fvalues = table.find(key);
		assertNotNull(fvalues);
		for (String name : pvalues.keySet())
		{
			assertEquals(fvalues.get(name), pvalues.get(name));
		}
	}
	
	public void testFindNullKey()
	{
		try
		{
			assertEquals(true, database.isOpen());
			DatabaseTable table = createSimpleTestTable();
			Map<String,Object> rvalues = table.find(null);
			assertTrue(rvalues.isEmpty());
		}
		
		catch (Throwable e)
		{
			// Expected exception
		}
	}

	public void testDescribeTable()
	{
		assertEquals(true, database.isOpen());
		DatabaseTable originalTable = createSimpleTestTable();
		assertNotNull(originalTable);
		DatabaseTable describedTable = database.describeTable(originalTable.getName());
		assertNotNull(describedTable);
		
	}
	
	public void testFind()
	{
		assertEquals(true, database.isOpen());
		Map<String,Object> pvalues = new TreeMap<String,Object>();
		DatabaseTable table = createSimpleTestTable();
		pvalues.put("B",  new Integer(2));
		pvalues.put("C",  Boolean.TRUE);
		pvalues.put("D",  "TestVarCharField");
		Object key = table.persist(pvalues);
		assertNotNull(key);
		Map<String,Object> fvalues = table.find(key);
		assertNotNull(fvalues);
		for (String name : pvalues.keySet())
		{
			assertEquals(fvalues.get(name), pvalues.get(name));
		}
		Map<String,Object> gvalues = table.find(new Long(-1));
		assertTrue(gvalues.isEmpty());
	}

	public void testMerge()
	{
		assertEquals(true, database.isOpen());
		DatabaseTable table = createSimpleTestTable();
		Map<String,Object> pvalues = new TreeMap<String,Object>();
		pvalues.put("B",  new Integer(2));
		pvalues.put("C",  Boolean.TRUE);
		pvalues.put("D",  "TestVarCharField");
		Object key = table.persist(pvalues);
		assertNotNull(key);
		pvalues = new TreeMap<String,Object>();
		pvalues.put("B",  new Integer(3));
		pvalues.put("C",  Boolean.FALSE);
		pvalues.put("D",  "Merged!");
		table.merge(key, pvalues);
		Map<String,Object> fvalues = table.find(key);
		assertNotNull(fvalues);
		for (String name : pvalues.keySet())
		{
			assertEquals(pvalues.get(name), fvalues.get(name));
		}
	}

	public void testTableClear()
	{
		assertEquals(true, database.isOpen());
		DatabaseTable table = createSimpleTestTable();
		Map<String,Object> pvalues = new TreeMap<String,Object>();
		pvalues.put("B",  new Integer(2));
		pvalues.put("C",  Boolean.TRUE);
		pvalues.put("D",  "TestVarCharField");
		Object key = table.persist(pvalues);
		assertNotNull(key);
		Map<String,Object> fvalues = table.find(key);
		assertNotNull(fvalues);
		table.clear();
		Map<String,Object> result = table.find(key);
		assertNotNull(result);
		assertTrue(result.isEmpty());
	}

	public void testDatabaseClear()
	{
		assertEquals(true, database.isOpen());
		DatabaseTable table1 = createSimpleTestTable();
		Map<String,Object> pvalues = new TreeMap<String,Object>();
		pvalues.put("B",  new Integer(2));
		pvalues.put("C",  Boolean.TRUE);
		pvalues.put("D",  "TestVarCharField");
		Object key = table1.persist(pvalues);
		assertNotNull(key);
		database.clear();
		DatabaseTable table2 = database.getTable(table1.getName());
		assertNull(table2);
	}

	public void testDelete()
	{
		assertEquals(true, database.isOpen());
		DatabaseTable table = createSimpleTestTable();
		Map<String,Object> pvalues = new TreeMap<String,Object>();
		pvalues.put("B",  new Integer(2));
		pvalues.put("C",  Boolean.TRUE);
		pvalues.put("D",  "TestVarCharField");
		Object key = table.persist(pvalues);
		assertNotNull(key);
		Map<String,Object> fvalues = table.find(key);
		assertNotNull(fvalues);
		table.delete(key);
		Map<String,Object> result = table.find(key);
		assertNotNull(result);
		assertTrue(result.isEmpty());
	}
	
	public void testDropTable1()
	{
		DatabaseTable table = createSimpleTestTable();
		assertNotNull(table);
		String tableName = table.getName();
		database.dropTable(table);
		table = database.getTable(tableName);
		assertNull(table);
	}

	public void testDropTable2()
	{
		DatabaseTable table = createSimpleTestTable();
		assertNotNull(table);
		String tableName = table.getName();
		database.dropTable(tableName);
		table = database.getTable(tableName);
		assertNull(table);
	}
	
	public void testSimpleEqualQuery()
	{
		DatabaseTable table = createSimpleTestTable();
		Map<String,Object> values = new TreeMap<String,Object>();
		values.put("G", new Float(1.0));
		values.put("F", new Double(1.0));
		table.persist(values);
		values = new TreeMap<String,Object>();
		values.put("G", new Float(3.0));
		values.put("F", new Double(2.0));
		table.persist(values);
		values = new TreeMap<String,Object>();
		values.put("G", new Float(3.0));
		values.put("F", new Double(2.0));
		table.persist(values);
		DatabaseQuery query = database.createQuery();
		query.addTable(table);
		query.addParam("F", QueryParameterType.EQUAL, "G");
		query.executeQuery();
		if (!query.isEmpty())
		{
			float f = query.getFloat("F");
			double g = query.getDouble("G");
			if (f != g) fail(f + " does not equal " + g); 
		}
		else fail("Query produced no results");
	}

	public void testSimpleLessThanQuery()
	{
		DatabaseTable table = createSimpleTestTable();
		Map<String,Object> values = new TreeMap<String,Object>();
		values.put("G", new Float(1.0));
		values.put("F", new Double(1.0));
		table.persist(values);
		values = new TreeMap<String,Object>();
		values.put("G", new Float(3.0));
		values.put("F", new Double(2.0));
		table.persist(values);
		values = new TreeMap<String,Object>();
		values.put("G", new Float(3.0));
		values.put("F", new Double(2.0));
		table.persist(values);
		DatabaseQuery query = database.createQuery();
		query.addTable(table);
		query.addParam("F", QueryParameterType.LESS_THAN, "G");
		query.executeQuery();
		if (!query.isEmpty())
		{
			float f = query.getFloat("F");
			double g = query.getDouble("G");
			if (!(f < g)) fail(f + " is not less than " + g); 
		}
		else fail("Query produced no results");
	}
	
	public void testSimpleLessThanOrEqualQuery()
	{
		DatabaseTable table = createSimpleTestTable();
		Map<String,Object> values = new TreeMap<String,Object>();
		values.put("G", new Float(1.0));
		values.put("F", new Double(1.0));
		table.persist(values);
		values = new TreeMap<String,Object>();
		values.put("G", new Float(3.0));
		values.put("F", new Double(2.0));
		table.persist(values);
		values = new TreeMap<String,Object>();
		values.put("G", new Float(3.0));
		values.put("F", new Double(2.0));
		table.persist(values);
		DatabaseQuery query = database.createQuery();
		query.addTable(table);
		query.addParam("F", QueryParameterType.LESS_THAN_OR_EQUAL, "G");
		query.executeQuery();
		if (!query.isEmpty())
		{
			float f = query.getFloat("F");
			double g = query.getDouble("G");
			if (!(f <= g)) fail(f + " is not less than or equal to " + g); 
		}
		else fail("Query produced no results");
	}
	
	public void testSimpleGreaterThanQuery()
	{
		DatabaseTable table = createSimpleTestTable();
		Map<String,Object> values = new TreeMap<String,Object>();
		values.put("G", new Float(1.0));
		values.put("F", new Double(1.0));
		table.persist(values);
		values = new TreeMap<String,Object>();
		values.put("G", new Float(3.0));
		values.put("F", new Double(2.0));
		table.persist(values);
		values = new TreeMap<String,Object>();
		values.put("G", new Float(3.0));
		values.put("F", new Double(2.0));
		table.persist(values);
		values = new TreeMap<String,Object>();
		values.put("G", new Float(2.0));
		values.put("F", new Double(3.0));
		table.persist(values);
		DatabaseQuery query = database.createQuery();
		query.addTable(table);
		query.addParam("F", QueryParameterType.GREATER_THAN, "G");
		query.executeQuery();
		if (!query.isEmpty())
		{
			float f = query.getFloat("F");
			double g = query.getDouble("G");
			if (!(f > g)) fail(f + " is not greater than " + g); 
		}
		else fail("Query produced no results");
	}
	
	public void testSimpleGreaterThanOrEqualQuery()
	{
		DatabaseTable table = createSimpleTestTable();
		Map<String,Object> values = new TreeMap<String,Object>();
		values.put("G", new Float(1.0));
		values.put("F", new Double(1.0));
		table.persist(values);
		values = new TreeMap<String,Object>();
		values.put("G", new Float(3.0));
		values.put("F", new Double(2.0));
		table.persist(values);
		values = new TreeMap<String,Object>();
		values.put("G", new Float(3.0));
		values.put("F", new Double(2.0));
		table.persist(values);
		DatabaseQuery query = database.createQuery();
		query.addTable(table);
		query.addParam("F", QueryParameterType.GREATER_THAN_OR_EQUAL, "G");
		query.executeQuery();
		if (!query.isEmpty())
		{
			while (query.next())
			{
				float f = query.getFloat("F");
				double g = query.getDouble("G");
				if (!(f >= g)) fail(f + " is not greater than or equal to " + g); 
			}
		}
		else fail("Query produced no results");
	}
	
	public void testQuerySize()
	{
		DatabaseTable table = createSimpleTestTable();
		DatabaseQuery query = database.createQuery();
		query.addTable(table);
		assertEquals(0, query.size());
		Map<String,Object> values = new TreeMap<String,Object>();
		values.put("G", new Float(1.0));
		values.put("F", new Double(1.0));
		table.persist(values);
		values = new TreeMap<String,Object>();
		values.put("G", new Float(3.0));
		values.put("F", new Double(2.0));
		table.persist(values);
		values = new TreeMap<String,Object>();
		values.put("G", new Float(3.0));
		values.put("F", new Double(2.0));
		table.persist(values);
		query = database.createQuery();
		query.addTable(table);
		assertEquals(3, query.size());
	}
	
	public void testQueryIsEmpty()
	{
		DatabaseTable table = createSimpleTestTable();
		DatabaseQuery query = database.createQuery();
		query.addTable(table);
		assertTrue(query.isEmpty());
		Map<String,Object> values = new TreeMap<String,Object>();
		values.put("G", new Float(1.0));
		values.put("F", new Double(1.0));
		table.persist(values);
		values = new TreeMap<String,Object>();
		values.put("G", new Float(3.0));
		values.put("F", new Double(2.0));
		table.persist(values);
		values = new TreeMap<String,Object>();
		values.put("G", new Float(3.0));
		values.put("F", new Double(2.0));
		table.persist(values);
		query.close();
		query = database.createQuery();
		query.addTable(table);
		assertFalse(query.isEmpty());
		query.close();
	}
}
