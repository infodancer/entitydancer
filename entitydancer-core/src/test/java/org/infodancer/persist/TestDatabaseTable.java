package org.infodancer.persist;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.infodancer.persist.dbapi.AbstractDatabaseTable;
import org.infodancer.persist.dbapi.DatabaseConnection;
import org.infodancer.persist.dbapi.DatabaseField;

public class TestDatabaseTable extends AbstractDatabaseTable
{
	AtomicLong key = new AtomicLong(0l);
	Map<Object,Map<String,Object>> cache = new HashMap<Object,Map<String,Object>>();
	
	public TestDatabaseTable(String name)
	{
		super(name);
	}

	@Override
	public Object persist(Map<String, Object> values)
	{
		Long value = key.getAndIncrement();
		cache.put(value, values);
		return value;
	}

	@Override
	public Map<String, Object> find(Object key)
	{
		return cache.get(key);
	}

	@Override
	public void delete(Object key)
	{
		cache.remove(key);
	}

	@Override
	public void merge(Object key, Map<String, Object> values)
	{
		cache.put(key, values);
	}

	@Override
	public void clear(Map<String, Object> values)
	{
		cache.clear();
	}

	@Override
	public void clear()
	{
		cache.clear();
	}

	@Override
	public void addField(DatabaseField field)
	{
		fields.put(field.getName(), field);
	}

	@Override
	public void alterTable(DatabaseField field)
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object persist(DatabaseConnection con, Map<String, Object> values)
	{
		return persist(values);
	}

	@Override
	public void merge(DatabaseConnection con, Object key,
			Map<String, Object> values)
	{
		merge(key, values);
	}

	@Override
	public Map<String, Object> find(DatabaseConnection con, Object key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void delete(DatabaseConnection con, Object key) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clear(DatabaseConnection con, Map<String, Object> values) {
		// TODO Auto-generated method stub
		
	}
}
