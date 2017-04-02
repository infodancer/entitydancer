package org.infodancer.persist;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.persistence.PersistenceException;

import org.infodancer.persist.dbapi.DatabaseConnection;

public class DatabaseRelationshipCollection implements Collection
{
	List<Object> cache;
	ServiceEntityManager manager;
	EntityField field;
	EntityType type;
	Object value;
	EntityType foreignType;
	EntityField foreignKeyField;
	Class foreignClass; 
	DatabaseConnection con;
			
	public DatabaseRelationshipCollection(ServiceEntityManager manager, EntityType type, 
		EntityField field, Object value, EntityType foreignType)
	{
		this.manager = manager;
		this.field = field;
		this.type = type;
		this.value = value;
		this.foreignType = foreignType;
		this.foreignClass = foreignType.getEntityType();
		this.foreignKeyField = foreignType.getPrimaryKey();
	}

	public DatabaseRelationshipCollection(ServiceEntityManager manager, DatabaseConnection con, 
			EntityType type, EntityField field, Object value, EntityType foreignType)
	{
		this.manager = manager;
		this.field = field;
		this.type = type;
		this.value = value;
		this.foreignType = foreignType;
		this.foreignClass = foreignType.getEntityType();
		this.foreignKeyField = foreignType.getPrimaryKey();
		this.con = con;
	}

	private Object getForeignKey(Object o)
	{
		try
		{
			return foreignKeyField.getFieldValue(o);
		}
		
		catch (Exception e)
		{
			throw new PersistenceException(e);
		}
	}
	
	public void refresh()
	{
		try
		{
			cache = manager.selectRelationshipKeys(type, field, value);
		}
		
		catch (Exception e)
		{
			throw new PersistenceException(e);
		}
	}
	
	public boolean add(Object o) 
	{
		if (cache == null) refresh();
		cache.add(getForeignKey(o));
		return true;
	}

	public boolean addAll(Collection c) 
	{
		if (cache == null) refresh();
		for (Object o : c)
		{
			add(o);
		}
		return true;
	}

	public void clear() 
	{
		if (cache != null) cache.clear();
	}

	public boolean contains(Object o) 
	{
		if (cache == null) refresh();
		Object key = getForeignKey(o);
		if (cache.contains(key)) return true;
		else return false;
	}

	public boolean containsAll(Collection c) 
	{
		if (cache == null) refresh();
		for (Object o : c)
		{
			if (!contains(o)) return false;
		}
		return true;
	}

	public boolean isEmpty()
	{
		if (cache == null) refresh();
		if (size() == 0) return true;
		else return false;
	}

	public Iterator iterator() 
	{	
		if (cache == null) refresh();
		return new DatabaseRelationshipIterator(manager, foreignType, cache);
	}

	public boolean remove(Object o) 
	{
		if (cache == null) refresh();
		Object key = getForeignKey(o);
		if (cache.contains(key))
		{
			cache.remove(key);
			return true;
		}
		else return false; 
	}

	public boolean removeAll(Collection c) 
	{
		if (cache == null) refresh();
		boolean result = false;
		for (Object o : c)
		{
			if (remove(o)) result = true;
		}
		return result;
	}

	public boolean retainAll(Collection c) 
	{
		throw new UnsupportedOperationException();
	}

	public int size() 
	{
		if (cache == null) refresh();
		return cache.size();
	}

	public Object[] toArray() 
	{
		if (cache == null) refresh();
		Object[] result = new Object[cache.size()];
		Iterator iterator = iterator();
		for (int i = 0; iterator.hasNext(); i++)
		{
			result[i++] = iterator.next();
		}
		return result;
	}

	public Object[] toArray(Object[] result) 
	{
		if (cache == null) refresh();
		if (result.length >= cache.size())
		{
			Iterator iterator = iterator();
			for (int i = 0; iterator.hasNext(); i++)
			{
				result[i++] = iterator.next();
			}			
			return result;
		}
		else return toArray();
	}
}
