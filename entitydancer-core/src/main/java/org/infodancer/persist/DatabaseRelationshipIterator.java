package org.infodancer.persist;

import java.util.Collection;
import java.util.Iterator;

import javax.persistence.EntityManager;


public class DatabaseRelationshipIterator implements Iterator<Object> 
{
	EntityManager manager;
	EntityType foreignType;
	Iterator i;
	
	public DatabaseRelationshipIterator(EntityManager manager, EntityType foreignType, Collection<Object> keys)
	{
		this.manager = manager;
		this.foreignType = foreignType;
		this.i = keys.iterator();
	}

	public boolean hasNext() 
	{
		return i.hasNext();
	}

	public Object next() 
	{
		while (i.hasNext())
		{
			Object key = i.next();
			if (key != null) return manager.find(foreignType.getEntityType(), key); 
		}
		return null;
	}

	public void remove() 
	{
		// TODO remove from relationship
	}	
}
