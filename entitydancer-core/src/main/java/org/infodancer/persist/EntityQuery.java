package org.infodancer.persist;

import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.persistence.FlushModeType;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import javax.persistence.TemporalType;

import org.infodancer.persist.dbapi.DatabaseException;
import org.infodancer.persist.dbapi.DatabaseQuery;
import org.infodancer.persist.dbapi.QueryType;

public class EntityQuery implements Query
{
	QueryType qtype;
	String[] rnames;
	Map<String,EntityType> rtypes;
	DatabaseQuery query;
	ServiceEntityManager manager;
	
	
	public EntityQuery(ServiceEntityManager manager, QueryType qtype, String[] rnames, Map<String,EntityType> rtypes, DatabaseQuery query)
	{
		this.query = query;
		this.qtype = qtype;
		this.rnames = rnames;
		this.rtypes = rtypes;
		this.manager = manager;
	}

	@Override
	public List getResultList()
	{
		try
		{
			List result = new LinkedList();
			query.executeQuery();
			if (!query.isEmpty())
			{
				do
				{
					if (rnames != null)
					{
						if (rnames.length == 1)
						{
							result.add(parseResult(rnames[0]));
						}
						else
						{
							Object[] row = new Object[rnames.length];
							for (int i = 0; i < row.length; i++)
							{
								result.add(parseResult(rnames[i]));
							}
							result.add(row);
						}
					}
				}
				while (query.next());
			}
			return result;
		}
		
		finally
		{
			try { if (query != null) query.close(); } catch (Exception e) { } 
		}
	}

	private Object parseResult(String name)
	{
		if (isAggregateResultValue(name))
		{
			// Not yet implemented
			throw new PersistenceException("Aggregate results are not yet supported!");
		}
		else
		{
			String fieldName = getFieldName(name);
			EntityType rtype = getEntityType(name);
			if (rtype == null) throw new PersistenceException(name + " is not a known Entity!");
			EntityField keyField = rtype.getPrimaryKey();
			String keyFieldName = keyField.getStoreName();
			Object key = query.getObject(keyFieldName);
			Object o = manager.find(rtype.getEntityType(), key);
			/** Old way -- hits the database each time
			Object o = rtype.newInstance();
			rtype.setPrimaryKeyValue(o, key);
			manager.refresh(o);
			**/
			if (fieldName == null) return o;
			else  
			{
				EntityField field = rtype.getFieldByJavaName(fieldName);
				if (field == null) throw new PersistenceException(fieldName + " is not a known field in entity " + rtype.getClass());
				return field.getFieldValue(o);
			}
		}
	}

	private EntityType getEntityType(String name)
	{
		EntityType result = null;
		int sep = name.indexOf('.');
		if (sep != -1)
		{
			String type = name.substring(0, sep - 1);
			result = rtypes.get(type);
			if (result != null) return result;
			else return manager.getEntityByName(type);
		}
		else 
		{
			result = rtypes.get(name);
			if (result != null) return result;
			else return manager.getEntityByName(name);
		}
	}

	private String getFieldName(String name)
	{
		int sep = name.indexOf('.');
		if (sep != -1)
		{
			return name.substring(sep + 1, name.length());
		}
		else return null;
	}

	boolean isAggregateResultValue(String name)
	{
		return false;
	}

	@Override
	public Object getSingleResult()
	{
		try
		{
			if (rnames != null)
			{
				if (rnames.length == 1) 
				{
					query.executeQuery();
					if (!query.isEmpty()) return parseResult(rnames[0]);
					else return null;
				}
				else 
				{
					throw new PersistenceException("getSingleResult() called with rnames.length=" + rnames.length + "!");
				}
			}
			else throw new PersistenceException("getSingleResult() called with null rnames!");
		}

		catch (DatabaseException e)
		{
			throw new PersistenceException(e);
		}
		
		finally
		{
			try { if (query != null) query.close(); } catch (Exception e) { } 
		}
	}

	@Override
	public int executeUpdate()
	{
		return query.executeUpdate();
	}

	@Override
	public Query setMaxResults(int maxResult)
	{
		query.setLimit(maxResult);
		return this;
	}

	@Override
	public Query setFirstResult(int startPosition)
	{
		query.setFirstResult(startPosition);
		return this;
	}

	@Override
	public Query setHint(String hintName, Object value)
	{
		throw new PersistenceException("This method is not yet implemented!");
	}

	@Override
	public Query setParameter(String name, Object value)
	{
		throw new PersistenceException("This method is not yet implemented!");
	}

	@Override
	public Query setParameter(String name, Date value, TemporalType temporalType)
	{
		throw new PersistenceException("This method is not yet implemented!");
	}

	@Override
	public Query setParameter(String name, Calendar value,
			TemporalType temporalType)
	{
		throw new PersistenceException("This method is not yet implemented!");
	}

	@Override
	public Query setParameter(int position, Object value)
	{
		throw new PersistenceException("This method is not yet implemented!");
	}

	@Override
	public Query setParameter(int position, Date value,
			TemporalType temporalType)
	{
		throw new PersistenceException("This method is not yet implemented!");
	}

	@Override
	public Query setParameter(int position, Calendar value,
			TemporalType temporalType)
	{
		throw new PersistenceException("This method is not yet implemented!");
	}

	@Override
	public Query setFlushMode(FlushModeType flushMode)
	{
		throw new PersistenceException("This method is not yet implemented!");
	}
}
