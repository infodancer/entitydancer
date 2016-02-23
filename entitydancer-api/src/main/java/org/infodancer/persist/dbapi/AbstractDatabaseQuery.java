package org.infodancer.persist.dbapi;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public abstract class AbstractDatabaseQuery implements DatabaseQuery
{
	protected long size = -1;
	protected long limit = 0;
	protected long first = 0;
	protected QueryType qtype; 
	protected Database database;
	protected TreeMap<String,DatabaseField> fields;
	protected TreeMap<String,DatabaseTable> tables;
	protected LinkedList<QueryParameter> params;
	protected List<String> orderby;
	protected boolean descending;
	
	protected AbstractDatabaseQuery(Database database)
	{
		this.database = database;
		this.fields = new TreeMap<String,DatabaseField>();
		this.tables = new TreeMap<String,DatabaseTable>();
		this.params = new LinkedList<QueryParameter>();
		this.orderby = new LinkedList<String>();
	}

	public void setOrderBy(List<String> orderby)
	{
		this.orderby = orderby;
	}
	
	public void setDescending(boolean descending)
	{
		this.descending = descending;
	}
	
	public Boolean isDescending()
	{
		return descending;
	}
	
	public List<String> getOrderBy()
	{
		return orderby;
	}

	public Map<String,DatabaseTable> getTableMap()
	{
		return tables;
	}
	
	public java.util.List<QueryParameter> getQueryParameters()
	{
		return params;
	}

	@Override
	public void addTable(String name)
	{
		DatabaseTable table = database.getTable(name);
		addTable(name, table);
	}
	
	@Override
	public void addTable(DatabaseTable table)
	{
		addTable(table.getName(), table);
	}

	@Override
	public void addParam(Collection<QueryParameter> params)
	{
		this.params.addAll(params);
	}

	@Override
	public void addTable(String name, DatabaseTable table)
	{
		tables.put(name, table);
	}
	
	@Override
	public void removeTable(String name)
	{
		tables.remove(name);
	}

	public void addParam(QueryParameter param)
	{
		this.params.add(param);
	}
	
	public abstract void addParam(String left, QueryParameterType type, String right);
	
	public void setQueryType(QueryType qtype)
	{
		this.qtype = qtype;
	}
	
	@Override
	public abstract int executeUpdate();
	
	@Override
	public abstract long size();
	
	public long getFirstResult()
	{
		return first;
	}
	
	public void setFirstResult(long first)
	{
		this.first = first;
	}
	
	@Override
	public long getLimit()
	{
		return limit;
	}
	
	@Override
	public void setLimit(long limit)
	{
		this.limit = limit;
	}
	
	@Override
	public boolean isEmpty()
	{
		if (size == -1) size = size();
		if (size == 0) return true;
		else return false;
	}
	
	@Override
	public void addField(DatabaseField field)
	{
		DatabaseTable table = field.getDatabaseTable();
		if (tables.containsValue(table)) fields.put(field.getName(), field);
		else 
		{
			StringBuilder msg = new StringBuilder();
			msg.append("Field ");
			msg.append(field.getName());
			msg.append(" from table ");
			msg.append(table.getName());
			msg.append(" is not available in this query!");
			throw new DatabaseException(msg.toString());
		}
	}

	@Override
	public void addField(String fieldName)
	{
		int sep = fieldName.indexOf('.');
		if (sep == -1)
		{
			// Search for a table that has that field
			DatabaseField field = null;
			for (DatabaseTable current : tables.values())
			{
				if (current.hasFieldName(fieldName))
				{
					if (field == null) field = current.getField(fieldName);
					else
					{
						StringBuilder msg = new StringBuilder();
						msg.append("Field ");
						msg.append(field.getName());
						msg.append(" is ambiguous in this query!");
						throw new DatabaseException(msg.toString());					
					}
				}
			}
			if (field != null) addField(fieldName, field);
			else
			{
				StringBuilder msg = new StringBuilder();
				msg.append("Field ");
				msg.append(fieldName);
				msg.append(" is not available in this query!");
				throw new DatabaseException(msg.toString());									
			}
		}
		else
		{
			// Decode the specified field into table and field names
			String tName = fieldName.substring(0, sep - 1);
			String fName = fieldName.substring(sep + 1, fieldName.length());
			DatabaseTable table = tables.get(tName);
			if (table != null)
			{
				DatabaseField field = table.getField(fName);
				if (field != null) addField(tName);
			}
		}
	}

	@Override
	public void addField(String name, DatabaseField field)
	{
		fields.put(name, field);
	}
}
