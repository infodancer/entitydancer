package org.infodancer.persist;

import java.util.Collection;
import java.util.Date;

import org.infodancer.persist.dbapi.AbstractDatabaseQuery;
import org.infodancer.persist.dbapi.DatabaseField;
import org.infodancer.persist.dbapi.DatabaseQuery;
import org.infodancer.persist.dbapi.DatabaseTable;
import org.infodancer.persist.dbapi.QueryParameter;
import org.infodancer.persist.dbapi.QueryParameterType;
import org.infodancer.persist.dbapi.QueryType;

public class TestDatabaseQuery extends AbstractDatabaseQuery implements DatabaseQuery
{
	TestDatabase database;
	
	public TestDatabaseQuery(TestDatabase database)
	{
		super(database);
		this.database = database;
	}
	
	@Override
	public void addField(DatabaseField field)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void addField(String name, DatabaseField field)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void addTable(DatabaseTable table)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void addTable(String name, DatabaseTable table)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void removeTable(String name)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void addParam(Collection<QueryParameter> params)
	{
		// TODO Auto-generated method stub

	}

	public void addParam(String left, String type, String right)
	{
		params.add(new TestQueryParameter(left, type, right));
	}

	@Override
	public void addParam(String left, QueryParameterType type, String right)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void setQueryType(QueryType type)
	{
		// TODO Auto-generated method stub

	}

	public void executeQuery()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public int executeUpdate()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean next()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setLimit(long limit)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public long getLimit()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	public String getString(String name)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public Integer getInteger(String name)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public Long getLong(String name)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public Float getFloat(String name)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public Double getDouble(String name)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public Object getObject(String name)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public Date getDate(String name)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public Date getTimestamp(String name)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public void clear()
	{
		// TODO Auto-generated method stub

	}

	public String getNativeQuery()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isEmpty()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long size()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	public void close()
	{
		// TODO Auto-generated method stub

	}

}
