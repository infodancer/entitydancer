package org.infodancer.persist;

import java.util.concurrent.Callable;

import org.infodancer.persist.dbapi.DatabaseConnection;

public abstract class DatabaseOperation
{
	Object value;
	DatabaseConnection con;
	ServiceEntityManager manager;
	
	public DatabaseOperation(ServiceEntityManager manager, DatabaseConnection con, Object o)
	{
		this.manager = manager;
		this.con = con;
		this.value = o;
	}

	public Object getValue()
	{
		return value;
	}

	public void setO(Object o)
	{
		this.value = o;
	}

	public DatabaseConnection getCon()
	{
		return con;
	}

	public void setCon(DatabaseConnection con)
	{
		this.con = con;
	}

	public ServiceEntityManager getManager()
	{
		return manager;
	}

	public void setManager(ServiceEntityManager manager)
	{
		this.manager = manager;
	}

	
}
