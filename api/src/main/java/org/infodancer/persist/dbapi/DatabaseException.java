package org.infodancer.persist.dbapi;

public class DatabaseException extends RuntimeException
{
	public DatabaseException()
	{
		super();
	}

	public DatabaseException(String msg)
	{
		super(msg);
	}

	public DatabaseException(Throwable e)
	{
		super(e);
	}
}
