package org.infodancer.persist.dbapi;

/**
 * Thrown when attempting to access a connection that has been locked.
 * @author matthew
 */
public class ConnectionLockedException extends DatabaseException
{
	public ConnectionLockedException()
	{
		super();
	}

	public ConnectionLockedException(String msg)
	{
		super(msg);
	}

	public ConnectionLockedException(Throwable e)
	{
		super(e);
	}

}
