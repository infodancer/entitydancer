package org.infodancer.persist.dbapi;

import java.util.concurrent.atomic.AtomicLong;

public abstract class DatabaseConnection
{
	protected DatabaseConnectionPool pool;
	private boolean locked;
	private static final AtomicLong connectionId = new AtomicLong();
	private final long id;
	private long acquiredTime;
	private StackTraceElement[] trace;
	public DatabaseConnection()
	{
		this.id = connectionId.addAndGet(1);
	}
	
	public long getConnectionId()
	{
		return connectionId.get();
	}
	
	public long getAcquiredTime()
	{
		return acquiredTime;
	}

	public void setAcquiredTime(long time)
	{
		this.acquiredTime = time;
	}
	
	public void setStackTrace(StackTraceElement[] trace)
	{
		this.trace = trace;
	}

	public StackTraceElement[] getStackTrace()
	{
		return trace;
	}

	public String toString()
	{
		return "DatabaseConnection.id: " + id;
	}
	
	/** 
	 * Specifies whether autocommit is currently enabled.
	 * @return
	 */
	public abstract boolean isAutoCommit();
	
	/**
	 * Specifies whether to automatically commit transactions.
	 * @param commit
	 */
	public abstract void setAutoCommit(boolean commit);
	
	/**
	 * Rolls back the current transaction.
	 */
	public abstract void rollback() throws DatabaseException;
	
	/**
	 * Commits the current transaction.
	 */
	public abstract void commit() throws DatabaseException;
	
	/**
	 * Locks the connection, making it unavailable for use.
	 */
	public void lock()
	{
		this.locked = true;
	}
	
	/**
	 * Unlocks the connection, making it available for general use.
	 */
	public void unlock()
	{
		this.locked = false;
	}
	
	/**
	 * Indicates whether the connection is locked (in the pool) or unlocked (available for use).
	 * @return boolean locked.
	 */
	public boolean isLocked()
	{
		return locked;	
	}

	public void setDatabaseConnectionPool(DatabaseConnectionPool pool)
	{
		this.pool = pool;
	}

	public DatabaseConnectionPool getDatabaseConnectionPool()
	{
		return pool;
	}
	
	/**
	 * Indicates whether the connection is still valid. 
	 */
	public abstract boolean isValidConnection();
	
	/**
	 * Closes the connection permanently, without returning to the pool.
	 */
	public abstract void destroy();	
}
