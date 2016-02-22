package org.infodancer.persist;

import org.infodancer.persist.dbapi.DatabaseConnection;
import org.infodancer.persist.dbapi.DatabaseException;

public class TestDatabaseConnection extends DatabaseConnection
{
	boolean valid;
	boolean autocommit;
	
	@Override
	public boolean isAutoCommit()
	{
		return autocommit;
	}

	@Override
	public void setAutoCommit(boolean commit)
	{
		this.autocommit = commit;
	}

	@Override
	public void rollback() throws DatabaseException
	{
		
	}

	@Override
	public void commit() throws DatabaseException
	{
		
	}

	@Override
	public boolean isValidConnection()
	{
		return valid;
	}

	@Override
	public void destroy()
	{
		valid = false;
	}

}
