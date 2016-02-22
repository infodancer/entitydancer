package org.infodancer.persist.sql;

import org.infodancer.persist.ServiceEntityTransaction;

public class SQLEntityTransaction extends ServiceEntityTransaction
{
	SQLConnection connection;
	
	public SQLEntityTransaction(SQLDatabase database)
	{
		super(database);
	}

	public SQLConnection getSQLConnection()
	{
		return connection;
	}
}
