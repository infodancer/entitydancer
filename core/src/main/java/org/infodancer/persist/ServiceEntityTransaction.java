package org.infodancer.persist;

import javax.persistence.EntityTransaction;

import org.infodancer.persist.dbapi.Database;
import org.infodancer.persist.dbapi.DatabaseConnection;

public class ServiceEntityTransaction implements EntityTransaction 
{
	boolean rollbackOnly = false;
	boolean active;
	
	Database database;
	DatabaseConnection connection;
	
	public ServiceEntityTransaction(Database database)
	{
		this.database = database;
	}
	
	DatabaseConnection getConnection()
	{
		return connection;
	}

	void getConnection(DatabaseConnection connection)
	{
		this.connection = connection;
	}
	
	public void begin() 
	{
		active = true;
		connection = database.getConnection();
		connection.setAutoCommit(false);
	}

	/**
	 * Commits the transaction and ends the transaction context.
	 */
	public void commit() 
	{
		connection.commit();
		database.putConnection(connection);
		active = false;
	}

	public boolean getRollbackOnly() 
	{
		return rollbackOnly;
	}

	public boolean isActive() 
	{
		return active;
	}

	/**
	 * Rolls back the transaction and ends the transaction context.
	 */

	public void rollback() 
	{
		connection.rollback();
		database.putConnection(connection);
		active = false;
	}

	public void setRollbackOnly() 
	{
		this.rollbackOnly = true;
	}
}
