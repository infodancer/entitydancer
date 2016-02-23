package org.infodancer.persist.dbapi;

public interface DatabaseConnectionFactory<T extends DatabaseConnection>
{
	public T createConnection();
}
