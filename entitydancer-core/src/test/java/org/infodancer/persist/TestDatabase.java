package org.infodancer.persist;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.infodancer.persist.dbapi.AbstractDatabase;
import org.infodancer.persist.dbapi.Database;
import org.infodancer.persist.dbapi.DatabaseConnection;
import org.infodancer.persist.dbapi.DatabaseField;
import org.infodancer.persist.dbapi.DatabaseQuery;
import org.infodancer.persist.dbapi.DatabaseTable;

public class TestDatabase extends AbstractDatabase implements Database 
{
	boolean closed;
	Map<String,DatabaseTable> tables = new TreeMap<String,DatabaseTable>();
	
	public void createTable(DatabaseTable table)
	{
		tables.put(table.getName(), table);
	}

	@Override
	public void updateTable(DatabaseTable table)
	{

	}

	@Override
	public void dropTable(String tableName)
	{
		tables.remove(tableName);
	}

	protected void initializeTableList()
	{
	}

	@Override
	public DatabaseTable createTable(String tableName)
	{
		return new TestDatabaseTable(tableName);
	}

	@Override
	public boolean isOpen()
	{
		return !closed;
	}

	@Override
	public void close()
	{
		closed = true;
	}

	@Override
	public DatabaseField createField(String fieldName, int sqlType)
	{
		return new TestDatabaseField(fieldName, sqlType);
	}

	@Override
	public DatabaseTable getTable(String name)
	{
		return tables.get(name);
	}

	@Override
	public Collection<DatabaseTable> getTables()
	{
		return tables.values();
	}

	@Override
	public void dropTable(DatabaseTable table)
	{
		tables.remove(table.getName());
	}

	@Override
	public void initialize(Properties properties)
	{
		
	}

	@Override
	public DatabaseTable describeTable(String name)
	{
		return tables.get(name);
	}

	public DatabaseQuery createQuery()
	{
		return new TestDatabaseQuery(this);
	}

	public void alterTable(DatabaseTable table)
	{
		// TODO Auto-generated method stub
		
	}

	public DatabaseConnection getConnection()
	{
		return new TestDatabaseConnection();
	}

	public void putConnection(DatabaseConnection con)
	{
				
	}
}
