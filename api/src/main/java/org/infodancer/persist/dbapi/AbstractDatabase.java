package org.infodancer.persist.dbapi;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;

public abstract class AbstractDatabase implements Database
{
	/** Connection pool **/
	protected Properties properties;
	
	protected void validateTable(DatabaseTable table)
	{
		if (table != null) 
		{
			String tableName = table.getName();
			validateTableName(tableName);
			DatabaseTable currentTable = getTable(tableName);
			if (currentTable != null) throw new DatabaseException("A table named \"" + tableName + "\" already exists!");
			if (table.getPrimaryKey() == null) throw new DatabaseException("Table must have a valid primary key!");			
		}
		else throw new DatabaseException("A null table is invalid!");
	}
	
	private void validateTableName(String name)
	{
		if (name != null)
		{
			if (name.trim().length() > 0)
			{
				char[] value = name.toCharArray();
				for (int i = 0; i < value.length; i++)
				{
					if (Character.isWhitespace(value[i]))
					{
						throw new DatabaseException("Whitespace is not valid in table names!");
					}
				}
			}
			else throw new DatabaseException("Table must have non-blank valid name! (\"" + name + "\"");
		}
		else throw new DatabaseException("Table must have a valid name (was null)!");
	}

	@Override
	public abstract DatabaseTable describeTable(String name);
	
	@Override
	public abstract boolean isOpen();

	@Override
	public abstract void close();

	@Override
	public abstract void initialize(Properties properties);
	
	/**
	 * Retrieves the list of tables from the underlying database.
	 */
	protected abstract void initializeTableList();
	
	@Override
	public abstract DatabaseField createField(String fieldName, int sqlType);

	@Override
	public abstract DatabaseTable createTable(String tableName);

	@Override
	public abstract DatabaseTable getTable(String name);

	@Override
	public abstract Collection<DatabaseTable> getTables();

	@Override
	public abstract void createTable(DatabaseTable table);

	@Override
	public abstract void updateTable(DatabaseTable table);

	@Override
	public abstract void dropTable(String tableName);

	@Override
	public abstract void dropTable(DatabaseTable table);

	@Override
	public void clear()
	{
		LinkedList<String> names = new LinkedList<String>();
		for (DatabaseTable table : getTables())
		{
			names.add(table.getName());
		}
		
		for (String name : names)
		{
			dropTable(name);
		}
	}
}
