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

	public abstract DatabaseTable describeTable(String name);
	
	public abstract boolean isOpen();

	public abstract void close();

	public abstract void initialize(Properties properties);
	
	/**
	 * Retrieves the list of tables from the underlying database.
	 */
	protected abstract void initializeTableList();
	
	public abstract DatabaseField createField(String fieldName, int sqlType);

	public abstract DatabaseTable createTable(String tableName);

	public abstract DatabaseTable getTable(String name);

	public abstract Collection<DatabaseTable> getTables();

	public abstract void createTable(DatabaseTable table);

	public abstract void updateTable(DatabaseTable table);

	public abstract void dropTable(String tableName);

	public abstract void dropTable(DatabaseTable table);

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
