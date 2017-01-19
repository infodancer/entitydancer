package org.infodancer.persist.dbapi;

public class AbstractDatabaseField implements DatabaseField
{
	int sqlType;
	int length;
	String name;
	String definition;
	boolean unique;
	boolean indexed;
	boolean nullable;
	boolean primaryKey;
	boolean generatedKey;
	DatabaseTable table;

	protected AbstractDatabaseField(String name, int sqlType)
	{
		this.name = name;
		this.sqlType = sqlType;
		this.unique = false;
		this.indexed = false;
		this.nullable = true;
		this.primaryKey = false;
		this.generatedKey = false;
	}

	protected AbstractDatabaseField(String name, int sqlType, String definition)
	{
		this.name = name;
		this.sqlType = sqlType;
		this.definition = definition;
		this.unique = false;
		this.indexed = false;
		this.nullable = true;
		this.primaryKey = false;
		this.generatedKey = false;
	}
	
	public boolean isUnique()
	{
		return unique;
	}

	public void setUnique(boolean unique)
	{
		this.unique = unique;
	}

	public String getName()
	{
		return name;
	}
	
	public void setName(String name)
	{
		this.name = name;
	}
	
	public int getSqlType()
	{
		return sqlType;
	}

	public void setSqlType(int sqlType)
	{
		this.sqlType = sqlType;
	}
	
	public int getLength()
	{
		return length;
	}

	public void setLength(int length)
	{
		this.length = length;
	}

	public String getDefinition()
	{
		return definition;
	}
	
	public void setDefinition(String definition)
	{
		this.definition = definition;
	}
	
	public boolean isPrimaryKey()
	{
		return primaryKey;
	}
	
	public void setPrimaryKey(boolean primaryKey)
	{
		this.primaryKey = primaryKey;
	}

	public boolean isGeneratedKey()
	{
		return generatedKey;
	}

	public void setGeneratedKey(boolean generatedKey)
	{
		this.generatedKey = generatedKey;
	}

	public void setNullable(boolean nullable)
	{
		this.nullable = nullable;
	}

	public boolean isNullable()
	{
		return nullable;
	}

	public DatabaseTable getDatabaseTable()
	{
		return table;
	}

	public void setDatabaseTable(DatabaseTable table)
	{
		this.table = table;
	}
	
	public boolean isIndexed()
	{
		return indexed;
	}
	
	public void setIndexed(boolean indexed)
	{
		this.indexed = indexed;
	}
}
