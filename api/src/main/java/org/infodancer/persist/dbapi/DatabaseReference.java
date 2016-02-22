package org.infodancer.persist.dbapi;

public class DatabaseReference
{
	String field;
	String foreignTable;
	String foreignField;
	
	public DatabaseReference(String field, String foreignTable, String foreignField)
	{
		this.field = field;
		this.foreignTable = foreignTable;
		this.foreignField = foreignField;
	}

	public String getField()
	{
		return field;
	}

	public void setField(String field)
	{
		this.field = field;
	}

	public String getForeignTable()
	{
		return foreignTable;
	}

	public void setForeignTable(String foreignTable)
	{
		this.foreignTable = foreignTable;
	}

	public String getForeignField()
	{
		return foreignField;
	}

	public void setForeignField(String foreignField)
	{
		this.foreignField = foreignField;
	}
}
