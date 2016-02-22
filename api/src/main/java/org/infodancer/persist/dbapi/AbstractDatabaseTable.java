package org.infodancer.persist.dbapi;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

public abstract class AbstractDatabaseTable implements DatabaseTable
{
	private static final Logger logger = Logger.getLogger("AbstractDatabaseTable");
	protected String name;
	protected boolean relationshipTable;
	protected List<DatabaseReference> references = new LinkedList<DatabaseReference>();
	protected Map<String,DatabaseField> fields = new TreeMap<String,DatabaseField>(); 

	protected AbstractDatabaseTable(String name)
	{
		this.name = name;
	}

	public void logValueMap(Map<String,Object> values)
	{
		StringBuilder msg = new StringBuilder();
		msg.append('[');
		for (String key : values.keySet())
		{
			Object value = values.get(key);
			msg.append(' ');
			msg.append(key);
			msg.append('=');
			if (value != null) 
			{
				msg.append('\"');
				msg.append(value.toString());
				msg.append('\"');
			}
			else msg.append("null");
		}
		msg.append(' ');
		msg.append(']');
		logger.finest(msg.toString());
	}
	
	public DatabaseField getField(String name)
	{
		return fields.get(name);
	}

	public boolean hasFieldName(String name)
	{
		return fields.containsKey(name);
	}
	
	public String getName()
	{
		return name;
	}
	
	public void setName(String name)
	{
		this.name = name;
	}

	public String getPrimaryKeyName()
	{
		for (DatabaseField field : fields.values())
		{
			if (field.isPrimaryKey())
			{
				return field.getName();
			}
		}
		throw new DatabaseException("Could not locate primary key in table " + name);
	}

	public DatabaseField getPrimaryKey()
	{
		for (DatabaseField field : fields.values())
		{
			if (field.isPrimaryKey())
			{
				return field;
			}
		}
		throw new DatabaseException("Could not locate primary key in table " + name);
	}

	public boolean isRelationshipTable()
	{
		return relationshipTable;
	}

	public void setRelationshipTable(boolean relationshipTable)
	{
		this.relationshipTable = relationshipTable;
	}

	public Collection<DatabaseField> getFields()
	{
		return fields.values();
	}

	public List<DatabaseReference> getReferences()
	{
		return references;
	}

	public void setReferences(List<DatabaseReference> references)
	{
		this.references = references;
	}

	public void addReference(String field, String foreignTable, String foreignField)
	{
		DatabaseReference reference = new DatabaseReference(field, foreignTable, foreignField);
		references.add(reference);
	}
}
