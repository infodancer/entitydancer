package org.infodancer.persist.dbapi;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.infodancer.persist.dbapi.DatabaseReference;

public interface DatabaseTable
{
	public DatabaseField getField(String name);
	public void addField(DatabaseField field);
	
	public String getName();
	
	public void setName(String name);
	
	public boolean isRelationshipTable();

	public void setRelationshipTable(boolean relationshipTable);

	public Collection<DatabaseField> getFields();
	public boolean hasFieldName(String name);
	
	public List<DatabaseReference> getReferences();

	/** 
	 * Alters the existing table by adding the provided field dynamically.
	 * Warning: This operation may be long-running and may fail.
	 * @param field
	 */
	public void alterTable(DatabaseField field);
	
	/** 
	 * Supplies the list of foreign key constraints.
	 * @param references
	 */
	public void setReferences(List<DatabaseReference> references);

	/**
	 * Adds a foreign key reference to the named field.
	 * @param field
	 * @param foreignTable
	 * @param foreignField
	 */
	public void addReference(String field, String foreignTable, String foreignField);
	
	/**
	 * Inserts the provided values into the database, returning the key value.
	 * The key value will be returned whether it was generated or already existed. 
	 * @param values the values to persist.
	 * @param con The DatabaseConnection to use.
	 * @return the primary key value of the persisted row.
	 */
	public abstract Object persist(DatabaseConnection con, Map<String,Object> values);

	/**
	 * Inserts the provided values into the database, returning the key value.
	 * The key value will be returned whether it was generated or already existed. 
	 * @param values
	 * @return the primary key value of the persisted row.
	 */
	public abstract Object persist(Map<String,Object> values);
	
	/**
	 * Retrieves the values matching the provided key.
	 * @param key
	 * @return
	 */
	public abstract Map<String,Object> find(Object key);
	/**
	 * Retrieves the values matching the provided key, using the provided connection.
	 * @param con
	 * @param key
	 * @return
	 */
	public abstract Map<String,Object> find(DatabaseConnection con, Object key);
	
	/**
	 * Deletes the row matching the provided key from the table.
	 * @param key
	 */
	public abstract void delete(Object key);

	/**
	 * Deletes the row matching the provided key from the table.
	 * @param key
	 */
	public abstract void delete(DatabaseConnection con, Object key);
	
	/**
	 * Updates the data from the provided values into the table, using
	 * the provided connection.
	 * @param values
	 */
	public abstract void merge(DatabaseConnection con, Object key, Map<String,Object> values);	

	/**
	 * Updates the data from the provided values into the table.
	 * @param values
	 */
	public abstract void merge(Object key, Map<String,Object> values);	

	public String getPrimaryKeyName();

	public DatabaseField getPrimaryKey();
	
	/**
	 * Clears rows from the table which match the provided key values (exact match).
	 * This method is intended to be used for clearing relationship tables. 
	 * @param values
	 */
	public abstract void clear(Map<String,Object> values);

	/**
	 * Clears rows from the table which match the provided key values (exact match).
	 * This method is intended to be used for clearing relationship tables. 
	 * @param values
	 */
	public abstract void clear(DatabaseConnection con, Map<String,Object> values);

	/**
	 * Clears ALL rows from the table.
	 */
	public abstract void clear();
}

