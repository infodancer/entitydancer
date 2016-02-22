package org.infodancer.persist.dbapi;

import java.util.Collection;
import java.util.Properties;

/**
 * Contains methods to manage the database which is the entity store.
 * This is intended to be transparent across different store implementations.1111111111111111111111111111111111111111111111
 * @author matthew
 *
 */

public interface Database
{	
	/**
	 * Indicates whether the database is open and ready to process requests.
	 * @return
	 */
	public boolean isOpen();
	
	/** 
	 * Closes the database.  No further requests will be processed.
	 */
	public void close();
	
	/**
	 * Initializes the database from the provided properties file. 
	 * @param properties
	 */
	public void initialize(Properties properties);
	
	/**
	 * Provides an implementation of DatabaseField that will store the provided fieldType
	 * in an appropriate column named fieldName.
	 * @param fieldName The name of the column to store the field.
	 * @param sqlType The type of the column to store the field.
	 * @return A concrete Database implementation with the above two parameters initialized.
	 */
	public DatabaseField createField(String fieldName, int sqlType);

	/**
	 * Provides an implementation of DatabaseTable suitable for the Database.
	 * The intended use of this method is for the caller to modify the table
	 * by adding fields to it, then call createTable(DatabaseTable).
	 * @param tableName The name of the table.
	 * @return A concrete DatabaseTable implementation with the provided name.
	 */
	public DatabaseTable createTable(String tableName);
	
	/**
	 * Describes an existing table from the underlying data store (not the 
	 * internal schema cache).  This representation may not reflect the underlying
	 * schema perfectly and should not update the internal schema cache.  
	 * Use with caution.
	 * 
	 * @param tableName
	 * @return DatabaseTable or null if the table does not exist.
	 */
	public DatabaseTable describeTable(String tableName);
	
	/**
	 * Gets the table specified by name.
	 * @param name
	 * @return DatabaseTable or null.
	 */
	public DatabaseTable getTable(String name);
	
	/**
	 * Gets a list of tables.
	 * This list is cached and may not reflect changes to the database performed outside
	 * this framework.
	 */
	public Collection<DatabaseTable> getTables();	
	
	/**
	 * Creates the table according to the specified schema.
	 * @param table
	 */
	public void createTable(DatabaseTable table);
	
	/**
	 * Updates the table's schema definition to match the provided one.
	 * Note: This will attempt to alter the existing table to match the provided one.
	 * @param table
	 */
	public void alterTable(DatabaseTable table);

	/**
	 * Updates the table's schema definition to match the provided one.
	 * Note: This does not rename tables, it drops the old and creates the new.
	 * @param table
	 */
	public void updateTable(DatabaseTable table);
	
	/**
	 * Drops the specified table, including all data, from the database.
	 * @param tableName
	 */
	public void dropTable(String tableName);
	/**
	 * Drops the specified table, including all data, from the database.
	 * @param table
	 */
	public void dropTable(DatabaseTable table);
	
	/**
	 * Drops all tables from the database -- DANGEROUS.
	 */
	public void clear();
	
	/**
	 * Provides a DatabaseQuery object.
	 * @return DatabaseQuery
	 */
	public DatabaseQuery createQuery();
	
	/** 
	 * Retrieves a DatabaseConnection object for use in transactions.
	 */
	public DatabaseConnection getConnection();
	
	/** 
	 * Retrieves a DatabaseConnection object for use in transactions.
	 */
	public void putConnection(DatabaseConnection con);

}
