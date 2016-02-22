package org.infodancer.persist.dbapi;

public interface DatabaseField
{
	public String getName();
	
	/**
	 * Provides the DatabaseTable object this field is associated with.
	 * @return DatabaseTable, or null if the field has not yet been associated.
	 */
	public DatabaseTable getDatabaseTable();

	/**
	 * Specifies the DatabaseTable object this field is associated with.
	 */
	public void setDatabaseTable(DatabaseTable table);
	
	public void setName(String name);
	
	/**
	 * Indicates whether this field should have a unique constraint.
	 * @param unique
	 */
	public void setUnique(boolean unique);
	
	/**
	 * Indicates whether this field has a unique constraint.
	 * @return
	 */
	public boolean isUnique();
	
	public int getSqlType();

	public void setSqlType(int sqlType);
	
	public int getLength();

	public void setLength(int length);

	public String getDefinition();
	
	public void setDefinition(String definition);
	
	/**
	 * Indicates whether the field has an index named 
	 * according to the field name + "_IDX".
	 * @return true if the field is indexed.
	 */
	public boolean isIndexed();
	
	/** 
	 * Flags the field for indexing.
	 * @param indexed
	 */
	public void setIndexed(boolean indexed);
	
	public boolean isPrimaryKey();
	
	public void setPrimaryKey(boolean primaryKey);
	
	/**
	 * Indicates whether the field should be a generated key field.
	 * This method can have different implications depending on the database.
	 * @return
	 */
	public boolean isGeneratedKey();

	/**
	 * Specifies whether the field should generate key values upon insert.
	 * @param generatedKey
	 */
	public void setGeneratedKey(boolean generatedKey);

	/**
	 * Specifies whether the field should allow null values.
	 * @param nullable
	 */
	public void setNullable(boolean nullable);
	
	/** 
	 * Indicates whether the field allows null values.
	 * @return true if null values are permitted.
	 */
	public boolean isNullable();
	
}
