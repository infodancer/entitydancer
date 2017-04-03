package org.infodancer.persist;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.MethodDescriptor;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Clob;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import javax.persistence.Entity;
import javax.persistence.EntityExistsException;
import javax.persistence.EntityListeners;
import javax.persistence.EntityManager;
import javax.persistence.EntityNotFoundException;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import javax.persistence.TransactionRequiredException;

import org.infodancer.persist.dbapi.Database;
import org.infodancer.persist.dbapi.DatabaseConnection;
import org.infodancer.persist.dbapi.DatabaseField;
import org.infodancer.persist.dbapi.DatabaseQuery;
import org.infodancer.persist.dbapi.DatabaseTable;
import org.infodancer.persist.dbapi.QueryParameter;
import org.infodancer.persist.dbapi.QueryParameterType;
import org.infodancer.persist.dbapi.QueryType;

/**
 * An EntityManager with no disk-based persistence.
 * @author matthew
 *
 */
public class ServiceEntityManager implements EntityManager 
{
	private static final String SERVICE_ENTITY_MANAGER_ENTITY_TYPE_TABLE = "ServiceEntityManager_EntityTypeTable";
	Logger logger = Logger.getLogger(ServiceEntityManager.class.getName());
	protected static final String DEFAULT_LENGTH = "256";
	protected static final String DEFAULT_PRECISION = "64";
	protected DatabaseTable entityTypeTable;
	protected Map<Class,Integer> supportedTypeMap = new ConcurrentHashMap<Class,Integer>();
	protected Map<String,Class> entityNameMap = new ConcurrentHashMap<String,Class>();
	protected Map<Class,EntityType> entityTypeMap = new ConcurrentHashMap<Class,EntityType>();
	protected Map<Class,Object> listeners = new ConcurrentHashMap<Class,Object>();
	protected Map<Long,WeakReference<ServiceEntityTransaction>> transactions = new ConcurrentHashMap<Long,WeakReference<ServiceEntityTransaction>>();
	protected FlushModeType mode = FlushModeType.AUTO;
	protected Database database;
	protected String propertiesFile;
	long cachetry = 0;
	long cachehit = 0;
	long cachemiss = 0; 

	public ServiceEntityManager()
	{
		supportedTypeMap.put(String.class, java.sql.Types.VARCHAR);
		supportedTypeMap.put(Integer.class, java.sql.Types.INTEGER);
		supportedTypeMap.put(int.class, java.sql.Types.INTEGER);
		supportedTypeMap.put(float.class, java.sql.Types.FLOAT);
		supportedTypeMap.put(Float.class, java.sql.Types.FLOAT);
		supportedTypeMap.put(Double.class, java.sql.Types.DOUBLE);
		supportedTypeMap.put(double.class, java.sql.Types.DOUBLE);
		supportedTypeMap.put(Long.class, java.sql.Types.BIGINT);
		supportedTypeMap.put(long.class, java.sql.Types.BIGINT);		
		supportedTypeMap.put(java.util.Date.class, java.sql.Types.TIMESTAMP);
		supportedTypeMap.put(java.sql.Date.class, java.sql.Types.DATE);
		supportedTypeMap.put(java.sql.Time.class, java.sql.Types.TIME);
		supportedTypeMap.put(java.sql.Timestamp.class, java.sql.Types.TIMESTAMP);
		supportedTypeMap.put(Boolean.class, java.sql.Types.BOOLEAN);
		supportedTypeMap.put(boolean.class, java.sql.Types.BOOLEAN);
		supportedTypeMap.put(byte[].class, java.sql.Types.VARBINARY);
		supportedTypeMap.put(Byte[].class, java.sql.Types.VARBINARY);
		supportedTypeMap.put(Blob.class, java.sql.Types.BLOB);
		supportedTypeMap.put(Clob.class, java.sql.Types.CLOB);
		supportedTypeMap.put(Enum.class, java.sql.Types.INTEGER);
	}
	
	public Database getDatabase()
	{
		return database;
	}
	
	public void setDatabase(Database database)
	{
		this.database = database;
		loadCache();
	} 
	
	/** 
	 * Creates an EntityType instance to manage persistence for the provided Entity.
	 * Implementations should return an EntityType specific to their storage methods.
	 * @throws ClassNotFoundException 
	 **/
	protected EntityType createEntityInstance(Class entityClass)
	throws IntrospectionException, ClassNotFoundException
	{
		if (entityClass != null)
		{
			EntityType type = new EntityType(this, entityClass);
			addCache(type);
			return type;
		}
		else throw new PersistenceException("entityClass must not be null!");
	}

	/**
	 * Closes any open resources.
	 */
	public void close()
	{
		if (database != null) database.close();
	}
	
	public void open()
	{
		if (!isOpen())
		{
			try
			{
				if (database == null) throw new PersistenceException("No database set!");
				initialize();
			}
			
			catch (Exception e)
			{
				e.printStackTrace();
				throw new PersistenceException("Could not initialize database!");
			}
		}
		else throw new PersistenceException("ServiceEntityManager already open!");
	}

	private void initialize()
	{
		
	}
	
	public boolean isOpen()
	{
		if (database != null) return database.isOpen();
		else return true;
	}

	/**
	 * Check if the instance belongs to the current persistence
	 * context.
	 * @return
	 */
	public boolean contains(Object o) 
	{
		// TODO This method is not transaction-aware, should check DB with a connection
		try
		{
			if (o != null)
			{
				synchronized (o)
				{
					Class c = o.getClass();
					if (isEntity(c))
					{
						EntityType entityType = getEntity(c);
						Object key = getPrimaryKeyValue(o);
						if (key != null)
						{
							Object result = find(entityType, key);
							if (result != null) return true;
							else return false;
						}
						else return false;
					}
					else throw new PersistenceException(c.getCanonicalName() + " is not an Entity!");
				}
			}
			else return false;
		}
		
		catch (Exception e)
		{
			e.printStackTrace();
			throw new PersistenceException(e);
		} 		
	}	
	
	protected <T> T find(EntityType type, Object key) 
	throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException, ClassNotFoundException, IntrospectionException 
	{
		DatabaseConnection con = null;
		
		try
		{
			con = getDatabaseConnection();
			return find(con, type, key);
		}
		
		finally
		{
			try { if (con != null) database.putConnection(con); } catch (Exception e) { } 
		}
	}
	
	protected <T> T find(DatabaseConnection con, EntityType type, Object key) 
	throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, ClassNotFoundException, IntrospectionException
	{
		String tableName = type.getTableName();
		DatabaseTable table = database.getTable(tableName);
		Map<String,Object> values = table.find(key);
		if (values.isEmpty()) return null;
		T result = (T) type.newInstance();
		type.setPrimaryKeyValue(result, key);
		for (EntityField field : type.getFields())
		{
			if (field.isRelationshipType())
			{
				if ((field.isOneToOne()) || (field.isManyToOne()))
				{
					Object value = values.get(field.getStoreName());
					logger.finest("Retrieved field " + field.getName() + ":" + value);
					EntityType foreignType = field.getForeignType();
					logger.finest("Retrieving dependent entity " + field.getName() + "<" + foreignType + ">:" + value);
					Object foreignValue = find(con, foreignType, value);
					logger.finest("Setting field value to retrieved entity: " + foreignValue);
					field.setFieldValue(result, foreignValue);							
				}
				else if (field.isOneToMany())
				{
					EntityType foreignType = field.getForeignType();
					DatabaseRelationshipCollection foreignValues = selectRelationship(con, type, field, result, foreignType);
					field.setFieldValue(result, foreignValues);
				}
				else if (field.isManyToMany())
				{
					EntityType foreignType = field.getForeignType();
					DatabaseRelationshipCollection foreignValues = selectRelationship(con, type, field, result, foreignType);
					field.setFieldValue(result, foreignValues);
				}
			}
			else 
			{
				Object value = values.get(field.getName());
				logger.finest("Retrieved field " + field.getName() + ":" + value);
				field.setFieldValue(result, value);
			}
		}						
		return result;
	}
	
	protected boolean isPrecisionType(int sqltype)
	{
		return false;		
	}

	/**
	 * Parses the query string and returns a Query object implementing it.
	 * There are certain specification features that are not (yet) supported:
	 * <ul>
	 * <li>Subqueries are not supported</li>
	 * <li>Aggregate values are not supported</li>
	 * <li>GROUP BY and HAVING clauses are not supported</li>
	 * <li>Functional Expressions are not supported</li>
	 * </ul>
	 * Some or all of these may be implemented later.
	 */
	public Query createQuery(String q)
	{
		logger.fine("Creating query: " + q);
		DatabaseQuery query = database.createQuery();
		Scanner scanner = new Scanner(q);
		// SELECT, UPDATE, DELETE is required
		QueryType qtype = parseQueryType(scanner);
		query.setQueryType(qtype);
		Map<String,EntityType> rtypes = null;
		if (qtype.equals(QueryType.SELECT))
		{
			String[] rnames = parseSelect(scanner);
			// FROM is mandatory
			rtypes = parseFrom(scanner, query);
			// WHERE is optional
			if (scanner.hasNext("WHERE"))
			{
				parseWhereClause(scanner, query);
			}
			if (scanner.hasNext("ORDER"))
			{
				parseOrderBy(scanner, query);
			}
			for (String key : rtypes.keySet())
			{
				EntityType type = rtypes.get(key);
				logger.finest("rtypes[" + key + "," + type.getClassName() + "]");
			}
			return new EntityQuery(this, qtype, rnames, rtypes, query);			
		}
		else if (QueryType.DELETE.equals(qtype))
		{
			throw new PersistenceException("DELETE queries are not yet supported!");
		}
		else if (QueryType.UPDATE.equals(qtype))
		{
			throw new PersistenceException("UPDATE queries are not yet supported!");
		}
		else throw new PersistenceException("Unrecognized query type!");
	}

	List<String> parseOrderBy(Scanner scanner, DatabaseQuery query)
	{
		List<String> results = new LinkedList<String>();		
		while (scanner.hasNext())
		{
			String token = scanner.next();
			
			if ("ORDER".equalsIgnoreCase(token)) continue;
			else if ("BY".equalsIgnoreCase(token)) 
			{
				scanner.useDelimiter("[\\ |,]");	
				continue;
			}
			else if ("ASC".equalsIgnoreCase(token)) break;
			else if ("DESC".equalsIgnoreCase(token)) 
			{
				query.setDescending(true);
				break;
			}
			else results.add(token.trim());
		}
		
		query.setOrderBy(results);
		scanner.useDelimiter(" ");
		return results;
	}

	String[] parseSelect(Scanner s)
	{
		List<String> results = new LinkedList<String>();
		if (s.hasNext("\\*")) return new String[0];
		while (!s.hasNext("FROM"))
		{
			String token = s.next();
			if (!"DISTINCT".equalsIgnoreCase(token)) results.add(token);
		}
		return results.toArray(new String[results.size()]);
	}

	List<QueryParameter> parseWhereClause(Scanner s, DatabaseQuery query)
	{
		String token = s.next();
		if ("WHERE".equalsIgnoreCase(token))
		{
			while (s.hasNext())
			{
				if ((s.hasNext("ORDER")) || s.hasNext("GROUP")) break;
				else if (s.hasNext("BETWEEN"))
				{
					// BETWEEN is handled differently 	
				}
				else if (s.hasNext("AND")) 
				{
					// For now, just advance to the next token
					s.next();
				}
				else if (s.hasNext("OR"))
				{
					throw new PersistenceException("OR is not yet supported in a WHERE clause!");
				}
				else
				{
					String left = parseQueryElement(s.next(), query);
					String type = s.next();
					String right = parseQueryElement(s.next(), query);
					query.addParam(left, type, right);
				}
			}
		}
		return query.getQueryParameters();
	}

	String parseQueryElement(String next, DatabaseQuery query)
	{
		int sep = next.indexOf('.');
		if (sep != -1)
		{
			String alias = next.substring(0, sep - 1);
			String field = next.substring(sep + 1, next.length());
			return next;
		}
		else return next;
	}

	/**
	 * This method will return null if the result type should be Object[].
	 * @param tokens
	 * @return
	 */
	Map<String,EntityType> parseFrom(Scanner s, DatabaseQuery query)
	{
		Map<String,EntityType> result = new TreeMap<String,EntityType>();
		String from = s.next();
		if ("FROM".equalsIgnoreCase(from))
		{
			do
			{
				String name = s.next();
				String alias = name;
				if (s.hasNext())
				{
					if (s.hasNext("AS")) s.next();
					else if (!s.hasNext("WHERE")) alias = s.next();
				}
				EntityType entityType = getEntityByName(name);
				result.put(alias, entityType);
				DatabaseTable table = database.getTable(name);
				if (table == null) 
				{
					createEntityStore(entityType);
					table = database.getTable(name);
				}
				query.addTable(alias, table);					
			}
			while ((s.hasNext()) && (!s.hasNext("WHERE")));
			return result;
		}
		else throw new PersistenceException(from + " is not the FROM keyword!");
	}

	QueryType parseQueryType(Scanner s)
	{
		String token = s.next();
		if ("SELECT".equalsIgnoreCase(token)) return QueryType.SELECT;
		else if ("DELETE".equalsIgnoreCase(token)) return QueryType.DELETE;
		else if ("UPDATE".equalsIgnoreCase(token)) return QueryType.UPDATE;
		else throw new PersistenceException(token + " is not a valid query type!  (SELECT, UPDATE, DELETE)");
	}

	public Object getDelegate()
	{
		throw new PersistenceException("getDelegate() method not implemented");
	}
	
	// Subclasses implement the methods in the above block

	protected String createRelationshipStoreName(EntityType typeA, EntityType typeB)
	{
		return typeA.getTableName() + "_" + typeB.getTableName();
	}

	/**
	 * Find by primary key.
	 * @return the found entity instance or null
	 *                if the entity does not exist
     */

	public <T> T find(Class<T> type, Object keyvalue)
	{
		T result = null;
		if (isEntity(type))
		{
			try
			{
				EntityType entity = getEntity(type);
				if (keyvalue != null)
				{
					if (cachetry == Long.MAX_VALUE)
					{
						cachetry = 0;
						cachehit = 0;
						cachemiss = 0;
						logger.info("GlobalCache: rolling over to 0");
					}
					cachetry++;
					result = (T) entity.getCache(keyvalue);
 					if (result == null) 
 					{
 						// System.out.println("Cache miss on " + type.getCanonicalName());
 						result = (T) find(entity, keyvalue);
 						entity.putCache(keyvalue, result);
 						cachemiss++;
 					}
 					else
 					{
 						// System.out.println("Cache hit on " + type.getCanonicalName());
 						cachehit++;
 					}
 					if ((cachetry % 10000) == 0)
 					{
 						DecimalFormat format = new DecimalFormat();
 						format.setMaximumFractionDigits(2);
 						double rate = ((double) cachehit / cachetry);
 						rate = rate * 100;
 						logger.info("GlobalCache[hit/miss/try]: " + cachehit + "/" + cachemiss + "/" + cachetry + "/" + format.format(rate) + "%");
 					}
					return result;
				}
				else throw new IllegalArgumentException("The key object must not be null!");
			}
			
			catch (Exception e)
			{
				// logger.throwing("ServiceEntityManager", "find", e);
				throw new PersistenceException(e);
			}			
		}
		else throw new IllegalArgumentException("This object is not an Entity!");
	}

	/**
	 * Merge the state of the given entity into the
	 * current persistence context.
	 * @return the instance that the state was merged to
	 * @throws IllegalArgumentException if instance is not an
	 *                 entity or is a removed entity
	 * @throws TransactionRequiredException if invoked on a
	 * container-managed entity manager of type
	 * PersistenceContextType.TRANSACTION and there is
	 * no transaction.
	 */

	public <T> T merge(T o)
	{
		if (o != null)
		{
			synchronized (o)
			{
				T result = null;
				Class type = o.getClass();
				if (isEntity(type))
				{
					try
					{
						EntityType entity = getEntity(type);
						preUpdate(o);
						result = (T) merge(entity, o);
						// TODO Due to possible bugs, clear the cache rather than populating it
						entity.putCache(entity.getPrimaryKeyValue(o), o);
						postUpdate(result);
						return result;
					}
		
					catch (Exception e)
					{
						e.printStackTrace();
						logger.throwing("ServiceEntityManager", "merge", e);
						throw new PersistenceException(e);
					}
				}
				else throw new IllegalArgumentException("This object is not an Entity!");
			}
		}
		else throw new IllegalArgumentException("Cannot merge a null object!");
	}

	/**
	 * Make an instance managed and persistent.
	 * @throws EntityExistsException if the entity already exists.
	 * (The EntityExistsException may be thrown when the persist
	 * operation is invoked, or the EntityExistsException or
	 * another PersistenceException may be thrown at flush or
	 * commit time.)
	 * @throws IllegalArgumentException if not an entity
	 * @throws TransactionRequiredException if invoked on a
	 * container-managed entity manager of type
	 * PersistenceContextType.TRANSACTION and there is
	 * no transaction.
	 */

	public void persist(Object o)
	{
		if (o != null)
		{
			synchronized (o)
			{
				Class type = o.getClass();
				if (isEntity(type))
				{
					try
					{
						if (!contains(o))
						{
							EntityType entity = getEntity(type);
							prePersist(o);
							persist(entity, o);
							// We only properly populate collections when we read an object in at the moment
							entity.putCache(entity.getPrimaryKeyValue(o), null);
							postPersist(o);
						}
						else 
						{
							logger.fine("persist() called on existing entity; converting to merge()");
							merge(o);
						}
					}
		
					catch (Exception e)
					{
						e.printStackTrace();
						logger.throwing("ServiceEntityManager", "persist", e);
						throw new PersistenceException(e);
					}
				}
				else throw new IllegalArgumentException("This object is not an Entity!");
			}
		}
		else throw new IllegalArgumentException("Cannot persist a null object!");
	}

	/**
	 * Determines whether an entity store for the provided EntityType already exists.
	 * @param entity
	 * @return true if a store already exists, or if the EntityManager is memory-only.
	 */
	protected boolean isExistingStore(EntityType entity)
	{
		if (database != null)
		{
			DatabaseTable table = database.getTable(entity.getTableName());
			if (table != null) return true;
			else return false;
		}
		else return true;
	}

	/**
	 * Updates an existing entity store to match the provided EntityType, if necessary.
	 * The update process should be as safe as possible.  No fields should be deleted 
	 * or renamed.  The only changes made to table structure should be the addition of 
	 * new fields.  It should be possible to update an entity class SAFELY without losing 
	 * or corrupting data.  Removing tables or fields is something the user will need to do 
	 * on their own.
	 * 
	 * The default implementation of this method is a no-op.
	 * @param entity
	 * @return true if any changes were made.
	 */
	protected boolean updateEntityStore(EntityType entity) 
	{ 
		try
		{
			boolean result = false;
			List<DatabaseTable> entitytables = defineEntityStore(entity);
			for (DatabaseTable entitytable : entitytables)
			{
				DatabaseTable dbtable = database.getTable(entitytable.getName()); 
				if (dbtable != null)
				{
					for (DatabaseField entityfield : entitytable.getFields())
					{
						DatabaseField dbfield = dbtable.getField(entityfield.getName());
						if (dbfield == null) 
						{
							dbtable.alterTable(entityfield);
							result = true;
						}
					}
				}
			}
			return result;
		}
		
		catch (Exception e)
		{
			e.printStackTrace();
			throw new PersistenceException("Could not update store for entity " + entity.getClassName(), e);
		}
	} 

	protected List<DatabaseTable> defineEntityStore(EntityType entity) 
	throws IntrospectionException, ClassNotFoundException
	{
		if (database != null)
		{
			List<DatabaseTable> tables = new LinkedList<DatabaseTable>();
			String tableName = entity.getTableName();
			EntityField primaryKey = entity.getPrimaryKey();
			DatabaseTable table = database.createTable(tableName);
			DatabaseField dbKey = database.createField(primaryKey.getName(), primaryKey.manager.getSQLType(primaryKey));
			dbKey.setPrimaryKey(true);
			dbKey.setGeneratedKey(primaryKey.isGeneratedKey());
			table.addField(dbKey);
			for (EntityField field : entity.getFields())
			{
				if (field.isRelationshipType())
				{
					if (field.isOneToOne())
					{
						if (field.isOwner())
						{
							EntityType foreignType = field.getForeignType();
							EntityField foreignKey = foreignType.getPrimaryKey();
							String fieldName = field.getName() + "_" + foreignKey.getName();
							int sqlType = foreignKey.manager.getSQLType(foreignKey);
							DatabaseField dbfield = database.createField(fieldName, sqlType);
							dbfield.setIndexed(true);
							table.addField(dbfield);
							table.addReference(fieldName, foreignType.getTableName(), foreignKey.getName());
						}
					}
					else if (field.isManyToOne())
					{
						if (field.isOwner())
						{
							EntityType foreignType = field.getForeignType();
							EntityField foreignKey = foreignType.getPrimaryKey();
							int sqlType = foreignKey.manager.getSQLType(foreignKey);
							String fieldName = field.getName() + "_" + foreignKey.getName();
							DatabaseField dbfield = database.createField(fieldName, sqlType);
							dbfield.setIndexed(true);
							table.addField(dbfield);
							table.addReference(fieldName, foreignType.getTableName(), foreignKey.getName());
						}
						else
						{
							StringBuilder msg = new StringBuilder();
							msg.append("Field ");
							msg.append(field.getName());
							msg.append(" is not the owner of a ManyToOne relationship!\n");
							msg.append("Perhaps you need to add or change a mappedBy annotation parameter?");
							throw new PersistenceException(msg.toString());
						}
					}
					else if (field.isOneToMany()) 
					{
						EntityType foreignType = field.getForeignType();
						EntityField foreignField = findRelationshipField(field, foreignType);
						if (foreignField == null)
						{
							DatabaseTable relTable = defineOneToManyStore(entity, field);
							if (relTable != null) tables.add(relTable);
						}
					}
					else if (field.isManyToMany()) 
					{
						DatabaseTable relTable = defineManyToManyStore(entity, field);
						if (relTable != null) tables.add(relTable);
					}
				}
				else
				{
					DatabaseField dbfield = createFieldDefinition(field);
					if (dbfield != null) table.addField(dbfield);
				}
			}
			tables.add(table);
			return tables;			
		}
		else 
		{
			logger.warning("ServiceEntityManager.defineEntityStore() failed because database was null!");
			return null;
		}
	}

	DatabaseField createFieldDefinition(EntityField field)
	throws IntrospectionException, ClassNotFoundException
	{
		String definition = field.getDefinition();
		DatabaseField result = database.createField(field.getName(), field.manager.getSQLType(field));
		if (definition != null) result.setDefinition(definition);
		if (field.isPrimaryKey())
		{
			result.setPrimaryKey(true);
			if (field.isGeneratedKey())
			{
				result.setGeneratedKey(true);
			}
		}
		if (field.getLength() != null)
		{
			result.setLength(field.getLength());
		}
		return result;
	}

	DatabaseTable defineManyToManyStore(EntityType entity, EntityField field)
	throws ClassNotFoundException, IntrospectionException
	{
		if (field.isOwner())
		{
			DatabaseTable result = null;
			EntityType typeB = field.getForeignType();
			EntityField fieldB = findRelationshipField(field, typeB);
			
			if (fieldB != null) 
			{
				result = defineBiManyToManyTable(entity, field, typeB, fieldB); 
			}
			else
			{
				result = defineUniManyToManyTable(entity, field, typeB);
			}
			return result;
		}
		else return null;
	}

	protected DatabaseTable defineRelationshipTable(EntityType typeA, EntityField fieldA, EntityType typeB)
	{
		String tableName = null;
		if (fieldA.isOwner())
		{
			tableName = createRelationshipStoreName(typeA, typeB);
		}
		else
		{
			tableName = createRelationshipStoreName(typeB, typeA);
		}
		
		DatabaseTable result = database.createTable(tableName);
		DatabaseField keyfield = database.createField("id", java.sql.Types.BIGINT);
		keyfield.setPrimaryKey(true);
		keyfield.setGeneratedKey(true);
		result.addField(keyfield);
		result.setRelationshipTable(true);
		return result;
	}

	protected DatabaseTable defineUniManyToManyTable(EntityType typeA, EntityField fieldA, EntityType typeB) 
	throws ClassNotFoundException, IntrospectionException
	{
		DatabaseTable result = defineRelationshipTable(typeA, fieldA, typeB);
		
		EntityField keyA = typeA.getPrimaryKey();
		EntityField keyB = typeB.getPrimaryKey();

		// Create the first key field
		String ownerKeyName = typeA.getTableName() + "_" + keyA.getName();
		DatabaseField ownerKeyField = database.createField(ownerKeyName, keyA.manager.getSQLType(keyA));
		ownerKeyField.setIndexed(true);
		result.addField(ownerKeyField);
		result.addReference(ownerKeyField.getName(), typeA.getTableName(), keyA.getName());

		// Create the second key field
		String ownedKeyName = fieldA.getName() + "_" + keyB.getName();
		DatabaseField ownedKeyField = database.createField(ownedKeyName, keyB.manager.getSQLType(keyB));
		ownedKeyField.setIndexed(true);
		result.addField(ownedKeyField);
		result.addReference(ownedKeyField.getName(),typeB.getTableName(), keyB.getName());
		
		return result;
	}

	protected DatabaseTable defineBiManyToManyTable(EntityType typeA, EntityField fieldA, EntityType typeB, EntityField fieldB) 
	throws ClassNotFoundException, IntrospectionException 
	{
		EntityField keyA = typeA.getPrimaryKey();
		EntityField keyB = typeB.getPrimaryKey();
		
		EntityField ownerKey = null;
		EntityType ownerType = null;
		EntityField ownerField = null;
		EntityField ownedKey = null;
		EntityType ownedType = null;
		EntityField ownedField = null;
		
		DatabaseTable result = defineRelationshipTable(typeA, fieldA, typeB);
		if (fieldA.isOwner())
		{
			ownerKey = keyA;
			ownerType = typeA;
			ownerField = fieldA;
			ownedKey = keyB;
			ownedType = typeB;
			ownedField = fieldB;
		}
		else
		{
			ownerKey = keyB;
			ownerType = typeB;
			ownerField = fieldB;
			ownedKey = keyA;
			ownedType = typeA;
			ownedField = fieldA;			
		}
		
		// This looks counter intuitive, but it's right per the specification.
		String ownerKeyName = ownedField.getName() + "_" + ownerKey.getName();
		String ownedKeyName = ownerField.getName() + "_" + ownedKey.getName();

		// Create the first key field
		DatabaseField ownerKeyField = database.createField(ownedKeyName, ownerKey.manager.getSQLType(ownerKey));
		ownerKeyField.setIndexed(true);
		result.addField(ownerKeyField);
		result.addReference(ownerKeyName, ownerType.getTableName(), ownerKey.getName());

		// Create the second key field
		DatabaseField ownedKeyField = database.createField(ownerKeyName, ownedKey.manager.getSQLType(ownedKey));
		ownedKeyField.setIndexed(true);
		result.addField(ownedKeyField);
		result.addReference(ownedKeyName, ownedType.getTableName(), ownedKey.getName());
		return result;
	}

	/**
	 * This defines the UniDirectional One to Many relationship store.
	 * @param typeA
	 * @param fieldA
	 * @return
	 * @throws ClassNotFoundException 
	 * @throws IntrospectionException 
	 */
	protected DatabaseTable defineOneToManyStore(EntityType typeA, EntityField fieldA)
	throws IntrospectionException, ClassNotFoundException
	{
		EntityType typeB = fieldA.getForeignType();
		EntityField keyA = typeA.getPrimaryKey();
		EntityField keyB = typeB.getPrimaryKey();
		
		DatabaseTable result = defineRelationshipTable(typeA, fieldA, typeB);
		String ownerKeyName = typeA.getTableName() + "_" + keyA.getName(); 
		DatabaseField ownerKeyField = database.createField(ownerKeyName, keyA.manager.getSQLType(keyA));
		ownerKeyField.setIndexed(true);
		result.addField(ownerKeyField);
		result.addReference(ownerKeyName, typeA.getTableName(), fieldA.getName());

		String ownedKeyName = fieldA.getName() + "_" + keyB.getName(); 
		DatabaseField ownedKeyField = database.createField(ownedKeyName, keyB.manager.getSQLType(keyB));
		ownedKeyField.setIndexed(true);
		result.addField(ownedKeyField);
		result.addReference(ownedKeyName, typeB.getTableName(), keyB.getName());
		return result;
	}

	protected EntityField findRelationshipField(EntityField fieldA, EntityType typeB)
	{
		String fieldAName = null;
		if (fieldA.isOwner())
		{
			fieldAName = fieldA.getName(); 
			if (fieldAName != null)
			{
				List<EntityField> fields = typeB.getFields();
				for (EntityField field : fields)
				{
					String fieldBName = field.getMappedBy();
					if (fieldAName.equals(fieldBName)) return field;
				}
			}
		}
		else 
		{
			fieldAName = fieldA.getMappedBy();
			if (fieldAName != null)
			{
				List<EntityField> fields = typeB.getFields();
				for (EntityField field : fields)
				{
					String fieldBName = field.getName();
					if (fieldAName.equals(fieldBName)) return field;
				}
			}			
		}
		return null;
	}

	
	protected synchronized String getTableNameForEntity(Class c)
	{
		Annotation[] annotations = c.getAnnotations();
		for (int ii = 0; ii < annotations.length; ii++)
		{
			String value = annotations[ii].toString();
			if (value.startsWith("@Table"))
			{
				// Return the specified table name
				return ((Entity) annotations[ii]).name(); 
			}
		}
		return c.getSimpleName().toUpperCase();
	}		

	/**
	 * Creates the entity's storage medium (database table, usually) if it does not 
	 * already exist.  It's necessary to do this separately to avoid reference loops 
	 * when entities reference each other.
	 * @param entity
	 * @throws PersistenceException
	 */
	protected void createEntityStore(EntityType entity)
	{
		try
		{
			List<DatabaseTable> tables = defineEntityStore(entity);
			for (DatabaseTable table : tables)
			{
				String name = table.getName();
				DatabaseTable currentTable = database.getTable(name);
				if (currentTable == null) database.createTable(table); 
				else database.updateTable(table);
			}
		}
		
		catch (Exception e)
		{
			throw new PersistenceException("Could not create entity store for " + entity.getClassName(), e);
		}
	}
	
	/**
	 * The initialize() method is not part of the persistence API.  
	 * It provides clients with the opportunity to present a complete list of entity 
	 * classes at runtime, rather than using bytecode injection.  It is not strictly 
	 * necessary to call this method, as new classes will be added dynamically as 
	 * they are found.  However, using it may make your life easier.    
	 *   
	 * @throws IntrospectionException 
	 * @throws ClassNotFoundException 
	 */
	public void initialize(List<Class> entities) 
	throws ClassNotFoundException, IntrospectionException
	{
		for (Class entityClass : entities)
		{
			getEntity(entityClass);
		}
	}
	
	/**
	 * The initialize method is called once when an EntityType is first encountered
	 * during the current application lifetime.  
	 * 
	 * Implementations should use this method to make any necessary schema changes 
	 * to support storing this type (eg, creating database tables, etc).
	 * 
	 * @param type
	 * @throws IntrospectionException 
	 * @throws ClassNotFoundException 
	 */
	protected void initialize(EntityType type) 
	throws ClassNotFoundException, IntrospectionException
	{
		if (isExistingStore(type)) updateEntityStore(type); 
		else createEntityStore(type);
	}
	
	protected void persist(EntityType type, Object o)
	{
		if (o != null)
		{
			synchronized (o)
			{
				DatabaseConnection con = null;
				
				try
				{
					con = database.getConnection();
					con.setAutoCommit(false);
					persist(con, type, o);
					con.commit();			
				}
				
				catch (Exception e)
				{
					try { con.rollback(); } catch (Exception ee) { e.printStackTrace(); }
					throw new PersistenceException(e);
				}	
				
				finally
				{
					try { database.putConnection(con); } catch (Exception ee) { }
				}
			}
		}
	}

	protected Object merge(EntityType type, Object o)
	{
		if (o != null)
		{
			synchronized (o)
			{
				DatabaseConnection con = null;
				
				try
				{
					ServiceEntityTransaction transaction = getServiceEntityTransaction();
					if (transaction != null) con = transaction.getConnection();
 					con = database.getConnection();
					con.setAutoCommit(false);
					Object result = merge(con, type, o);
					con.commit();
					return result;
				}
				
				catch (Exception e)
				{
					e.printStackTrace();
					try { con.rollback(); } catch (Exception ee) { ee.printStackTrace(); }
					throw new PersistenceException(e);
				}	
				
				finally
				{
					try { con.setAutoCommit(true); database.putConnection(con); } catch (Exception ee) { }
				}
			}
		}
		else return o;
	}
		
	/**
	 * Refresh the state of the instance from the database,
	 * overwriting changes made to the entity, if any.
	 * @throws IllegalArgumentException if not an entity
	 *                 or entity is not managed
	 * @throws TransactionRequiredException if invoked on a
	 * container-managed entity manager of type
	 * PersistenceContextType.TRANSACTION and there is
	 * no transaction.
	 * @throws EntityNotFoundException if the entity no longer
	 *                 exists in the database
	 */

	public void refresh(Object o)
	{
		if (o != null)
		{
			synchronized (o)
			{
				Class type = o.getClass();
				if (isEntity(type))
				{
					try
					{
						EntityType entity = getEntity(type);
						preUpdate(o);
						refresh(entity, o);
						postUpdate(o);
					}
					
					catch (Exception e)
					{
						logger.throwing("ServiceEntityManager", "refresh", e);
						throw new PersistenceException(e);
					}
				}
				else throw new IllegalArgumentException("This object is not an Entity!");
			}
		}
		else throw new IllegalArgumentException("Cannot refresh a null object!");
	}

	/**
	 * Remove the entity instance.
	 * @throws IllegalArgumentException if not an entity
	 *                 or if a detached entity
	 * @throws TransactionRequiredException if invoked on a
	 * container-managed entity manager of type
	 * PersistenceContextType.TRANSACTION and there is
	 * no transaction.
	 */

	public void remove(Object o)
	{
		if (o != null)
		{
			synchronized (o)
			{
				Class type = o.getClass();
				if (isEntity(type))
				{
					try
					{
						EntityType entity = getEntity(type);
						preRemove(o);
						entity.putCache(entity.getPrimaryKeyValue(o), null);
						remove(entity, o);
						postRemove(o);
					}
					
					catch (Exception e)
					{
						logger.throwing("ServiceEntityManager", "remove", e);
						throw new PersistenceException(e);
					}
				}
				else throw new IllegalArgumentException("This object is not an Entity!");
			}
		}
		else throw new IllegalArgumentException("Cannot remove a null object!");
	}
	
	public synchronized EntityType getEntity(Class entityClass)
	throws IntrospectionException, ClassNotFoundException
	{
		EntityType type = entityTypeMap.get(entityClass);
		if (type == null) 
		{
			type = createEntityInstance(entityClass);
			initialize(type);
		}
		return type;
	}

	void addCache(EntityType entityType)
	{
		try
		{
			if (entityTypeTable == null) createCacheTable();
			Class entityClass = entityType.getEntityType();
			entityTypeMap.put(entityClass, entityType);
			logger.fine("Added " + entityClass + " to entityTypeMap");
			entityNameMap.put(entityClass.getSimpleName(), entityClass);
			logger.fine("Added " + entityClass.getSimpleName() + " to entityNameMap");
			Map<String,Object> values = new HashMap<String,Object>();
			values.put("CLASS_NAME", entityClass.getName());
			entityTypeTable.persist(values);
		}
		
		catch (Exception e)
		{
			// This will fail sometimes with a duplicate constraint violation
			// I choose to tolerate that
		}
	}

	public void createCacheTable()
	{
		entityTypeTable = database.getTable(SERVICE_ENTITY_MANAGER_ENTITY_TYPE_TABLE);
		if (entityTypeTable == null)
		{
			entityTypeTable = database.createTable(SERVICE_ENTITY_MANAGER_ENTITY_TYPE_TABLE);
			DatabaseField idField = database.createField("ID", java.sql.Types.BIGINT);
			idField.setPrimaryKey(true);
			idField.setGeneratedKey(true);
			entityTypeTable.addField(idField);
			DatabaseField classNameField = database.createField("CLASS_NAME", java.sql.Types.VARCHAR);
			classNameField.setUnique(true);
			classNameField.setNullable(false);
			entityTypeTable.addField(classNameField);
			database.createTable(entityTypeTable);
		}
	}
	
	public LinkedList<Class> loadCache()
	{
		LinkedList<Class> classes = new LinkedList<Class>();
		DatabaseQuery q = null;
		
		try
		{
			if (entityTypeTable == null) createCacheTable();
			q = database.createQuery();
			q.addTable(entityTypeTable);
			q.executeQuery();
			if (!q.isEmpty())
			{
				do
				{
					String className = q.getString("CLASS_NAME");
					if (className != null) 
					{
						try
						{
							classes.add(Class.forName(className));
						}
						
						catch (Exception e)
						{
							logger.warning("Could not load " + className);
						}
					}
				}
				while (q.next());
				initialize(classes);
			}
			return classes;
		}
		
		catch (Exception e)
		{
			// We expect not to load this on an empty DB
			e.printStackTrace();
			return classes;
		}
		
		finally
		{
			try { if (q != null) q.close(); } catch (Exception e) { } 
		}
	}
	
	protected String getShortName(String longname)
	{
		int start = longname.lastIndexOf('.');
		int end = longname.length(); 
		if (start == -1) return longname;
		else return longname.substring(start, end);
	}
	
	protected EntityType getEntityByName(String name)
	{
		try
		{
			Class entityClass = entityNameMap.get(name);
			if (entityClass != null) return getEntity(entityClass);
			else 
			{
				for (String key : entityNameMap.keySet())
				{
					entityClass = entityNameMap.get(key);
				}
				throw new PersistenceException(name + " is not a known entity!");
			}
		}
		
		catch (Exception e)
		{
			throw new PersistenceException(name + " is not a known entity!");
		}
	}
	
	/**
	 * Get an instance, whose state may be lazily fetched.
	 * If the requested instance does not exist in the database,
	 * the EntityNotFoundException is thrown when the instance
	 * state is first accessed. (The persistence provider runtime is
	 * permitted to throw the EntityNotFoundException when
	 * getReference is called.)
	 * The application should not expect that the instance state will
	 * be available upon detachment, unless it was accessed by the
	 * application while the entity manager was open.
	 * @return the found entity instance
	 * @throws EntityNotFoundException if the entity state
	 *                     cannot be accessed
	 */

	public <T> T getReference(Class<T> type, Object key) 
	{
		T result = find(type, key);
		if (result == null) throw new EntityNotFoundException();
		else return result;
	}

	/**
     * Set the lock mode for an entity object contained
     * in the persistence context.
     * @throws PersistenceException if an unsupported lock call
     *            is made
     * @throws IllegalArgumentException if the instance is not
     *            an entity or is a detached entity
     * @throws TransactionRequiredException if there is no
     *            transaction
     */

	public void lock(Object arg0, LockModeType arg1) 
	{
		// TODO Auto-generated method stub		
	}

	/**
	 * Get the flush mode that applies to all objects contained
	 * in the persistence context.
	 * @return flushMode
	 */

	public FlushModeType getFlushMode() 
	{
		return mode;
	}

	/**
	 * Set the flush mode that applies to all objects contained
	 * in the persistence context.
	 * @param mode
	 */

	public void setFlushMode(FlushModeType mode) 
	{
		this.mode = mode;
	}
	
	/**
	 * Synchronize the persistence context to the
	 * underlying database.
	 * @throws TransactionRequiredException if there is
	 *                 no transaction
	 * @throws PersistenceException if the flush fails
	 */

	public void flush()
	{
		
	}

	/**
	 * Return the resource-level transaction object.
	 * The EntityTransaction instance may be used serially to
	 * begin and commit multiple transactions.
	 * @return EntityTransaction instance
	 * @throws IllegalStateException if invoked on a JTA
	 *       EntityManager.
	 */

	ServiceEntityTransaction getServiceEntityTransaction(long threadid) 
	{
		ServiceEntityTransaction transaction = null;		
		WeakReference<ServiceEntityTransaction> ref = transactions.get(threadid);
		if (ref != null) transaction = ref.get();
		return transaction;
	}

	ServiceEntityTransaction getServiceEntityTransaction() 
	{
		return getServiceEntityTransaction(Thread.currentThread().getId());
	}
	
	ServiceEntityTransaction createServiceEntityTransaction()
	{
		return createServiceEntityTransaction(Thread.currentThread().getId());
	}
	
	ServiceEntityTransaction createServiceEntityTransaction(long threadid)
	{
		ServiceEntityTransaction transaction = new ServiceEntityTransaction(database);
		transactions.put(threadid, new WeakReference<ServiceEntityTransaction>(transaction));
		return transaction;
	}
	
	/**
	 * Internal method to provide a DatabaseConnection in a transaction-safe manner.
	 * 
	 * @return
	 */
	DatabaseConnection getDatabaseConnection()
	{
		ServiceEntityTransaction transaction = getServiceEntityTransaction();
		if (transaction != null)
		{
			if (transaction.isActive())
			{
				return transaction.getConnection();
			}
			else return database.getConnection();
		}
		else return database.getConnection();
	}
	
	/**
	 * Return the resource-level transaction object.
	 * The EntityTransaction instance may be used serially to
	 * begin and commit multiple transactions.
	 * @return EntityTransaction instance
	 * @throws IllegalStateException if invoked on a JTA
	 *       EntityManager.
	 */

	public EntityTransaction getTransaction() 
	{
		Long threadid = Thread.currentThread().getId();
		ServiceEntityTransaction transaction = getServiceEntityTransaction(threadid);
		if (transaction == null) transaction = createServiceEntityTransaction(threadid);
		return transaction;
	}

	/**
	 * Indicate to the EntityManager that a JTA transaction is
	 * active. This method should be called on a JTA application
	 * managed EntityManager that was created outside the scope
	 * of the active transaction to associate it with the current
	 * JTA transaction.
	 * @throws TransactionRequiredException if there is
	 * no transaction.
	 */

	public void joinTransaction() 
	{
		Long threadid = Thread.currentThread().getId();
		WeakReference<ServiceEntityTransaction> ref = transactions.get(threadid);
		if (ref != null)
		{
			ServiceEntityTransaction transaction = ref.get(); 
			if (transaction != null) transactions.put(threadid, ref);
			else 
			{
				// Clean up the WeakReference, just because we can
				transactions.remove(threadid);
				throw new TransactionRequiredException("The thread id " + threadid + " is not associated with a transaction (but was previously).");
			}
		}
		else throw new TransactionRequiredException("The thread id " + threadid + " is not associated with a transaction.");
	}
	
	/**
	 * Clear the persistence context, causing all managed
	 * entities to become detached. Changes made to entities that
	 * have not been flushed to the database will not be
	 * persisted.
	 */

	public void clear() 
	{
		logger.warning("Clearing entities...");
		if (entityNameMap != null) entityNameMap.clear();
		if (entityTypeMap != null) 
		{
			for (EntityType type : entityTypeMap.values())
			{
				String tableName = type.getTableName();
				database.dropTable(tableName);
			}
			entityTypeMap.clear();
		}
		if (entityTypeTable != null) entityTypeTable.clear();
	}

	public static boolean isFieldPrimaryKey(Field field)
	{
		Annotation[] annotations = field.getAnnotations();
		for (int ii = 0; ii < annotations.length; ii++)
		{
			String value = annotations[ii].toString();
			if (value.startsWith("@javax.persistence.Id")) return true;
		}
		return false;
	}

	public static boolean isFieldCompositeKey(Field field)
	{
		Annotation[] annotations = field.getAnnotations();
		for (int ii = 0; ii < annotations.length; ii++)
		{
			String value = annotations[ii].toString();
			if (value.startsWith("@javax.persistence.EmbeddedId")) return true;
		}
		return false;
	}

	/** 
	 * Creates a Stack containing the class heirarchy of the provided Class.
	 * The order of the returned stack is top-down, eg, Object-Superclass-Subclass.
	 */
	public static final Stack<Class> createClassStack(Class c)
	{
		Stack<Class> result = new Stack<Class>();
		Class current = c;
		do { result.push(current); } 
		while ((current = current.getSuperclass()) != null);
		return result;
	}

	/**
	 * Provides the Set<Object> of EntityListener classes.
	 * The object being modified will always be a member of the resulting Set. 
	 * @param value
	 * @return
	 * @throws IntrospectionException
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	final List<Object> getEntityListeners(Class c) 
	throws IntrospectionException, InstantiationException, IllegalAccessException
	{
		ArrayList<Object> result = new ArrayList<Object>();
		Stack<Class> targets = createClassStack(c);
		for (Class target : targets)
		{
			Annotation[] annotations = target.getDeclaredAnnotations();
			for (Annotation annotation : annotations)
			{
				String name = annotation.toString();
				if (name.startsWith("@javax.persistence.EntityListener"))
				{
					EntityListeners els = (EntityListeners) annotation;
					Class[] value = els.value();
					for (Class lc : value)
					{
						Object listener = listeners.get(lc);
						if (listener == null)
						{
							listener = lc.newInstance();
							listeners.put(lc, listener);
						}
						if (!result.contains(listener)) result.add(listener);
					}
				}
			}
		}
		return result;
	}
	
	static final Set<MethodDescriptor> findMethodsWithAnnotation(String name, Object o) 
	throws IntrospectionException
	{
		TreeSet<MethodDescriptor> result = new TreeSet<MethodDescriptor>();

		Class target = o.getClass();
		BeanInfo info = Introspector.getBeanInfo(target);
		MethodDescriptor[] methods = info.getMethodDescriptors();
		for (MethodDescriptor method : methods)
		{
			Annotation[] annotations = target.getDeclaredAnnotations();
			for (Annotation annotation : annotations)
			{
				String value = annotation.toString();
				if (name.equalsIgnoreCase(value))
				{
					result.add(method);
				}
			}
		}
		return result;
	}

	/** 
	 * Notifies the appropriate listeners of the provided event.
	 * @param o
	 * @param event
	 * @throws IntrospectionException 
	 * @throws InvocationTargetException 
	 * @throws IllegalAccessException 
	 * @throws IllegalArgumentException 
	 * @throws InstantiationException 
	 */
	protected void notifyListeners(Object o, String event) 
	throws IntrospectionException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, InstantiationException
	{
		Class c = o.getClass();
		// Notify the object itself
		Set<MethodDescriptor> descriptors = findMethodsWithAnnotation(event, c);
		for (MethodDescriptor descriptor : descriptors)
		{
			Method method = descriptor.getMethod();
			method.invoke(o);
		}
		
		// Notify EntityListeners
		List<Object> listeners = getEntityListeners(c);
		for (Object listener : listeners)
		{
			descriptors = findMethodsWithAnnotation(event, listener);
			for (MethodDescriptor descriptor : descriptors)
			{
				Method method = descriptor.getMethod();
				method.invoke(listener, o);
			}
		}				
	}
	
	/**
	 * Notifies appropriate listeners that this object has been loaded.
	 * @param o
	 * @throws IntrospectionException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws InstantiationException 
	 */
	protected void postLoad(Object o) 
	throws IntrospectionException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, InstantiationException
	{
		notifyListeners(o, "@javax.persistence.PostLoad");
	}

	/**
	 * Notifies appropriate listeners that this object is about to be updated.
	 * @param o
	 * @throws IntrospectionException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws InstantiationException 
	 */
	protected void preUpdate(Object o) 
	throws IntrospectionException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, InstantiationException
	{
		notifyListeners(o, "@javax.persistence.PreUpdate");
	}

	/**
	 * Notifies appropriate listeners that this object has been updated.
	 * @param o
	 * @throws IntrospectionException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws InstantiationException 
	 */
	protected void postUpdate(Object o) 
	throws IntrospectionException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, InstantiationException
	{
		notifyListeners(o, "@javax.persistence.PostUpdate");
	}

	/**
	 * Notifies appropriate listeners that this object is about to be removed.
	 * @param o
	 * @throws IntrospectionException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws InstantiationException 
	 */
	protected void preRemove(Object o) 
	throws IntrospectionException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, InstantiationException
	{
		notifyListeners(o, "@javax.persistence.PreRemove");
	}

	/**
	 * Notifies appropriate listeners that this object has been removed.
	 * @param o
	 * @throws IntrospectionException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws InstantiationException 
	 */
	protected void postRemove(Object o) 
	throws IntrospectionException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, InstantiationException
	{
		notifyListeners(o, "@javax.persistence.PostRemove");
	}

	/**
	 * Notifies appropriate listeners that this object has been persisted.
	 * @param o
	 * @throws IntrospectionException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws InstantiationException 
	 */
	protected void postPersist(Object o) 
	throws IntrospectionException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, InstantiationException
	{
		notifyListeners(o, "@javax.persistence.PostPersist");
	}

	/**
	 * Notifies appropriate listeners that this object is about to be persisted.
	 * @param o
	 * @throws IntrospectionException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws InstantiationException 
	 */
	protected void prePersist(Object o) 
	throws IntrospectionException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, InstantiationException
	{
		notifyListeners(o, "@javax.persistence.PrePersist");
	}
	
	protected boolean isManyToOne(PropertyDescriptor info) 
	{
		Annotation[] annotations = info.getPropertyType().getAnnotations();
		Annotation annotation = findAnnotation(annotations, "@javax.persistence.ManyToOne");
		if (annotation != null) return true;
		else return false;
	}

	protected boolean isOneToOne(PropertyDescriptor info) 
	{
		Annotation[] annotations = info.getPropertyType().getAnnotations();
		Annotation annotation = findAnnotation(annotations, "@javax.persistence.OneToOne");
		if (annotation != null) return true;
		else return false;
	}

	protected boolean isOneToMany(PropertyDescriptor info) 
	{
		Annotation[] annotations = info.getPropertyType().getAnnotations();
		Annotation annotation = findAnnotation(annotations, "@javax.persistence.OneToMany");
		if (annotation != null) return true;
		else return false;
	}

	protected boolean isManyToMany(PropertyDescriptor info) 
	{
		Annotation[] annotations = info.getPropertyType().getAnnotations();
		Annotation annotation = findAnnotation(annotations, "@javax.persistence.ManyToMany");
		if (annotation != null) return true;
		else return false;
	}

	/** 
	 * Determines whether this class is a persistent Entity class.
	 * @param c
	 * @return
	 */
	public static boolean isEntity(Class c)
	{
		Annotation[] annotations = c.getAnnotations();
		Annotation annotation = findAnnotation(annotations, "@javax.persistence.Entity");
		if (annotation != null) return true;
		else return false;
	}

	/**
	 * Returns the annotation matching the provided String, or null 
	 * if there is no such annotation.
	 * @param annotations
	 * @param name
	 * @return
	 */
	public static Annotation findAnnotation(Annotation[] annotations, String name)
	{
		for (Annotation annotation : annotations)
		{
			String value = annotation.toString();
			if (value.startsWith(name))
			{
				return annotation;
			}
		}	
		return null;
	}
	
	protected Object getPrimaryKeyValue(Object o) 
	throws IllegalAccessException, IntrospectionException, InvocationTargetException, ClassNotFoundException
	{
		Class c = o.getClass();
		EntityType entity = getEntity(c);
		return entity.getPrimaryKeyValue(o);
	}	
	
	/**
	 * Determines whether the provided object has been changed since it was last seen.
	 * The object is checked against the cached version, and returns true if there is no cached version.
	 * @return
	 * @throws ClassNotFoundException 
	 * @throws InvocationTargetException 
	 * @throws IntrospectionException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public boolean isModified(Object current) 
	throws IllegalAccessException, IntrospectionException, InvocationTargetException, ClassNotFoundException, InstantiationException
	{
		if (current != null) 
		{
			Object currentKey = getPrimaryKeyValue(current);
			if (currentKey == null) return true;
			else
			{
				Class clazz = current.getClass();
				EntityType type = getEntity(clazz);
				Object cached = type.getCache(currentKey);
				if (cached == null) return true;
				else if (current == cached) return false;
				else
				{
					for (EntityField field : type.getFields())
					{
						Object currentValue = field.getFieldValue(current);
						Object cachedValue = field.getFieldValue(cached);
						if (currentValue != cachedValue)
						{
							if (currentValue != null)
							{
								// This happens when storing and retrieving date/time values 
								if ((currentValue instanceof java.util.Date) && (cachedValue instanceof java.sql.Timestamp))
								{
									java.util.Date currentDate = (java.util.Date) currentValue;
									java.sql.Timestamp cachedTimestamp = (java.sql.Timestamp) cachedValue;
									long currentLong = currentDate.getTime();
									long cachedLong = cachedTimestamp.getTime();
									if (currentLong == cachedLong) return true;
									// Account for lack of precision in DB stored value
									else if ((Math.abs(cachedLong - currentLong)) > 10000) return true;
								}
								// Normal case
								else if (!currentValue.equals(cachedValue)) return true;
							}
							else return true;
						}
					}
					return false;
				}
			}
		}
		else return false;
	}
	
	protected void persist(DatabaseConnection con, EntityType type, Object o)
	throws IllegalAccessException, IntrospectionException, InvocationTargetException, ClassNotFoundException, InstantiationException
	{
		if (o == null) throw new PersistenceException("Cannot persist a null object!");
		synchronized (o)
		{
			boolean key = false;
			Map<String,Object> values = new TreeMap<String,Object>();
			EntityField primaryKey = type.getPrimaryKey();
			Object primaryKeyValue = type.getPrimaryKeyValue(o);
			if (!primaryKey.isGeneratedKey()) key = true;
			if (primaryKeyValue != null) key = true; 
			if (key) values.put(primaryKey.getName(), primaryKeyValue);
			for (EntityField field : type.getFields())
			{
				// Relationship types need special handling
				if (field.isRelationshipType())
				{
					if (field.isOneToOne())
					{
						Object foreignValue = field.getFieldValue(o);
						if (foreignValue != null)
						{
							EntityType foreignType = field.getForeignType();
							logger.fine("Persisting foreign entity " + foreignValue + "<" + foreignType + ">");
							if (!contains(foreignValue)) persist(con, foreignType, foreignValue);
							else if (isModified(foreignValue)) merge(con, foreignType, foreignValue);
							Object value = foreignType.getPrimaryKeyValue(foreignValue);
							logger.fine("Setting field " + field.getStoreName() + " to value " + value);
							values.put(field.getStoreName(), value);
						}
						else values.put(field.getStoreName(), null);
					}
					else if (field.isManyToOne())
					{
						Object foreignValue = field.getFieldValue(o);
						if (foreignValue != null)
						{
							EntityType foreignType = field.getForeignType();
							logger.fine("Persisting foreign entity " + foreignValue + "<" + foreignType + ">");
							if (!contains(foreignValue)) persist(con, foreignType, foreignValue);
							else if (isModified(foreignValue)) merge(con, foreignType, foreignValue);	
							// Update the key we keep
							Object value = foreignType.getPrimaryKeyValue(foreignValue);
							values.put(field.getStoreName(), value);
						}
						else values.put(field.getStoreName(), null);
					}
				}
				else 
				{
					String name = field.getName();
					Object value = field.getFieldValue(o);
					logger.finest(name + ":" + value);
					values.put(name, value);
				}
			}
	
			String tableName = type.getTableName();
			DatabaseTable table = database.getTable(tableName);
			if (table == null) 
			{
				List<DatabaseTable> tables = defineEntityStore(type);
				for (DatabaseTable t : tables)
				{
					database.createTable(t);
				}
				table = database.getTable(tableName);
			}
			Object generatedKey = table.persist(con, values);
			if (generatedKey == null) 
			{
				throw new PersistenceException("No key was generated for " + type.getTableName());
			}
			type.setPrimaryKeyValue(o, generatedKey);
			
			// Now that we have a key, deal with complex relationships
			for (EntityField field : type.getFields())
			{
				if (field.isManyToMany())
				{
					Collection foreignValues = (Collection) field.getFieldValue(o);
					if (foreignValues != null)
					{
						// Clear the join table
						clearJoinTable(type, field, type.getPrimaryKeyValue(o));
	
						EntityType foreignType = field.getForeignType();
						EntityField foreignKey = field.getForeignKey();
						for (Object foreignValue : foreignValues)
						{
							// persist or merge each value in the collection, as needed 
							if (!contains(foreignValue)) persist(foreignType, foreignValue);
							else merge(foreignType, foreignValue);
							// Update the join table
							insertManyToManyRelationship(con, type, field, o, foreignType, foreignKey, foreignValue);
						}
					}
				}
				else if (field.isOneToMany())
				{
					Collection foreignValues = (Collection) field.getFieldValue(o);
					if (foreignValues != null)
					{
						EntityType foreignType = field.getForeignType();
						EntityField foreignField = findRelationshipField(field, foreignType);
						if (foreignField != null)
						{
							// Bidirectional relationships use a key value
							for (Object foreignValue : foreignValues)
							{
								foreignField.setFieldValue(foreignValue, o);
								// persist or merge each value in the collection, as needed 
								if (!contains(foreignValue)) persist(con, foreignType, foreignValue);
								else if (isModified(foreignValue)) merge(con, foreignType, foreignValue);
							}							
						}
						else
						{
							// Unidirectional relationships use a join table
							clearJoinTable(type, field, type.getPrimaryKeyValue(o));
	
							// Store each object, then write an entry in the join table
							for (Object foreignValue : foreignValues)
							{
								// persist or merge each value in the collection, as needed 
								if (!contains(foreignValue)) persist(con, foreignType, foreignValue);
								else if (isModified(foreignValue)) merge(con, foreignType, foreignValue);
								
								insertOneToManyRelationship(con, type, field, o, foreignType, foreignValue);
							}
						}
					}
				}
			}
		}
	}

	private void insertOneToManyRelationship(DatabaseConnection con, EntityType type,
			EntityField field, Object o, EntityType foreignType, Object foreignValue)
	throws IllegalAccessException, IntrospectionException, InvocationTargetException, ClassNotFoundException
	{
		EntityField primaryKey = type.getPrimaryKey();
		EntityField foreignKey = foreignType.getPrimaryKey();
		String tableName = null;
		String keyAName = null;
		String keyBName = null;
		if (field.isOwner())
		{
			tableName = type.getTableName() + "_" + foreignType.getTableName();
			keyAName = type.getTableName() + "_" + primaryKey.getName();
			keyBName = type.getTableName() + "_" + primaryKey.getName();
		}
		else
		{
			EntityField foreignField = findRelationshipField(field, foreignType);
			tableName = foreignType.getTableName() + "_" + type.getTableName(); 
			keyAName = foreignType.getTableName() + "_" + foreignKey.getName();
			keyBName = foreignField.getName() + "_" + foreignKey.getName();			
		}
		DatabaseTable table = database.getTable(tableName);
		Map<String,Object> values = new TreeMap<String,Object>();
		values.put(keyAName, type.getPrimaryKeyValue(o));
		values.put(keyBName, foreignType.getPrimaryKeyValue(foreignValue));	
		table.persist(con, values);
	}

	/**
	 * Given a type, a field, and an Entity, this method will clear the join table 
	 * for those values (meaning, any objects linked to that field will no longer be linked).
	 * @param type
	 * @param field
	 * @param o
	 * @throws ClassNotFoundException
	 * @throws IntrospectionException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 */
	protected void clearJoinTable(EntityType type, EntityField field, Object o) 
	throws ClassNotFoundException, IntrospectionException, IllegalAccessException, InvocationTargetException
	{
		if (o == null) 
		{
			StringBuilder msg = new StringBuilder();
			msg.append("Field ");
			msg.append(field.getName());
			msg.append(" in type ");
			msg.append(type.getClassName());
			msg.append(" tried to clear a relationship table for a null object!");
			throw new PersistenceException(msg.toString());
		}
		
		String relationshipField = null;
		String tableName = field.getRelationshipTableName();
		DatabaseTable table = database.getTable(tableName);
		if (table == null)
		{
			StringBuilder msg = new StringBuilder();
			msg.append("Field ");
			msg.append(field.getName());
			msg.append(" in type ");
			msg.append(type.getClassName());
			msg.append(" tried referencing a table that has not been defined!");
			throw new PersistenceException(msg.toString());			
		}
		if (field.isManyToMany())
		{
			EntityField primaryKey = type.getPrimaryKey();
			EntityType foreignType = field.getForeignType();
			EntityField foreignKey = field.getForeignKey();
			EntityField foreignField = findRelationshipField(field, foreignType);

			// Unidirectional
			if (foreignField == null)
			{				
				relationshipField = type.getTableName() + "_" + foreignKey.getStoreName();
			}
			else // Bidirectional
			{
				relationshipField = foreignField.getName() + "_" + primaryKey.getName();				
			}
		}
		else if (field.isOneToMany())
		{
			// We known this is Unidirectional since it uses a join table
			EntityField primaryKey = type.getPrimaryKey();
			if (field.isOwner())
			{
				relationshipField = type.getTableName() + "_" + primaryKey.getStoreName();
			}
			else
			{
				StringBuilder msg = new StringBuilder();
				msg.append("Field ");
				msg.append(field.getName());
				msg.append(" in type ");
				msg.append(type.getClassName());
				msg.append(" tried to clear a unidirectional OneToMany relationship table!");
				throw new PersistenceException(msg.toString());				
			}			
		}
		
		Map<String,Object> values = new HashMap<String,Object>();
		values.put(relationshipField, o);
		table.clear(values);
		logger.exiting("DatabaseEntityManager", "clearJoinTable");		
	}
	
	protected <T> T merge(DatabaseConnection con, EntityType type, T o) 
	throws IllegalAccessException, IntrospectionException, InvocationTargetException, ClassNotFoundException, InstantiationException 
	{
		synchronized (o)
		{
			Map<String,Object> values = new TreeMap<String,Object>();
			String tableName = type.getTableName();
			DatabaseTable table = database.getTable(tableName);
			Object key = type.getPrimaryKeyValue(o);
			for (EntityField field : type.getFields())
			{
				if (field.isRelationshipType())
				{
					if (field.isOneToOne())
					{
						Object foreignValue = field.getFieldValue(o);
						if (foreignValue != null)
						{
							EntityType foreignType = field.getForeignType();
							logger.finest("Persisting foreign entity " + foreignValue + "<" + foreignType + ">");
							if (!contains(foreignValue)) persist(con, foreignType, foreignValue); 
							else if (isModified(foreignValue)) merge(con, foreignType, foreignValue); 
							Object value = foreignType.getPrimaryKeyValue(foreignValue);
							logger.finest("Setting field " + field.getName() + " to value " + value);
							values.put(field.getStoreName(), value);
						}
						else values.put(field.getStoreName(), null);
					}
					else if (field.isManyToOne())
					{
						// This is set like a normal field in this case
						EntityType foreignType = field.getForeignType();
						EntityField foreignKey = foreignType.getPrimaryKey();
						Object value = field.getFieldValue(o);
						if (value != null)
						{
							logger.finest("Setting field " + field.getName() + " to value " + value);
							values.put(field.getStoreName(), foreignKey.getFieldValue(value));
						}
						else values.put(field.getStoreName(), null);
					}
					/* This can cause an infinite loop
					else if (field.isOneToMany())
					{
						EntityType foreignType = field.getForeignType();
						EntityField foreignField = findRelationshipField(field, foreignType);
						Collection foreignValues = (Collection) field.getFieldValue(o);
						if (foreignValues != null)
						{
							for (Object foreignValue : foreignValues)
							{
								foreignField.setFieldValue(foreignValue, o);
								if (!contains(foreignValue)) persist(foreignType, foreignValue);
								else merge(foreignType, foreignValue);
							}
						}
					}
					*/
					else if (field.isManyToMany())
					{
						Collection foreignValues = (Collection) field.getFieldValue(o);
						if (foreignValues != null)
						{
							clearJoinTable(type, field, type.getPrimaryKeyValue(o));
							EntityType foreignType = field.getForeignType();
							EntityField foreignKey = field.getForeignKey();
							for (Object foreignValue : foreignValues)
							{
								if (foreignValue != null)
								{
									// persist or merge each value in the collection, as needed 
									if (!contains(foreignValue)) persist(con, foreignType, foreignValue);
									else if (isModified(foreignValue)) merge(con, foreignType, foreignValue);
									// Update the join table
									insertManyToManyRelationship(con, type, field, o, foreignType, foreignKey, foreignValue);
								}
							}
						}
					}
				}
				else values.put(field.getStoreName(), field.getFieldValue(o));
			}	
			table.merge(con, key, values);
			return o;
		}
	}

	protected void refresh(EntityType type, Object o)
	throws IllegalAccessException, IntrospectionException, InvocationTargetException, ClassNotFoundException, IllegalArgumentException, InstantiationException
	{
		DatabaseConnection con = null;
		
		try
		{
			con = database.getConnection();
			refresh(con, type, o);
		}
		
		finally
		{
			try { if (con != null) database.putConnection(con); } catch (Exception e) { } 
		}
	}
	
	protected void refresh(DatabaseConnection con, EntityType type, Object o) 
	throws IllegalAccessException, IntrospectionException, InvocationTargetException, ClassNotFoundException, IllegalArgumentException, InstantiationException 
	{
		synchronized (o)
		{
			Object key = type.getPrimaryKeyValue(o);
			String tableName = type.getTableName();
			DatabaseTable table = database.getTable(tableName);
			Map<String,Object> values = table.find(key);
			for (EntityField field : type.getFields())
			{
				if (field.isRelationshipType())
				{
					if ((field.isOneToOne()) || (field.isManyToOne()))
					{
						Object value = values.get(field.getStoreName());
						if (value != null)
						{
							EntityType foreignType = field.getForeignType();
							Object foreignValue = find(con, foreignType, value);						
							logger.finest("Setting field " + field.getName() + " to value " + value);
							field.setFieldValue(o, foreignValue);
						}
						else field.setFieldValue(o, null);
					}
					else if (field.isOneToMany())
					{
						EntityType foreignType = field.getForeignType();
						DatabaseRelationshipCollection foreignValues = selectRelationship(con, type, field, o, foreignType);
						field.setFieldValue(o, foreignValues);
					}
					else if (field.isManyToMany())
					{
						EntityType foreignType = field.getForeignType();
						DatabaseRelationshipCollection foreignValues = selectRelationship(con, type, field, o, foreignType);
						field.setFieldValue(o, foreignValues);
					}
				}
				else
				{
					Object value = values.get(field.getName());
					field.setFieldValue(o, value);				
				}
			}
		}
	}
	
	protected ServiceEntityTransaction createTransaction()
	{
		return new ServiceEntityTransaction(database);
	}

	protected void remove(EntityType type, Object o) 
	throws IllegalAccessException, IntrospectionException, InvocationTargetException 
	{
		synchronized (o)
		{
			DatabaseConnection connection = getDatabaseConnection();
			String tableName = type.getTableName();
			DatabaseTable table = database.getTable(tableName);			
			Object key = type.getPrimaryKeyValue(o);
			table.delete(key);
			
		}
	}
	
	/**
	 * Retrieves a list of keys.
	 * @param con
	 * @param type
	 * @param field
	 * @param o
	 * @param foreignType
	 * @return
	 * @throws InvocationTargetException 
	 * @throws IntrospectionException 
	 * @throws IllegalAccessException 
	 * @throws ClassNotFoundException 
	 */
	List<Object> selectRelationshipKeys(EntityType type, EntityField field, Object o)
	throws IllegalAccessException, IntrospectionException, InvocationTargetException, ClassNotFoundException
	{	
		if (field.isManyToMany())
		{
			return selectManyToManyRelationshipKeys(type, field, o);			
		}
		else if (field.isOneToMany())
		{
			return selectOneToManyRelationshipKeys(type, field, o);
		}
		else throw new PersistenceException("Unknown relationship type!");
	}
	
	DatabaseQuery createManyToManyRelationshipKeyQuery(EntityType type, EntityField field, Object o)
	{
		EntityType foreignType = field.getForeignType();
		EntityField foreignKey = foreignType.getPrimaryKey();
		EntityField foreignField = findRelationshipField(field, foreignType);
		String tableName = field.getRelationshipTableName();
		DatabaseQuery query = database.createQuery();
		DatabaseTable table = database.getTable(tableName);
		if (foreignField != null)
		{
			// This algorithm is the same regardless of ownership for bidirectional queries
			// We might need to change it for unidirectional
			String keyName = field.getStoreName() + "_" + foreignKey.getName();
			query.addTable(table);
			query.addField(table.getField(keyName));
		}
		else
		{
			// If we are here at all from a unidirectional relationship, we are the owner
			if (field.isOwner())
			{
				String keyName = field.getStoreName() + "_" + foreignKey.getName();
				query.addTable(table);
				query.addField(table.getField(keyName));
			}
			else throw new PersistenceException("Cannot query from non-owning unidirectional relationship field!");
		}
		return query;
	}
	
	List<Object> selectManyToManyRelationshipKeys(EntityType type, EntityField field, Object o)
	{
		DatabaseQuery query = null;
		
		try
		{
			List<Object> results = new LinkedList<Object>();
			EntityField key = type.getPrimaryKey();
			EntityType foreignType = field.getForeignType();
			EntityField foreignKey = foreignType.getPrimaryKey();
			EntityField foreignField = findRelationshipField(field, foreignType);
			String foreignKeyFieldName = field.getName() + "_" + foreignKey.getName();
			query = createManyToManyRelationshipKeyQuery(type, field, o);
			if (field.isOwner())
			{
				String keyName;
				if (foreignField != null) keyName = foreignField.getName() + "_" + key.getName();
				else keyName = type.getTableName() + "_" + key.getName();
				Object keyValue = type.getPrimaryKeyValue(o);
				query.addParam(keyName, QueryParameterType.EQUAL, keyValue.toString());
			}
			else
			{
				String keyName;
				if (foreignField != null) keyName = foreignField.getName() + "_" + key.getName();
				else keyName = type.getTableName() + "_" + key.getName();
				Object keyValue = type.getPrimaryKeyValue(o);
				query.addParam(keyName, QueryParameterType.EQUAL, keyValue.toString());
			}
			
			query.executeQuery();
			if (!query.isEmpty())
			{
				do
				{
					Object fkv = query.getObject(foreignKeyFieldName);
					results.add(fkv);
				}
				while (query.next());
			}
			return results;
		}
		
		catch (Exception e) 
		{
			throw new PersistenceException(e);
		}
		
		finally
		{
			try { if (query != null) query.close(); } catch (Exception e) { } 
		}
	}

	List<Object> selectOneToManyRelationshipKeys(EntityType type, EntityField field, Object o)
	{
		DatabaseQuery query = null;
		
		try
		{
			List<Object> results = new LinkedList<Object>();
			EntityField key = type.getPrimaryKey();
			EntityType foreignType = field.getForeignType();
			EntityField foreignKey = foreignType.getPrimaryKey();
			EntityField foreignField = findRelationshipField(field, foreignType);
			String tableName = field.getRelationshipTableName();
			query = database.createQuery();
			query.addTable(tableName);
			String keyName = null;
			if (foreignField != null)
			{
				// Bidirectional
				if (!field.isOwner())
				{
					keyName = foreignField.getName() + "_" + key.getName();
					query.addParam(keyName, QueryParameterType.EQUAL, type.getPrimaryKeyValue(o).toString());
					query.executeQuery();
					if (!query.isEmpty())
					{
						do
						{
							Object fkv = query.getObject(foreignKey.getStoreName());
							results.add(fkv);
						}
						while (query.next());
					}
					return results;
				}
				else
				{
					// We should never reach this spot
					// This case is handled elsewhere
					throw new PersistenceException("Tried to selectOneToManyRelationshipKeys() from relationship owner!");
				}
			}
			else
			{
				// Unidirectional
				keyName = type.getTableName() + "_" + key.getName();
				String foreignKeyFieldName = field.getName() + "_" + foreignKey.getName();
				query.addParam(keyName, QueryParameterType.EQUAL, type.getPrimaryKeyValue(o).toString());					
				query.executeQuery();
				if (!query.isEmpty())
				{
					do
					{
						Object fkv = query.getObject(foreignKeyFieldName);
						results.add(fkv);
					}
					while (query.next());
				}
				return results;
			}
		}
		
		catch (Exception e) 
		{
			if (query != null) logger.severe(query.toString());
			throw new PersistenceException(e);
		}
	
		finally
		{
			try { if (query != null) query.close(); } catch (Exception e) { } 
		}
	}

	/**
	 * Creates a RelationshipCollection that can handle entity relationships.
	 * @param con
	 * @param type
	 * @param field
	 * @param o
	 * @param foreignType
	 * @return
	 */
	protected DatabaseRelationshipCollection selectRelationship(DatabaseConnection con, EntityType type, EntityField field, Object o, EntityType foreignType)
	{
		return new DatabaseRelationshipCollection(this, con, type, field, o, foreignType);
	}

	public Query createNamedQuery(String name)
	{
		throw new PersistenceException("This method is not yet supported!");
	}

	public Query createNativeQuery(String sqlString)
	{
		throw new PersistenceException("This method is not yet supported!");
	}

	public Query createNativeQuery(String sqlString, Class resultClass)
	{
		throw new PersistenceException("This method is not yet supported!");
	}

	public Query createNativeQuery(String sqlString, String resultSetMapping)
	{
		throw new PersistenceException("This method is not yet supported!");
	}
	
	protected void insertManyToManyRelationship(DatabaseConnection con, EntityType typeA, EntityField fieldA, Object o, EntityType typeB, EntityField keyB, Object foreignValue) 
	throws IllegalAccessException, IntrospectionException, InvocationTargetException, ClassNotFoundException
	{
		logger.entering("DatabaseEntityManager", "insertManyToManyRelationship");
		String tableName = null; 
		String keyAName = null;
		String keyBName = null;
		EntityField keyA = typeA.getPrimaryKey();
		EntityField fieldB = findRelationshipField(fieldA, typeB);
		Map<String,Object> values = new TreeMap<String,Object>();
		if (fieldB == null)
		{
			// Unidirectional, fieldA is owner
			tableName = fieldA.getJoinTable();
			keyAName = typeA.getTableName() + "_" + keyA.getName();
			keyBName = fieldA.getName() + "_" + keyB.getName();
			values.put(keyAName, typeA.getPrimaryKeyValue(o));
			values.put(keyBName, typeB.getPrimaryKeyValue(foreignValue));
		}
		else if (fieldA.isOwner())
		{
			// Bidirectional, fieldA is owner
			tableName = fieldA.getJoinTable();
			keyAName = fieldB.getName() + "_" + keyA.getName();
			keyBName = fieldA.getName() + "_" + keyB.getName();
			values.put(keyAName, typeA.getPrimaryKeyValue(o));
			values.put(keyBName, typeB.getPrimaryKeyValue(foreignValue));
		}
		else 
		{
			// Bidirectional, fieldB is owner
			tableName = fieldB.getJoinTable();
			keyAName = fieldA.getMappedBy() + "_" + keyB.getName();
			keyBName = fieldB.getName() + "_" + keyA.getName();
			values.put(keyAName, typeA.getPrimaryKeyValue(o));
			values.put(keyBName, typeB.getPrimaryKeyValue(foreignValue));
		}
		DatabaseTable table = database.getTable(tableName);
		table.persist(con, values);
		logger.exiting("DatabaseEntityManager", "insertManyToManyRelationship");				
	}

	public int getSQLType(EntityField entityField) 
	throws IntrospectionException, ClassNotFoundException
	{
		// If field is an entity, extract the key first
		if (isEntity(entityField.type))
		{
			EntityType foreignType = getEntity(entityField.type);
			EntityField foreignKey = foreignType.getPrimaryKey();
			return getSQLType(foreignKey);
		}
		else 
		{
			Integer result = supportedTypeMap.get(entityField.getFieldType());
			if (result != null) return result;
			else
			{
				Class c = entityField.getFieldType();
				if (c.isEnum()) return java.sql.Types.INTEGER;
				else
				{
					StringBuilder message = new StringBuilder();
					message.append(entityField.type.getCanonicalName());
					message.append(" is not a supported type!");
					entityField.logger.severe(message.toString());
					throw new PersistenceException(message.toString());
				}
			}
		}
	}

	/**
	 * Determines whether a particular entity field is supported.
	 * @return
	 */
	public boolean isSupportedFieldType(Class fieldType)
	{
		Integer sqltype = supportedTypeMap.get(fieldType);
		if (sqltype != null) return true;
		else if (fieldType.isEnum()) return true;
		else if (fieldType.isAssignableFrom(java.util.Collection.class)) return true;
		else if (fieldType.isAssignableFrom(java.util.Set.class)) return true;
		else if (isEntity(fieldType)) return true;
		else return false; 		
	}	
}
