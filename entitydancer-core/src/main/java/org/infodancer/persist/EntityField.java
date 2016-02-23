package org.infodancer.persist;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.EmbeddedId;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.MapKey;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.PersistenceException;
import javax.persistence.Transient;

public class EntityField
{
	Logger logger = Logger.getLogger(EntityField.class.getName());
	/** Holds the PropertyDescriptor of the field this DatabaseField maps **/
	protected PropertyDescriptor info;
	/** The java type of the field **/
	protected Class type;
	/** The name of the field in the class **/
	protected String name;
	protected String definition;
	protected Method getMethod;
	protected Method setMethod;
	protected boolean indexed;
	protected boolean primaryKey;
	protected boolean compositeKey;
	protected boolean alternateKey;
	protected boolean generatedKey;
	protected boolean persistent;
	protected String generator;
	protected Integer length;
	protected Integer precision;
	protected List<Annotation> annotations;
	protected GenerationType generationType;
	protected ServiceEntityManager manager;
	protected EntityType entityType;
	protected EntityTypeReference foreignType;
	protected EntityField foreignKey;

	OneToOne onetoone;
	ManyToOne manytoone;
	OneToMany onetomany;
	Embeddable embeddable;
	
	// JoinTable fields
	JoinTable jointable;
	JoinColumn joincolumn;
	JoinColumn[] joincolumns;
	JoinColumn[] inverseJoinColumns;
	
	// ManyToMany fields
	ManyToMany manytomany;
	String mappedBy;
	CascadeType[] cascade;
	
	@SuppressWarnings("unchecked")
	public EntityField(ServiceEntityManager manager, EntityType entityType, PropertyDescriptor info) 
	throws IntrospectionException, ClassNotFoundException
	{
		this.info = info;
		this.manager = manager;
		this.name = info.getDisplayName();
		this.type = info.getPropertyType();
		this.setMethod = info.getWriteMethod();
		this.getMethod = info.getReadMethod();
		this.entityType = entityType;
		this.persistent = true;
		this.onetoone = null;
		this.manytoone = null;
		this.onetomany = null;
		this.manytomany = null;
		this.primaryKey = false;
		this.compositeKey = false;
		this.alternateKey = false;
		this.generatedKey = false;
		this.annotations = new ArrayList<Annotation>();
		initialize(getMethod.getAnnotations());
	}

	protected void initialize(Annotation[] annotations) throws IntrospectionException, ClassNotFoundException
	{	
		for (Annotation annotation : annotations)
		{
			if (annotation instanceof Id)
			{
				processIdAnnotation((Id) annotation);
			}
			else if (annotation instanceof Embeddable)
			{
				processEmbeddableAnnotation((Embeddable) annotation);
			}
			else if (annotation instanceof EmbeddedId)
			{
				processEmbeddedIdAnnotation((EmbeddedId) annotation);
			}
			else if (annotation instanceof GeneratedValue)
			{
				processGeneratedValueAnnotation((GeneratedValue) annotation);
			}
			else if (annotation instanceof Transient)
			{
				processTransientAnnotation((Transient) annotation);
			}
			else if (annotation instanceof OneToOne)
			{
				processOneToOneAnnotation((OneToOne) annotation);
			}
			else if (annotation instanceof ManyToMany)
			{
				processManyToManyAnnotation((ManyToMany) annotation);
			}
			else if (annotation instanceof ManyToOne)
			{
				processManyToOneAnnotation((ManyToOne) annotation);
			}
			else if (annotation instanceof OneToMany)
			{				
				processOneToManyAnnotation((OneToMany) annotation);
			}
			else if (annotation instanceof Column)
			{
				processColumnAnnotation((Column) annotation);
			}
			else if (annotation instanceof JoinTable)
			{
				processJoinTableAnnotation((JoinTable) annotation);
			}
			else if (annotation instanceof JoinColumn)
			{
				processJoinColumnAnnotation((JoinColumn) annotation);
			}
			else if (annotation instanceof MapKey)
			{
				processMapKeyAnnotation((MapKey) annotation);
			}
			this.annotations.add(annotation);
		}
	}
	
	private void processMapKeyAnnotation(MapKey annotation) 
	{
		
	}

	private void processTransientAnnotation(Transient annotation) 
	{
		persistent = false;
	}

	private void processGeneratedValueAnnotation(GeneratedValue annotation) 
	{
		generatedKey = true;
	}

	private void processEmbeddedIdAnnotation(EmbeddedId annotation) 
	{
		compositeKey = true;
	}

	private void processIdAnnotation(Id annotation) 
	{
		primaryKey = true;
	}

	private void processJoinTableAnnotation(JoinTable annotation) 
	{
		jointable = annotation;
		jointable.joinColumns();
	}

	JoinColumn[] getJoinColumns()
	{
		if (joincolumns != null) return joincolumns;
		else return null;
	}
	
	String getJoinTable() throws ClassNotFoundException, IntrospectionException
	{
		String result = null; 
		if (jointable != null)
		{
			result = jointable.name();
		}
		if (result == null)
		{
			// use the default
			if (isOwner())
			{
				result = entityType.getTableName() + "_" + foreignType.getEntity().getTableName();
			}
		}
		return result;
	}
	
	private void processEmbeddableAnnotation(Embeddable annotation) 
	{
		embeddable = annotation;
	}

	private void processJoinColumnAnnotation(JoinColumn annotation) 
	{
		joincolumn = annotation;
	}

	private void processOneToOneAnnotation(OneToOne annotation) 
	throws IntrospectionException, ClassNotFoundException 
	{
		onetoone = annotation;
		foreignType = new EntityTypeReference(manager, type);
	}

	private void processManyToOneAnnotation(ManyToOne annotation) 
	throws IntrospectionException, ClassNotFoundException
	{
		manytoone = annotation;
		foreignType = getForeignTypeManyToOne(annotation);
	}

	/** 
	 * Extract the type of the target entity for a ManyToMany relationship.
	 * @param annotation
	 * @return
	 * @throws IntrospectionException
	 * @throws ClassNotFoundException 
	 */
	private EntityTypeReference getForeignTypeManyToOne(ManyToOne annotation) throws IntrospectionException, ClassNotFoundException 
	{
		// Did the developer specify a target entity?
		Class targetEntityClass = annotation.targetEntity();
		if (void.class.equals(targetEntityClass))
		{
			Type genericReturnType = getMethod.getGenericReturnType();
			String className = genericReturnType.toString();
			targetEntityClass = Class.forName(className.substring(6));
		}
		return new EntityTypeReference(manager, targetEntityClass);
	}

	private void processOneToManyAnnotation(OneToMany annotation) 
	throws IntrospectionException, ClassNotFoundException 
	{
		onetomany = annotation;
		mappedBy = annotation.mappedBy();
		foreignType = getForeignTypeOneToMany(annotation);
	}

	private void processColumnAnnotation(Column annotation)
	{
		Column column = (Column) annotation;
		length = Utility.parseAnnotationInteger(column.length(), 256);
		precision = Utility.parseAnnotationInteger(column.precision(), 0);
		name = Utility.parseAnnotationString(column.name(), name);
		definition = Utility.parseAnnotationString(column.columnDefinition(), definition);		
	}
	
	private void processManyToManyAnnotation(ManyToMany annotation) 
	throws IntrospectionException, ClassNotFoundException
	{
		manytomany = annotation;
		foreignType = getForeignTypeManyToMany(annotation);
		mappedBy = annotation.mappedBy();
		if (mappedBy == null) mappedBy = name;
		cascade = annotation.cascade();
	}

	/** 
	 * Extract the type of the target entity for a ManyToMany relationship.
	 * @param annotation
	 * @return
	 * @throws IntrospectionException
	 * @throws ClassNotFoundException 
	 */
	private EntityTypeReference getForeignTypeOneToMany(OneToMany annotation) throws IntrospectionException, ClassNotFoundException 
	{
		// Did the developer specify a target entity?
		Class targetEntityClass = annotation.targetEntity();
		if (void.class.equals(targetEntityClass))
		{
			Type genericReturnType = getMethod.getGenericReturnType();
			String targetEntityClassName = parseTargetEntityClassName(genericReturnType.toString());
			if (targetEntityClassName != null)
			{
				targetEntityClass = Class.forName(targetEntityClassName);
			}
			else
			{
				StringBuilder msg = new StringBuilder();
				msg.append(entityType.className);
				msg.append('.');
				msg.append(name);
				msg.append(" does not specify a type for the Collection!\n");
				msg.append("Please specify a type via annotations or templates.");
				throw new PersistenceException(msg.toString());
			}
		}
		return new EntityTypeReference(manager, targetEntityClass);
	}

	/** 
	 * Extract the type of the target entity for a ManyToMany relationship.
	 * @param annotation
	 * @return
	 * @throws IntrospectionException
	 * @throws ClassNotFoundException 
	 */
	private EntityTypeReference getForeignTypeManyToMany(ManyToMany annotation) throws IntrospectionException, ClassNotFoundException 
	{
		// Did the developer specify a target entity?
		Class targetEntityClass = annotation.targetEntity();
		if (void.class.equals(targetEntityClass))
		{
			Type genericReturnType = getMethod.getGenericReturnType();
			String targetEntityClassName = parseTargetEntityClassName(genericReturnType.toString());
			targetEntityClass = Class.forName(targetEntityClassName);
		}
		return new EntityTypeReference(manager, targetEntityClass);
	}

	private String parseTargetEntityClassName(String name) 
	{
		logger.entering("EntityField", "parseTargetEntityClassName");
		if (name != null)
		{
			int begin = name.indexOf('<');
			int end = name.indexOf('>');
			if ((begin != -1) && (end != -1) && (begin < end))
			{
				String cname = name.substring(0, begin);
				if (isCollectionTypeSupported(cname))
				{
					String result = name.substring(begin + 1, end);
					logger.exiting("EntityField", "parseTargetEntityClassName");
					return result;
				}
				else throw new PersistenceException(cname + " is not a supported Collection type!");
			}
			else throw new PersistenceException("Invalid type specification " + name);
		}
		logger.exiting("EntityField", "parseTargetEntityClassName");
		return null;
	}
	
	public static boolean isCollectionTypeSupported(String cname)
	{
		if ("java.util.Collection".equals(cname)) return true;
		if ("java.util.Set".equals(cname)) return true;
		return false;
	}
	
	/**
	 * Provides the EntityType which owns this field.
	 * @return
	 */
	public EntityType getEntityType()
	{
		return entityType;
	}
	
	/**
	 * Provides the name of the table implementing this field's external relationship, if any.
	 * @return NULL if there is no such table.
	 */
	public String getRelationshipTableName()
	{
		try
		{
			if (isRelationshipType())
			{
				EntityType foreignEntity = foreignType.getEntity();
				if (isOneToOne())
				{
					return foreignEntity.getTableName();
				}
				else if (isOneToMany())
				{	
					EntityField foreignField = manager.findRelationshipField(this, foreignEntity);
					if (isOwner())
					{
						if (foreignField != null)
						{
							// Bidirectional
							return entityType.getTableName();
						}
						else
						{
							// Unidirectional
							return entityType.getTableName() + "_" + foreignEntity.getTableName(); 
						}
					}
					else
					{
						if (foreignField != null)
						{
							// Bidirectional
							return foreignEntity.getTableName();
						}
						else
						{
							// Unidirectional
							return foreignEntity.getTableName() + "_" + entityType.getTableName();
						}
					}
				}
				else if (isManyToMany())
				{
					if (isOwner())
					{
						return entityType.getTableName()  + "_" + foreignEntity.getTableName();
					}
					else
					{
						return foreignEntity.getTableName() + "_" + entityType.getTableName();
					}
				}
			}
			return null;
		}
		
		catch (Exception e)
		{
			throw new PersistenceException(e);
		}
	}
	
	/**
	 * Provides the foreign type for fields involving one or more external entities.
	 * @return
	 */
	public EntityType getForeignType()
	{
		try
		{
			if (foreignType != null) return foreignType.getEntity();
			else
			{
				StringBuilder msg = new StringBuilder();
				msg.append("Field " + name + " in type " + entityType.getClassName() + " references an unknown foreign type.");
				throw new PersistenceException(msg.toString());
			}
		}
		
		catch (Exception e)
		{
			e.printStackTrace();
			throw new PersistenceException(e);
		}
	}

	/**
	 * Provides the foreign key field for fields involving one or more external entities.
	 * @return
	 * @throws IntrospectionException 
	 * @throws ClassNotFoundException 
	 */
	public EntityField getForeignKey() throws ClassNotFoundException, IntrospectionException
	{
		if (foreignKey == null)
		{
			if (isOneToMany())
			{
				// In this relationship type, we provide the field in the foreign type
				EntityField keyField = entityType.getPrimaryKey();
				String keyName = keyField.getName();
				String tableName = entityType.getTableName();
				String foreignKeyName = tableName + "_" + keyName;
				EntityType foreignEntityType = foreignType.getEntity();
				return foreignEntityType.getFieldByStoreName(foreignKeyName);
			}
			else
			{
				// Provide the actual foreign key
				EntityType foreignType = getForeignType();
				foreignKey = foreignType.getPrimaryKey();
			}
		}
		return foreignKey;
	}
	
	public Integer getLength()
	{
		return length;
	}
	
	public Integer getPrecision()
	{
		return precision;
	}
	
	public Object getFieldValue(Object o) 
	{
		if (o != null)
		{
			if (getMethod != null)
			{
				try
				{
					return getMethod.invoke(o);
				}
				
				catch (Exception e)
				{
					e.printStackTrace();
					throw new PersistenceException("Exception invoking get method: " + getMethod);
				}
			}
			else
			{
				StringBuilder msg = new StringBuilder();
				msg.append("Property ");
				msg.append(name);
				msg.append(" of type ");
				msg.append(entityType.getClassName());
				msg.append(" does not have a get method!");
				throw new PersistenceException(msg.toString());			
			}
		}
		else
		{
			StringBuilder msg = new StringBuilder();
			msg.append("Property ");
			msg.append(name);
			msg.append(" of type ");
			msg.append(entityType.getClassName());
			msg.append(" cannot be retrieved from an null object!");
			throw new PersistenceException(msg.toString());						
		}
	}

	public String toString()
	{
		StringBuilder result = new StringBuilder();
		result.append(name);
		result.append('<');
		result.append(type);
		result.append('>');
		return result.toString();
	}
	
	public void setFieldValue(Object o, Object value) 
	{
		if (setMethod != null)
		{
			try
			{
				if (type.isEnum())
				{
					if (value == null)
					{
						throw new IllegalArgumentException("Enum types can not be null!");
					}
					else if (value instanceof Integer)
					{
						Integer i = (Integer) value;
						Object[] pvs = type.getEnumConstants();
						for (Object p : pvs)
						{
							int pid = ((Enum) p).ordinal();
							if (pid == i) 
							{
								setMethod.invoke(o, p);
							}
						}
					}
					else if (value instanceof String)
					{
						String s = (String) value;
						Object[] pvs = type.getEnumConstants();
						for (Object p : pvs)
						{
							String sid = ((Enum) p).name();
							if (sid.equalsIgnoreCase(s))
							{
								setMethod.invoke(o, p);
							}
						}						
						throw new IllegalArgumentException("Enum types can only be set as Integers, Strings, or their own type");
					}
					else if (type.isAssignableFrom(value.getClass()))
					{
						setMethod.invoke(o, value);	
					}
					else throw new IllegalArgumentException("Enum types can only be set as Integers, Strings, or their own type");
				}
				else
				{
					setMethod.invoke(o, value);
				}
			}
			
			catch (IllegalArgumentException e)
			{
				StringBuilder msg = new StringBuilder();
				msg.append("Property ");
				msg.append(name);
				msg.append(" of type ");
				msg.append(entityType.getClassName());
				msg.append(" expects type ");
				msg.append(type.getName());

				if (value != null)
				{
					msg.append(" but was set with type ");
					msg.append(value.getClass().getName());
				}
				else 
				{
					msg.append(" but was set with null!");
				}

				throw new PersistenceException(msg.toString());
			}
			
			catch (IllegalAccessException e)
			{
				StringBuilder msg = new StringBuilder();
				msg.append("Property ");
				msg.append(name);
				msg.append(" of type ");
				msg.append(entityType.getClassName());
				msg.append("does not allow access (IllegalAccessException)!");
				throw new PersistenceException(msg.toString());
			}
			
			catch (InvocationTargetException e)
			{
				e.printStackTrace();
				throw new PersistenceException(e);
			}
		}
		else
		{
			StringBuilder msg = new StringBuilder();
			msg.append("Property ");
			msg.append(name);
			msg.append(" of type ");
			msg.append(entityType.getClassName());
			msg.append(" does not have a set method!");
			throw new PersistenceException(msg.toString());
		}
	}

	public Class getFieldType()
	{
		return type;
	}
	
	/**
	 * Provides the Java name of the field.
	 * @return
	 */
	public String getName() 
	{
		return name;
	}

	/**
	 * Provides the datastore name of the field.
	 * @return
	 */
	public String getStoreName() 
	{
		try
		{
			if (isManyToOne())
			{
				EntityType foreignEntity = foreignType.getEntity();
				EntityField foreignKey = foreignEntity.getPrimaryKey();
				return name + "_" + foreignKey.getName();
			}
			else if (isOneToOne())
			{
				if (isOwner())
				{
					EntityType foreignEntity = foreignType.getEntity();
					EntityField foreignKey = foreignEntity.getPrimaryKey();
					return name + "_" + foreignKey.getName();
				}
				else return null;
			}
			else return name;
		}
		
		catch (Exception e)
		{
			e.printStackTrace();
			throw new PersistenceException("Error retrieving store name for field " + name);
		}
	}

	public String getGenerator()
	{
		return generator;
	}

	public GenerationType getGenerationType()
	{
		return generationType;
	}

	public boolean isGeneratedKey()
	{
		return generatedKey;
	}
	
	public boolean isPrimaryKey() 
	{
		return primaryKey;
	}

	public boolean isCompositeKey() 
	{
		return compositeKey;
	}

	public boolean isAlternateKey() 
	{
		return alternateKey;
	}

	public boolean isIndexed() 
	{
		return indexed;
	}

	public List<Annotation> getAnnotations() 
	{
		return annotations;
	}

	public boolean isEmbeddable()
	{
		if (embeddable != null) return true;
		else return false;
	}
	
	public boolean isOneToOne() 
	{
		if (onetoone != null) return true;
		else if (onetomany != null) return false;
		else if (manytomany != null) return false;
		else if (manytoone != null) return false;
		else if (manager.isEntity(type)) return true;
		else return false;
	}

	public boolean isOneToMany() 
	{
		if (onetomany != null) return true;
		else return false;
	}

	public boolean isManyToMany() 
	{
		if (manytomany != null) return true;
		else return false;
	}

	public boolean isManyToOne() 
	{
		if (manytoone != null) return true;
		else return false;
	}

	public boolean isRelationshipType() 
	{
		if (isOneToOne()) return true;
		else if (isOneToMany()) return true;
		else if (isManyToMany()) return true;
		else if (isManyToOne()) return true;
		else return false;
	}

	public String getDefinition() 
	{
		return definition;
	}
	
	/**
	 * Returns the data for this field as an array of bytes (primarily for Cassandra support).
	 * @return
	 * @throws InvocationTargetException 
	 * @throws IntrospectionException 
	 * @throws IllegalAccessException 
	 */
	public byte[] getByteValue(Object o) throws IllegalAccessException, IntrospectionException, InvocationTargetException
	{
		if (type.equals(java.lang.String.class))
		{
			String value = (String) getFieldValue(o);
			return value.getBytes();
		}
		else if (type.equals(java.lang.Integer.class))
		{
			Integer value = (Integer) getFieldValue(o);
			return value.toString().getBytes();
		}
		else if (type.equals(java.lang.Float.class))
		{
			Float value = (Float) getFieldValue(o);
			return value.toString().getBytes();
		}
		else if (type.equals(java.lang.Double.class))
		{
			Double value = (Double) getFieldValue(o);
			return value.toString().getBytes();
		}

		return null;
	}
	
	/**
	 * Sets the value of this field based on the provided byte array.
	 * @param value
	 * @throws InvocationTargetException 
	 * @throws IllegalAccessException 
	 * @throws IllegalArgumentException 
	 */
	public void setByteValue(Object o, byte[] value) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException
	{
		if (type.equals(java.lang.String.class))
		{
			setFieldValue(o, new String(value));
		}
	}

	public boolean isOwner()
	{
		if (isManyToMany())
		{
			if (manytomany != null)
			{
				if (manytomany.mappedBy().length() == 0) return true;
				else return false;
			}
			if (jointable != null) return true;
		}
		else if (isOneToMany())
		{
			if (onetomany != null)
			{
				if (onetomany.mappedBy().length() == 0) return true;
				else return false;
			}
			if (jointable != null) return true;			
		}
		else if (isOneToOne())
		{
			if (onetoone != null)
			{
				if (onetoone.mappedBy().length() == 0) return true;
				else return false;
			}
			return true;
		}
		else if (isManyToOne()) return true;
		return false;
	}

	String getMappedBy()
	{
		return mappedBy;
	}

	CascadeType[] getCascade()
	{
		return cascade;
	}
}
