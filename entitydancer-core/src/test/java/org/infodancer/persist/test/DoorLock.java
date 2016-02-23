package org.infodancer.persist.test;

import java.util.Collection;
import java.util.HashSet;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;

/**
 * This class is used to test the unidirectional OneToMany relationship annotation.
 * @author matthew
 */
@Entity
public class DoorLock 
{
	Long id;
	String name;
	Collection<DoorKey> keys = new HashSet<DoorKey>();
	
	@Id
	@GeneratedValue
	public Long getId() 
	{
		return id;
	}

	public void setId(Long id) 
	{
		this.id = id;
	}

	public String getName() 
	{
		return name;
	}

	public void setName(String name) 
	{
		this.name = name;
	}

	@OneToMany
	public Collection<DoorKey> getKeys()
	{
		return keys;
	}
	
	public void setKeys(Collection<DoorKey> keys)
	{
		this.keys = keys;
	}
}
