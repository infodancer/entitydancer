package org.infodancer.persist.test;

import java.util.Collection;
import java.util.HashSet;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToMany;

/**
 * Used to test unidirectional ManyToMany.
 * @author matthew
 *
 */
@Entity
public class House 
{
	Long id;
	String address;
	Collection<Resident> residents = new HashSet<Resident>();
	
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
	
	public String getAddress() 
	{
		return address;
	}
	
	public void setAddress(String address) 
	{
		this.address = address;
	}
	
	@ManyToMany
	public Collection<Resident> getResidents() 
	{
		return residents;
	}
	
	public void setResidents(Collection<Resident> residents) 
	{
		this.residents = residents;
	}
	
	
}
