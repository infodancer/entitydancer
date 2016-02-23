package org.infodancer.persist.test;

import java.util.ArrayList;
import java.util.Collection;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToMany;

/**
 * Used to test ManyToMany relationships.
 * @author matthew
 */
@Entity
public class Driver 
{
	private Long id;
	private String name;
	private Collection<Car> cars = new ArrayList<Car>();
	
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
	
	@ManyToMany(mappedBy="drivers")
	public Collection<Car> getCars() 
	{
		return cars;
	}
	
	public void setCars(Collection<Car> cars) 
	{
		this.cars = cars;
	}
	
}
