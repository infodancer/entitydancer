package org.infodancer.persist;

/**
 * An experimental interface to allow the entitymanager to check whether an object
 * has been changed since it was last persisted.  This allows for potential performance
 * and correctness optimizations that would otherwise require class rewriting, which 
 * I am trying to avoid.
 * @author matthew
 */
public interface Changeable
{
	/**
	 * Indicate whether this Entity has been changed.  It is up to the entity code to update
	 * this flat in its set methods.
	 * @return
	 */
	public boolean isChanged();
	
	/**
	 * Indicate whether this Entity has been changed.  This method should be called with "true"
	 * in any set method where the value is not the same as it was before the set method was 
	 * called, and will be called by the entitymanager with "false" after the current object 
	 * state has been persisted, merged, or refreshed. 
	 * @param changed
	 */
	public void setChanged(boolean changed);
}
