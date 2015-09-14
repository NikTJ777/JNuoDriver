package com.nuodb.sales.jnuodriver.dao;

import java.util.List;

/**
 * Created by nik on 7/2/15.
 */
public interface Repository<T extends Entity> {

    /**
     * initialise the Repository before it is used.
     *
     * @throws ConfigurationException if the Repository is not correctly configured.
     */
    public void init() throws ConfigurationException;

    /**
     * Find an Entity by its id.
     *
     * @param id
     * @return the retrieved Entity, or null.
     * @throws PersistenceException if the database throws an Exception.
     */
    public T findById(long id);

    /**
     * Execute a query.
     *
     * @param column String - name of the column to query
     * @param param Object - value of the column to query
     *
     * @return ResultSet containing the result(s) of the query
     * @throws PersistenceException if an error occurs in creating or executing the query
     */
    public List<T> findAllBy(String column, Object ... param);

    /**
     * retrieve a single value from the database.
     *
     * @param column - String the name of the column to retrieve
     * @param criteria - the criteria to use to select the row to be retrieved
     *
     * @return String - the column value as a String
     *
     * @throws PersistenceException if no value can be found, or is an Exception is thrown by the database
     */
    public String getValue(String column, String criteria);

    /**
     * save an Entity into the database.
     * If the Entity is already persistent, then an exception is thrown.
     * If the database is configured to allocate the key, then any key in the Entity is ignored.
     * @param entity to persist
     * @return the key allocated by the database.
     * @throws PersistenceException if the database throws an exception
     */
    public long persist(T entity);

    /**
     * Update one or more column(s) in an existing row in the database
     *
     * @param id long - the id of the row to update
     * @param columns String - comma-separated list of column(s) to update
     * @param values Object vararg - A list of values corresponding to the named columns.
     *
     * @throws PersistenceException if the database throws an exception.
     */
    public void update(long id, String columns, Object ... values);
}
