package com.nuodb.sales.jnuodriver.dao;


import com.nuodb.sales.jnuodriver.SqlSession;

import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.logging.Logger;

/**
 * Created by nik on 7/5/15.
 */
public abstract class AbstractRepository<T extends Entity> implements Repository<T> {

    private final String tableName;
    private final String[] columns;
    private final Map<String, Integer> ordinal;

    private final String fields;
    private final String params;

    private final int maxRetry = 3;
    private final long retrySleep = 2000;

    public static enum UpdateMode { CREATE, UPDATE };

    protected static Logger log = Logger.getLogger("Repository");


    /**
     * query string for findById
     * @param String table name
     * @param long id
     */
    protected static String findSql = "SELECT * from %s where id = '%d'";

    /**
     * query string for findALlBy
     * @param String tableName
     * @param String columnName
     */
    protected static String findBySql = "SELECT * from %s where %s = '%s'";

    /**
     * query string for update statement
     * @param String comma-separated list of columns to update ex: name, date
     * @param String table name
     * @param String comma-separated list of parameter names ex: ?name, ?date
     */
    protected static String persistSql = "INSERT into %s (%s) values (%s)";

    /**
     * sql statement to update an existing row
     * @param String comma-separated list of columns to update; ex: name, date
     * @param String table name
     * @param String comma-separated list of parameter names; ex: ?name, ?date
     * @param long record id
     */
    protected static String updateSql = "UPDATE %s SET %s = (%s) where id = '%d'";

    /**
     * sql statement to retrieve a single column value
     * @param column - String name of column to retrieve
     * @param tableName - String the table to query (resolved with the tableName ni this AbstractRepository object
     * @param criteria - String SQL clauses to specify which column to select
     */
    protected static final String getSql = "SELECT %s from %s %s";

    public AbstractRepository(String tableName, String... columns) {

        this.tableName = tableName;
        this.columns = columns;

        fields = Arrays.toString(columns).replaceAll("\\[|\\]", "");

        StringBuilder builder = new StringBuilder();
        for (String name : columns) {
            if (builder.length() > 0) builder.append(", ");
            builder.append('?'); //.append(name);
        }

        params = builder.toString();

        ordinal = new HashMap<String, Integer>(columns.length * 2);
        for (int cx = 0; cx < columns.length; cx++) {
            ordinal.put(columns[cx], new Integer(cx+2));    // +2 => skip id, plus ordinal is 1-based
        }
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public T findById(long id) {

        String sql = String.format(findSql, tableName, id);
        try (ResultSet row = SqlSession.getCurrent().getStatement(sql).executeQuery()) {
            if (row == null || row.next() == false) return null;

            return mapIn(row);
        } catch (SQLException e) {
            log.info(String.format("FindById failed due to %s", e.toString()));
            return null;
        }
    }

    @Override
    public long persist(T entity)
            throws PersistenceException
    {

        if (entity.isPersistent()) {
            throw new PersistenceException("Attempt to persist already persistent object %s", entity.toString());
        }

        String sql = String.format(persistSql, tableName, fields, params);
        for (int retry = 0; ; retry++) {
            SqlSession session = SqlSession.getCurrent();

            try {
                PreparedStatement update = session.getStatement(sql);

                mapOut(entity, update);

                return session.update(update);
            } catch (SQLTransientException te) {
                if (retry < maxRetry) {
                    log.info(String.format("Retriable exception in persist: %s; retrying...", te.toString()));
                    try { Thread.sleep(retrySleep); } catch (InterruptedException e) {}
                    continue;
                }

                throw new PersistenceException(te, "Permanent error after %d retries", maxRetry);
            } catch (SQLException e) {
                throw new PersistenceException(e, "Error persisting new Entity %s", entity.toString());
            }
        }
    }

    @Override
    public void update(long id, String columns, Object ... values)
            throws PersistenceException
    {
        StringBuilder builder = new StringBuilder();
        for (int x = values.length; x > 0; x--) {
            if (builder.length() > 0) builder.append(", ");
            builder.append('?'); //.append(name);
        }

        String params = builder.toString();

        String sql = String.format(updateSql, tableName, columns, params, id);
        SqlSession session = SqlSession.getCurrent();
        try (PreparedStatement update = session.getStatement(sql)) {
            setParams(update, columns, values);
            session.update(update);
        } catch (SQLException e) {
            throw new PersistenceException(e, "Error updating table %s, id %d", getTableName(), id);
        }
    }


    @Override
    public List<T> findAllBy(String column, Object ... param)
        throws PersistenceException
    {
        List<T> result = new ArrayList(1024);
        SqlSession session = SqlSession.getCurrent();

        try (ResultSet row = queryBy(column, param)) {
            while (row != null && row.next()) {
                result.add(mapIn(row));
            }

            return result;
        } catch (SQLException e) {
            throw new PersistenceException(e, "Error in find all %s by %s = '%s'", tableName, column, param.toString());
        }
    }

    @Override
    public String getValue(String column, String criteria)
        throws PersistenceException
    {
        SqlSession session = SqlSession.getCurrent();

        try (PreparedStatement sql = session.getStatement(String.format(getSql, column, getTableName(), criteria))) {
            try (ResultSet row = sql.executeQuery()) {
                if (row.next()) {
                    return row.getString(1);
                } else {
                    throw new PersistenceException("No matching value found: select %s from %s %s",
                        column, getTableName(), criteria);
                }
            }
        } catch (SQLException e) {
            throw new PersistenceException(e, "Error querying for single value: %s from %s %s",
                    column, getTableName(), criteria);
        }
    }

    protected ResultSet queryBy(String column, Object ... param)
        throws SQLException
    {
        StringBuilder sql = new StringBuilder().append(String.format(findBySql, tableName, column, param[0].toString()));
        for (int px = 1; px < param.length; px++) {
            sql.append(String.format(" OR %s = '%s'", column, param[px].toString()));
        }

        return SqlSession.getCurrent().getStatement(sql.toString()).executeQuery();
    }

    @Override
    public abstract void init() throws ConfigurationException;

    protected abstract T mapIn(ResultSet row) throws SQLException;

    protected abstract void mapOut(T entity, PreparedStatement sql) throws SQLException;

    /**
     * set parameters into a PreparedStatement
     *
     * @param sp PreparedStatement - the prepared statement to set the parameters into
     * @param columns String - a comma-separated list of columns to update - in the form "a, b, c"
     * @param values Object[] - the array of values to be set into the prepared statement - one per column name
     *
     * @throws PersistenceException if the number of values is less than the number of column names
     * @throws SQLException if the PreparedStatement throws any exception
     */
    protected void setParams(PreparedStatement sp, String columns, Object[] values)
        throws PersistenceException, SQLException
    {
        String[] fields = columns.split(", ");
        if (values.length < fields.length)
            throw new PersistenceException("Invalid update request: insufficient values for named columns: %s < %s", Arrays.toString(values), columns);

        for (int vx = 0; vx < values.length; vx++) {
            Class type = values[vx].getClass();

            if (type == Integer.class) {
                sp.setInt(vx+1, (Integer) values[vx]);
            }
            else if (type == Long.class) {
                sp.setLong(vx+1, (Long) values[vx]);
            }
            else if (type == String.class) {
                sp.setString(vx+1, values[vx].toString());
            }
            else if (type == Boolean.class) {
                sp.setBoolean(vx+1, (Boolean) values[vx]);
            }
            else if (type == Date.class) {
                sp.setDate(vx+1, new java.sql.Date(((Date) values[vx]).getTime()));
            }
        }
    }
}
