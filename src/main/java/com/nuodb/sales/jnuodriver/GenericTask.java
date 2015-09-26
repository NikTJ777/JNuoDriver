package com.nuodb.sales.jnuodriver;

import com.nuodb.sales.jnuodriver.dao.ConfigurationException;
import com.nuodb.sales.jnuodriver.dao.PersistenceException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

/**
 * Created by nik on 22/09/2015.
 */
public class GenericTask implements Runnable {

    private final Controller.TaskContext context;
    private final Controller.TaskType type;

    private final List<String> sql;
    private final List<ArrayBlockingQueue<String[]>> values;

    private final int maxRetry;

    static final Logger log = Logger.getLogger(GenericTask.class.getName());

    public GenericTask(long unique, Controller.TaskType type, Controller.TaskContext context) {
        this.context = context;
        this.type = type;

        String[] commands;
        String[] formats;

        switch (type) {
            case UPDATE:
                sql = context.updateSql;
                values = context.updateValues;
                break;

            case QUERY:
                sql = context.querySql;
                values = context.queryValues;
                break;

            default:
                throw new RuntimeException(String.format("Unrecognised TaskType: %s", type.name()));

        }

        maxRetry = context.maxRetry;
    }

    @Override
    public void run()
    {
        do {
            for (int retry = 0; retry <= maxRetry; retry++) {
                try (SqlSession session = new SqlSession(SqlSession.Mode.TRANSACTIONAL)) {

                    try {
                        // get the prepared statements
                        PreparedStatement[] psList = new PreparedStatement[sql.size()];
                        for (int sx = 0; sx < psList.length; sx++) psList[sx] = session.getStatement(sql.get(sx));

                        for (int px = 0; px < psList.length; px++) {
                            setParams(psList[px], values.get(px).take());

                            psList[px].execute();
                        }
                    } catch (Exception e) {
                        log.info(String.format("Error executing task %s\n\t%s", context.name, e.toString()));
                        if (!session.retry(e)) break;
                    }
                }
            }
        } while (true);
    }

    /**
     * set parameters into a PreparedStatement
     *
     * @param sp PreparedStatement - the prepared statement to set the parameters into
     * @param values Object[] - the array of values to be set into the prepared statement - one per column name
     *
     * @throws PersistenceException if the number of values is less than the number of column names
     * @throws SQLException if the PreparedStatement throws any exception
     */
    protected void setParams(PreparedStatement sp, Object[] values)
            throws PersistenceException, SQLException
    {
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