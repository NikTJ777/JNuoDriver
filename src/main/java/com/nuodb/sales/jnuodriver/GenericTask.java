package com.nuodb.sales.jnuodriver;

import com.nuodb.sales.jnuodriver.dao.ConfigurationException;
import com.nuodb.sales.jnuodriver.dao.PersistenceException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

/**
 * Created by nik on 22/09/2015.
 */
public class GenericTask implements Runnable {

    private final Controller.TaskContext context;
    private final Controller.TaskType type;
    private Map<String, Object> globals = new HashMap<String, Object>(256);

    private final List<String> sql;
    private final List<ArrayBlockingQueue<String[]>> values;
    private final List<String> actions;

    private final int maxRetry;

    static final Logger log = Logger.getLogger(GenericTask.class.getName());

    public GenericTask(Controller.TaskType type, Controller.TaskContext context) {
        this.context = context;
        this.type = type;

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

        // extract any actions from the sql
        actions = new ArrayList<String>(sql.size());
        for (int sx = 0; sx < sql.size(); sx++) {
            String statement = sql.get(sx);
            if (statement.charAt(0) == '{') {
                actions.add(statement);
                sql.remove(sx);
                sx--;
            }
            else {
                actions.add(" ");
            }
        }

        maxRetry = context.maxRetry;
    }

    @Override
    public void run()
    {
        Object[][] params = new Object[sql.size()][];

        long totalTime = 0;
        long elapsedTime = 0;
        int statementCount = 0;
        long rowCount = 0;

        try {
            for (int px = 0; px < params.length; px++) {
                Object[] val = values.get(px).take();
                if (val.length == 0) {
                    log.info("got empty value set - deactivating task: " + context.name);
                    context.setActive(false);
                    return;
                }

                params[px] = val;
            }
        }
        catch (InterruptedException e) {}

        for (int retry = 0; retry <= maxRetry; retry++) {

            totalTime = 0;
            elapsedTime = 0;
            statementCount = 0;
            rowCount = 0;

            try (SqlSession session = new SqlSession(SqlSession.Mode.TRANSACTIONAL)) {

                try {
                    // get the prepared statements
                    PreparedStatement[] psList = new PreparedStatement[sql.size()];
                    for (int sx = 0; sx < psList.length; sx++) psList[sx] = session.getStatement(sql.get(sx));

                    ResultSet result = null;
                    for (int px = 0; px < psList.length; px++) {

                        statementCount++;

                        setParams(psList[px], params[px]);

                        String verb = sql.get(px).substring(0, sql.get(px).indexOf(" ")).toUpperCase();

                        String act = actions.get(px).replaceAll("\\{|\\}", "");
                        char op = (act.length() > 0 ? act.charAt(0) : ' ');

                        int repeat = (op == 'x' || op == '*'
                                ? ((Integer) FormatSetReader.resolveValue(act.substring(1), globals)).intValue()
                                : 1);

                        for (int lx = 0; lx < repeat; lx++) {

                            rowCount++;

                            long start = System.nanoTime();

                            switch (verb) {
                                case "INSERT":
                                    if (context.bulkCommitMode == SqlSession.Mode.BATCH) {
                                        psList[px].addBatch();
                                        result = null;
                                    }
                                    else {
                                        psList[px].executeUpdate();
                                        result = psList[px].getGeneratedKeys();
                                    }
                                    break;

                                case "UPDATE":
                                case "DELETE":
                                    psList[px].executeUpdate();
                                    break;

                                case "SELECT":
                                case "CALL":
                                case "EXECUTE":
                                    result = psList[px].executeQuery();
                                    break;

                                default:
                                    psList[px].execute();
                                    result = null;
                            }

                            elapsedTime = System.nanoTime() - start;

                            // retrieve any return value(s)
                            if (op == '=' && result != null) {
                                if (Character.isAlphabetic(act.charAt(1))) {
                                    String varName = act.substring(1);
                                    Object value = null;
                                    List<Object> list = null;
                                    List<Object> rowList = null;

                                    switch (act.charAt(act.length()-1)) {
                                        case 'd':
                                            value = (result != null && result.next() ? result.getLong(1) : 0);
                                            break;

                                        case 'D':
                                            list = new ArrayList<Object>(64);
                                            value = list;
                                            while (result.next()) list.add(result.getLong(1));
                                            break;

                                        case 'f':
                                            value = (result != null && result.next() ? result.getDouble(1) : 0f);
                                            break;

                                        case 'F':
                                            list = new ArrayList<Object>(64);
                                            value = list;
                                            while (result.next()) list.add(result.getDouble(1));
                                            break;

                                        case 't':
                                            value = (result != null && result.next() ? result.getDate(1) : null);
                                            break;

                                        case 'T':
                                            list = new ArrayList<Object>(64);
                                            value = list;
                                            while (result.next()) list.add(result.getDate(1));
                                            break;

                                        case 's':
                                            value = (result != null && result.next() ? result.getString(1) : "*none*");
                                            break;

                                        case 'S': {
                                            list = new ArrayList<Object>(64);
                                            value = list;

                                            int columnCount = result.getMetaData().getColumnCount();
                                            while (result.next()) {
                                                rowList = new ArrayList<Object>(columnCount);
                                                list.add(rowList);

                                                for (int rx = 1; rx <= columnCount; rx++) {
                                                    rowList.add(result.getString(rx));
                                                }
                                            }

                                            break;
                                        }

                                        case 'r':
                                            value = result;
                                            break;

                                        case 'R': {
                                            list = new ArrayList<Object>(64);
                                            value = list;
                                            int columnCount = result.getMetaData().getColumnCount();

                                            while (result.next()) {
                                                rowList = new ArrayList<Object>(columnCount);
                                                list.add(rowList);

                                                for (int rx = 1; rx <= columnCount; rx++) {
                                                    rowList.add(result.getObject(rx));
                                                }
                                            }
                                            break;
                                        }

                                        default:
                                            throw new ConfigurationException("Invalid SQL action type: %s", act.charAt(act.length()-1));
                                    }

                                    globals.put(varName, value);
                                }
                            }

                            totalTime += elapsedTime;
                        }

                    }

                    context.updateTimes(type, totalTime, statementCount, rowCount);

                } catch (Exception e) {
                    log.info(String.format("Error executing task %s\n\t%s", context.name, e.toString()));
                    if (!session.retry(e)) break;
                }
            }
        }
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
