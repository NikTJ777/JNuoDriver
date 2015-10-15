package com.nuodb.sales.jnuodriver;

import com.nuodb.sales.jnuodriver.dao.ConfigurationException;
import com.nuodb.sales.jnuodriver.dao.PersistenceException;

import java.net.StandardSocketOptions;
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

    private SqlSession.Mode localMode;
    private SqlSession.Mode parentMode;

    private final int maxRetry;

    static final Logger log = Logger.getLogger(GenericTask.class.getName());

    public GenericTask(Controller.TaskType type, Controller.TaskContext context) {
        this.context = context;
        this.type = type;

        switch (type) {
            case UPDATE:
                sql = context.updateSql;
                values = context.updateValues;
                actions = context.updateActions;
                break;

            case QUERY:
                sql = context.querySql;
                values = context.queryValues;
                actions = context.queryActions;
                break;

            default:
                throw new RuntimeException(String.format("Unrecognised TaskType: %s", type.name()));

        }

        //log.info("sql=" + sql.toString());

        localMode = (context.txModel == Controller.TxModel.DISCRETE ? SqlSession.Mode.AUTO_COMMIT : SqlSession.Mode.TRANSACTIONAL);
        switch (context.sessionModel) {
            case BATCHED:
                localMode = SqlSession.Mode.AUTO_COMMIT;
                parentMode = SqlSession.Mode.TRANSACTIONAL;
                break;

            case CACHED:
                parentMode = SqlSession.Mode.AUTO_COMMIT;
                break;

            case POOLED:
                parentMode = null;
        }

        maxRetry = context.maxRetry;
    }

    @Override
    public void run()
    {
        //log.info("running SQL " + sql.toString());

        Object[][] params = new Object[sql.size()][];

        long totalTime = 0;
        long elapsedTime = 0;
        int statementCount = 0;
        long rowCount = 0;

        long accumTime = 0;

        try {
            for (int px = 0; px < params.length; px++) {
                ArrayBlockingQueue<String[]> queue = values.get(px);

                if (queue == null) {
                    params[px] = null;
                    continue;
                }

                if (queue.size() <= 1)
                    context.getMoreValues();

                if (queue.size() < 1)
                    log.info("Value queue is empty - may have to wait...");

                long start = System.nanoTime();
                Object[] val = queue.take();
                long elapsed = System.nanoTime() - start;
                accumTime += elapsed;
                log.info(String.format("Got value - in %f milliseconds", (1.0d/Controller.Nano2Millis) * elapsed));

                if (val != null) {
                    //log.info(String.format("SQL[%d] - Got %d params", px, val.length));
                } else {
                    log.info("got empty value set - deactivating task: " + context.name);
                    context.setActive(false);
                    return;
                }

                params[px] = val;
            }
        }
        catch (InterruptedException e) {}
        catch (Exception e1) {
            log.info(String.format("Error getting parameters for Task %s\n%s", context.name, e1.toString()));
            return;
        }

        //log.info("sql=" + sql.toString() + "; action=" + actions.toString());

        long begin = System.nanoTime();

        // parent TX is used for BATCHED and CACHED sessions
        SqlSession cachedSession = null;
        if (parentMode != null) {
            cachedSession = SqlSession.getCurrent();
            if (cachedSession == null) cachedSession = new SqlSession(parentMode);
        }

        for (int retry = 0; retry <= maxRetry; retry++) {

            totalTime = 0;
            elapsedTime = 0;
            statementCount = 0;
            rowCount = 0;

            if (retry > 0)
                log.info("**RETRYING retry=" + retry);

            try (SqlSession session = new SqlSession(localMode)) {

                try {
                    long start = System.nanoTime();
                    // get the prepared statements
                    PreparedStatement[] psList = new PreparedStatement[sql.size()];
                    for (int sx = 0; sx < psList.length; sx++) psList[sx] = session.getStatement(sql.get(sx));

                    long elapsed = System.nanoTime() - start;
                    accumTime += elapsed;
                    log.info(String.format("Got prepared statements in %.2f ms", 1.0d/Controller.Nano2Millis * elapsed));

                    ResultSet result = null;
                    for (int px = 0; px < psList.length; px++) {

                        statementCount++;

                        start = System.nanoTime();
                        if (params[px] != null) {
                            //log.info("resolving params...");
                            ValueGenerator.resolveDeferredValues(params[px], globals);
                            //log.info("setting params...");
                            setParams(psList[px], params[px]);
                        }
                        elapsed = System.nanoTime() - start;
                        accumTime += elapsed;
                        log.info(String.format("Got resolved deferred values in %.2f ms", 1.0d/Controller.Nano2Millis * elapsed));

                        log.info(String.format("Running %s with %s", context.name, Arrays.toString(params[px])));

                        start = System.nanoTime();
                        String verb = sql.get(px).trim().split(" ")[0].toUpperCase();
                        //log.info("verb=" + verb);

                        String act = actions.get(px).replaceAll("\\{|\\}", "");
                        char op = (act.length() > 0 ? act.charAt(0) : ' ');

                        int repeat = (op == 'x' || op == '*'
                                ? ((Integer) FormatSetReader.resolveValue(act.substring(1), globals)).intValue()
                                : 1);
                        elapsed = System.nanoTime() - start;
                        accumTime += elapsed;

                        for (int lx = 0; lx < repeat; lx++) {

                            rowCount++;

                            start = System.nanoTime();

                            //log.info(String.format("executing %s using %s", sql.get(px), Arrays.toString(params[px])));

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
                            accumTime += elapsedTime;

                            //log.info("Action is " + act + "; result is " + result);

                            // retrieve any return value(s)
                            if (op == '=' && result != null) {
                                //log.info("capturing result");
                                if (Character.isAlphabetic(act.charAt(2))) {
                                    String varName = act.substring(2);
                                    //log.info("assigning to " + varName);
                                    Object value = null;
                                    List<Object> list = null;
                                    List<Object> rowList = null;

                                    switch (act.charAt(1)) {
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

                                    log.info(varName + " => " + value.toString());
                                    globals.put(varName, value);
                                }
                            }

                            totalTime += elapsedTime;
                        }

                    }

                    context.updateTimes(type, totalTime, statementCount, rowCount);
                    report(context.name, statementCount, totalTime);
                    break;

                } catch (Exception e) {
                    log.info(String.format("Error executing task %s\n\t%s", context.name, e.toString()));
                    e.printStackTrace();
                    if (!session.retry(e)) break;
                }
            }

            // allow the session to expire
            if (cachedSession != null)
                cachedSession.park();
        }

        log.info(String.format("Task %s complete. Work time= %.2fms; Elapsed time= %.2fms; accumulated time=%.2fms", context.name, 1.0d/Controller.Nano2Millis * totalTime, 1.0d/Controller.Nano2Millis * (System.nanoTime() - begin), 1.0d/Controller.Nano2Millis * accumTime));
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
        //log.info(String.format("Setting %d param into PreparedStatement", values.length));

        for (int vx = 0; vx < values.length; vx++) {

            //log.info(String.format("param %d => %s", vx, values[vx].toString()));
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

    protected static void report(String name, int count, long duration) {
        double rate = (count > 0 && duration > 0 ? Controller.Nano2Seconds * count / duration : 0);
        log.info(String.format("Thread %s: Processed %s (%,d statement); duration=%.2f ms; rate=%.2f", Thread.currentThread().getName(), name, count, 1d / Controller.Nano2Millis * duration, rate));
    }
}
