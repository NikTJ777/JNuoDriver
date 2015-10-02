package com.nuodb.sales.jnuodriver;

import com.nuodb.sales.jnuodriver.dao.PersistenceException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private static final Pattern sqlAction = Pattern.compile("^\\{(.*)(.)\\}\\s*(\\S*)");

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
            Matcher match = sqlAction.matcher(statement);
            if (match.find()) {
                actions.add(match.group(2) + match.group(1));
                sql.set(sx, match.group(3));
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
            try (SqlSession session = new SqlSession(SqlSession.Mode.TRANSACTIONAL)) {

                try {
                    // get the prepared statements
                    PreparedStatement[] psList = new PreparedStatement[sql.size()];
                    for (int sx = 0; sx < psList.length; sx++) psList[sx] = session.getStatement(sql.get(sx));

                    ResultSet result;
                    for (int px = 0; px < psList.length; px++) {

                        setParams(psList[px], params[px]);

                        String verb = sql.get(px).substring(0, sql.get(px).indexOf(" ")).toUpperCase();

                        String act = actions.get(px);
                        char op = act.charAt(0);

                        int repeat = (op == 'x' || op == '*'
                                ? ((Integer) FormatSetReader.generateValue(act.substring(1), globals)).intValue()
                                : 1);

                        for (int lx = 0; lx < repeat; lx++) {
                            switch (verb) {
                                case "INSERT":
                                case "UPDATE":
                                case "DELETE":
                                    psList[px].executeUpdate();
                                    result = (verb.equals("INSERT") ? psList[px].getGeneratedKeys() : null);
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

                            // retrieve any return value(s)
                            if (op == '=' && result != null) {
                                if (act.charAt(0) == '@') {
                                    String varName = act.substring(2);
                                    Object value = null;
                                    switch (act.charAt(1)) {
                                        case 'd':
                                            value = (result != null && result.next() ? result.getLong(1) : 0);
                                            break;

                                        case 'D':


                                        case 'f':
                                            value = (result != null && result.next() ? result.getDouble(1) : 0f);
                                            break;

                                        case 't':
                                            value = (result != null && result.next() ? result.getDate(1) : null);
                                            break;

                                        case 's':
                                            value = (result != null && result.next() ? result.getString(1) : "");
                                            break;

                                        case 'r':
                                            value = result;
                                            break;
                                    }

                                    globals.put(varName, value);
                                }
                            }
                        }

                    }
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
