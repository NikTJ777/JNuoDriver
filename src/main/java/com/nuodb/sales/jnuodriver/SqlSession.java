package com.nuodb.sales.jnuodriver;


import com.nuodb.jdbc.TransactionIsolation;
import com.nuodb.sales.jnuodriver.dao.PersistenceException;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Created by nik on 7/5/15.
 */
public class SqlSession implements AutoCloseable {

    private final Mode mode;
    private final Mode commitMode;
    private final String type;
    private SqlSession parent;
    private CommunicationMode commsMode;

    private Connection connection;
    private PreparedStatement batch;
    private List<PreparedStatement> statements;

    private static Map<String, Properties> connectionProperties = new HashMap<String, Properties>(16);
    private static Map<String, DataSource> dataSources;

    private static int updateIsolation;
    private static CommunicationMode globalCommsMode;
    private static String SpNamePrefix;

    private static ThreadLocal<SqlSession> current = new ThreadLocal<SqlSession>();
    private static Map<SqlSession, String> sessions;

    private static Logger log = Logger.getLogger("SqlSession");

    //private SqlSession(Mode mode) {
    //    this.mode = mode;
    //}

    public enum Mode { AUTO_COMMIT, TRANSACTIONAL, BATCH, READ_ONLY };
    public enum CommunicationMode { SQL, CALL, STORED_PROCEDURE };

    private static final String DEFAULT_DATASOURCE = "DEFAULT";

    public static void init(Properties properties, int maxThreads) {

        log.info("\n******************* Database Properties ***********************");
        StringBuilder builder = new StringBuilder();
        for (String key : properties.stringPropertyNames()) {
            builder.append(String.format("db property: %s = %s\n", key, properties.getProperty(key)));
        }
        log.info(builder.toString());
        log.info("***************************************************************\n");

        dataSources = new ConcurrentHashMap<String, DataSource>(maxThreads, 0.85f, 256);

        // store the default properties under the default name
        connectionProperties.put(DEFAULT_DATASOURCE, properties);

        // retrieve the list of named connections
        String[] nameList = properties.getProperty(String.format("%s.%s", Controller.LIST_PREFIX, "names")).split(",");

        // iterate the list
        for (String name : nameList) {
            log.info(String.format("Registering named connection: %s", name));

            Properties props = new Properties(properties);      // create a new properties object
            connectionProperties.put(name, props);

            // "dereference" and insert all associated properties
            String prefix = String.format("%s.%s.", Controller.LIST_PREFIX, name);
            log.info("prefix=" + prefix);

            for (String key : properties.stringPropertyNames()) {
                log.info("key=" + key);
                if (key.startsWith(prefix)) {
                    log.info(String.format("Adding named property: %s = %s  (from %s)",
                            key.substring(prefix.length()), properties.getProperty(key), key));

                    props.put(key.substring(prefix.length()), properties.getProperty(key));
                }
            }
        }

        sessions = new ConcurrentHashMap<SqlSession, String>(maxThreads, 0.85f, 256);

        String isolation = properties.getProperty(Controller.UPDATE_ISOLATION);
        if (isolation == null || isolation.length() == 0) isolation = "CONSISTENT_READ";
        switch (isolation) {
            case "READ_COMMITTED":
                updateIsolation = Connection.TRANSACTION_READ_COMMITTED;
                break;

            case "SERIALIZABLE":
                updateIsolation = Connection.TRANSACTION_SERIALIZABLE;
                break;

            case "CONSISTENT_READ":
                updateIsolation = TransactionIsolation.TRANSACTION_CONSISTENT_READ;
                break;

            case "WRITE_COMMITTED":
                updateIsolation = TransactionIsolation.TRANSACTION_WRITE_COMMITTED;
                break;
        }
    }

    public static void setGlobalCommsMode(CommunicationMode globalMode) {
        globalCommsMode = globalMode;
    }

    public static void setSpNamePrefix(String prefix) {
        SpNamePrefix = prefix;
    }

    public SqlSession(Mode mode)
    { this(mode, DEFAULT_DATASOURCE); }

    public SqlSession(Mode mode, String type) {
        this.mode = mode;
        commitMode = (mode == Mode.AUTO_COMMIT || mode == Mode.READ_ONLY ? Mode.AUTO_COMMIT : Mode.TRANSACTIONAL);
        commsMode = (mode == Mode.BATCH ? CommunicationMode.SQL : globalCommsMode);
        this.type = type;

        if (! connectionProperties.containsKey(type)) {
            log.info(String.format("No config found for SqlSession type %s - using default session", type));
        }

        // any existing session is our parent
        parent = current.get();

        //session = new SqlSession(mode);
        current.set(this);
        sessions.put(this, Thread.currentThread().getName());

        //return session;
    }

    public static void cleanup() {
        if (sessions.size() == 0)
            return;

        int released = 0;
        for (Map.Entry<SqlSession, String> entry : sessions.entrySet()) {
            log.info(String.format("cleaning up unclosed session from %s", entry.getValue()));
            entry.getKey().close();
            released++;
        }

        throw new PersistenceException("%d unclosed SqlSessions were cleaned up", released);
    }

    public static SqlSession getCurrent() {
        SqlSession session = current.get();
        if (session == null)
            throw new PersistenceException("No current session");

        return session;
    }

    public void rollback() {
        if (connection != null && commitMode != Mode.AUTO_COMMIT) {
            try { connection.rollback(); }
            catch (SQLException e) {}
        }
    }

    /**
     * something fatal has happened in this session,
     * so release any dead/invalid resources, and be ready to try again.
     *
     * Does NOT close (commit) resources.
     * The current SqlSession is left active.
     */
    public boolean retry(Exception e)
    {
        rollback();
        releaseResources();

        // return true if the operation could be retried
        if (e instanceof SQLTransientException){
           return true;
        }

        return false;
    }

    @Override
    public void close()
    {
        current.set(parent);
        sessions.remove(this);

        try {
            closeResources();
        }
        catch (Exception e) {
            throw new PersistenceException(e, "Error closing session");
        }
        finally {
            releaseResources();
        }
    }

    public PreparedStatement getStatement(String sql) throws SQLException {
        if (mode == Mode.BATCH && batch != null) {
            batch.clearParameters();
            return batch;
        }

        // delegate to our parent - centralises caching, etc
        //if (parent != null)
          //  return parent.getStatement(sql);

        if (statements == null) {
            statements = new ArrayList<PreparedStatement>(16);
            //statements = new HashMap<String, PreparedStatement>(16);
        }

        //PreparedStatement ps = statements.get(sql);

        //if (ps == null) {
            int returnMode = (mode != Mode.BATCH ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
            //int returnMode = (commitMode == Mode.AUTO_COMMIT ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS);
            //int returnMode = Statement.RETURN_GENERATED_KEYS;
            PreparedStatement ps = connection().prepareStatement(sql, returnMode);
            statements.add(ps);
            //statements.put(sql, ps);
        //} else {
        //    ps.clearParameters();
        //}

        batch = (mode == Mode.BATCH ? ps : null);

        return ps;
    }

    public void execute(String[] script) {
        if (script == null || script.length == 0) return;

        String statement = "";
        try (Statement sql = connection().createStatement()) {
            assert sql != null;

            for (String line : script) {
                statement = line;
                if (statement.charAt(0) == '{') {
                    log.info("ignoring action: " + statement);
                    continue;
                }
                
                log.info(String.format("executing statement %s", statement));
                sql.execute(statement);
            }

            System.out.println("committing...");
            connection().commit();
        } catch (SQLException e) {
            throw new PersistenceException(e, "Error executing SQL: %s", statement);
        }
    }

    public long update(PreparedStatement statement)
        throws SQLException
    {
        if (mode == Mode.BATCH) {
            statement.addBatch();
        } else {
            statement.executeUpdate();

            //if (commitMode == Mode.AUTO_COMMIT) {
                try (ResultSet keys = statement.getGeneratedKeys()) {
                    if (keys != null && keys.next()) {
                        return keys.getLong(1);
                    }

                    return (keys != null && keys.next() == true ? keys.getLong(1) : 0);
                }
            //}
        }

        return 0;
        //return (mode == Mode.AUTO_COMMIT ? statement.getGeneratedKeys() : null);
    }

    protected Connection connection()
            throws SQLException
    {
        if (parent != null)
            return parent.connection();

        if (connection == null) {

            // get the existing DataSource object
            DataSource ds = dataSources.get(type);
            if (ds == null) {
                ds = new com.nuodb.jdbc.DataSource(connectionProperties.get(type));
                dataSources.put(type, ds);
            }

            log.info(String.format("Opening connection on DataSource %s", type));

            connection = ds.getConnection();
            switch (mode) {
                case READ_ONLY:
                    connection.setReadOnly(true);
                    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                    connection.setAutoCommit(true);
                    break;
                case AUTO_COMMIT:
                    connection.setReadOnly(false);
                    connection.setAutoCommit(true);
                    connection.setTransactionIsolation(updateIsolation);
                    break;
                default:
                    connection.setReadOnly(false);
                    connection.setAutoCommit(false);
                    connection.setTransactionIsolation(updateIsolation);
            }
        }

        assert connection != null;

        return connection;
    }

    protected void closeResources()
        throws Exception
    {
        if (batch != null) {
            long batchStart = System.currentTimeMillis();
            batch.executeBatch();
            long count = batch.getUpdateCount();
            long duration = System.currentTimeMillis() - batchStart;

            double rate = (count > 0 && duration > 0 ? 1000.0 * count / duration : 0);
            log.info(String.format("Batch commit complete duration=%d ms; rate=%.2f ips", duration, rate));
        }

        if (connection != null) {
            if (commitMode != Mode.AUTO_COMMIT) {
                connection.commit();
            }
        }
    }

    protected void releaseResources()
    {
        batch = null;

        if (statements != null) {

            for (PreparedStatement ps : statements) {
                //for (PreparedStatement ps : statements.values()) {
                try {
                    ps.close();
                } catch (Exception e) {
                }
            }

            statements.clear();
        }

        try {
            connection.close();
        } catch (Exception e) {}

        connection = null;
    }
}
