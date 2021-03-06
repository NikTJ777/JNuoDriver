package com.nuodb.sales.jnuodriver;

import com.nuodb.sales.jnuodriver.dao.ConfigurationException;
import com.nuodb.sales.jnuodriver.service.Service;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by nik on 7/6/15.
 */
public class Controller implements AutoCloseable {

    ArrayList<TaskContext> taskList;
    ScheduledExecutorService taskExecutor;
    ValueGenerator valueGenerator;
    Service service;

    Properties fileProperties;
    Properties appProperties;

    long runTime;
    float timingSpeedup;
    int maxQueued, queryBackoff, updateThreads, queryThreads;
    boolean initDb = false;
    boolean queryOnly = false;

    AtomicLong totalStatements = new AtomicLong();
    AtomicLong totalStatementTime = new AtomicLong();

    AtomicLong totalRetrieved = new AtomicLong();
    AtomicLong totalRetrievedRecords = new AtomicLong();
    AtomicLong totalRetrieveTime = new AtomicLong();

    long unique;

    long totalEvents;
    long wallTime;

    private Random random = new Random();

    private static final Properties defaultProperties = new Properties();

    public static final String TASK_NAME =          "task.name";
    public static final String PROPERTIES_PATH =    "properties.path";
    public static final String DB_PROPERTIES_PATH = "db.properties.path";
    public static final String TASK_PROPERTIES_PATHS = "task.properties.paths";
    public static final String UPDATE_TASK_CLASS =  "update.task.class";
    public static final String QUERY_TASK_CLASS =   "query.task.class";
    public static final String VALUE_READER_CLASSES = "valueReader.classes";
    public static final String AVERAGE_RATE =       "timing.rate";
    public static final String MAX_RETRY =          "max.retry";
    public static final String RATE_SMOOTHING =     "rate.smoothing";
    public static final String MIN_VIEW_DELAY =     "timing.min.view.delay";
    public static final String MAX_VIEW_DELAY =     "timing.max.view.delay";
    public static final String TIMING_SPEEDUP =     "timing.speedup";
    public static final String UPDATE_THREADS =     "update.threads";
    public static final String QUERY_THREADS =      "query.threads";
    public static final String MAX_QUEUED =         "max.queued";
    public static final String RUN_TIME =           "run.time";
    public static final String MIN_FANOUT =         "min.fanout";
    public static final String MAX_FANOUT =         "max.fanout";
    public static final String MIN_LEAVES =         "min.leaves";
    public static final String MAX_LEAVES =         "max.leaves";
    public static final String BURST_PROBABILITY_PERCENT = "burst.probability.percent";
    public static final String MIN_BURST =          "min.burst";
    public static final String MAX_BURST =          "max.burst";
    public static final String DB_INIT =            "db.init";
    public static final String DB_INSTRUMENT_SQL =  "db.instrument.sql";
    public static final String DB_CLEAR_SQL =       "db.clear.sql";
    public static final String DB_INIT_SQL =        "db.init.sql";
    public static final String DB_UPDATE_SQL =      "db.update.sql";
    public static final String DB_UPDATE_VALUES =   "db.update.values";
    public static final String DB_QUERY_SQL =       "db.query.sql";
    public static final String DB_QUERY_VALUES =    "db.query.values";
    public static final String DB_CATCH_BLOCK =     "db.catch.block";
    public static final String DB_SCHEMA =          "db.schema";
    public static final String TX_MODEL =           "tx.model";
    public static final String SESSION_MODEL =      "session.model";
    public static final String SESSION_MAX_USE =    "session.max.use";
    public static final String SESSION_MAX_TIME =   "session.max.time";
    public static final String COMMUNICATION_MODE = "communication.mode";
    public static final String BULK_COMMIT_MODE =   "bulk.commit.mode";
    public static final String SP_NAME_PREFIX=      "sp.name.prefix";
    public static final String QUERY_ONLY =         "query.only";
    public static final String QUERY_BACKOFF =      "query.backoff";
    public static final String UPDATE_ISOLATION =   "update.isolation";
    public static final String CONNECTION_TIMEOUT = "connection.timeout";
    public static final String DB_PROPERTY_PREFIX = "db.property.prefix";
    public static final String LIST_PREFIX =        "@list";

    protected enum TxModel { DISCRETE, UNIFIED }
    protected enum SessionModel { POOLED, CACHED, BATCHED }

    public enum TaskType { UPDATE, QUERY }

    static final Pattern VariableReferencePattern = Pattern.compile("\\$\\{[^\\}]+\\}");
    static final Pattern ScriptStatementPattern = Pattern.compile("^\\s*\\{\\s*(\\S*?)\\s*,?\\s*(\\p{Alpha})?\\}\\s*([\\p{Punct}\\p{Alpha}])\\s*(.*)$");

    private static Logger appLog = Logger.getLogger("JNuoTest");
    private static Logger insertLog = Logger.getLogger("InsertLog");
    private static Logger viewLog = Logger.getLogger("EventViewer");

    static final double Nano2Millis = 1000000.0;
    static final double Nano2Seconds = 1000000000.0;
    static final double Millis2Seconds = 1000.0;

    private static final long Millis = 1000;

    private static final float Percent = 100.0f;

    public Controller() {
        defaultProperties.setProperty(PROPERTIES_PATH, "classpath://properties/Application.properties");
        defaultProperties.setProperty(DB_PROPERTIES_PATH, "classpath://properties/Database.properties");
        defaultProperties.setProperty(UPDATE_TASK_CLASS, GenericTask.class.getName());
        defaultProperties.setProperty(QUERY_TASK_CLASS, GenericTask.class.getName());
        defaultProperties.setProperty(AVERAGE_RATE, "0");
        defaultProperties.setProperty(MIN_VIEW_DELAY, "0");
        defaultProperties.setProperty(MAX_VIEW_DELAY, "0");
        defaultProperties.setProperty(TIMING_SPEEDUP, "1");
        defaultProperties.setProperty(RATE_SMOOTHING, "10");
        defaultProperties.setProperty(UPDATE_THREADS, "1");
        defaultProperties.setProperty(QUERY_THREADS, "1");
        defaultProperties.setProperty(MAX_QUEUED, "0");
        defaultProperties.setProperty(MIN_FANOUT, "1");
        defaultProperties.setProperty(MAX_FANOUT, "5");
        defaultProperties.setProperty(MIN_LEAVES, "500");
        defaultProperties.setProperty(MAX_LEAVES, "3500");
        defaultProperties.setProperty(BURST_PROBABILITY_PERCENT, "0");
        defaultProperties.setProperty(MIN_BURST, "0");
        defaultProperties.setProperty(MAX_BURST, "0");
        defaultProperties.setProperty(MAX_RETRY, "3");
        defaultProperties.setProperty(RUN_TIME, "5");
        defaultProperties.setProperty(TX_MODEL, "DISCRETE");
        defaultProperties.setProperty(SESSION_MODEL, "POOLED");
        defaultProperties.setProperty(SESSION_MAX_USE, "100");
        defaultProperties.setProperty(SESSION_MAX_TIME, "2000");
        defaultProperties.setProperty(COMMUNICATION_MODE, "SQL");
        defaultProperties.setProperty(BULK_COMMIT_MODE, "BATCH");
        defaultProperties.setProperty(SP_NAME_PREFIX, "importer_");
        defaultProperties.setProperty(DB_INIT, "false");
        defaultProperties.setProperty(DB_INSTRUMENT_SQL, "true");
        defaultProperties.setProperty(QUERY_ONLY, "false");
        defaultProperties.setProperty(QUERY_BACKOFF, "0");
        defaultProperties.setProperty(UPDATE_ISOLATION, "CONSISTENT_READ");
        defaultProperties.setProperty(CONNECTION_TIMEOUT, "300");
    }

    public void configure(String[] args)
        throws Exception
    {
        // create 2 levels of file properties (application.properties; and database.properties)
        Properties prop = new Properties(defaultProperties);
        fileProperties = new Properties(prop);

        // create app properties, using fileProperties as default values
        appProperties = new Properties(fileProperties);

        // parse the command line into app properties, as command line overrides all others
        parseCommandLine(args, appProperties);

        if ("true".equalsIgnoreCase(appProperties.getProperty("help"))) {
            System.out.println("\njava -jar <jarfilename> [option=value [, option=value, ...] ]\nwhere <option> can be any of:\n");

            String[] keys = appProperties.stringPropertyNames().toArray(new String[0]);
            Arrays.sort(keys);
            for (String key : keys) {
                System.out.println(String.format("%s\t\t\t\t(default=%s)", key, defaultProperties.getProperty(key)));
            }

            System.out.println("\nHelp called - nothing to do; exiting.");
            System.exit(0);
        }

        // load properties from application.properties file into first (lower-priority) level of fileProperties
        loadProperties(prop, appProperties.getProperty(PROPERTIES_PATH));

        // now load database properties into second (higher-priority) level of fileProperties
        loadProperties(fileProperties, appProperties.getProperty(DB_PROPERTIES_PATH));

        appLog.info(String.format("appProperties: %s", appProperties));

        StringBuilder builder = new StringBuilder(1024);
        builder.append("\n***************** Resolved Properties ********************\n");
        String[] keys = appProperties.stringPropertyNames().toArray(new String[0]);
        Arrays.sort(keys);
        for (String key : keys) {
            builder.append(String.format("%s = %s\n", key, appProperties.getProperty(key)));
        }
        appLog.info(builder.toString() + "**********************************************************\n");

        runTime = Integer.parseInt(appProperties.getProperty(RUN_TIME)) * Millis;
        timingSpeedup = Float.parseFloat(appProperties.getProperty(TIMING_SPEEDUP));
        maxQueued = Integer.parseInt(appProperties.getProperty(MAX_QUEUED));
        initDb = Boolean.parseBoolean(appProperties.getProperty(DB_INIT));
        queryOnly = Boolean.parseBoolean(appProperties.getProperty(QUERY_ONLY));
        queryBackoff = Integer.parseInt(appProperties.getProperty(QUERY_BACKOFF));

        String threadParam = appProperties.getProperty(UPDATE_THREADS);
        updateThreads = (threadParam != null ? Integer.parseInt(threadParam) : 1);

        threadParam = appProperties.getProperty(QUERY_THREADS);
        queryThreads = (threadParam != null ? Integer.parseInt(threadParam) : 1);

        int totalThreads = updateThreads + queryThreads;

        // filter out database properties, and strip off the prefix
        Properties dbProperties = new Properties();
        String dbPropertyPrefix = appProperties.getProperty(DB_PROPERTY_PREFIX);
        if (! dbPropertyPrefix.endsWith(".")) dbPropertyPrefix = dbPropertyPrefix + ".";

        for (String key : appProperties.stringPropertyNames()) {
            if (key.startsWith(dbPropertyPrefix)) {
                dbProperties.setProperty(key.substring(dbPropertyPrefix.length()), appProperties.getProperty(key));
            }
        }

        //String insertIsolation = appProperties.getProperty(UPDATE_ISOLATION);
        //DataSource dataSource = new com.nuodb.jdbc.DataSource(dbProperties);
        SqlSession.init(dbProperties, totalThreads);

        SqlSession.CommunicationMode commsMode;
        try { commsMode = Enum.valueOf(SqlSession.CommunicationMode.class, appProperties.getProperty(COMMUNICATION_MODE));}
        catch (Exception e) { commsMode = SqlSession.CommunicationMode.SQL; }

        SqlSession.setGlobalCommsMode(commsMode);
        appLog.info(String.format("SqlSession.globalCommsMode set to %s", commsMode));

        SqlSession.setSpNamePrefix(appProperties.getProperty(SP_NAME_PREFIX));

        int maxUse = Integer.parseInt(appProperties.getProperty(SESSION_MAX_USE));
        long maxTime = Long.parseLong(appProperties.getProperty(SESSION_MAX_TIME));
        SqlSession.setExpiry(maxUse, maxTime);

        String taskPaths = appProperties.getProperty(TASK_PROPERTIES_PATHS);
        if (taskPaths == null || taskPaths.length() == 0) {
            System.out.println("No Task properties defined - nothing to do");
            System.exit(0);
        }

        String valueReaderClasses = appProperties.getProperty(VALUE_READER_CLASSES);
        if (valueReaderClasses != null && valueReaderClasses.length() > 0) {
            for (String defn : valueReaderClasses.split(",")) {
                String[] token = defn.split("(=|:)");

                if (token.length == 2)
                    ValueGenerator.addReader(token[0], token[1]);
                else
                    throw new ConfigurationException("Invalid reader class definition. Expected: 'type=className'; got: ", defn);
            }
        }

        taskList = new ArrayList<TaskContext>(64);
        valueGenerator = new ValueGenerator();

        String[] tasks = taskPaths.split(",");
        for (String task : tasks) {
            Properties taskProperties = new Properties(appProperties);
            loadProperties(taskProperties, task);
            TaskContext context = new TaskContext(task, taskProperties);
            taskList.add(context);
            valueGenerator.addSource(context);
        }

        for (TaskContext task : taskList) {
            builder.append("\n*** ").append(task.path).append(" ***\n");
            for (Map.Entry<Object, Object> entry : task.properties.entrySet()) {
                builder.append(String.format("%s = %s\n", entry.getKey().toString(), entry.getValue().toString()));
            }
            builder.append("\n********\n");
        }

        //service = new GenericService();

        taskExecutor = Executors.newScheduledThreadPool(totalThreads);

        if ("true".equalsIgnoreCase(appProperties.getProperty("check.config"))) {
            appLog.info("\n***** check.config set - testing ValueGenerator *****\n");
            valueGenerator.start();
            try { Thread.sleep(500); }
            catch (InterruptedException e) {}

            System.out.println("check.config set - nothing to do; exiting.");
            System.exit(0);
        }
    }

    /**
     * perform any logic required after configuration, and before the Controller can be used
     */
    public void init() {
        if (initDb) {
            initializeDatabase();
            unique = 1;
        } else {
            try (SqlSession session = new SqlSession(SqlSession.Mode.AUTO_COMMIT)) {
                //unique = service.getUnique() + 1;
                appLog.info(String.format("lastEventID = %s", unique));
            }
        }
    }

    /**
     * Start the controller.
     *
     * @throws InterruptedException
     */
    public void run()
        throws InterruptedException, Exception
    {
        long start = System.currentTimeMillis();
        long endTime = start + runTime;
        long now;

        totalEvents = 0;
        wallTime = 0;

        double burstRate = 0.0;
        int burstSize = 0;

        long sleepTime;

        // ensure that first sample time is different from start time...
        long settleTime = 2 * Millis;
        appLog.info(String.format("Settling for %d: ", settleTime));
        Thread.sleep(settleTime);

        // start the value generator
        valueGenerator.start();

        do {
            //totalEvents++;
            appLog.info(String.format("Scheduling Task. Queue size=%d", ((ThreadPoolExecutor) taskExecutor).getQueue().size()));

            now = System.currentTimeMillis();
            wallTime = now - start;
            appLog.info(String.format("now=%d; endTime=%d;  elapsed=%d; time left=%d", now, endTime, wallTime, endTime - now));

            int inactive = 0;

            sleepTime = endTime - now;
            long delay = 0;
            for (TaskContext task : taskList) {
                if (! task.isActive()) {
                    inactive++;
                    continue;
                }

                delay = task.schedule();
                
                if (delay < 0) {
                    task.setActive(false);
                    inactive++;
                    continue;
                }

                totalEvents++;
                if (delay < sleepTime) sleepTime = delay;
            }

            appLog.info("inactive=" + inactive);

            if (inactive >= taskList.size()) {
                appLog.info("All tasks are inactive - exiting");
                return;
            }

            // sleep until the next task is scheduled to be executed
            if (delay > 0) {
                appLog.info(String.format("Sleeping %,d ms until next scheduled execution time", sleepTime));
                Thread.sleep(sleepTime);
            }

            while (maxQueued >= 0 && ((ThreadPoolExecutor) taskExecutor).getQueue().size() > maxQueued) {
                appLog.info(String.format("Queue size %d is over limit %d - sleeping", ((ThreadPoolExecutor) taskExecutor).getQueue().size(), maxQueued));
                Thread.sleep(1 * Millis / (((ThreadPoolExecutor) taskExecutor).getQueue().size() > 1 ? 2 : 20));
            }

            appLog.info(String.format("Sleeping done. Queue size=%d", ((ThreadPoolExecutor) taskExecutor).getQueue().size()));

            appLog.info(String.format("Scheduled %,d events; Processed %,d statements in %.2f secs"
                            + "\n\tThroughput:\t%.2f scheduled and %.2f processed/sec;"
                            + "\n\tSpeed:\t\t%,d statements in %.2f secs = %.2f /sec"
                            + "\n\tSecondary:\t%,d statements processed %,d rows in %.2f secs at %.2f /sec",
                    totalEvents, totalStatements.get(), (wallTime / Millis2Seconds), (Millis2Seconds * totalEvents / wallTime), (Millis2Seconds * totalStatements.get() / wallTime),
                    totalStatements.get(), (totalStatementTime.get() / Nano2Seconds), (Nano2Seconds * totalStatements.get() / totalStatementTime.get()),
                    totalRetrieved.get(), totalRetrievedRecords.get(), (totalRetrieveTime.get() / Nano2Seconds), (Nano2Seconds * totalRetrieved.get() / totalRetrieveTime.get())));

            // display individual metrics
            for (TaskContext task : taskList) {
                appLog.info(String.format("Task: %s\tSpeed: total %,d statements at %.2f sps", task.name, task.totalUpdates.get(), (Nano2Seconds * task.totalUpdates.get() / task.totalUpdateTime.get())));
            }
        } while (System.currentTimeMillis() < endTime);
    }

    public void close()
    {
        try {
            valueGenerator.stop();
            taskExecutor.shutdownNow();
            taskExecutor.awaitTermination(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            System.out.println("Interrupted while waiting for shutdown - exiting");
        }

        appLog.info(String.format("Exiting with %d items remaining in the queue.\n\tScheduled %,d events; Processed %,d statements in %.2f secs"
                        + "\n\tThroughput:\t%.2f scheduled and %.2f processed /sec;"
                        + "\n\tSpeed:\t\t%,d statements in %.2f secs = %.2f /sec"
                        + "\n\tSecondary:\t%,d statements processed %,d rows in %.2f secs at %.2f /sec",
                ((ThreadPoolExecutor) taskExecutor).getQueue().size(),
                totalEvents, totalStatements.get(), (wallTime / Millis2Seconds), (Millis2Seconds * totalEvents / wallTime), (Millis2Seconds * totalStatements.get() / wallTime),
                totalStatements.get(), (totalStatementTime.get() / Nano2Seconds), (Nano2Seconds * totalStatements.get() / totalStatementTime.get()),
                totalRetrieved.get(), totalRetrievedRecords.get(), (totalRetrieveTime.get() / Nano2Seconds), (Nano2Seconds * totalRetrieved.get() / totalRetrieveTime.get())));

        //appLog.info(String.format("Exiting with %d items remaining in the queue.\n\tProcessed %,d events containing %,d records in %.2f secs\n\tThroughput:\t%.2f events/sec at %.2f ips;\n\tSpeed:\t\t%,d inserts in %.2f secs = %.2f ips",
        //        ((ThreadPoolExecutor) insertExecutor).getQueue().size(),
        //        totalEvents, totalStatements/*.get()*/, (wallTime / Millis2Seconds), (Millis2Seconds * totalEvents / wallTime), (Millis2Seconds * totalStatements/*.get()*/ / wallTime),
        //        totalStatements/*.get()*/, (totalStatementTime/*.get()*/ / Nano2Seconds), (Nano2Seconds * totalStatements/*.get()*/ / totalStatementTime/*.get()*/)));

        SqlSession.cleanup();
    }

    protected void initializeDatabase() {

        List<TaskContext> reverseTaskList = new ArrayList<TaskContext>(taskList);
        Collections.reverse(reverseTaskList);

        try (SqlSession session = new SqlSession(SqlSession.Mode.AUTO_COMMIT)) {

            // run the clear scripts in reverse order
            for (TaskContext task : reverseTaskList) {
                List<String> script = task.dbClearSQL;
                if (script == null) {
                    appLog.info("Somehow clear script is NULL in " + task.name);
                    continue;
                }

                appLog.info(String.format("running clear sql from %s (%d statements): %s", task.path, script.size(), script.toString()));
                session.execute(script.toArray(new String[0]));
            }

            // run the init scripts in the specified order
            for (TaskContext task : taskList) {
                List<String> script = task.dbInitSQL;
                if (script == null) {
                    appLog.info("Somehow init script is NULL in " + task.name);
                    continue;
                }

                appLog.info(String.format("running init sql from %s (%d statements): %s", task.path, script.size(), script.toString()));
                session.execute(script.toArray(new String[0]));
            }
        }
    }

    protected void parseCommandLine(String[] args, Properties props) {

        for (String param : args) {
            String[] keyVal = param.split("=");
            if (keyVal.length == 2) {
                props.setProperty(keyVal[0].trim().replaceAll("-", ""), keyVal[1]);
            }
            else {
                props.setProperty(param.trim().replaceAll("-", ""), "true");
            }
        }
    }

    protected void loadProperties(Properties props, String path)
        throws MalformedURLException, IOException
    {
        assert path != null && path.length() > 0;

        appLog.info(String.format("loading properties: from %s", path));

        InputStream is = null;

        if (path.startsWith("classpath://")) {
            is = getClass().getClassLoader().getResourceAsStream(path.substring("classpath://".length()));
            appLog.info(String.format("loading resource: %s", path.substring("classpath://".length())));
        } else {
            is = new URL(path).openStream();
        }

        if (is == null) return;

        try { props.load(is); }
        finally { is.close(); }

        resolveReferences(props);

        appLog.info(String.format("Loaded properties %s", props));
    }

    protected void resolveReferences(Properties props) {
        StringBuffer newVar = new StringBuffer();

        for (Map.Entry<Object, Object> entry : props.entrySet()) {

            appLog.info(String.format("resolving %s", entry.getValue()));

            resolveReferences(entry.getValue().toString(), newVar);

            if (newVar.length() > 0) {
                appLog.info(String.format("Replacing updated property %s=%s", entry.getKey(), newVar));
                entry.setValue(newVar.toString());
            }
        }
    }

    protected void resolveReferences(String property, StringBuffer output) {

        Matcher match = VariableReferencePattern.matcher(property);
        output.setLength(0);

        while (match.find()) {

            //appLog.info(String.format("match.group=%s", match.group()));
            //String val = props.getProperty(match.group().replaceAll("\\$|\\{|\\}", ""));
            String val = appProperties.getProperty(match.group().replaceAll("\\$|\\{|\\}", ""));
            appLog.info(String.format("resolving var reference %s to %s", match.group(), val));

            if (val != null) match.appendReplacement(output, val);
        }

        if (output.length() > 0) {
            //appLog.info(String.format("Replacing updated property %s=%s", entry.getKey(), newVar));
            match.appendTail(output);
        } else {
            output.append(property);
        }
    }

    class TaskContext {

        private volatile boolean active = false;

        public final String name;
        public final String path;
        public final Properties properties;
        public final float averageRate, burstProbability;
        public final int rateSmoothing, maxRetry;
        public final int minBurst, maxBurst;
        public final int minViewDelay, maxViewDelay;
        public final TxModel txModel;
        public final SessionModel sessionModel;
        public final SqlSession.Mode bulkCommitMode;
        public final boolean instrumentSQL;
        public final List<String> dbClearSQL;
        public final List<String> dbInitSQL;
        public final String catchBlock;
        public final Class<Runnable> updateTaskType;
        public final Class<Runnable> queryTaskType;
        public final List<String> updateSql;
        public final List<String> updateActions;
        public final List<String> querySql;
        public final List<String> queryActions;
        public final String[] updateValuesURI;
        public final String[] queryValuesURI;
        public final List<ArrayBlockingQueue<String[]>> updateValues;
        public final List<ArrayBlockingQueue<String[]>> queryValues;

        public final long averageDelay;

        public long[] timestamp;
        public int[] count;

        AtomicLong totalUpdates = new AtomicLong();
        AtomicLong totalUpdateTime = new AtomicLong();

        AtomicLong totalQueries = new AtomicLong();
        AtomicLong totalQueryRecords = new AtomicLong();
        AtomicLong totalQueryTime = new AtomicLong();

        public TaskContext(String path, Properties properties)
                throws Exception {
            this.path = path;
            this.properties = properties;

            name = properties.getProperty(TASK_NAME);

            appLog.info(String.format("Task %s properties: %s", name, properties));

            averageRate = Float.parseFloat(properties.getProperty(AVERAGE_RATE));
            minBurst = Integer.parseInt(properties.getProperty(MIN_BURST));
            maxBurst = Integer.parseInt(properties.getProperty(MAX_BURST));
            rateSmoothing = Integer.parseInt(properties.getProperty(RATE_SMOOTHING));
            maxRetry = Integer.parseInt(properties.getProperty(MAX_RETRY));
            instrumentSQL = Boolean.parseBoolean(properties.getProperty(DB_INSTRUMENT_SQL));

            minViewDelay = Integer.parseInt(properties.getProperty(MIN_VIEW_DELAY));
            int delay = Integer.parseInt(properties.getProperty(MAX_VIEW_DELAY));
            maxViewDelay = (delay > 0 && delay < minViewDelay ? minViewDelay : delay);

            burstProbability = (minBurst < maxBurst
                    ? Float.parseFloat(properties.getProperty(BURST_PROBABILITY_PERCENT))
                    : 0);

            if (maxBurst <= minBurst) {
                appLog.info(String.format("maxBurst (%d) <= minBurst (%d); burst disabled", maxBurst, minBurst));
            }

            TxModel model;
            try {
                model = Enum.valueOf(TxModel.class, properties.getProperty(TX_MODEL));
            } catch (Exception e) {
                model = TxModel.DISCRETE;
            }
            txModel = model;

            SessionModel smodel;
            try {
                smodel = Enum.valueOf(SessionModel.class, properties.getProperty(SESSION_MODEL));
            } catch (Exception e) {
                smodel = SessionModel.POOLED;
            }
            sessionModel = smodel;

            SqlSession.Mode mode;
            try {
                mode = Enum.valueOf(SqlSession.Mode.class, properties.getProperty(BULK_COMMIT_MODE));
            } catch (Exception e) {
                mode = SqlSession.Mode.BATCH;
            }
            bulkCommitMode = mode;

            String typeName = properties.getProperty(UPDATE_TASK_CLASS);
            updateTaskType = (Class<Runnable>) Class.forName(typeName);
            updateSql = parseScript(properties.getProperty(DB_UPDATE_SQL));

            // extract any actions from the sql
            if (updateSql != null && updateSql.size() > 0) {
                updateActions = new ArrayList<String>(updateSql.size());
                for (int sx = 0; sx < updateSql.size(); sx++) {
                    String statement = updateSql.get(sx);
                    if (statement.charAt(0) == '{' && statement.endsWith("}")) {
                        appLog.info("found action: " + statement);
                        updateActions.add(statement);
                        updateSql.remove(sx);
                        sx--;
                    } else {
                        updateActions.add(" ");
                    }
                }
                appLog.info("update sql now=" + updateSql.toString());
            } else {
                updateActions = null;
            }

            typeName = properties.getProperty(QUERY_TASK_CLASS);
            queryTaskType = (Class<Runnable>) Class.forName(typeName);
            querySql = parseScript(properties.getProperty(DB_QUERY_SQL));

            // extract any actions from the sql
            if (querySql != null && querySql.size() > 0) {
                queryActions = new ArrayList<String>(querySql.size());
                for (int sx = 0; sx < querySql.size(); sx++) {
                    String statement = querySql.get(sx);
                    if (statement.charAt(0) == '{' && statement.endsWith("}")) {
                        appLog.info("found action: " + statement);
                        queryActions.add(statement);
                        querySql.remove(sx);
                        sx--;
                    } else {
                        queryActions.add(" ");
                    }
                }
                appLog.info("query sql now=" + querySql.toString());
            } else {
                queryActions = null;
            }


            updateValuesURI = properties.getProperty(DB_UPDATE_VALUES).split("\n");
            queryValuesURI = properties.getProperty(DB_QUERY_VALUES).split("\n");

            appLog.info("updateThreads= " + updateThreads + "; queryThreads= " + queryThreads);

            updateValues = new ArrayList<ArrayBlockingQueue<String[]>>(updateValuesURI.length);
            for (int ix = 0; ix < updateValuesURI.length; ix++) {
                if (updateValuesURI[ix].trim().startsWith("none://"))
                    updateValues.add(null);
                else
                    updateValues.add(new ArrayBlockingQueue<String[]>(10 * updateThreads));
            }

            queryValues = new ArrayList<ArrayBlockingQueue<String[]>>(queryValuesURI.length);
            for (int ix = 0; ix < queryValuesURI.length; ix++) {
                if (queryValuesURI[ix].trim().startsWith("none://"))
                    queryValues.add(null);
                else
                    queryValues.add(new ArrayBlockingQueue<String[]>(10 * queryThreads));
            }

            catchBlock = properties.getProperty(DB_CATCH_BLOCK);
            dbClearSQL = parseScript(properties.getProperty(DB_CLEAR_SQL));
            dbInitSQL = parseScript(properties.getProperty(DB_INIT_SQL));

            averageDelay = (long) (Millis2Seconds / averageRate);
            timestamp = new long[rateSmoothing];
            count = new int[rateSmoothing];

            timestamp[0] = System.currentTimeMillis();

            active = true;
        }

        public void setActive(boolean active)
        {
            appLog.info("task active set to " + active);
            this.active = active;
        }

        public boolean isActive()
        { return active; }

        public void getMoreValues() {
            valueGenerator.awaken();
        }

        public void updateTimes(Controller.TaskType type, long time, long statements, long rows)
            throws Exception
        {
            switch (type) {
                case UPDATE:
                    totalUpdates.addAndGet(statements);
                    totalUpdateTime.addAndGet(time);

                    totalStatements.addAndGet(statements);
                    totalStatementTime.addAndGet(time);
                    break;

                case QUERY:
                    totalQueries.addAndGet(statements);
                    totalQueryRecords.addAndGet(rows);
                    totalQueryTime.addAndGet(time);

                    totalRetrieved.addAndGet(statements);
                    totalRetrievedRecords.addAndGet(rows);
                    totalRetrieveTime.addAndGet(time);
                    break;

                default:
                    throw new ConfigurationException("Invalid task type %s", type.name());
            }
        }

        public long schedule()
            throws Exception
        {
            long now = System.currentTimeMillis();

            if (! active) {
                appLog.info("Task inactive: " + name);
                return -1;
            }

            int first = 0;
            int last = timestamp.length-1;

            // how far away is the last scheduled task?
            long delay = timestamp[last] - now;

            // if the last task is in the future, we still have a task queued - don't queue any more
            if (delay > 0) return delay;

            // move the rate calculation window forward
            if (timestamp[first+1] == 0) timestamp[first+1] = timestamp[first];
            System.arraycopy(timestamp, first+1, timestamp, first, timestamp.length-1);
            System.arraycopy(count, first+1, count, first, count.length-1);

            // randomly create a burst
            if (burstProbability > 0 && Percent * random.nextFloat() <= burstProbability) {
                int burstSize = minBurst + random.nextInt(maxBurst - minBurst);
                appLog.info(String.format("Task %s: creating burst of %d", name, burstSize));

                delay = 0;

                for (int bx = 0; bx < burstSize; bx++) {
                    scheduleTasks(delay);
                }

                count[last] = burstSize;
            }

            else if (averageRate > 0) {
                long duration = timestamp[last] - timestamp[0];

                int totalCount = 0;
                for (int c : count) totalCount += c;

                double currentRate = (Millis2Seconds * totalCount) / duration;

                delay = (long) (averageDelay * (currentRate / averageRate));

                appLog.info(String.format("Task %s: current Rate= %.2f; scheduling for now + %,d ms", name, currentRate, delay));

                if (timingSpeedup > 1) {
                    delay /= timingSpeedup;
                    appLog.info(String.format("Warp-drive: speedup %f; scheduling %s for now + %d ms", timingSpeedup, name, delay));
                }

                scheduleTasks(delay);

                count[last] = 1;
            }

            else {
                delay = 0;
                scheduleTasks(delay);
                count[last] = 1;
            }

            timestamp[last] = now + delay;
            return delay;
        }

        protected void scheduleTasks(long delay)
            throws Exception
        {
            if (!queryOnly) {

                if (delay > 0)
                    taskExecutor.schedule(newTask(TaskType.UPDATE), delay, TimeUnit.DAYS.MILLISECONDS);
                else
                    taskExecutor.execute(newTask(TaskType.UPDATE));
            }

            if (queryOnly || (minViewDelay > 0 && maxViewDelay > 0)) {
                scheduleViewTask(unique);
            }
        }

        protected void scheduleViewTask(long unique)
            throws Exception
        {
            long delay = (minViewDelay > 0 || maxViewDelay > 0)
                ? minViewDelay + random.nextInt(maxViewDelay - minViewDelay)
                : averageDelay;

            // implement warp-drive...
            if (timingSpeedup > 1) delay /= timingSpeedup;

            taskExecutor.schedule(newTask(TaskType.QUERY), (long) delay, TimeUnit.SECONDS);

            appLog.info(String.format("Scheduled View task for now +%d", delay));
        }

        protected List<String> parseScript(String script)
            throws Exception
        {

            if (script == null || script.length() == 0)
                return null;

            StringBuilder statement = new StringBuilder(2048);
            StringBuilder varList = new StringBuilder(2048);

            List<String> statements;
            List<String> sql = new ArrayList<String>(1024);

            // for diagnostics...
            int lineNumber = 0;

            boolean multiLine = false;

            statements = new LinkedList<String>(Arrays.asList(script.split(";")));
            for (int sx = 0; sx < statements.size(); sx++) {
                String stmnt = statements.get(sx).trim();

                System.out.println("stmnt=" + stmnt);

                // support sql in discrete files
                if (stmnt.trim().toLowerCase().startsWith("file://")) {
                    statements.remove(sx);

                    List<String> scriptlet = Arrays.asList(readScript(script).split(";"));
                    appLog.info(String.format("scriptlet (%d lines)=", scriptlet.size(), scriptlet.toString()));

                    if (sx < statements.size()-1)
                        statements.addAll(sx, scriptlet);
                    else
                        statements.addAll(scriptlet);

                    // continue with the replaced line
                    sx--;
                    continue;
                }

                System.out.println("stmnt has become: " + stmnt);

                // extract action, and store it as a statement annotation (on a preceding line)
                Matcher match = ScriptStatementPattern.matcher(stmnt);
                if (match.find()) {
                    String type = match.group(2);
                    if (type == null || type.length() == 0) type = "s";
                    sql.add(String.format("{%s%s%s}", match.group(3), type, match.group(1)));
                    appLog.info("action=" + sql.get(sql.size()-1));
                    stmnt = match.group(4);
                }

                System.out.println("extracted stmnt is: " + stmnt);

                // assemble multi-statement commands
                if (stmnt.toUpperCase().startsWith("CREATE PROCEDURE")) {
                    multiLine = true;
                    lineNumber = 0;
                    varList.setLength(0);
                    varList.append("''");
                }

                else if (stmnt.toUpperCase().startsWith("CREATE TRIGGER")) {
                    multiLine = true;
                    lineNumber = 0;
                    varList.setLength(0);
                    varList.append("''");
                }

                else if (stmnt.equalsIgnoreCase("END_PROCEDURE")) {
                    if (instrumentSQL) stmnt = String.format(catchBlock, "'*none in scope*'") + ";\n" + stmnt;
                    multiLine = false;
                }

                else if (stmnt.equalsIgnoreCase("END_TRIGGER")) {
                    if (instrumentSQL) stmnt = String.format(catchBlock, "'*none in scope'") + ";\n" + stmnt;
                    multiLine = false;
                }

                appLog.info("stmnt is now: " + stmnt);

                if (stmnt != null && stmnt.length() > 0) {

                    String[] lines = stmnt.split("\n");
                    boolean foundSQL = false;

                    for (int lx = 0; lx < lines.length; lx++) {
                        String line = lines[lx].trim();
                        lineNumber++;

                        if (line.startsWith("//") || line.startsWith("--")) {
                            continue;
                        }

                        // skip "GO" commands
                        else if (line.equalsIgnoreCase("GO")) {
                            multiLine = false;
                            lines[lx] = "";
                        }

                        // insert TRY immediately after the AS element
                        else if (multiLine && instrumentSQL) {

                            // insert debug vars and a try-catch block at the very start
                            if (line.toUpperCase().startsWith("AS")) {
                                lines[lx] = "AS\n   VAR \"_debug_line_number\" INT, \"_debug_line_text\" STRING;\nTRY";
                            }

                            // since statements can be multi-line, identify the first obvious SQL statement in the block
                            else if (foundSQL == false
                                    && (line.toUpperCase().startsWith("UPDATE")
                                    || line.toUpperCase().startsWith("SELECT")
                                    || line.toUpperCase().startsWith("INSERT")
                                    || line.toUpperCase().startsWith("DELETE")
                            )) {
                                lines[lx] = String.format("   \"_debug_line_number\" = %d; \"_debug_line_text\" = '%s';\n   TRY\n\t%2$s",
                                        lineNumber, line.replaceAll("'", ""));

                                int last = lines.length-1;
                                lines[last] = lines[last] + ";\n   " + String.format(catchBlock, varList);
                                foundSQL = true;
                            }

                            // maintain a list of all VAR declarations - for printout on error
                            else if (line.toUpperCase().startsWith("VAR")) {
                                for (String var : line.split(",")) {
                                    if (var.toUpperCase().startsWith("VAR"))
                                        var = var.substring("VAR".length()).trim();

                                    String[] token = var.trim().split(" ");

                                    if (varList.length() > 0) varList.append("||\n");
                                    varList.append(String.format("'%s = '||%s", token[0], token[0]));
                                }
                            }
                        }
                    }

                    // now assemble the statement
                    if (statement.length() > 0) {
                        if (instrumentSQL && multiLine) {
                            statement.append(";\n   \"_debug_line_number\" = '").append(lineNumber).append("'");
                            statement.append(";\n   \"_debug_line_text\" = '").append(lines[0].replaceAll("'", "")).append("'");
                        }

                        statement.append("; ");
                    }

                    // reassemble the statement from the lines
                    for (String line : lines) {
                        if (line.length() > 0) statement.append('\n').append(line);
                    }
                }

                //log.info("multiLine? " + multiLine);
                if (multiLine)
                    continue;

                appLog.info("statement = " + statement.toString() + '\n');

                sql.add(statement.toString());
                statement.setLength(0);
            }

            if (instrumentSQL) {
                appLog.info("Instrumented SQL init script");
                for (String line : sql) System.out.println("\t" + line);
            }

            return sql;
        }

        protected String readScript(String path)
            throws Exception
        {
            try (LineNumberReader reader = new LineNumberReader(new FileReader(path.substring("file://".length())))) {
                StringBuffer buffer = new StringBuffer(1024);
                String line= null;
                do {
                    line = reader.readLine();
                    appLog.info("read script line: " + line);
                    if (line != null && line.length() > 0) buffer.append(line).append('\n');
                } while (line != null);

                appLog.info("read file: " + buffer.toString());

                // resolve any references in the script
                resolveReferences(buffer.toString(), buffer);

                appLog.info("resolved file: " + buffer.toString());

                return buffer.toString();
            } catch (Exception e) {
                throw new ConfigurationException("Error reading SQL script file: %s\n(%s)", path, e.toString());
            }
        }

        protected Runnable newTask(TaskType taskType)
            throws Exception
        {
            String errors = "";

            Class<? extends Runnable> type;
            List<String> sql;


            switch (taskType) {
                case UPDATE:
                    type = updateTaskType;
                    sql = updateSql;
                    break;

                case QUERY:
                    type = queryTaskType;
                    sql = querySql;
                    break;

                default:
                    throw new ConfigurationException("Unrecognised TaskType: %s", taskType.name());
            }

            try { return type.getConstructor(TaskType.class, TaskContext.class).newInstance(taskType, this); }
            catch (Exception e) { errors = errors + " (TaskType, TaskContext);"; }

            try { return type.getConstructor(List.class, Properties.class).newInstance(sql, properties); }
            catch (Exception e) { errors = errors + " (List<String>, Properties);"; }

            try { return type.getConstructor(Properties.class).newInstance(properties); }
            catch (Exception e) { errors = errors + " (Properties);"; }

            try { return type.getConstructor(String.class).newInstance(name); }
            catch (Exception e) { errors = errors + " (String);"; }

            try { return type.getConstructor().newInstance(); }
            catch (Exception e) {
                throw new ConfigurationException("Task construction failure. Could not find a suitable Constructor for %s. Tried %s",
                        type.getName(), errors);
            }
        }
    }
}