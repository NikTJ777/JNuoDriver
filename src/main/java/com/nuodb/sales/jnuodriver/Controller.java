package com.nuodb.sales.jnuodriver;

import com.nuodb.sales.jnuodriver.dao.PersistenceException;
import com.nuodb.sales.jnuodriver.service.GenericService;
import com.nuodb.sales.jnuodriver.service.Service;

import java.io.IOException;
import java.io.InputStream;
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

    ScheduledExecutorService taskExecutor;
    Service service;

    Properties fileProperties;
    Properties appProperties;

    long runTime;
    float averageRate, timingSpeedup;
    int minViewAfterInsert, maxViewAfterInsert;
    int minGroups, maxGroups;
    int minData, maxData;
    float burstProbability;
    int minBurst, maxBurst;
    int maxQueued, queryBackoff;
    boolean initDb = false;
    boolean queryOnly = false;

    TxModel txModel;
    SqlSession.Mode bulkCommitMode;

    AtomicLong totalInserts = new AtomicLong();
    AtomicLong totalInsertTime = new AtomicLong();

    AtomicLong totalQueries = new AtomicLong();
    AtomicLong totalQueryRecords = new AtomicLong();
    AtomicLong totalQueryTime = new AtomicLong();

    long unique;

    long totalEvents;
    long wallTime;

    private Random random = new Random();

    private static final Properties defaultProperties = new Properties();

    public static final String PROPERTIES_PATH =    "properties.path";
    public static final String AVERAGE_RATE =       "timing.rate";
    public static final String MIN_VIEW_DELAY =     "timing.min.view.delay";
    public static final String MAX_VIEW_DELAY =     "timing.max.view.delay";
    public static final String TIMING_SPEEDUP =     "timing.speedup";
    public static final String INSERT_THREADS =     "insert.threads";
    public static final String QUERY_THREADS =      "query.threads";
    public static final String MAX_QUEUED =         "max.queued";
    public static final String DB_PROPERTIES_PATH = "db.properties.path";
    public static final String RUN_TIME =           "run.time";
    public static final String MIN_GROUPS =         "min.groups";
    public static final String MAX_GROUPS =         "max.groups";
    public static final String MIN_DATA =           "min.data";
    public static final String MAX_DATA =           "max.data";
    public static final String BURST_PROBABILITY_PERCENT = "burst.probability.percent";
    public static final String MIN_BURST =          "min.burst";
    public static final String MAX_BURST =          "max.burst";
    public static final String DB_INIT =            "db.init";
    public static final String DB_INIT_SQL =        "db.init.sql";
    public static final String DB_SCHEMA =          "db.schema";
    public static final String TX_MODEL =           "tx.model";
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

    private static Logger appLog = Logger.getLogger("JNuoTest");
    private static Logger insertLog = Logger.getLogger("InsertLog");
    private static Logger viewLog = Logger.getLogger("EventViewer");

    private static final double Nano2Millis = 1000000.0;
    private static final double Nano2Seconds = 1000000000.0;
    private static final double Millis2Seconds = 1000.0;

    private static final long Millis = 1000;

    private static final float Percent = 100.0f;

    public Controller() {
        defaultProperties.setProperty(PROPERTIES_PATH, "classpath://properties/Application.properties");
        defaultProperties.setProperty(DB_PROPERTIES_PATH, "classpath://properties/Database.properties");
        defaultProperties.setProperty(AVERAGE_RATE, "0");
        defaultProperties.setProperty(MIN_VIEW_DELAY, "0");
        defaultProperties.setProperty(MAX_VIEW_DELAY, "0");
        defaultProperties.setProperty(TIMING_SPEEDUP, "1");
        defaultProperties.setProperty(INSERT_THREADS, "1");
        defaultProperties.setProperty(QUERY_THREADS, "1");
        defaultProperties.setProperty(MAX_QUEUED, "0");
        defaultProperties.setProperty(MIN_GROUPS, "1");
        defaultProperties.setProperty(MAX_GROUPS, "5");
        defaultProperties.setProperty(MIN_DATA, "500");
        defaultProperties.setProperty(MAX_DATA, "3500");
        defaultProperties.setProperty(BURST_PROBABILITY_PERCENT, "0");
        defaultProperties.setProperty(MIN_BURST, "0");
        defaultProperties.setProperty(MAX_BURST, "0");
        defaultProperties.setProperty(RUN_TIME, "5");
        defaultProperties.setProperty(TX_MODEL, "DISCRETE");
        defaultProperties.setProperty(COMMUNICATION_MODE, "SQL");
        defaultProperties.setProperty(BULK_COMMIT_MODE, "BATCH");
        defaultProperties.setProperty(SP_NAME_PREFIX, "importer_");
        defaultProperties.setProperty(DB_INIT, "false");
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
        loadProperties(prop, PROPERTIES_PATH);

        // now load database properties into second (higher-priority) level of fileProperties
        loadProperties(fileProperties, DB_PROPERTIES_PATH);

        appLog.info(String.format("appProperties: %s", appProperties));

        //resolveReferences(appProperties);

        StringBuilder builder = new StringBuilder(1024);
        builder.append("\n***************** Resolved Properties ********************\n");
        String[] keys = appProperties.stringPropertyNames().toArray(new String[0]);
        Arrays.sort(keys);
        for (String key : keys) {
            builder.append(String.format("%s = %s\n", key, appProperties.getProperty(key)));
        }
        appLog.info(builder.toString() + "**********************************************************\n");

        runTime = Integer.parseInt(appProperties.getProperty(RUN_TIME)) * Millis;
        averageRate = Float.parseFloat(appProperties.getProperty(AVERAGE_RATE));
        minViewAfterInsert = Integer.parseInt(appProperties.getProperty(MIN_VIEW_DELAY));
        maxViewAfterInsert = Integer.parseInt(appProperties.getProperty(MAX_VIEW_DELAY));
        timingSpeedup = Float.parseFloat(appProperties.getProperty(TIMING_SPEEDUP));
        minGroups = Integer.parseInt(appProperties.getProperty(MIN_GROUPS));
        maxGroups = Integer.parseInt(appProperties.getProperty(MAX_GROUPS));
        minData = Integer.parseInt(appProperties.getProperty(MIN_DATA));
        maxData = Integer.parseInt(appProperties.getProperty(MAX_DATA));
        burstProbability = Float.parseFloat(appProperties.getProperty(BURST_PROBABILITY_PERCENT));
        minBurst = Integer.parseInt(appProperties.getProperty(MIN_BURST));
        maxBurst = Integer.parseInt(appProperties.getProperty(MAX_BURST));
        maxQueued = Integer.parseInt(appProperties.getProperty(MAX_QUEUED));
        initDb = Boolean.parseBoolean(appProperties.getProperty(DB_INIT));
        queryOnly = Boolean.parseBoolean(appProperties.getProperty(QUERY_ONLY));
        queryBackoff = Integer.parseInt(appProperties.getProperty(QUERY_BACKOFF));

        String threadParam = appProperties.getProperty(INSERT_THREADS);
        int insertThreads = (threadParam != null ? Integer.parseInt(threadParam) : 1);

        threadParam = appProperties.getProperty(QUERY_THREADS);
        int queryThreads = (threadParam != null ? Integer.parseInt(threadParam) : 1);

        int totalThreads = insertThreads + queryThreads;

        if (maxViewAfterInsert > 0 && maxViewAfterInsert < minViewAfterInsert) {
            maxViewAfterInsert = minViewAfterInsert;
        }

        if (maxBurst <= minBurst) {
            appLog.info(String.format("maxBurst (%d) <= minBurst (%d); burst disabled"));
            burstProbability = minBurst = maxBurst = 0;
        }

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

        service = new GenericService();

        try { txModel = Enum.valueOf(TxModel.class, appProperties.getProperty(TX_MODEL)); }
        catch (Exception e) { txModel = TxModel.DISCRETE; }

        try { bulkCommitMode = Enum.valueOf(SqlSession.Mode.class, appProperties.getProperty(BULK_COMMIT_MODE)); }
        catch (Exception e) { bulkCommitMode = SqlSession.Mode.BATCH; }

        taskExecutor = Executors.newScheduledThreadPool(totalThreads);

        if ("true".equalsIgnoreCase(appProperties.getProperty("check.config"))) {
            System.out.println("CheckConfig called - nothing to do; exiting.");
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
                String lastEventId = service.getSequence("");
                unique = Long.parseLong(lastEventId) + 1;
                appLog.info(String.format("lastEventID = %s", lastEventId));
            }
        }

    }

    /**
     * Start the controller.
     *
     * @throws InterruptedException
     */
    public void run()
        throws InterruptedException
    {
        long start = System.currentTimeMillis();
        long endTime = start + runTime;
        long now;

        double currentRate = 0.0;
        long averageSleep = (long) (Millis2Seconds / averageRate);

        totalEvents = 0;
        wallTime = 0;

        double burstRate = 0.0;
        int burstSize = 0;

        // ensure that first sample time is different from start time...
        long settleTime = 2 * Millis;
        appLog.info(String.format("Settling for %d: ", settleTime));
        Thread.sleep(settleTime);

        // just run some queries
        if (queryOnly) {

            appLog.info("Query-Only...");

            long eventId = 1;

            while (System.currentTimeMillis() < endTime) {
                queryExecutor.schedule(new EventViewTask(eventId++), 2, TimeUnit.MILLISECONDS);
                totalEvents++;

                appLog.info(String.format("Processed %,d events containing %,d records in %.2f secs"
                                + "\n\tThroughput:\t%.2f events/sec at %.2f ips;"
                                + "\n\tSpeed:\t\t%,d inserts in %.2f secs = %.2f ips"
                                + "\n\tQueries:\t%,d queries got %,d records in %.2f secs at %.2f qps",
                        totalEvents, totalInserts.get(), (wallTime / Millis2Seconds), (Millis2Seconds * totalEvents / wallTime), (Millis2Seconds * totalInserts.get() / wallTime),
                        totalInserts.get(), (totalInsertTime.get() / Nano2Seconds), (Nano2Seconds * totalInserts.get() / totalInsertTime.get()),
                        totalQueries.get(), totalQueryRecords.get(), (totalQueryTime.get() / Nano2Seconds), (Nano2Seconds * totalQueries.get() / totalQueryTime.get())));

                //if (((ThreadPoolExecutor) queryExecutor).getQueue().size() > 10) {
                if (totalEvents + 10 > totalQueries.get()) {
                    appLog.info(String.format("%d queries waiting - sleeping", totalEvents - totalQueries.get()));
                    try {
                        Thread.sleep((200));
                    } catch (InterruptedException e) {
                    }
                }
            }

            return;
        }


        do {
            insertExecutor.execute(new EventGenerator(unique++));

            totalEvents++;

            appLog.info(String.format("Event scheduled. Queue size=%d", ((ThreadPoolExecutor) insertExecutor).getQueue().size()));

            now = System.currentTimeMillis();
            currentRate = (Millis2Seconds * totalEvents) / (now - start);

            appLog.info(String.format("now=%d; endTime=%d;  elapsed=%d; time left=%d", now, endTime, now - start, endTime - now));

            // randomly create a burst
            if (burstSize == 0 && burstProbability > 0 && Percent * random.nextFloat() <= burstProbability) {
                burstSize = minBurst + random.nextInt(maxBurst - minBurst);
                appLog.info(String.format("Creating burst of %d", burstSize));
            }

            if (burstSize > 0) {
                burstSize--;
            } else {
                if (averageRate > 0) {
                    long sleepTime = (long) (averageSleep * (currentRate / averageRate));
                    if (now + sleepTime > endTime) sleepTime = 1 * Millis;

                    appLog.info(String.format("Current Rate= %.2f; sleeping for %,d ms", currentRate, sleepTime));

                    if (timingSpeedup > 1) {
                        sleepTime /= timingSpeedup;
                        appLog.info(String.format("Warp-drive: speedup %f; sleeping for %d ms", timingSpeedup, sleepTime));
                    }

                    Thread.sleep(sleepTime);
                }

                while (maxQueued >= 0 && ((ThreadPoolExecutor) insertExecutor).getQueue().size() > maxQueued) {
                    appLog.info(String.format("Queue size %d is over limit %d - sleeping", ((ThreadPoolExecutor) insertExecutor).getQueue().size(), maxQueued));
                    Thread.sleep(1 * Millis / (((ThreadPoolExecutor) insertExecutor).getQueue().size() > 1 ? 2 : 20));
                }

                appLog.info(String.format("Sleeping done. Queue size=%d", ((ThreadPoolExecutor) insertExecutor).getQueue().size()));

            }

            wallTime = System.currentTimeMillis() - start;

            appLog.info(String.format("Processed %,d events containing %,d records in %.2f secs"
                            + "\n\tThroughput:\t%.2f events/sec at %.2f ips;"
                            + "\n\tSpeed:\t\t%,d inserts in %.2f secs = %.2f ips"
                            + "\n\tQueries:\t%,d queries got %,d records in %.2f secs at %.2f qps",
                    totalEvents, totalInserts.get(), (wallTime / Millis2Seconds), (Millis2Seconds * totalEvents / wallTime), (Millis2Seconds * totalInserts.get() / wallTime),
                    totalInserts.get(), (totalInsertTime.get() / Nano2Seconds), (Nano2Seconds * totalInserts.get() / totalInsertTime.get()),
                    totalQueries.get(), totalQueryRecords.get(), (totalQueryTime.get() / Nano2Seconds), (Nano2Seconds * totalQueries.get() / totalQueryTime.get())));


        } while (System.currentTimeMillis() < endTime);
    }

    public void close()
    {
        try {
            taskExecutor.shutdownNow();
            taskExecutor.awaitTermination(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            System.out.println("Interrupted while waiting for shutdown - exiting");
        }

        appLog.info(String.format("Exiting with %d items remaining in the queue.\n\tProcessed %,d events containing %,d records in %.2f secs"
                        + "\n\tThroughput:\t%.2f events/sec at %.2f ips;"
                        + "\n\tSpeed:\t\t%,d inserts in %.2f secs = %.2f ips"
                        + "\n\tQueries:\t%,d queries got %,d records in %.2f secs at %.2f qps",
                ((ThreadPoolExecutor) insertExecutor).getQueue().size(),
                totalEvents, totalInserts.get(), (wallTime / Millis2Seconds), (Millis2Seconds * totalEvents / wallTime), (Millis2Seconds * totalInserts.get() / wallTime),
                totalInserts.get(), (totalInsertTime.get() / Nano2Seconds), (Nano2Seconds * totalInserts.get() / totalInsertTime.get()),
                totalQueries.get(), totalQueryRecords.get(), (totalQueryTime.get() / Nano2Seconds), (Nano2Seconds * totalQueries.get() / totalQueryTime.get())));

        //appLog.info(String.format("Exiting with %d items remaining in the queue.\n\tProcessed %,d events containing %,d records in %.2f secs\n\tThroughput:\t%.2f events/sec at %.2f ips;\n\tSpeed:\t\t%,d inserts in %.2f secs = %.2f ips",
        //        ((ThreadPoolExecutor) insertExecutor).getQueue().size(),
        //        totalEvents, totalInserts/*.get()*/, (wallTime / Millis2Seconds), (Millis2Seconds * totalEvents / wallTime), (Millis2Seconds * totalInserts/*.get()*/ / wallTime),
        //        totalInserts/*.get()*/, (totalInsertTime/*.get()*/ / Nano2Seconds), (Nano2Seconds * totalInserts/*.get()*/ / totalInsertTime/*.get()*/)));

        SqlSession.cleanup();
    }

    protected void initializeDatabase() {
        String script = appProperties.getProperty(DB_INIT_SQL);
        if (script == null) appLog.info("Somehow script is NULL");

        appLog.info(String.format("running init sql (length: %d): %s", script.length(), script));
        try (SqlSession session = new SqlSession(SqlSession.Mode.AUTO_COMMIT)) {
            session.execute(script);
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

    protected void loadProperties(Properties props, String key)
        throws MalformedURLException, IOException
    {
        assert key != null && key.length() > 0;

        String path = appProperties.getProperty(key);
        if (path == null || path.length() == 0) {
            appLog.info(String.format("loadProperties: key %s not in app properties", key));
            return;
        }

        appLog.info(String.format("loading properties: %s from %s", key, path));

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

        appLog.info(String.format("Loaded properties %s: %s", key, props));
    }

    protected void resolveReferences(Properties props) {
        Pattern var = Pattern.compile("\\$\\{[^\\}]+\\}");
        StringBuffer newVar = new StringBuffer();

        for (Map.Entry<Object, Object> entry : props.entrySet()) {

            Matcher match = var.matcher(entry.getValue().toString());
            while (match.find()) {
                //appLog.info(String.format("match.group=%s", match.group()));
                //String val = props.getProperty(match.group().replaceAll("\\$|\\{|\\}", ""));
                String val = appProperties.getProperty(match.group().replaceAll("\\$|\\{|\\}", ""));
                appLog.info(String.format("resolving var reference %s to %s", match.group(), val));

                if (val != null) match.appendReplacement(newVar, val);
            }

            if (newVar.length() > 0) {
                appLog.info(String.format("Replacing updated property %s=%s", entry.getKey(), newVar));
                match.appendTail(newVar);
                entry.setValue(newVar.toString());
                newVar.setLength(0);
            }
        }
    }

    protected void scheduleViewTask(long eventId) {
        if (minViewAfterInsert <= 0 || maxViewAfterInsert <= 0) return;

        long delay = (minViewAfterInsert + random.nextInt(maxViewAfterInsert - minViewAfterInsert));

        // implement warp-drive...
        if (timingSpeedup > 1) delay /= timingSpeedup;

        taskExecutor.schedule(new EventViewTask(eventId), (long) delay, TimeUnit.SECONDS);

        appLog.info(String.format("Scheduled EventViewTask for now +%d", delay));
    }

    class EventGenerator implements Runnable {

        private final long unique;
        private Date dateStamp = new Date();

        EventGenerator(long unique) {
            this.unique = unique;
        }

        public void run() {

            long start = System.nanoTime();

            long ownerId;
            long eventId;
            long groupId;

            // open optional outer session/transaction
            SqlSession outerTx = (txModel == TxModel.UNIFIED ? new SqlSession(SqlSession.Mode.TRANSACTIONAL) : null);

            try (SqlSession session = new SqlSession(SqlSession.Mode.AUTO_COMMIT)) {
                ownerId = ownerRepository.persist(generateOwner());
                System.out.println("\n------------------------------------------------");
                report("Owner", 1, System.nanoTime() - start);

                eventId = eventRepository.persist(generateEvent(ownerId));
            }

            int groupCount = minGroups + random.nextInt(maxGroups - minGroups);
            appLog.info(String.format("Creating %d groups", groupCount));

            int total = 2 + groupCount;

            Map<String, Data> dataRows = new HashMap<String, Data>(maxData);

            // data records per group
            int dataCount = (minData + random.nextInt(maxData - minData)) / groupCount;
            appLog.info(String.format("Creating %d Data records @ %d records per group", dataCount * groupCount, dataCount));

            for (int gx = 0; gx < groupCount; gx++) {
                try (SqlSession session = new SqlSession(SqlSession.Mode.AUTO_COMMIT)) {
                    groupId = groupRepository.persist(generateGroup(eventId, gx));
                }

                total += dataCount;

                dataRows.clear();
                for (int dx = 0; dx < dataCount; dx++) {
                    Data data = generateData(groupId, dx);
                    dataRows.put(data.getInstanceUID(), data);
                }

                // in SP mode, the SP does the uniqueness testing - in BATCH mode we do it separately
                if (bulkCommitMode == SqlSession.Mode.BATCH) {
                    try (SqlSession session = new SqlSession(SqlSession.Mode.AUTO_COMMIT)) {
                        long uniqueRows = dataRepository.checkUniqueness(dataRows);

                        appLog.info(String.format("%d rows out of %d new rows are unique", uniqueRows, dataCount));

                        groupRepository.update(groupId, "dataCount", uniqueRows);
                    }
                }

                long dataStart = System.nanoTime();
                int count = 0;
                try (SqlSession session = new SqlSession(bulkCommitMode)) {
                    for (Data data : dataRows.values()) {
                        dataRepository.persist(data);
                        count++;
                    }
                    appLog.info(String.format("inserting %d data rows", count));
                } catch (Exception e) {
                    appLog.info(String.format("Error inserting data row %s", e.toString()));
                }

                report("Data Group", dataCount, System.nanoTime() - dataStart);
            }

            if (outerTx != null) outerTx.close();

            long duration = System.nanoTime() - start;
            report("All Data", total, duration);

            //totalInserts += total;
            //totalInsertTime += duration;

            totalInserts.addAndGet(total);
            totalInsertTime.addAndGet(duration);

            scheduleViewTask(eventId);
        }

        protected Owner generateOwner() {

            Owner owner = new Owner();
            owner.setCustomerId(unique % 200);
            owner.setOwnerGuid(String.format("Owner-GUID-%d", unique));
            owner.setDateCreated(dateStamp);
            owner.setLastUpdated(dateStamp);
            owner.setName(String.format("Owner-%d", unique));
            owner.setMasterAliasId(unique);
            owner.setRegion(unique % 2 == 0 ? "Region_A" : "Region_B");

            return owner;
            //return ownerRepository.persist(owner);
        }

        protected Event generateEvent(long ownerId) {

            Event ev = new Event();
            ev.setCustomerId(unique % 200);
            ev.setOwnerId(ownerId);
            ev.setEventGuid(String.format("Event-GUID-%d", unique));
            ev.setName(String.format("Event-%d", unique));
            ev.setDescription("Test data generated by CSNuoTest");
            ev.setDateCreated(dateStamp);
            ev.setLastUpdated(dateStamp);
            ev.setRegion(unique % 2 == 0 ? "Region_A" : "Region_B");

            return ev;
            //return eventRepository.persist(event);
        }

        protected Group generateGroup(long eventId, int index) {

            Group group = new Group();
            group.setEventId(eventId);
            group.setGroupGuid(String.format("Group-%d-%d", unique, index));
            //group.setGroupGuid(String.Join("-", "Group", unique, index);
            group.setDataCount(0);
            group.setDateCreated(dateStamp);
            group.setLastUpdated(dateStamp);
            group.setRegion(unique % 2 == 0 ? "Region_A" : "Region_B");
            group.setWeek(unique / 35000);

            return group;
            //return groupRepository.persist(group);
        }

        protected Data generateData(long groupId, int index) {
            //String suffix = string.Join("-", unique, groupId, index);
            //String instanceUID = "image-"+suffix;
            String instanceUID = String.format("image-%d-%d-%d", unique, groupId, index);

            Data data = new Data();
            data.setGroupId(groupId);
            data.setDataGuid("Data-" + instanceUID);
            data.setInstanceUID(instanceUID);
            data.setCreatedDateTime(dateStamp);
            data.setAcquiredDateTime(dateStamp);
            data.setVersion(0);
            data.setActive(true);
            data.setSizeOnDiskMB(65.5F);
            data.setRegionWeek(String.format("%s%d", (unique % 2 == 0 ? "Region_A-" : "Region_B-"), (unique / 35000)));

            return data;
        }

        private void report(String name, int count, long duration) {
            double rate = (count > 0 && duration > 0 ? Nano2Seconds * count / duration : 0);
            appLog.info(String.format("Run %d; generated %s (%,d records); duration=%.2f ms; rate=%.2f", unique, name, count, 1d / Nano2Millis * duration, rate));
        }
    }

    class EventViewTask implements Runnable {
        private final long eventId;

        public EventViewTask(long eventId) {
            this.eventId = eventId;
        }

        @Override
        public void run() {

            //long x = totalInserts;

            viewLog.info(String.format("Running view query for event %d", eventId));

            try (SqlSession session = new SqlSession(SqlSession.Mode.READ_ONLY, "Query")) {

                long start = System.nanoTime();
                EventDetails details = eventRepository.getDetails(eventId);
                if (details == null) return;

                long duration = System.nanoTime() - start;

                totalQueries.incrementAndGet();
                totalQueryRecords.addAndGet(details.getData().size());
                totalQueryTime.addAndGet(duration);

                appLog.info(String.format("Event viewed. Query response time= %.2f secs; %,d Data objects attached in %d groups.",
                        (duration / Nano2Seconds), details.getData().size(), details.getGroups().size()));

            } catch (PersistenceException e) {
                viewLog.info(String.format("Error retrieving Event: %s", e.toString()));
                e.printStackTrace(System.out);
            }


            if (queryBackoff > 0 && ((ThreadPoolExecutor) insertExecutor).getQueue().size() > maxQueued) {
                appLog.info(String.format("(query) Queue size > maxQueued (%d); sleeping for %d ms...", maxQueued, queryBackoff));
                try { Thread.sleep(queryBackoff); } catch (InterruptedException e) {}
            }
        }
    }
}