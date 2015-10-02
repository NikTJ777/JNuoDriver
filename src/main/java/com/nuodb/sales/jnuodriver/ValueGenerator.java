package com.nuodb.sales.jnuodriver;

import com.nuodb.sales.jnuodriver.dao.ConfigurationException;

import java.io.*;
import java.lang.reflect.Constructor;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by nik on 24/09/2015.
 */
public class ValueGenerator implements Runnable {

    private List<SetReader> input = new ArrayList<SetReader>(64);
    private List<ArrayBlockingQueue<String[]>> output = new ArrayList<ArrayBlockingQueue<String[]>>(64);
    private Map<String, Object> locals = new HashMap<String, Object>(256);

    private static Map<String, Constructor<? extends SetReader>> typeMap = new HashMap<String, Constructor<? extends SetReader>>(32);
    private static final long fullSleepTime = 20;
    private static final long emptySleepTime = 200;

    private volatile boolean running = false;

    public static final String[] EMPTY_STRING_ARRAY = new String[0];

    static final Logger log = Logger.getLogger(ValueGenerator.class.getName());

    static {
        try {
            addReader("none", NullSetReader.class.getName());
            addReader("file", FileSetReader.class.getName());
            addReader("format", FormatSetReader.class.getName());
        } catch (Exception e) {
            throw new RuntimeException("Erk! Invalid SetReader implementation (class undefined, or missing null constructor)\n" + e.toString());
        }
    }

    public static void addReader(String type, String className)
        throws Exception
    {
        Class<? extends SetReader> classType = (Class<? extends SetReader>) Class.forName(className);
        Constructor<? extends SetReader> ctor = classType.getConstructor();

        if (typeMap.containsKey(type)) {
            String oldType = typeMap.remove(type).getDeclaringClass().getName();
            log.info(String.format("Redefining SetReader for type %s from %s -> %s", type, oldType, className));
        }

        typeMap.put(type, ctor);
    }

    public void addSource(Controller.TaskContext context)
        throws Exception
    {
        for (int ix = 0; ix < context.updateValuesURI.length; ix++) {
            input.add(getReader(context.updateValuesURI[ix]));
            output.add(context.updateValues.get(ix));
        }

        for (int ix = 0; ix < context.queryValuesURI.length; ix++) {
            input.add(getReader(context.queryValuesURI[ix]));
            output.add(context.queryValues.get(ix));
        }
    }

    public void start() {
        Thread thread = new Thread(this);
        thread.setDaemon(true);
        thread.start();
    }

    public void stop() {
        running = false;
    }

    @Override
    public void run() {

        String[] set = null;
        int full, empty;
        long sleepTime;

        running = true;

        long unique = 0;
        long uniqueSet = 0;

        while (running) {

            full = empty = 0;

            locals.put("taskId", new Long(uniqueSet++));

            for (int ix = 0; ix < input.size(); ix++) {

                SetReader in = input.get(ix);
                ArrayBlockingQueue<String[]> out = output.get(ix);

                // skip if we have no room
                if (out.remainingCapacity() <= 0) {
                    full++;
                    continue;
                }

                // skip if we have no input
                if (! in.isOpen()) {
                    empty++;
                    continue;
                }

                locals.put("statementId", new Long(unique++));

                set = in.read(unique);

                // close the reader if it has reached EOF
                if (set == null || set.length == 0) {
                    in.close();
                    empty++;
                    continue;
                }

                log.info("values: " + Arrays.toString(set));

                if (set.length > 0) {
                    if (! out.offer(set)) {
                        log.info(String.format("Internal error: failed to add values to queue: %s", in.getName()));
                    }
                } else {
                    empty++;
                }
            }

            //log.info("input.size=" + input.size() + ";  empty=" + empty + "; full=" + full);
            sleepTime = 0;

            // if we are waiting for work to do - sleep a little
            if (empty + full >= input.size()) {
                sleepTime = (full > 0 ? fullSleepTime : emptySleepTime);
                log.info("No work done - sleeping for " + sleepTime);

                try { Thread.sleep(sleepTime); }
                catch (InterruptedException e) {}

                sleepTime = 0;
            }
        }

        // clean up
        for (SetReader reader : input) {
            reader.close();
        }

        log.info(String.format("Exiting (running = %b)", running));
    }

    protected SetReader getReader(String uri)
        throws Exception {
        log.info("getReader - uri = " + uri);

        String type = uri.substring(0, uri.indexOf(':'));

        try {
            SetReader reader = typeMap.get(type).newInstance();
            reader.setURI(uri);
            reader.setGlobalMap(locals);

            return reader;
        }
        catch (Exception e) {
            throw new ConfigurationException("Invalid SetReader type: %s:\n", type, e.toString());
        }
    }

    public interface SetReader {
        public void setURI(String uri) throws Exception;
        public void setGlobalMap(Map<String, Object> global);
        public String getName();
        public String[] read(long unique);
        public void close();
        public boolean isOpen();
    }

    public interface SetParser {
        public String[] parse(String line);
    }
}

class NullSetReader implements ValueGenerator.SetReader {
    private String uri;

    public NullSetReader() {}

    @Override
    public void setURI(String uri) throws Exception {
        this.uri = uri;
    }

    @Override
    public void setGlobalMap(Map<String, Object> global) {}

    @Override
    public String getName() {
        return uri;
    }

    @Override
    public String[] read(long unique) {
        return ValueGenerator.EMPTY_STRING_ARRAY;
    }

    @Override
    public void close() {}

    @Override
    public boolean isOpen() {
        return true;
    }
}

class FileSetReader implements ValueGenerator.SetReader {

    private volatile boolean open = false;
    private String url;
    private Map<String, Object> globals;

    private LineNumberReader reader;
    ValueGenerator.SetParser parser;

    public FileSetReader() {}

    public void setURI(String url)
        throws Exception
    {
        this.url = url;
        reader = new LineNumberReader(new FileReader(url.substring("file://".length())));

        int lastDot = url.lastIndexOf(".");
        String suffix = (lastDot < url.length()-1 ? url.substring(lastDot+1) : "csv").toLowerCase();

        switch (suffix) {
            case "csv":
                parser = new CsvSetParser();
                break;

            default:
                throw new ConfigurationException("Unsupported file type: %s", suffix);
        }

        open = true;
    }

    public void setGlobalMap(Map<String, Object> globals) {
        this.globals = globals;
    }

    public String getName() {
        return url + " at line " + reader.getLineNumber();
    }

    public void close() {
        open = false;

        if (reader != null) {
            try { reader.close(); }
            catch (Exception e) {}
        }
    }

    public boolean isOpen()
    { return open; }

    public String[] read(long unique) {
        try {
            String line = reader.readLine();
            if (line == null || line.length() == 0) return ValueGenerator.EMPTY_STRING_ARRAY;

            String [] result = parser.parse(line);

            // resolve any global variable references
            for (int rx = 0; rx < result.length; rx++) {
                String val = result[rx];
                if (val.startsWith("${") && val.endsWith("}")) {
                    Object resolved = globals.get(val.replaceAll("\\$|\\{|\\}", ""));
                    result[rx] = (resolved != null ? resolved.toString() : "");
                    //Logger.getLogger("readValue").info(String.format("resolved var %s -> %s", val, result[rx]));
                }
            }

            return result;
        }
        catch (Exception e) {
            Logger.getLogger("FileFormatter.read").info("Error reading line: " + e.toString());
            return ValueGenerator.EMPTY_STRING_ARRAY;
        }
    }
}

class CsvSetParser implements ValueGenerator.SetParser {

    private List<String> set = new ArrayList<String>(512);

    private static final Pattern doubleQuote = Pattern.compile("\"\"");

    @Override
    public String[] parse(String line) {

        set.clear();
        int start = 0;
        boolean quoted = false;

        ValueGenerator.log.info("CSV line=" + line);

        int max = line.length()-1;
        for (int cx = 0; cx <= max; cx++) {

            char c = line.charAt(cx);
            if (c == '"') {
                quoted = !quoted;
            }

            else if ((c == ',' && quoted == false) || cx == max) {
                if (!quoted) {
                    String val = line.substring(start, cx).trim();

                    ValueGenerator.log.info("CSV token=" + val);

                    // strip enclosing quotes
                    if (val.startsWith("\"") && val.endsWith("\"")) {
                        val = val.substring(1, val.length() - 1);
                        val = doubleQuote.matcher(val).replaceAll("\"");
                    }

                    set.add(val);

                    start = cx+1;
                }
            }
        }

        //if (start < line.length()-1) set.add(line.substring(start).trim());

        return set.toArray(ValueGenerator.EMPTY_STRING_ARRAY);
    }
}

class FormatSetReader implements ValueGenerator.SetReader {

    private volatile boolean open = false;
    private String uri;
    private Map<String, Object> globals;
    private String[] value;
    private String[] format;

    private static Pattern genVal = Pattern.compile("(.*)%([~|@].*)\\$([\\p{Digit}\\p{Punct}]*)(\\p{Alpha}+)(.*)");
    private static Random random = new Random();

    private static final DateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd");
    private static final DateFormat dateTimeFormatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    private static final long msecsInADay = 3600*24*1000;

    private static long NOW = System.currentTimeMillis();

    //private static Calendar nowCalendar = new GregorianCalendar();
    //private static long timezoneOffset = nowCalendar.get(Calendar.ZONE_OFFSET) + nowCalendar.get(Calendar.DST_OFFSET);

    private static char[] printable = new char[] {
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
            'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
            'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
            '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'
    };

    private static Logger log = Logger.getLogger(FormatSetReader.class.getName());

    public FormatSetReader() {}

    @Override
    public void setURI(String uri) {
        this.uri = uri;

        String[] fields = uri.substring(uri.indexOf(':') + 3).split(",");
        //log.info("fields=" + Arrays.toString(fields));

        value = new String[fields.length];
        format = new String[fields.length];

        for (int fx = 0; fx < fields.length; fx++) {
            Matcher match = genVal.matcher(fields[fx].trim());
            if (match.find()) {
                //for (int gx = 1; gx <= match.groupCount(); gx++) {
                //    log.info(String.format("group[%d] = %s", gx, match.group(gx)));
                //}
                value[fx] = String.format("%s%s%s", match.group(2).charAt(0), match.group(4).charAt(0), match.group(2).substring(1));
                format[fx] = String.format("%s%%%d$%s%s%s", match.group(1), fx+1, match.group(3), match.group(4), match.group(5));
                //log.info("value specs=" + Arrays.toString(value) + "; format specs=" + Arrays.toString(format));
            } else {
                value[fx] = "";
                format[fx] = fields[fx].trim();
            }

            //log.info("value: " + value[fx] + "; format: " + format[fx]);
        }

        open = true;
    }

    @Override
    public void setGlobalMap(Map<String, Object> globals) {
        this.globals = globals;
    }

    @Override
    public String getName() {
        return uri;
    }

    @Override
    public void close()
    { open = false; }

    public boolean isOpen()
    { return open; }

    @Override
    public String[] read(long unique) {

        Object[] genval = new Object[format.length];
        String[] result = new String[format.length];

        // generate the value array
        for (int fx = 0; fx < format.length; fx++) {

            String genSpec = value[fx];

            if (genSpec != null && genSpec.length() > 0) {

                //log.info("value spec: " + genSpec);

                try {
                    genval[fx] = generateValue(genSpec, globals);
                } catch (Exception e) {
                    log.info(String.format("Error generating value for %s:\n%s", genSpec, e.toString()));
                    return ValueGenerator.EMPTY_STRING_ARRAY;
                }

            }
        }

        // now format the results
        for (int fx = 0; fx < format.length; fx++) {
            //log.info("formatting " + format[fx] + " using " + Arrays.toString(genval));
            result[fx] = String.format(format[fx], genval);
        }

        log.info("spec: " + Arrays.toString(value) + " -> " + Arrays.toString(result));
        return result;
    }

    public static Object generateValue(String genSpec, Map<String, Object> globals)
        throws Exception
    {
        char type = genSpec.charAt(0);

        if (type == '@') {
                // global var reference
                // log.info("locals=" + locals.toString());
                return globals.get(genSpec.substring(2));
        }

        else if (type != '~') {
            log.info(String.format("Unrecognised value generation char: %s", type));
            return ValueGenerator.EMPTY_STRING_ARRAY;
        }

        String[] range;

        if (genSpec.length() > 2) {
            range = genSpec.substring(2).split("-");
        } else {
            range = ValueGenerator.EMPTY_STRING_ARRAY;
        }

        StringBuilder str = new StringBuilder(64);

        //log.info("type=" + value[fieldNo].charAt(0));

        switch (genSpec.charAt(1)) {
            case 'd':
            case 'o':
            case 'x':
            case 'X': {
                if (range.length == 0)
                    return random.nextInt();

                long min = (range.length > 1 ? Long.parseLong(range[0]) : 0);
                long max = (range.length > 1 ? Long.parseLong(range[1]) : Long.parseLong(range[0]));

                // if both min and max are Integer values, return an Integer...
                if (min >= Integer.MIN_VALUE && max <= Integer.MAX_VALUE)
                    return ((int) (min + random.nextInt((int) (max - min))));

                // otherwise, return a Long
                else
                    return (long) (min + (random.nextDouble() * (max - min)));
            }

            case 'f':
            case 'e':
            case 'E':
            case 'g':
            case 'G':
            case 'a':
            case 'A': {
                if (range.length == 0)
                    return random.nextDouble();

                double min = (range.length > 1 ? Double.parseDouble(range[0]) : 0);
                double max = (range.length > 1 ? Double.parseDouble(range[1]) : Double.parseDouble(range[0]));

                return (min + ((max - min) * random.nextDouble()));
            }

            case 's':
            case 'S': {
                int min = (range.length > 0 ? Integer.parseInt(range[0]) : 1);
                int max = (range.length > 1 ? Integer.parseInt(range[1]) : min);

                int size = (range.length > 1 ? min + random.nextInt(max - min) : max);

                str.setLength(0);
                for (int cx = 0; cx < size; cx++) {
                    str.append(printable[random.nextInt(printable.length)]);
                }

                //log.info("random string=" + str.toString());
                return str.toString();
            }

            case 't':
            case 'T': {
                DateFormat dateFmt = (range.length == 0 || range[0].contains(" ") ? dateTimeFormatter : dateFormatter);

                //long min = (range.length > 0 ? dateFmt.parse(range[0]).getTime() : NOW - (NOW % msecsInADay));
                long min = (range.length > 0 ? dateFmt.parse(range[0]).getTime() : NOW);
                long max = (range.length > 1 ? dateFmt.parse(range[1]).getTime() : min + msecsInADay-1);

                //log.info(String.format("min=%d (%1$tF %1$tR); max=%d (%2$tF %2$tR)", min, max));

                return new Date(min + (long) (random.nextDouble() * (max-min))).getTime();
            }

            default:
                log.info(String.format("Unhandled data type: %s", genSpec.charAt(0)));
                return '?';
        }
    }
}