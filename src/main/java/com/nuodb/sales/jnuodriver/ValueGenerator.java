package com.nuodb.sales.jnuodriver;

import com.nuodb.sales.jnuodriver.dao.ConfigurationException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by nik on 24/09/2015.
 */
public class ValueGenerator implements Runnable {

    private List<SetReader> input = new ArrayList<SetReader>(64);
    private List<ArrayBlockingQueue<String[]>> output = new ArrayList<ArrayBlockingQueue<String[]>>(64);

    private static final long fullSleepTime = 20;
    private static final long emptySleepTime = 200;

    private long unique;
    private volatile boolean running = false;

    public static final String[] EMPTY_STRING_ARRAY = new String[0];

    static final Logger log = Logger.getLogger(ValueGenerator.class.getName());

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

        while (running) {

            full = empty = 0;

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

                set = in.read(unique++);

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
        throws Exception
    {
        log.info("getReader - uri = " + uri);

        String prefix = uri.substring(0, uri.indexOf(':'));
        switch (prefix) {
            case "file":
                return new FileSetReader(uri);

            case "format":
                return new FormatSetReader(uri);

            default:
                throw new ConfigurationException("ValueGenerator: unsupported value URI type: %s", prefix);
        }
    }

    public interface SetReader {
        public String getName();
        public String[] read(long unique);
        public void close();
        public boolean isOpen();
    }

    public interface SetParser {
        public String[] parse(String line);
    }
}

class FileSetReader implements ValueGenerator.SetReader {

    private volatile boolean open = false;
    private String url;
    private LineNumberReader reader;
    ValueGenerator.SetParser parser;

    public FileSetReader(String url)
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

            return parser.parse(line);
        }
        catch (Exception e) {
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

        for (int cx = 0; cx < line.length(); cx++) {
            switch (line.charAt(cx)) {
                case '"':
                    quoted = !quoted;
                    break;

                case ',':
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
                    break;
            }
        }

        if (start < line.length()-1) set.add(line.substring(start).trim());

        return set.toArray(ValueGenerator.EMPTY_STRING_ARRAY);
    }
}

class FormatSetReader implements ValueGenerator.SetReader {

    private volatile boolean open = false;
    private String uri;
    private String[] value;
    private String[] format;

    private static Pattern genVal = Pattern.compile("%([~|#].*)\\$(.*)");
    private Random random = new Random();

    private static char[] printable = new char[] {
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
            'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
            'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
            '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'
    };

    private static Logger log = Logger.getLogger(FormatSetReader.class.getName());

    public FormatSetReader(String uri) {
        this.uri = uri;

        String[] fields = uri.substring(uri.indexOf(':') + 2).split(",");

        value = new String[fields.length];
        format = new String[fields.length];

        for (int fx = 0; fx < fields.length; fx++) {
            Matcher match = genVal.matcher(fields[fx].trim());
            if (match.find()) {
                value[fx] = match.group(2).charAt(match.group(2).length()-1) + match.group(1);
                format[fx] = String.format("%%%d$%s", fx+1, match.group(2));
            } else {
                value[fx] = "";
                format[fx] = fields[fx].trim();
            }

            //log.info("value: " + value[fx] + "; format: " + format[fx]);
        }

        open = true;
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

            if (value[fx] != null && value[fx].length() > 0) {

                //log.info("value spec: " + value[fx]);

                switch (value[fx].charAt(1)) {
                    case '~':
                        // random value
                        genval[fx] = generateValue(fx);
                        break;

                    case '#':
                        // unique value
                        genval[fx] = unique;
                        break;

                    default:
                        log.info(String.format("Unrecognised value generation char: %s", value[fx].charAt(1)));
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

    protected Object generateValue(int fieldNo) {

        String[] range;
        if (value[fieldNo].length() > 2) {
            range = value[fieldNo].substring(2).split("-");
        } else {
            range = ValueGenerator.EMPTY_STRING_ARRAY;
        }

        StringBuilder str = new StringBuilder(64);

        //log.info("type=" + value[fieldNo].charAt(0));

        switch (value[fieldNo].charAt(0)) {
            case 'd':
            case 'o':
            case 'x':
            case 'X': {
                if (range.length == 0)
                    return random.nextInt();

                int min = Integer.parseInt(range[0]);
                int max = (range.length > 1 ? Integer.parseInt(range[1]) : min);

                if (range.length > 1)
                    return (min + random.nextInt(max - min));
                else
                    return random.nextInt(max);
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

                double min = Double.parseDouble(range[0]);
                double max = (range.length > 1 ? Double.parseDouble(range[1]) : min);

                if (range.length > 1)
                    return (min + ((max - min) * random.nextDouble()));
                else
                    return max * random.nextDouble();
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

            default:
                log.info(String.format("Unhandled data type: %s", value[fieldNo].charAt(0)));
                return '?';
        }
    }
}