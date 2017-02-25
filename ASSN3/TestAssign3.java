import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.regex.*;
import java.sql.*;

class TestAssign3 {
    
    static final String DEFAULT_CONNECT_PARAMS = "writerparams.txt";  
    static final String DEFAULT_TESTDATA = "testdata.txt";
    
    static final String USAGE = 
        "usage: java TestAssign3 [-h] [-p Params] [-g Output]\n" +
        "                        [-t TestData] [database]\n";
         
    static final String HELP_TEXT = 
        "Test contents of database generated for Assignment 3\n" +
        "\n" +
        "  -h          show this help message and exit.\n" +
        "  -p Params   name of connection parameters file. If not\n" +
        "              specified will use " + DEFAULT_CONNECT_PARAMS + "\n" +
        "  -t TestData name of the file containing test queries and\n" +
        "              expected results. If not specified will use\n" +
        "              " + DEFAULT_TESTDATA + "\n" +
        "  -g Output   generate test data file in correct format. When\n" +
        "              this option is given, the data portion of the\n" +
        "              TestData file is ignored. Headers are checked.\n" +
        "\n" +
        "  database    Is the name of the database to connect to. If not\n" +
        "              given, the database specified by the connection\n" +
        "              parameters file is used.\n";
     
    // These variables store the results of argument processing
    static String connectParamsFile = DEFAULT_CONNECT_PARAMS;
    static String testDataFile = DEFAULT_TESTDATA;
    static boolean generate = false;
    static String generateOutputFile = null;
    static String databaseName = null;
     
    // Internal variables used by the program
    static Connection connection = null;
    static Scanner input;

    public static void main(String[] args) throws Exception {
        if (processArgs(args)) {
            
            // System.out.printf("connectParams = %s%ntestData = %s%ngenerate = %s, output = %s%ndatabase = %s%n",
            //         connectParamsFile, testDataFile, generate, generateOutputFile, databaseName);
            
            Properties connectProps = new Properties();
            connectProps.load(new FileInputStream(connectParamsFile));
            setupConnection(connectProps, databaseName);
            if (generate)
                doGenerate();
            else
                doRunTest();
            closeConnection();
        }
    }
    
    static boolean processArgs(String[] args) {
        boolean error = false;
        int argno = 0;
        
        while (!error && argno < args.length && args[argno].startsWith("-")) {
            String param = args[argno];
            argno += 1;
            switch (param) {
                case "-h":
                    printUsage(false);
                    return false;
                case "-p":
                    if (argno < args.length) {
                        connectParamsFile = args[argno];
                        argno += 1;
                    } else {
                        System.err.println("Missing file name for -p");
                        error = true;
                    }
                    break;
                case "-t":
                    if (argno < args.length) {
                        testDataFile = args[argno];
                        argno += 1;
                    } else {
                        System.err.println("Missing file name for -t");
                        error = true;
                    }
                    break;
                case "-g":
                    if (argno < args.length) {
                        generate = true;
                        generateOutputFile = args[argno];
                        argno += 1;
                    } else {
                        System.err.println("Missing file name for -g");
                        error = true;
                    }
                    break;
                default:
                    System.err.println("Unrecognized option: " + param);
                    error = true;
                    break;
            }
        }

        if (!error && argno < args.length) {
            databaseName = args[argno];
            argno += 1;
            if (argno < args.length) {
                System.err.println("Unexpected additional arguments");
                error = true;
            }
        }
        
        if (error) {
            printUsage(true);
            return false;
        } else {
            return true;
        }
    }
    
    static void printUsage(boolean error) {
        PrintStream output = error ? System.err : System.out;
        printText(USAGE, output);
        if (!error) {
            output.println();
            printText(HELP_TEXT, output);
        }
    }
    
    static void printText(String text, PrintStream output) {
        String lineSep = System.getProperty("line.separator");
        output.print(text.replace("\n", lineSep));
    }

    static Pattern CONNECT_URL = Pattern.compile("(jdbc\\:mysql\\:\\/\\/[a-zA-Z0-9.:-_]*\\/)([a-zA-Z0-9_]*)");
    
    static void setupConnection(Properties connectProps, String db) throws SQLException {
        String dburl = connectProps.getProperty("dburl");
        if (db != null) {
            Matcher match = CONNECT_URL.matcher(dburl);
            if (!match.matches())
                throw new IllegalArgumentException(db + " is not a valid database url");
            dburl = match.group(1) + db;
        }
        String username = connectProps.getProperty("user");
        // System.out.printf("Attempting connect '%s' for user %s with password %s%n",
        //         dburl, username, connectProps.getProperty("password"));
        connection = DriverManager.getConnection(dburl, connectProps);
        System.out.printf("Test connection %s %s established.%n", dburl, username);
    } 

    static void closeConnection() throws SQLException {
        connection.close();
    }
    
    static void doRunTest() throws Exception{
        input = new Scanner(new File(testDataFile));
        int testno = 0;
        while (input.hasNextLine()) {
            String query = getQuery(false);
            if (query != null) {
                System.out.println("Query: " + query);
                String[][] data = getData();
                testno += 1;
                System.out.printf("Test #%d ...", testno);
                System.out.flush();
                String[][] results = runTest(query);
                if (testResults(data, results)) {
                    System.out.println(" OK");
                } else {
                    System.out.println(" Failed!");
                    printFailure(data, results);
                    System.out.println();
                }
            }
        }      
    }
    
    static void doGenerate() throws Exception {
            input = new Scanner(new File(testDataFile));
            FileWriter generateOut = new FileWriter(generateOutputFile);
            
            int testno = 0;
            while (input.hasNextLine()) {
                String query = getQuery(true);
                if (query != null) {
                    String[][] data = getData();
                    testno += 1;
                    String[][] results = runTest(query);
                    if (data.length == 0 || results.length == 0
                            || !Arrays.equals(data[0], results[0])) {
                        throw new AssertionError(
                            String.format("Header mismatch on test #%d", testno));
                    }
                    generateResults(generateOut, testno, query, results);
                }
            }
            
            generateOut.close();
    }
    
    static String getQuery(boolean preserveWhiteSpace) {
        String line = getNextQueryLine();
        if (line == null)
            return null;
        StringBuilder query = new StringBuilder();
        if (preserveWhiteSpace) {
            query.append(line);
            query.append('\n');
        } else {
            query.append(line.trim());
            query.append(' ');
        }
        while (true) {
            line = getNextQueryLine();
            if (line == null)
                return query.toString();
            if (preserveWhiteSpace) {
                query.append(line);
                query.append('\n');
            } else {
                query.append(line.trim());
                query.append(' ');
            }
        }
    }
    
    static String getNextQueryLine() {
        while (true) {
            String line = input.nextLine();
            String trimmed = line.trim();
            if (trimmed.length() == 0)
                return null;
            if (!trimmed.startsWith("#"))
                return line;
       }
    }    
    
    static String[][] getData() {
        List<String> lines = new ArrayList<>();
        String firstLine = input.nextLine().trim();
        while (input.hasNextLine()) {
            String nextLine = input.nextLine().trim();
            if (nextLine.length() == 0)
                break;
            lines.add(nextLine);
        }
        String[][] result = new String[lines.size() + 1][];
        result[0] = firstLine.split("\\t");
        int i = 0;
        for (String line : lines) {
            i ++;
            result[i] = line.split("\\t");
        }
        return result;
    }
    
    static String[][] runTest(String query) throws SQLException {
        List<String[]> results = new ArrayList<>(); 
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query.replace('\n', ' '));
        ResultSetMetaData rsmd = rs.getMetaData();
        int colCount = rsmd.getColumnCount();
        String[] headers = new String[colCount];
        for (int i = 0; i < colCount; i++) {
            headers[i] = rsmd.getColumnLabel(i + 1);
        }
        results.add(headers);
        while (rs.next()) {
            String[] rowData = new String[colCount];
            for (int i = 0; i < colCount; i++) {
                String data = rs.getString(i + 1);
                rowData[i] = (data != null) ? data : "null";
            }
            results.add(rowData);
        }
        rs.close();
        stmt.close();
        return results.toArray(new String[0][]);
    }
    
    static void generateResults(FileWriter out, int testno, String query, String[][]results) throws IOException {
        if (testno > 1)
            out.write("\n");
        out.write(query);
        out.write("\n");
        for (String[] resultLine : results) {
            boolean first = true;
            for (String result : resultLine) {
                if (!first)
                    out.write("\t");
                first = false;
                out.write(result);
            }
            out.write("\n");
        }
    }
    
    static boolean testResults(String[][] data, String[][] results) {
        if (data.length != results.length)
            return false;
        for (int i = 1; i < results.length; i ++) {
            if (!Arrays.equals(data[i], results[i])) {
                return false;
            }
        }
        return true;
    }
    
    static void printFailure(String[][] data, String[][] results) {
        printArray("Expected", data, null, Integer.MAX_VALUE);
        printArray("Actual", results, data, data.length + 10);
        System.out.println();
    }
    
    static void printArray(String title, String[][] data, String[][] compare, int limit) {
        System.out.print(title);
        if (data.length > limit)
            System.out.printf(" (only showing %d of %d rows)", limit - 1, data.length - 1);
        if (data.length == 1) {
            System.out.println(" zero rows");
            return;
        }
        System.out.println(" ...");
        int[] widths = new int[data[0].length];
        Arrays.fill(widths, 0);
        for (String[] row : data) {
            for (int c = 0; c < row.length; c++) {
                if (row[c].length() > widths[c])
                    widths[c] = row[c].length();
            }
        }
        String format = "";
        for (int w : widths) {
            format = format + String.format("%%%ds ", w);
        }
        format = format + "%n";
        boolean firstRow = true;
        int numrow = 0;
        for (String[] row : data) {
            char flag = ' ';
            if (compare != null && numrow > 0 && 
                    (numrow >= compare.length || !Arrays.equals(row, compare[numrow])))
               flag = 'X';
            System.out.print(flag);
            System.out.printf(format, (Object[])row);
            numrow++;
            if (numrow >= limit)
                break;
        }
    }
}