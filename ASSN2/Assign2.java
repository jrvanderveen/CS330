import java.util.*;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class Assign2 {

    static Connection conn = null;
    public static HashMap<Date, Double> splits = new HashMap<Date, Double>();
    public static HashMap<Date, Double> dividend = new HashMap<Date, Double>();
    public static int numSplits = 0;
    public static double divisor = 1;
    public static int numTrans = 0;
    public static double netCash = 0;
    public static int netShares = 0;

    
    public static void main(String[] args) throws Exception {
        // Get connection properties
        String paramsFile = "connectparams.txt";
        if (args.length >= 1) {
            paramsFile = args[0];
        }
        Properties connectprops = new Properties();
        connectprops.load(new FileInputStream(paramsFile));

        try {
            // Get connection
            Class.forName("com.mysql.jdbc.Driver");
            String dburl = connectprops.getProperty("dburl");
            String username = connectprops.getProperty("user");
            conn = DriverManager.getConnection(dburl, connectprops);
            System.out.printf("Database connection %s %s established.%n", dburl, username);

            // Enter Ticker and TransDate, Fetch data for that ticker and date range
            Scanner in = new Scanner(System.in);
            while (true) {
                System.out.printf("Enterticker symbol [start/end dates]: ");
                String[] data = in.nextLine().trim().split("\\s+");
                if (data.length < 1)
                    break;
                displayName(data);
                
                Assign2.divisor = 1;
                Assign2.numSplits = 0;
                Assign2.numTrans = 0;
                Assign2.netCash = 0;
                Assign2.netShares = 0;
                Assign2.splits.clear();
                Assign2.dividend.clear();
            }
            conn.close();
        } catch (SQLException ex) {
            System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
                                    ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
        }
    }

    static void displayName(String[] data) throws SQLException {
        // Prepare query
        String ticker = data[0];
        PreparedStatement pstmt = conn.prepareStatement(
                "select Name " +
                "  from Company " +
                "  where Ticker = ?");
        // Fill in the blanks
        pstmt.setString(1, ticker);
        ResultSet rs = pstmt.executeQuery();

        // Did we get anything? If so, output data.
        if (rs.next()) {
            System.out.printf("%s\n", rs.getString(1));
            displayResult(data);
                    
        } else {
            System.out.printf("Ticker %s not found.%n", ticker);
        }
        pstmt.close();
    }
    
    static void displayResult(String[] data) throws SQLException{
        int numDiv = 0;
        int numDays = 0;
        String maxDate = "9999-12-30";
        String minDate = "0000-00-00";
        String ticker = data[0];
        // Prepare query
        PreparedStatement pstmt = conn.prepareStatement(
                "select OpenPrice, ClosePrice, TransDate, Volume" +
                "  from PriceVolume" +
                "  where Ticker = ? and TransDate > ? and TransDate < ?" + 
                "  order by TransDate DESC");
        // Fill in the blanks
        if (data.length == 3){
            minDate = data[1];
            maxDate = data[2];
        }
        pstmt.setString(1, ticker);
        pstmt.setString(2, minDate);
        pstmt.setString(3, maxDate);
        ResultSet rs = pstmt.executeQuery();
        
        if(rs.next()){
            //populate hashmap with dividend dates and amounts
            numDiv = popDividend(data);
            //calculate investment strategy
            numDays = investmentStrat(rs);
        }
        else{
            System.out.printf("No stock data found.\n");
        }

        System.out.printf("%d splits in %d trading days\n%d Dividends\n\n", Assign2.numSplits, numDays, numDiv);
        System.out.printf("Executing investment strategy\nTransactions executed: %d\nNet cash: %.2f\n\n", Assign2.numTrans, Assign2.netCash);
        
        pstmt.close();
    }
    
    static int investmentStrat(ResultSet rs)throws SQLException{
        double average = 0.0;
        int numDays = 0;
        double open = 0.0;
        double close = 0.0;
        double prevClose = 0.0;
        int payedDiv = 0;
        boolean buy = false;
        ArrayList<Double> runingAverageList = new ArrayList<Double>();
        rs = findSplits(rs);
        //
        rs.next();
        double holder = 0.0;
        while(rs.previous()){
            //System.out.printf("%s open: %.2f close: %.2f average: %.2f\n", rs.getDate(3), ((rs.getDouble(1)/Assign2.divisor)), (rs.getDouble(2)/Assign2.divisor), (average/50.0));
            holder = rs.getDouble(2)/Assign2.divisor;
            average += holder;
            runingAverageList.add(holder);
            numDays += 1;
            if(numDays > 50){
                average -= runingAverageList.get(0);
                runingAverageList.remove(0);
                if(Assign2.dividend.get(rs.getDate(3)) != null){
                    Assign2.netCash += Assign2.dividend.get(rs.getDate(3)) * Assign2.netShares;
                    payedDiv += 1;
                    //System.out.printf("DIVIDEND: %s, %f, %.2f\n",rs.getDate(3), Assign2.dividend.get(rs.getDate(3)), Assign2.netCash);
                }
                open = rs.getDouble(1)/Assign2.divisor;
                close = rs.getDouble(2)/Assign2.divisor;
                if(buy == true){
                    Assign2.netShares += 100;
                    Assign2.netCash -= open * 100;
                    Assign2.netCash -= 8;
                    Assign2.numTrans += 1;
                    //System.out.printf("BUY %s, %.2f\n", rs.getDate(3), Assign2.netCash);
                    buy = false;
                }
                if(close < (average/50) && (close/open) <= 0.97000001){
                    buy = true;
                }
                else if(Assign2.netShares >= 100 && open > (average/50.0) && (open/prevClose) >= 1.00999999){
                    Assign2.netShares -= 100;
                    Assign2.netCash += 100*((open+close)/2);
                    Assign2.netCash -= 8;
                    Assign2.numTrans += 1;
                    //System.out.printf("SELL %s, %.2f\n", rs.getDate(3), Assign2.netCash);
                }
                prevClose = close;
            }
            if(Assign2.splits.get(rs.getDate(3)) != null){
                Assign2.divisor /= Assign2.splits.get(rs.getDate(3));
            }
        }
        rs.next();
        Assign2.netCash += rs.getDouble(1) * Assign2.netShares;
        //System.out.printf("Dividends payed %d\n", payedDiv);
        return numDays;
    }
    
    static int popDividend(String data[]) throws SQLException{
        String maxDate = "9999-12-30";
        String minDate = "0000-00-00";
        String ticker = data[0];
        int numDiv = 0;
        // Prepare query
        PreparedStatement pstmt = conn.prepareStatement(
                "select DivDate, Amount" +
                "  from Dividend" +
                "  where Ticker = ? and DivDate > ? and DivDate < ?" +
                "  order by DivDate DESC");
        // Fill in the blanks
        if (data.length == 3){
            minDate = data[1];
            maxDate = data[2];
        }
        pstmt.setString(1, ticker);
        pstmt.setString(2, minDate);
        pstmt.setString(3, maxDate); 
        ResultSet rs = pstmt.executeQuery();
        if(rs.next()){
            rs.previous();
            while(rs.next()){
                numDiv += 1;
                Assign2.dividend.put(rs.getDate(1),rs.getDouble(2));
            }
        }
        return numDiv;
    }
    
    static ResultSet findSplits(ResultSet rs) throws SQLException{
        double ratio = 0.0;
        double open = 0.0;
        double close = 0.0;
        open = rs.getDouble(1);
        while (rs.next()) {
            close = rs.getDouble(2);
            ratio = close/open;
            if(Math.abs(ratio - 3) < .30){
                System.out.printf("3:1 split on %s %.2f --> %.2f\n", rs.getDate(3), close, open);
                Assign2.splits.put(rs.getDate(3), 3.0);
                Assign2.numSplits += 1;
                Assign2.divisor *= 3;
            }
            else if(Math.abs(ratio - 2) < .20){
                System.out.printf("2:1 split on %s %.2f --> %.2f\n", rs.getDate(3), close, open);
                Assign2.splits.put(rs.getDate(3), 2.0);
                Assign2.numSplits += 1;
                Assign2.divisor *= 2;
            }
            else if(Math.abs(ratio - 1.5) < .15){
                System.out.printf("3:2 split on %s %.2f --> %.2f\n", rs.getDate(3), close, open);
                Assign2.splits.put(rs.getDate(3), 1.5);
                Assign2.numSplits += 1;
                Assign2.divisor *= 1.5;
            }
            open = rs.getDouble(1);
        }
        return rs;
    }
}          
    
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                