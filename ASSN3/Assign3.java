/*Johan van der Veen Assign3 implementation
 *Analyze stocks in a given industry for intervals of 60 days
*/

import java.util.*;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class Assign3 {

    static Connection connR = null;
    static Connection connW = null;
    
/*void main(String[] args)
 *Set up database connections for read-reedy330 and write-vander64
 *call findIndustries which starts the main functions
*/
    public static void main(String[] args) throws Exception {
        // Get connection properties
        String readParamsFile = "readerparams.txt";
        String writeParamsFile = "writerparams.txt";
        if (args.length >= 2) {
            readParamsFile = args[0];
            writeParamsFile = args[1];
        }
        Properties connectpropsR = new Properties();
        Properties connectpropsW = new Properties();
        connectpropsR.load(new FileInputStream(readParamsFile));
        connectpropsW.load(new FileInputStream(writeParamsFile));
        try {
            Class.forName("com.mysql.jdbc.Driver");
            // Get connection for reedy330 read
            String dburl = connectpropsR.getProperty("dburl");
            String username = connectpropsR.getProperty("user");
            connR = DriverManager.getConnection(dburl, connectpropsR);
            System.out.printf("\nDatabase connection %s %s established.", dburl, username);
            // Get connection for vander64 write
            dburl = connectpropsW.getProperty("dburl");
            username = connectpropsW.getProperty("user");
            connW = DriverManager.getConnection(dburl, connectpropsW);
            System.out.printf("\nDatabase connection %s %s established.%n\n", dburl, username);            
            //calculate data for all industries found in the database
            findIndustries();
            connR.close();
            connW.close();
        } catch (SQLException ex) {
            System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
                                    ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
        }
        System.out.println();
    }
/*void findIndustries()
 *Create a list of all the industries found in the database
 *loop through them calling compareIndustry() on each
*/
    private static void findIndustries() throws SQLException {
        ArrayList<String> industries = new ArrayList<String>();
        
        //Drop and create the Preformance table
        PreparedStatement pstmtD = deletePerformance();
        PreparedStatement pstmtC = createPerformance();
        pstmtD.executeUpdate();
        pstmtC.executeUpdate();
        pstmtD.close();  
        pstmtC.close();  
        //create a result set of all ticker names in the industry
        PreparedStatement pstmt = tickNames(); 
        ResultSet rs = pstmt.executeQuery();

        // Did we get anything? If so, output data.
        if (rs.next()) {
            rs.previous();
            while(rs.next()){
                System.out.printf("%s\n", rs.getString(1));
                industries.add(rs.getString(1));
            }
            pstmt.close();
            //Compare stocks for each company in a given industry
            for(String industry: industries){
                compareIndustry(industry);
            }
            //for testing only check specific industry
            //compareIndustry("Materials");
        }
        else {
            System.out.printf("No industries found. Database ERROR.");
        }
    }
/*void compareIndustry(String industry)
 *look through a given industry
 *find all the Tickers that have at least 150 trading days
 *then look through their start and end dates for the max min date and the min max date
 *then call computereturns() with the data found
*/
    
    private static void compareIndustry(String industry) throws SQLException {
        HashMap<String, Double> divisor = new HashMap<String, Double>();
        HashMap<String, Double> prevClose = new HashMap<String, Double>();
        
        String minDate;
        String maxDate;
        String biasCompany;
        int tradingDays = 99999999;
        int numTickers = 0;
        
        //create result set wich includes max and min stat dates for all tickers with more than 150 days
        PreparedStatement pstmt = industryTicksCheck(industry);
        ResultSet rs = pstmt.executeQuery();
        
        System.out.printf("\n\nProcessing %s",industry);
        if(rs.next()){
            biasCompany = rs.getString(1);
            minDate = rs.getString(2);
            maxDate = rs.getString(3);
            rs.previous();
            while(rs.next()){
                divisor.put(rs.getString(1), 1.0);
                prevClose.put(rs.getString(1), 0.0);
                numTickers++;
                if(minDate.compareTo(rs.getString(2)) < 0){
                    minDate = rs.getString(2);
                }
                if(maxDate.compareTo(rs.getString(3)) > 0){
                    minDate = rs.getString(2);
                }
                if(tradingDays > rs.getInt(4)){
                    tradingDays = rs.getInt(4);
                }
            }
            System.out.printf("\n%d accepted tickers for %s(%s - %s), %d common dates",numTickers ,industry, minDate, maxDate, tradingDays, biasCompany);     
            computeReturns(industry, minDate, maxDate, (tradingDays / 60), numTickers, biasCompany, divisor, prevClose);
            pstmt.close();
        }
        else{
            System.out.println("No tickers in the industry meet the requirements");
        }
    }
/*void computeReturns(String industry, String minSystem.out.printf("\n Ticker: %s, biasCompany: %s,  dayCount: %d", tickName, biasCompany, dayCount);Date, String maxDate, int intervals, int numTickers, String biasCompany, HashMap<String,Double> divisor, HashMap<String, Double> prevClose)
 *An interval is 60 days of the biasCompany which is the alphabetically first ticker
 *at the begining of the interval collect open prices for all tickers
 *for each day for each ticker record the closeing price and the next day determine weather a split occured or not
 *once near the end of the interval record closing prices
 *once on the last day peek ahead for the biasCompany and then call storeReturnData() to insert into the Performance table
*/
    private static void computeReturns(String industry, String minDate, String maxDate, int intervals, int numTickers, String biasCompany, HashMap<String,Double> divisor, HashMap<String, Double> prevClose)throws SQLException{
        HashMap<String, Double> openPrice = new HashMap<String, Double>();
        HashMap<String, Double> closePrice = new HashMap<String, Double>();
        int dayCount = 0;
        int recordedTicks = 0;
        boolean finished = false;
        double div = 0;
        String tickName;
        double tickOpen;
        double tickClose;
        String startDate = minDate;
        String endDate = "";
        
        Scanner sc = new Scanner(System.in);
        String test;
    
        //create result set with all info needed for each ticker
        PreparedStatement pstmt = tickInfo(industry, minDate, maxDate); 
        ResultSet rs = pstmt.executeQuery();
        //check if results were found
        if(rs.next()){
            //loop for (numtradingdays / 60) - 1 times aka the number of intervals for that industry   
            for(int i = 0; i <= intervals; i++){
                finished = false;
                recordedTicks = 0;
                dayCount = 0;
                openPrice.clear();
                closePrice.clear();
                //for each interval walk through all the data calulating splits along the way for each ticker
                //remember the opening date/price for the first day and the closeing date/price for the last then calculate final result
                rs.previous();
                while(!finished && rs.next()){
                    tickName = rs.getString(1);
                    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    //This is statement exists if there are anycompanys have less then 150 trading days
                    //right now my query finds the min a max days for all company that has 150 then the next query takes all tickers in that range
                    //including ones with less than 150 days.
                    if(prevClose.containsKey(tickName)){
                        tickOpen = rs.getDouble(3);
                        tickClose = rs.getDouble(4);
                        //only increment the day if we find the first company
                        if(tickName.equals(biasCompany)){
                            dayCount += 1;
                        }
                        
                        //Determine if a split occurs if so then store for each ticker.
                        div = findSplits(tickOpen, prevClose.get(tickName));
                        if(div != 0){
                            //System.out.printf("\n%s %.2f:1 split on %s ----- open: %.2f  close: %.2f", tickName, div, rs.getString(2), tickOpen, prevClose.get(tickName));
                            divisor.put(tickName, (divisor.get(tickName) * div));
                        }
                        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                        //while not all of the tickers in the industy have had their opening values stored 
                        //check if their ticker is in the hashmap and if not then store and increment 
                        if(recordedTicks < numTickers){
                            //System.out.printf("\n%s:  %d out of %d",tickName, recordedTicks, numTickers);
                            if(!openPrice.containsKey(tickName)){
                                openPrice.put(tickName, (tickOpen*divisor.get(tickName)));
                                recordedTicks += 1;
                            }
                        }
                        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                        //since not all dates line up for intervals as a saftey precaucion start recording all the closing data from day 55
                        //this ensures all tickers will have their close values stored even when dates dont line u
                        else if(dayCount >= 55){assert tickName.equals(rs.getString(1));
                            closePrice.put(tickName, (tickClose*divisor.get(tickName)));
                        }//else close
                        prevClose.put(tickName, tickClose);
                    }//if key exists closers
                    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    //once the day count reaches 60 peak a head at the next Ticker if it is the same as the biasCompany then end while loop
                    if(dayCount > 59){
                        endDate = rs.getString(2);
                        //rs.previous();
                        //System.out.printf("Interval: %d DayCount: %d Previous: %s", i, dayCount, rs.getString(1));
                        //rs.next();
                        //System.out.printf(" Current: %s", rs.getString(1));
                        if(rs.next()){
                        //System.out.printf(" Next: %s\n", rs.getString(1));
                            if(rs.getString(1).equals(biasCompany)){
                                finished = true;
                                storeReturnData(industry, openPrice, closePrice, startDate, endDate);
                                startDate = rs.getString(2);
                            }
                            else{
                                rs.previous();
                                assert tickName.equals(rs.getString(1));
                            }
                        }
                    }
                }//while loop close
            }//for loop close
        }//initial if close
    }
/*double findSplits(Double open, Double close)
 *determine if a split occured and return the apropriate value
*/
    private static double findSplits(Double open, Double close){
        double ratio = 0.0;
        ratio = close/open;
        if(Math.abs(ratio - 3) < .30){
            return 3;
        }
        else if(Math.abs(ratio - 2) < .20){
            return 2;
        }
        else if(Math.abs(ratio - 1.5) < .15){
            return 1.5;
        }
        
        return 0;
    }
/*void storeReturnData(String industry, HashMap<String,Double> openPrice, HashMap<String,Double> closePrice, String startDate, String endDate)
 *Calculate and store ticker return data and keep a sum of all ticker return values before the - 1
 *calculate the industry return value for every ticker 
 *insert the tr and ir into the table at the end of each interval
*/
    private static void storeReturnData(String industry, HashMap<String,Double> openPrice, HashMap<String,Double> closePrice, String startDate, String endDate)throws SQLException{
        HashMap<String, Double> tickerReturn = new HashMap<String, Double>();
        double tr = 0.0;
        double ir = 0.0;
        double tickSum = 0.0;
        int numTickers = 0;
        for(String tickName : openPrice.keySet()){
            tr = closePrice.get(tickName)/openPrice.get(tickName);
            tickSum += tr;
            tickerReturn.put(tickName, (tr - 1));
            numTickers += 1;
        }
        numTickers -= 1;
        for(String tickName : tickerReturn.keySet()){
            tr = tickerReturn.get(tickName);
            ir = (((1/(double)numTickers)*(tickSum - (tr + 1))) - 1);
            PreparedStatement pstmt = insertRowPerformance(industry, tickName, startDate, endDate, tr, ir);
            pstmt.executeUpdate();
        }
        
    }
    
/*
 *All prepared statments used
*/
        
    private static PreparedStatement createPerformance()throws SQLException{
        PreparedStatement pstmt = connW.prepareStatement(
                "create table Performance (" +
                "   Industry VARCHAR(50)," +
                "   Ticker CHAR(12)," +
                "   StartDate CHAR(10)," + 
                "   EndDate CHAR(10)," +
                "   TickerReturn NUMERIC(12, 7)," + 
                "   IndustryReturn NUMERIC(12, 7))");
        return pstmt;
    }
    private static PreparedStatement deletePerformance()throws SQLException{
        PreparedStatement pstmt = connW.prepareStatement(
                "drop table if exists Performance");    
        return pstmt;
    }
    private static PreparedStatement insertRowPerformance(String industry, String ticker, String startDate, String endDate, Double tickerReturn, Double IndustryReturn) throws SQLException{
        PreparedStatement pstmt = connW.prepareStatement(
                "insert into Performance values(?, ?, ?, ?, ?, ?)");
        pstmt.setString(1, industry);
        pstmt.setString(2, ticker);
        pstmt.setString(3, startDate);
        pstmt.setString(4, endDate);
        pstmt.setString(5, String.valueOf(tickerReturn));
        pstmt.setString(6, String.valueOf(IndustryReturn));
        
        return pstmt;
    }
    private static PreparedStatement tickNames()throws SQLException{
        PreparedStatement pstmt = connR.prepareStatement(
                "select Industry " +
                "  from Company" + 
                "  group by Industry");
        return pstmt;
    }
    private static PreparedStatement industryTicksCheck(String industry)throws SQLException{
        PreparedStatement pstmt = connR.prepareStatement(
                "select Ticker, min(TransDate), max(TransDate)," +       
                "        count(distinct TransDate) as TradingDays" + 
                "   from Company left outer join PriceVolume using(Ticker)" +
                "   where Industry = ?" +
                "   group by Ticker" +
                "   having TradingDays >= 150" +
                "   order by Ticker ");
        pstmt.setString(1, industry);
        
        return pstmt;
    }    
    
    private static PreparedStatement tickInfo(String industry, String minDate, String maxDate)throws SQLException{
        PreparedStatement pstmt = connR.prepareStatement(
                "select Ticker, TransDate, openPrice, closePrice" +
                "   from Company join PriceVolume using(Ticker)" +   
                "   where Industry = ?" +
                "       and TransDate between ? and ?" +   
                "   order by TransDate, Ticker" + 
                "   ");       
        pstmt.setString(1, industry);
        pstmt.setString(2, minDate);
        pstmt.setString(3, maxDate);
        
        return pstmt;
    }
}          

                
                
           