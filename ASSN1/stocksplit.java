import java.io.*;
import java.util.*;

public class stocksplit {
    public static void main(String[] args) throws IOException{
        FileInputStream fstream = new FileInputStream("stockdata.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        String strLine = "";
        String[] splitln;
        String company = "";
        double ratio = 0.0;
        double open = 0.0;
        double close = 0.0;
        int counter = 0;

        if((strLine = br.readLine()) != null){
            splitln = strLine.split("\\s+");
            open = Double.parseDouble(splitln[2]);
            company = splitln[0];
            System.out.printf("Processing %s...\n", company);
        }
        else{
            System.out.println("No data in file.");
            System.exit(0);
        }
        //Read File Line By Line
        //Keep track of the opening value of each line
        while ((strLine = br.readLine()) != null)   {
            splitln = strLine.split("\\s+");
            if(company.equals(splitln[0])){
                close = Double.parseDouble(splitln[5]);
                ratio = close/open;
                if(Math.abs(ratio - 3) < .30){
                    System.out.printf("3:1 split on %s %.2f --> %.2f\n", splitln[1], close, open);
                    counter += 1;
                }
                else if(Math.abs(ratio - 2) < .20){
                    System.out.printf("2:1 split on %s %.2f --> %.2f\n", splitln[1], close, open);
                    counter += 1;
                }
                else if(Math.abs(ratio - 1.5) < .15){
                    System.out.printf("3:2 split on %s %.2f --> %.2f\n", splitln[1], close, open);
                    counter += 1;
                }
                open = Double.parseDouble(splitln[2]);
            }
            else{
                System.out.printf("splits: %d\n", counter);
                counter = 0;
                open = Double.parseDouble(splitln[2]);
                company = splitln[0];
                System.out.printf("\nProcessing %s...\n", company);
            }
        }
        //Close the input stream
        br.close();
    }
}