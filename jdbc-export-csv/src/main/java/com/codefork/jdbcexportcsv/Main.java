package com.codefork.jdbcexportcsv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) throws Exception {

        FileWriter output = new FileWriter ("/home/jeff/git/ccer-assignment/personnel.csv");
        CSVPrinter csvOut = new CSVPrinter(output, CSVFormat.EXCEL);

        Driver myDriver = new net.ucanaccess.jdbc.UcanaccessDriver();
        DriverManager.registerDriver( myDriver );

        Connection connection = DriverManager.getConnection("jdbc:ucanaccess:///home/jeff/git/ccer-assignment/data_sources/2016-2017_Final_S-275_Personnel_Database.accdb", "", "");

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from `2016-2017S275FinalForPublic`");

        ResultSetMetaData meta = rs.getMetaData();
        int numCols = meta.getColumnCount();
        List<String> columns = new ArrayList<>();
        for(int i = 0; i < numCols; i++) {
            columns.add(meta.getColumnName(i+1));
        }
        csvOut.printRecord(columns);

        csvOut.printRecords(rs);

        csvOut.flush();
        csvOut.close();
    }

}
