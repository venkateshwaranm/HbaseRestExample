package com.aramsoft.hbase.service.HbaseRestExample.service;

import com.aramsoft.hbase.service.HbaseRestExample.PrintValues;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Service
public class ExportDataService {

    public void getAllData() {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf("table2"));

            SingleColumnValueFilter exceptionFilter = new SingleColumnValueFilter(
                    Bytes.toBytes("cf1"),
                    Bytes.toBytes("Exception"),
                    CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes("False")));

            exceptionFilter.setFilterIfMissing(true);

            SingleColumnValueFilter jobStatusFilter = new SingleColumnValueFilter(
                    Bytes.toBytes("cf1"),
                    Bytes.toBytes("JobStatus"),
                    CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes("Done")));

            jobStatusFilter.setFilterIfMissing(true);

            SingleColumnValueFilter duplicateFilter = new SingleColumnValueFilter(
                    Bytes.toBytes("cf1"),
                    Bytes.toBytes("Duplicate"),
                    CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes("False")));

            duplicateFilter.setFilterIfMissing(true);


            List<Filter> listOfFilters = new ArrayList<>();
            listOfFilters.add(jobStatusFilter);
            listOfFilters.add(exceptionFilter);

            FilterList filters = new FilterList(listOfFilters);


            Date endDate = new Date();
            Date startDate = DateUtils.addDays(endDate, -3);
            Scan readJobData = new Scan();
            readJobData.setTimeRange(startDate.getTime(), endDate.getTime());
            readJobData.setFilter(filters);
            ResultScanner readJobDataScanResult = table.getScanner(readJobData);

            for (Result res : readJobDataScanResult) {

                PrintValues.printAllValues(res);
            }
            readJobDataScanResult.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}
