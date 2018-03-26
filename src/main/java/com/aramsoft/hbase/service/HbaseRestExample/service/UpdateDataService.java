package com.aramsoft.hbase.service.HbaseRestExample.service;

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
import java.util.NavigableMap;

@Service
public class UpdateDataService {


    public void updateData(String jobId) {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf("table2"));

            //Get the job start date to update first
            SingleColumnValueFilter jobIdFilter = new SingleColumnValueFilter(
                    Bytes.toBytes("cf1"),
                    Bytes.toBytes("JobId"),
                    CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes(jobId)));

            jobIdFilter.setFilterIfMissing(true);

            List<Filter> listOfFilters = new ArrayList<>();
            listOfFilters.add(jobIdFilter);
            FilterList filters = new FilterList(listOfFilters);
            Date endDate = new Date();
            Date startDate = DateUtils.addDays(endDate, -3);
            Scan readJobData = new Scan();
            readJobData.setTimeRange(startDate.getTime(), endDate.getTime());
            readJobData.setFilter(filters);
            ResultScanner readJobDataScanResult = table.getScanner(readJobData);
            String jobStartDate = "";
            String rowID = "0";
            for (Result res : readJobDataScanResult) {

                jobStartDate = getJobDetails(res);
                rowID = getRowId(res);
            }
            readJobDataScanResult.close();


            //Update the the job details
            Put put = new Put(Bytes.toBytes(rowID));

            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("JobId"), Bytes.toBytes(jobId));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("JobEndDate"), Bytes.toBytes(jobStartDate));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("JobStatus"), Bytes.toBytes("Done"));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("Exception"), Bytes.toBytes("False"));

            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getJobDetails(Result result) {

        String jobStartDate = "";
        NavigableMap<byte[],
                NavigableMap<byte[],
                        NavigableMap<Long, byte[]>>> resultMap = result.getMap();

        for (byte[] columnFamily : resultMap.keySet()) {
            String cf = Bytes.toString(columnFamily);
            if (cf.equals("cf1")) {

                NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = resultMap.get(columnFamily);

                for (byte[] column : columnMap.keySet()) {
                    String col = Bytes.toString(column);
                    if (col.equalsIgnoreCase("JobStartDate")) {
                        NavigableMap<Long, byte[]> timestampMap = columnMap.get(column);

                        for (Long timestamp : timestampMap.keySet()) {
                            String ts = timestamp.toString();
                            String value = Bytes.toString(timestampMap.get(timestamp));
                            System.out.println("Column Family: " + cf
                                    + " Column: " + col + " Value: " + value);
                            return value;
                        }
                    }
                }
            }

        }
        return jobStartDate;
    }


    public String getRowId(Result result) {

        String jobStartDate = "";
        NavigableMap<byte[],
                NavigableMap<byte[],
                        NavigableMap<Long, byte[]>>> resultMap = result.getMap();

        for (byte[] columnFamily : resultMap.keySet()) {
            String cf = Bytes.toString(columnFamily);
            if (cf.equals("cf1")) {

                NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = resultMap.get(columnFamily);

                for (byte[] column : columnMap.keySet()) {
                    String col = Bytes.toString(column);
                    if (col.equalsIgnoreCase("UUID")) {
                        NavigableMap<Long, byte[]> timestampMap = columnMap.get(column);

                        for (Long timestamp : timestampMap.keySet()) {
                            String ts = timestamp.toString();
                            String value = Bytes.toString(timestampMap.get(timestamp));
                            System.out.println("Column Family: " + cf
                                    + " Column: " + col + " Value: " + value);
                            return value;
                        }
                    }
                }
            }

        }
        return jobStartDate;
    }


}
