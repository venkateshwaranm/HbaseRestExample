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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class UploadDataService {

    public void uploadData(String jobId, String jsDate, String jeDate) throws ParseException {


        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date jobStartDate = sdf.parse(jsDate);
        Date jobEndDate = sdf.parse(jeDate);

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

            SingleColumnValueFilter jobEndDateFilter = new SingleColumnValueFilter(
                    Bytes.toBytes("cf1"),
                    Bytes.toBytes("JobEndDate"),
                    CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes(jeDate)));

            jobEndDateFilter.setFilterIfMissing(true);

            SingleColumnValueFilter jobStartDateFilter = new SingleColumnValueFilter(
                    Bytes.toBytes("cf1"),
                    Bytes.toBytes("JobStartDate"),
                    CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes(jsDate)));

            jobStartDateFilter.setFilterIfMissing(true);


            List<Filter> listOfFilters = new ArrayList<>();
            listOfFilters.add(jobIdFilter);
            listOfFilters.add(jobEndDateFilter);
            listOfFilters.add(jobStartDateFilter);
            FilterList filters = new FilterList(listOfFilters);
            Date endDate = new Date();
            Date startDate = DateUtils.addDays(endDate, -3);
            Scan readJobData = new Scan();
            readJobData.setTimeRange(startDate.getTime(), endDate.getTime());
            readJobData.setFilter(filters);
            ResultScanner readJobDataScanResult = table.getScanner(readJobData);
            Boolean found = false;
            for (Result res : readJobDataScanResult) {

                found = getJobDetails(res);
            }
            readJobDataScanResult.close();

            Boolean exception = true;
            String jobStatus = "InProgress";
            if (jobStartDate.compareTo(jobEndDate) < 0) {
                exception = false;
                jobStatus = "Done";
            }

            Random random = new Random();
            String rowId = "U-" + String.format("%04d", random.nextInt(10000));
            //Update the the job details
            Put put = new Put(Bytes.toBytes(rowId));

            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("JobId"), Bytes.toBytes(jobId));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("UUID"), Bytes.toBytes(rowId));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("JobStartDate"), Bytes.toBytes(jobStartDate.toString()));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("JobEndDate"), Bytes.toBytes(jobEndDate.toString()));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("JobStatus"), Bytes.toBytes(jobStatus));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("Duplicate"), Bytes.toBytes(found.toString()));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("Exception"), Bytes.toBytes(exception.toString()));

            table.put(put);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Boolean getJobDetails(Result result) {

        NavigableMap<byte[],
                NavigableMap<byte[],
                        NavigableMap<Long, byte[]>>> resultMap = result.getMap();

        if (resultMap.size() >= 0) {
            return true;
        }
        else{
            return false;
        }


    }


}
