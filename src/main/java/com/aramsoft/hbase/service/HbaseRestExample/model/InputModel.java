package com.aramsoft.hbase.service.HbaseRestExample.model;

import java.util.Date;

public class InputModel {

   // JobId: String, JobStartDate: String, JobEndDate: String

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public Date getJobStartDate() {
        return jobStartDate;
    }

    public void setJobStartDate(Date jobStartDate) {
        this.jobStartDate = jobStartDate;
    }

    public Date getJobEndDate() {
        return jobEndDate;
    }

    public void setJobEndDate(Date jobEndDate) {
        this.jobEndDate = jobEndDate;
    }

    private String jobId;

private Date jobStartDate;

private Date  jobEndDate;


}
