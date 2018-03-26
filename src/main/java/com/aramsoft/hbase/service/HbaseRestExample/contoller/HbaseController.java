package com.aramsoft.hbase.service.HbaseRestExample.contoller;

import com.aramsoft.hbase.service.HbaseRestExample.model.InputModel;
import com.aramsoft.hbase.service.HbaseRestExample.service.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;


import java.text.ParseException;

@RestController
public class HbaseController {

    @Autowired
    private HbaseService hbaseService;


    @Autowired
    private ReadDataService readDataService;


    @Autowired
    private UpdateDataService updateDataService;

    @Autowired
    private UploadDataService uploadDataService;


    @Autowired
    private ExportDataService exportDataService;



    @GetMapping(value = "/data/upload/{id}/{startDate}/{endDate}")
    public ResponseEntity<InputModel> uploadData(@PathVariable String id, @PathVariable String startDate, @PathVariable String endDate) throws ParseException {
        uploadDataService.uploadData(id,startDate,endDate);
        return new ResponseEntity("Upload the data", new HttpHeaders(), HttpStatus.OK);

    }
    @GetMapping(value = "/data/read")
    public ResponseEntity<InputModel> readData() {
        readDataService.getAllData();
        return new ResponseEntity("Read the data", new HttpHeaders(), HttpStatus.OK);

    }


    @GetMapping("/data/update/{id}")
    public ResponseEntity<InputModel> updateData(@PathVariable String id) {
        updateDataService.updateData(id);
        return new ResponseEntity("Updated  the data for the job " + id, new HttpHeaders(), HttpStatus.OK);

    }

    @GetMapping(value = "/data/export")
    public ResponseEntity<InputModel> exportData() {
        exportDataService.getAllData();
        return new ResponseEntity("Exporting the data", new HttpHeaders(), HttpStatus.OK);
        // TODO return the csv
    }




//
//    @PostMapping(value = "/data/uploads", consumes = "application/json", produces = "application/json")
//    public ResponseEntity<InputModel> saveQuote(@RequestBody InputModel quote) {
//        InputModel result = hbaseService.uploadData(quote);
//        return new ResponseEntity(result, new HttpHeaders(), HttpStatus.OK);
//    }


}
