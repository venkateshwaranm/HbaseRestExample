package com.aramsoft.hbase.service.HbaseRestExample.service;

import com.aramsoft.hbase.service.HbaseRestExample.model.InputModel;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Service;

@Service
public class HbaseService {

   // @Autowired
    private HbaseTemplate hbaseTemplate = new HbaseTemplate();

    public InputModel uploadData(InputModel inRec)

    {
        hbaseTemplate.put("table1", inRec.getJobId(),
                "data", inRec.getJobEndDate().toString() ,
                        Bytes.toBytes(inRec.getJobId()));
        return inRec;
    }


}
