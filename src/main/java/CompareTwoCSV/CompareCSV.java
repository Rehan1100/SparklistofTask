package CompareTwoCSV;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.hive.HiveContext;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

public class CompareCSV {


    public static void main(String args[]) throws FileNotFoundException, IOException
    {
        SparkContext sparkContxt = new SparkContext(new SparkConf());

        //That's changes store in development branch

        //now learning for stash
        SQLContext spark = new HiveContext(sparkContxt);
        spark.sql("SET spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict");
        spark.sql("SET spark.hadoop.hive.exec.dynamic.partition=true");
        spark.sql("SET spark.hadoop.hive.ql.exec.UDF=true");




        System.out.print("Exiting spark session");
        // Table name Extract from Model


        Dataset<Row> dataFrameReadngFromIntegrationKuduTable = spark.read()
                    .option("kudu.master","10.15.55.22:7051")
                .option("kudu.table",  "tfcsemantic" + "." + "recyclecapacityoperationcentersreport")
                .format("kudu").load();


        System.out.print("dataFrameReadngFromIntegrationKuduTable");

        dataFrameReadngFromIntegrationKuduTable.createOrReplaceTempView("dataFrameReadngFromIntegrationKuduTable");

        dataFrameReadngFromIntegrationKuduTable.show();


    }
}
