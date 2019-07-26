package ccic.net.bi.chexian.bjfx


import ccic.net.bi.chexian.bjfx.common._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object PG_I_DM_Vehicle_Quotation {

  def run(v_startDay: String, v_endDay: String ,batch_id: String) = {
    //val sparkconf = new SparkConf().setAppName("clbj")
    //val sc = new SparkContext(sparkconf)
    //val sqlc = new SQLContext(sc)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("PG_I_DM_Vehicle_Quotation").enableHiveSupport().getOrCreate()
    //import spark.implicits._

    val v_workdate: String = v_endDay
    println("workdate为:" + v_workdate)
    val workdate = v_workdate.replace("-", "")

    spark.sql("use ccic_car")

    var v_begin_time = getnow

    //
    //  spark.udf.register("myUDF", (arg1: Int, arg2: String) => arg2 + arg1)

    spark.udf.register("compute_quote", compute_quote _)
    spark.udf.register("lpad", lpad _)
    spark.udf.register("addMonth", addMonth _)
    spark.udf.register("diff_date", diff_date _)

    spark.udf.register("vehicle_classification", vehicle_classification _)

    //取增量范围内的报价单
    val df_quono_i = spark.sql("""select t1.QUOTATIONNO 
        from CCIC_SOURCE.t21_T_CPI_MAIN t1
        where to_date(substr(t1.OPERATETIMEFORHIS,1,10)) >= to_date('""" + v_startDay + """')
        and to_date(substr(t1.OPERATETIMEFORHIS,1,10)) < to_date('""" + v_endDay + """')""")
    df_quono_i.createTempView("df_quono_i")

    //报价单关联报价车
    spark.sql("""insert overwrite table tmp_vehicle_eaifid_i select distinct t1.eaif_id from CCIC_SOURCE.t21_T_CPI_ITEM_CAR t1
           inner join df_quono_i t2   
           on t1.QUOTATIONNO=t2.QUOTATIONNO 
           and t1.intg_datastate<>'D' """)
    //包括历史上所有相关车的报价单和报价单上车辆静态信息
    val DF_T21_T_CPI_ITEM_CAR = spark.sql("""select t1.QUOTATIONNO
      ,t1.EAIF_ID           AS VEHICLE_ID 
      ,t1.FRAMENO 
      ,t1.LICENSENO 
      ,t1.BRANDNAME 
      ,t1.LICENSECOLORCODE 
      ,t1.CARKINDCODE 
      ,t1.HKFLAG 
      ,t1.HKLICENSENO 
      ,t1.ENGINENO 
      ,t1.ENROLLDATE 
      ,t1.USEYEARS 
      ,t1.USENATURECODE 
      ,t1.PURCHASEPRICE 
      ,t1.SEATCOUNT 
      ,t1.TONCOUNT 
      ,t1.POWERSCALE 
      ,t1.EXHAUSTSCALE 
      ,t1.FUELTYPE 
      ,t1.AUTOINSURANCESCORE  
      ,t1.INTG_DATASOURCE
        from CCIC_SOURCE.t21_T_CPI_ITEM_CAR t1
         inner join  tmp_vehicle_eaifid_i t2 
        on t1.eaif_id=t2.eaif_id 
        and t1.intg_datastate<>'D'""")
    DF_T21_T_CPI_ITEM_CAR.createTempView("DF_T21_T_CPI_ITEM_CAR")

    val DF_CPI_MAIN_CAR_LIST1 = spark.sql("""select  
      T1.QUOTATIONNO
      ,T1.PROPOSALNO 
      ,CASE WHEN T1.INTG_DATASOURCE!='98' THEN
          CASE WHEN nvl(t1.RENEWALNO,'')!=''  OR t1.ISRENEWED = '1' THEN 
                      '续保' 
                     ELSE 
                      (CASE WHEN (diff_date( t1.STARTDATE , t2.ENROLLDATE ) +1 )/ 365 < 0.75 THEN 
                         '新车' 
                        ELSE         
                         '首年投保或转保' 
                      END)           
                   END 
       ELSE
          CASE WHEN nvl(t1.RENEWALNO,'')!=''  OR t1.ISRENEWED = '2' THEN 
                      '续保' 
                     ELSE 
                      (CASE WHEN (diff_date( t1.STARTDATE , t2.ENROLLDATE ) +1 )/ 365 < 0.75 THEN 
                         '新车' 
                        ELSE         
                         '首年投保或转保' 
                      END)           
                   END 
       END AS RENEW_FLAG 
      ,T1.COMCODE 
      ,T1.OperateTimeForHis as quote_time 
      ,T1.BUSINESSNATURE 
      ,T1.BUSINESSNATURE2 
      ,NVL(T1.BUSINESSNATURE2,T1.BUSINESSNATURE)   AS CHN_CODE 
      ,T1.STARTDATE 
      ,T1.ENDDATE 
      ,T1.SUMPREMIUM 
      ,T1.RISKCODE
      ,T2.VEHICLE_ID 
      ,T2.FRAMENO 
      ,T2.LICENSENO 
      ,T2.BRANDNAME 
      ,T2.LICENSECOLORCODE 
      ,T2.CARKINDCODE 
      ,T2.HKFLAG 
      ,T2.HKLICENSENO 
      ,T2.ENGINENO 
      ,T2.ENROLLDATE 
      ,T2.USEYEARS 
      ,T2.USENATURECODE 
      ,T2.PURCHASEPRICE 
      ,T2.SEATCOUNT 
      ,T2.TONCOUNT 
      ,T2.POWERSCALE 
      ,T2.EXHAUSTSCALE 
      ,T2.FUELTYPE 
      , T2.AUTOINSURANCESCORE 
      ,CASE WHEN SUBSTR(T1.QUOTATIONNO,1,3) in('QBI','QDE') THEN '1' ELSE '0' END AS BUSI_QUOTE_FLAG 
      ,CASE WHEN SUBSTR(T1.QUOTATIONNO,1,3) in('QCI','QDF') THEN '1' ELSE '0' END AS TRA_QUOTE_FLAG 
      ,T2.INTG_DATASOURCE
      FROM CCIC_SOURCE.t21_T_CPI_MAIN T1
      INNER JOIN DF_T21_T_CPI_ITEM_CAR T2 
       ON T1.QUOTATIONNO=T2.QUOTATIONNO 
      WHERE NVL(T2.VEHICLE_ID,'')!=''""")

    //DF_CPI_MAIN_CAR_LIST1.createOrReplaceTempView("DF_CPI_MAIN_CAR_LIST1")

    DF_CPI_MAIN_CAR_LIST1.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list1")
    spark.sql("refresh table tmp_cpi_main_car_list1")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'车辆和报价表关联'  as step_name "
      + ",'tmp_cpi_main_car_list1' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow

    val quoteDF1 = spark.sql("""select
         vehicle_id
         ,QuotationNo
         ,cast(to_date(quote_time)as string) as quote_time
      from tmp_CPI_MAIN_CAR_LIST1""")

    quoteDF1.createTempView("quoteDF1")

    val quoteDF2 = spark.sql("""select
        t.vehicle_id
        ,concat_ws('|',collect_set(concat_ws('#',t.QuotationNo,t.quote_time))) as quote_rount 
      from quoteDF1 t 
      where nvl(t.vehicle_id,'')!='' 
      group by t.vehicle_id""")

    quoteDF2.createTempView("quoteDF2")

    val qrdf = spark.sql("""select vehicle_id, split(arrstring,'\\|') as quote_rount
        from (select vehicle_id,compute_quote(quote_rount) as quote_rount from quoteDF2) t
        lateral view explode(t.quote_rount) adtable as arrstring""")
    //qrdf.createOrReplaceTempView("qrdf")

    qrdf.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_quote_rount")

    val df_quote_rount2 = spark.sql("select QUOTE_ROUNT[0] as QUOTATIONNO,QUOTE_ROUNT[1] as QUOTE_ROUNT from TMP_QUOTE_ROUNT")

    df_quote_rount2.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_quote_rount2")

    val DF_CPI_MAIN_CAR_LIST1_1 = spark.sql("""SELECT 
      T2.QUOTE_ROUNT
      ,T1.QUOTATIONNO        
      ,T1.PROPOSALNO         
      ,T1.RENEW_FLAG         
      ,T1.COMCODE            
      ,T1.QUOTE_TIME         
      ,T1.BUSINESSNATURE     
      ,T1.BUSINESSNATURE2    
      ,T1.CHN_CODE           
      ,T1.STARTDATE          
      ,T1.ENDDATE            
      ,T1.SUMPREMIUM         
      ,T1.RISKCODE           
      ,T1.VEHICLE_ID          
      ,T1.FRAMENO            
      ,T1.LICENSENO          
      ,T1.BRANDNAME          
      ,T1.LICENSECOLORCODE   
      ,T1.CARKINDCODE        
      ,T1.HKFLAG             
      ,T1.HKLICENSENO        
      ,T1.ENGINENO           
      ,T1.ENROLLDATE         
      ,T1.USEYEARS           
      ,T1.USENATURECODE      
      ,T1.PURCHASEPRICE      
      ,T1.SEATCOUNT          
      ,T1.TONCOUNT           
      ,T1.POWERSCALE         
      ,T1.EXHAUSTSCALE       
      ,T1.FUELTYPE           
      , T1.AUTOINSURANCESCORE 
      ,T1.BUSI_QUOTE_FLAG     
      ,T1.TRA_QUOTE_FLAG       
      ,T1.INTG_DATASOURCE
      FROM TMP_CPI_MAIN_CAR_LIST1 T1 
      LEFT JOIN tmp_quote_rount2 T2 
      ON T1.QUOTATIONNO=T2.QUOTATIONNO  """)

    DF_CPI_MAIN_CAR_LIST1_1.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list1_1")
    spark.sql("refresh table tmp_cpi_main_car_list1_1")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'生成报价轮次'  as step_name "
      + ",'tmp_cpi_main_car_list1_1' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow

    val df_cpi_main_extend = spark.sql("select quotationno,demandno "
      + ",effectivetrafficviolation "
      + ", serioustrafficviolation "
      + "from CCIC_SOURCE.t21_t_cpi_main_extend where intg_datastate<>'D'")
    


    df_cpi_main_extend.createTempView("df_cpi_main_extend")

    val DF_CPI_MAIN_CAR_LIST2 = spark.sql("""select t1.QUOTE_ROUNT 
      ,t1.QUOTATIONNO        
      ,t1.PROPOSALNO         
      ,t1.RENEW_FLAG         
      ,t1.COMCODE            
      ,t1.QUOTE_TIME         
      ,t1.BUSINESSNATURE     
      ,t1.BUSINESSNATURE2    
      ,t1.CHN_CODE           
      ,t1.STARTDATE          
      ,t1.ENDDATE            
      ,t1.SUMPREMIUM         
      ,t1.RISKCODE           
      ,t1.VEHICLE_ID         
      ,t1.FRAMENO            
      ,t1.LICENSENO          
      ,t1.BRANDNAME          
      ,t1.LICENSECOLORCODE   
      ,t1.CARKINDCODE        
      ,t1.HKFLAG             
      ,t1.HKLICENSENO        
      ,t1.ENGINENO           
      ,t1.ENROLLDATE         
      ,t1.USEYEARS           
      ,t1.USENATURECODE      
      ,t1.PURCHASEPRICE      
      ,t1.SEATCOUNT          
      ,t1.TONCOUNT           
      ,t1.POWERSCALE         
      ,t1.EXHAUSTSCALE       
      ,t1.FUELTYPE           
      , t1.AUTOINSURANCESCORE 
      ,t1.BUSI_QUOTE_FLAG     
      ,t1.TRA_QUOTE_FLAG      
      ,t2.DEMANDNO                
      ,t2.EFFECTIVETRAFFICVIOLATION 
      ,t2.SERIOUSTRAFFICVIOLATION    
      ,t1.INTG_DATASOURCE
      from tmp_cpi_main_car_list1_1 t1 
      LEFT JOIN df_cpi_main_extend T2 
        ON T1.QUOTATIONNO=T2.QUOTATIONNO and nvl(T2.DEMANDNO,'')!=''""")

    DF_CPI_MAIN_CAR_LIST2.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list2")
    spark.sql("refresh table tmp_cpi_main_car_list2")
    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'关联出违章系数'  as step_name "
      + ",'tmp_cpi_main_car_list2' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow

    val df_cpi_main_car_list3 = spark.sql(""" select 
     t1.QUOTE_ROUNT         
      ,t1.QUOTATIONNO         
      ,t1.PROPOSALNO          
      ,t1.RENEW_FLAG          
      ,t1.COMCODE             
      ,t1.QUOTE_TIME          
      ,t1.BUSINESSNATURE      
      ,t1.BUSINESSNATURE2     
      ,t1.CHN_CODE            
      ,t1.STARTDATE           
      ,t1.ENDDATE             
      ,t1.SUMPREMIUM          
      ,t1.RISKCODE            
      ,t1.VEHICLE_ID          
      ,t1.FRAMENO             
      ,t1.LICENSENO           
      ,t1.BRANDNAME           
      ,t2.factoryName           
      ,t1.LICENSECOLORCODE    
      ,t1.CARKINDCODE         
      ,t1.HKFLAG              
      ,t1.HKLICENSENO         
      ,t1.ENGINENO            
      ,t1.ENROLLDATE          
      ,t1.USEYEARS            
      ,t1.USENATURECODE       
      ,t1.PURCHASEPRICE       
      ,t1.SEATCOUNT           
      ,t1.TONCOUNT            
      ,t1.POWERSCALE          
      ,t1.EXHAUSTSCALE        
      ,t1.FUELTYPE            
      , t1.AUTOINSURANCESCORE 
      ,t1.BUSI_QUOTE_FLAG     
      ,t1.TRA_QUOTE_FLAG      
      ,t1.DEMANDNO            
      ,t1.EFFECTIVETRAFFICVIOLATION  
      ,t1.SERIOUSTRAFFICVIOLATION     
      ,t1.INTG_DATASOURCE
      ,vehicle_classification(t2.factoryName)    as CLASS_TYPE   
      from tmp_cpi_main_car_list2 t1 
      left join (select Demandno,max(factoryName) as   factoryName 
      from CCIC_SOURCE.t03_prpccarmodel where nvl(Demandno,'')!='' or nvl(factoryName,'')!='' and intg_datastate != 'D'
      group by Demandno) t2 
         on t1.Demandno=t2.Demandno distribute by QUOTATIONNO""")
    df_cpi_main_car_list3.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list3")
    spark.sql("refresh table tmp_cpi_main_car_list3")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'关联出车辆系列'  as step_name "
      + ",'tmp_cpi_main_car_list3' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow
    // df_cpi_main_car_list3.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list3")

    //df_cpi_main_car_list3.createOrReplaceTempView("df_cpi_main_car_list3")

    val df_cpi_main_car_list4 = spark.sql("""SELECT 
        T1.*                                                                                                                                                                                                         
        ,T2.CUSTNO                                                                                                                                                                                                   
        ,T2.INSUREDNAME                                                                                                                                                                                              
        ,CASE WHEN  LENGTH(T2.IDENTIFYNUMBER) = 15 AND PMOD(CAST(SUBSTR(T2.IDENTIFYNUMBER, 15, 1) AS INT), 2) = 0                                                                                                    
                    THEN  '女'                                                                                                                                                                                      
              WHEN  LENGTH(T2.IDENTIFYNUMBER) = 15 AND PMOD(CAST(SUBSTR(T2.IDENTIFYNUMBER, 15, 1) AS INT), 2) = 1                                                                                                    
                    THEN  '男'                                                                                                                                                                                      
              WHEN  LENGTH(T2.IDENTIFYNUMBER) = 18 AND PMOD(CAST(SUBSTR(T2.IDENTIFYNUMBER, 17, 1) AS INT), 2) = 0                                                                                                    
                    THEN  '女'                                                                                                                                                                                      
              WHEN  LENGTH(T2.IDENTIFYNUMBER) = 18 AND PMOD(CAST(SUBSTR(T2.IDENTIFYNUMBER, 17, 1) AS INT), 2) = 1                                                                                                    
                    THEN  '男'                                                                                                                                                                                      
              ELSE  '无' END AS INSUREDSEX                                                                                                                                                                          
        ,case when  length(t2.IdentifyNumber) = 15 and  cast(substr('""" + workdate + """',5,4) as int) >= cast(concat(substr(t2.IdentifyNumber, 9, 2) , substr(t2.IdentifyNumber, 11, 2)) as int) then                        --已过生日年龄
                      cast(floor(datediff( '""" + v_workdate + """', concat('19', substr(t2.IdentifyNumber, 7, 2) , '-' , substr(t2.IdentifyNumber, 9, 2) , '-' , substr(t2.IdentifyNumber, 11, 2))) / 365.25)+1 as string)  
              when  length(t2.IdentifyNumber) = 15 and  cast(substr('""" + workdate + """',5,4) as int) < cast(concat(substr(t2.IdentifyNumber, 9, 2) , substr(t2.IdentifyNumber, 11, 2)) as int) then                         --未过生日年龄
                      cast(floor(datediff( '""" + v_workdate + """', concat('19', substr(t2.IdentifyNumber, 7, 2) , '-' , substr(t2.IdentifyNumber, 9, 2) , '-' , substr(t2.IdentifyNumber, 11, 2))) / 365.25) as string)    
              when  length(t2.IdentifyNumber) = 18 and  cast(substr('""" + workdate + """',5,4) as int) >= cast(concat(substr(t2.IdentifyNumber, 11, 2) , substr(t2.IdentifyNumber, 13, 2)) as int) then                       --已过生日年龄
                      cast(floor(datediff( '""" + v_workdate + """', concat(substr(t2.IdentifyNumber, 7, 4) , '-' , substr(t2.IdentifyNumber, 11, 2) , '-' , substr(t2.IdentifyNumber, 13, 2))) / 365.25) as string)          
              when  length(t2.IdentifyNumber) = 18 and  cast(substr('""" + workdate + """',5,4) as int) < cast(concat(substr(t2.IdentifyNumber, 11, 2) , substr(t2.IdentifyNumber, 13, 2)) as int) then                        --未过生日年龄
                      cast(floor(datediff( '""" + v_workdate + """', concat(substr(t2.IdentifyNumber, 7, 4) , '-' , substr(t2.IdentifyNumber, 11, 2) , '-' , substr(t2.IdentifyNumber, 13, 2))) / 365.25) as string)          
              else  '99'    END AS INSUREDAGE                                                                                                                                                                        
       FROM tmp_cpi_main_car_list3 T1                                                                                                                                                                                
        LEFT JOIN (SELECT DISTINCT QUOTATIONNO,IDENTIFYNUMBER,CUSTNO,INSUREDNAME from CCIC_SOURCE.T21_T_CPI_INSURED where ((intg_datasource ='98' and InsuredFlag='3') or (intg_datasource !='98' and substr(InsuredFlag,3,1)='1')) and intg_datastate<>'D') T2
        ON T1.QUOTATIONNO=T2.QUOTATIONNO """)

    df_cpi_main_car_list4.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list4")
    spark.sql("refresh table tmp_cpi_main_car_list4")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'关联出客户信息'  as step_name "
      + ",'tmp_cpi_main_car_list4' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow

    val DF_PRPCIINSDEMANDPROFIT = spark.sql("""select Demandno, PROFITRATE,PROFITCODE from CCIC_SOURCE.t06_PRPCIINSDEMANDPROFIT
      where PROFITCODE in ('14','13') and nvl(Demandno,'')!='' and intg_datastate !='D'
      """)
    DF_PRPCIINSDEMANDPROFIT.createOrReplaceTempView("DF_PRPCIINSDEMANDPROFIT")
    spark.sql("cache table DF_PRPCIINSDEMANDPROFIT")
    val df_cpi_main_car_list5 = spark.sql("""select                    
       t1.*                     
       ,cast(t2.PROFITRATE as string)                         as profitrate_ncd     --商业险报价单NCD系数
       ,cast(t3.PROFITRATE as string)                         as profitrate_violation  --商业险报价单交通违章系数
      from tmp_cpi_main_car_list4 t1                    
       left join DF_PRPCIINSDEMANDPROFIT t2  
         on t1.Demandno=t2.Demandno      and nvl(t1.demandno,'')!=''                 
         and t2.PROFITCODE in ('13')   
       left join DF_PRPCIINSDEMANDPROFIT t3 
         on t1.Demandno=t3.Demandno                       
         and t3.PROFITCODE in ('14')     and nvl(t1.demandno,'')!=''
         """)

    df_cpi_main_car_list5.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list5")
    spark.sql("refresh table tmp_cpi_main_car_list5")
    spark.sql("uncache table DF_PRPCIINSDEMANDPROFIT")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'关联出商业险NCD和交通违章系数'  as step_name "
      + ",'tmp_cpi_main_car_list5' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow
    //df_cpi_main_car_list5.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list5")

    spark.sql("cache table tmp_cpi_main_car_list5")

    val df_cpi_item_kind_grp = spark.sql("""insert overwrite table TMP_CPI_ITEM_KIND_GRP  SELECT                  
         T2.QUOTATIONNO                                                                                                  
      ,SUM(CASE WHEN T2.KINDCODE not in('BZ','C100120','C100150')  THEN T2.AMOUNT ELSE 0 END )       AS AMOUNT                  
      ,SUM(T2.PREMIUM)                                                                     AS PREMIUM                 
      ,SUM(T2.BENCHMARKPREMIUM)                                                            AS BENCHMARKPREMIUM        
      ,SUM(CASE WHEN SUBSTR(T2.QUOTATIONNO,1,3) in('QBI','QDE') THEN T2.PREMIUM ELSE 0 END)          AS BUSI_PREMIUM            
      ,SUM(CASE WHEN SUBSTR(T2.QUOTATIONNO,1,3) in('QBI','QDE') THEN T2.BENCHMARKPREMIUM ELSE 0 END) AS BUSI_BENCHMARKPREMIUM   
      ,LPAD(SUM(CASE WHEN T2.KINDCODE IN('A','C100121','C100147','C100151')                                        
              THEN 1                                                                                                  
            WHEN T2.KINDCODE IN ('L', 'F', 'X', 'G', 'G1', 'C', 'C100125','C100126','C100128','C100129','C100155','C100156','C100158','C100159','C100414','C101028')                                                       
              THEN 10                                                                                                 
            WHEN T2.KINDCODE IN ('B','C100122','C100152')                                                                                    
              THEN 100                                                                                                
            WHEN T2.KINDCODE IN ('D1', 'D2', 'D3', 'D4','C100123','C100124','C100132','C100153','C100154','C100160','C101018','C101059')                                                              
              THEN 1000                                                                                               
            ELSE  10000 END  ) ,5,'0')                                                       AS CODESUM               
      ,MAX(CASE  WHEN (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) >= 0.6 THEN   '>=0.6'                
               WHEN (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) < 0.6 AND                               
                    (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) >= 0.5 THEN   '0.5-0.6'                 
               WHEN (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) < 0.5 AND                               
                    (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) >= 0.4 THEN   '0.4-0.5'                 
               WHEN (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) < 0.4 AND                               
                    (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) >= 0.3 THEN   '0.3-0.4'                 
               WHEN (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) < 0.3 AND                               
                    (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) >= 0.2 THEN   '0.2-0.3 '                
               WHEN (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) < 0.2 AND                               
                    (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) >= 0.1 THEN   '0.1-0.2'                 
               WHEN (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) < 0.1 AND                               
                    (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) >= 0.0 THEN   '0-0.1'                   
               WHEN (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) < 0.0 AND                               
                    (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) >= -0.1 THEN   '-0.1-0'                 
               WHEN (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) < -0.1 AND                              
                    (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) >= -0.2 THEN   '-0.2--0.1'              
               WHEN (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) < -0.2 AND                              
                    (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) >= -0.3 THEN   '-0.3--0.2'              
               WHEN (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) < -0.3 AND                              
                    (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) > -0.5 THEN   '-0.5--0.3'               
               WHEN (1 - (1 - T2.DISCOUNT / 100) * (1 - T2.ADJUSTRATE / 100)) <= -0.5 THEN   '<=-0.5'                 
             END)                                                                     AS   DISCOUNT_RANGE             --折扣区间
      FROM CCIC_SOURCE.T21_T_CPI_ITEM_KIND T2       inner join TMP_CPI_MAIN_CAR_LIST5 t1 on t1.QUOTATIONNO=t2.QUOTATIONNO and t2.intg_datastate<>'D'
      GROUP BY t2.QUOTATIONNO distribute by  QUOTATIONNO""")

    //df_cpi_item_kind_grp.createOrReplaceTempView("df_cpi_item_kind_grp")

    val df_cpi_main_car_list6 = spark.sql(""" SELECT                                                         
       T1.*                                                                                           
       ,T2.AMOUNT                                                                                     
       ,T2.BUSI_PREMIUM                                                                               
       ,T2.BUSI_BENCHMARKPREMIUM                                                                      
       ,CASE WHEN NVL(T1.PROPOSALNO,'')!='' THEN T2.PREMIUM ELSE 0 END           AS PREMIUM           
       ,CASE WHEN NVL(T1.PROPOSALNO,'')!='' THEN T2.BENCHMARKPREMIUM ELSE 0 END  AS BENCHMARKPREMIUM  
       ,CASE WHEN substr(T2.CODESUM,-1)='0' THEN '01'                                                     
                              WHEN substr(T2.CODESUM,-4) ='0111' THEN '03'                                  
                              WHEN substr(T2.CODESUM,-4) ='1101' THEN '04'                                  
                              WHEN substr(T2.CODESUM,-4) ='1111' THEN '02'                                  
                              WHEN substr(T2.CODESUM,-2) ='11'   THEN '05'                                  
                         ELSE '06' END AS                                   INSURANCE_CODE            --承保组合
       ,T2.DISCOUNT_RANGE                                                                             
      FROM TMP_CPI_MAIN_CAR_LIST5 T1                                                                  
      LEFT JOIN TMP_CPI_ITEM_KIND_GRP T2                                                              
       ON T1.QUOTATIONNO=T2.QUOTATIONNO            """)

    df_cpi_main_car_list6.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list6")
    spark.sql("refresh table tmp_cpi_main_car_list6")
    spark.sql("uncache table tmp_cpi_main_car_list5")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'关联出承保组合'  as step_name "
      + ",'tmp_cpi_main_car_list6' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow
    //df_cpi_main_car_list6.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list6")

    val df_cpi_main_car_list7 = spark.sql(""" select 
      t1.*                                                          
      ,T1.SUMPREMIUM * (1 - NVL(T2.TOTALCFEERATE,0) / 100)               AS NET_FEE   --净费
      FROM  tmp_cpi_main_car_list6 t1 
      LEFT JOIN (select QUOTATIONNO,TOTALCFEERATE from CCIC_SOURCE.T21_T_CPI_CFEEPREPOSE where intg_datastate<>'D') T2
        on t1.quotationno=t2.quotationno  """)
    df_cpi_main_car_list7.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list7")
    spark.sql("refresh table tmp_cpi_main_car_list7")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'关联出净费'  as step_name "
      + ",'tmp_cpi_main_car_list7' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow
    val df_cpi_main_car_list8 = spark.sql("""select        
         t1.QUOTE_ROUNT
         ,t1.QUOTATIONNO         
         ,coalesce(t1.proposalno,t2.proposalno,t3.proposalno)   as    proposalno    
         ,t1.RENEW_FLAG          
         ,t1.COMCODE             
         ,t1.QUOTE_TIME          
         ,t1.BUSINESSNATURE      
         ,t1.BUSINESSNATURE2     
         ,t1.CHN_CODE            
         ,t1.STARTDATE           
         ,t1.ENDDATE             
         ,t1.SUMPREMIUM          
         ,t1.RISKCODE            
         ,t1.VEHICLE_ID          
         ,t1.FRAMENO             
         ,t1.LICENSENO           
         ,t1.BRANDNAME           
         ,t1.factoryName           
         ,t1.LICENSECOLORCODE    
         ,t1.CARKINDCODE         
         ,t1.HKFLAG              
         ,t1.HKLICENSENO         
         ,t1.ENGINENO            
         ,t1.ENROLLDATE          
         ,t1.USEYEARS            
         ,t1.USENATURECODE       
         ,t1.PURCHASEPRICE       
         ,t1.SEATCOUNT           
         ,t1.TONCOUNT            
         ,t1.POWERSCALE          
         ,t1.EXHAUSTSCALE        
         ,t1.FUELTYPE            
         ,t1.AUTOINSURANCESCORE  
         ,t1.BUSI_QUOTE_FLAG      
         ,t1.TRA_QUOTE_FLAG       
         ,t1.DEMANDNO                   
         ,t1.EFFECTIVETRAFFICVIOLATION  
         ,t1.SERIOUSTRAFFICVIOLATION    
         ,t1.CLASS_TYPE          
         ,t1.CUSTNO                     
         ,t1.INSUREDNAME                
         ,t1.INSUREDSEX                 
         ,t1.INSUREDAGE                 
         ,t1.PROFITRATE_NCD             
         ,t1.PROFITRATE_VIOLATION        
         ,t1.AMOUNT                 
         ,t1.PREMIUM                
         ,t1.BENCHMARKPREMIUM       
         ,t1.BUSI_PREMIUM           
         ,t1.BUSI_BENCHMARKPREMIUM  
         ,t1.INSURANCE_CODE                
         ,t1.DISCOUNT_RANGE         
         ,t1.NET_FEE                
         ,case when nvl(t1.proposalno,'')!='' and nvl(t2.proposalno,'')!='' and nvl(t3.proposalno,'')!='' then t1.NET_FEE else 0 end      as net_fee_premium --本轮下单净保费
         ,coalesce(t1.proposalno,t2.proposalno,t3.proposalno)                 as qty_proposalno 
         ,t1.INTG_DATASOURCE
      from tmp_cpi_main_car_list7 t1 
        left join (select distinct COALESCE(quotationnobi,quotationnoci) as  quotationno
                 ,proposalno from CCIC_SOURCE.T21_T_CPI_MAIN_SUB where intg_datastate<>'D' ) t2
        on t1.quotationno=t2.quotationno 
        left join (select distinct COALESCE(quotationnobi,quotationnoci) as  quotationno
                 ,proposalno from CCIC_SOURCE.t03_prptmainext where intg_datastate != 'D') t3
        on t1.quotationno=t3.quotationno   """)
    df_cpi_main_car_list8.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list8")
    spark.sql("refresh table tmp_cpi_main_car_list8")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'关联出下单净保费和投保单数量'  as step_name "
      + ",'tmp_cpi_main_car_list8' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow
    val df_proposalno = spark.sql(""" select vehicle_id,quote_rount,quotationno,proposalno 
      from tmp_cpi_main_car_list8 
      where  nvl(proposalno,'')!=''  """)

    df_proposalno.createOrReplaceTempView("df_proposalno")
    spark.sql("cache table df_proposalno")

    val df_quotano_under = spark.sql("""select distinct 
      t1.quotationno 
      ,t2.underwriteenddate 
      from df_proposalno t1 
      left join (select * from CCIC_SOURCE.t03_prptmain where intg_datastate != 'D') t2
       on t1.proposalno=t2.proposalno 
       and nvl(t2.proposalno,'')!='' """)

    df_quotano_under.createOrReplaceTempView("df_quotano_under")

    val df_cpi_main_car_list9 = spark.sql("""select 
      t1.* 
      ,t2.underwriteenddate 
      from tmp_cpi_main_car_list8 t1 
      left join df_quotano_under t2 
       on t1.quotationno=t2.quotationno """)
    df_cpi_main_car_list9.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list9")

    spark.sql("refresh table tmp_quotano_policy")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'关联出核保完成时间'  as step_name "
      + ",'tmp_cpi_main_car_list9' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow
    val df_quotano_policy = spark.sql("""SELECT DISTINCT 
            T1.VEHICLE_ID
            ,T1.QUOTE_ROUNT
            ,T1.QUOTATIONNO 
            ,T2.POLICYNO 
            ,T2.UNDERWRITEENDDATE
            ,T2.STARTDATE
            ,T2.ENDDATE
            ,T2.CLASSCODE
            ,'Y' INITSTAT
      from df_proposalno t1 
      left join (select * from CCIC_SOURCE.t03_prpcmain where intg_datastate != 'D') t2
       on t1.proposalno=t2.proposalno 
       and nvl(t2.proposalno,'')!=''  where nvl(t2.policyno,'')!='' and datediff(t2.enddate,t2.startdate)>183   """)

    df_quotano_policy.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_quotano_policy")
    spark.sql("refresh table tmp_quotano_policy")
    spark.sql("uncache table df_proposalno")

    val df_split_policy1 = spark.sql("""select * from (select t.*
                               ,row_number() over(distribute by policyno sort by UNDERWRITEENDDATE desc) as ro
                               from tmp_quotano_policy t) d where d.ro=1""").drop("ro")

    df_split_policy1.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_quotano_policy2")
    spark.sql("refresh table tmp_quotano_policy2")

    //保单状态统计

    val df_policy_status = spark.sql(""" select t1.policyno
                                              ,count(*) cnt
                                              ,sum(case when kindcode in('BZ','C100120','C100150') then 0
                                                 else  1 end)    tra_cnt
                                 from tmp_quotano_policy2 t1
                                 inner join CCIC_SOURCE.t03_prpcitemkind T2
                                      ON t1.POLICYNO = T2.POLICYNO
                                      where intg_datastate != 'D'
                                 group by t1.policyno
                           """)
    df_policy_status.createOrReplaceTempView("df_policy_status")

    val df_policy_status_t = spark.sql("""select  policyno
                          ,case when cnt=1 and tra_cnt=0 then '1'    --单交强
                                when cnt=tra_cnt then '2'    --单商业
                                when cnt>tra_cnt then '3' --交强商业混合
                                 else '4' end          as policy_stat   --未知
                   from df_policy_status
                """)

    df_policy_status_t.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_policy_status_t")
    spark.sql("refresh table tmp_policy_status_t")

    spark.sql("""insert overwrite table tmp_policy_status_t2 select 
                             T1.VEHICLE_ID           
                            ,T1.QUOTE_ROUNT          
                            ,T1.QUOTATIONNO          
                            ,T1.POLICYNO             
                            ,T1.UNDERWRITEENDDATE    
                            ,T1.STARTDATE            
                            ,T1.ENDDATE              
                            ,T1.INITSTAT   
                            ,T2.POLICY_STAT
                            FROM tmp_quotano_policy2 T1
                            LEFT JOIN tmp_policy_status_t T2         
                             ON T1.POLICYNO=T2.POLICYNO
                             """)
    //df_policy_status_t.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_policy_status_t2")
    //spark.sql("refresh table tmp_policy_status_t2")

    spark.sql("cache table tmp_policy_status_t2")
    //拆单   U代表交强  D代表商业
    //1单商业虚拟交强
    val df_policy_split1 = spark.sql("""SELECT              
                   VEHICLE_ID   
                  ,QUOTE_ROUNT        
                  ,QUOTATIONNO         
                  ,concat(SUBSTR(POLICYNO, 1, 1) , 'U' , SUBSTR(POLICYNO, 3)) AS POLICYNO            
                  ,UNDERWRITEENDDATE   
                  ,STARTDATE           
                  ,ENDDATE
                  ,'1'  as policy_stat        
                  ,'N'  AS INITSTAT
            FROM tmp_policy_status_t2 where policy_stat='2'
            """)
    //2单商业
    val df_policy_split2 = spark.sql("""SELECT              
                   VEHICLE_ID   
                  ,QUOTE_ROUNT        
                  ,QUOTATIONNO         
                  ,POLICYNO            
                  ,UNDERWRITEENDDATE   
                  ,STARTDATE           
                  ,ENDDATE  
                  ,'2' as   policy_stat         
                  ,INITSTAT
            FROM tmp_policy_status_t2 where policy_stat='2'
            """)
    //3单交强虚拟商业
    val df_policy_split3 = spark.sql("""SELECT              
                   VEHICLE_ID   
                  ,QUOTE_ROUNT        
                  ,QUOTATIONNO         
                  ,concat(SUBSTR(POLICYNO, 1, 1) , 'D' , SUBSTR(POLICYNO, 3)) AS POLICYNO            
                  ,UNDERWRITEENDDATE   
                  ,STARTDATE           
                  ,ENDDATE
                  ,'2'  as policy_stat        
                  ,'Y'  AS INITSTAT
            FROM tmp_policy_status_t2 where policy_stat='1'
            """)
    //4单交强
    val df_policy_split4 = spark.sql("""SELECT              
                   VEHICLE_ID   
                  ,QUOTE_ROUNT        
                  ,QUOTATIONNO         
                  ,concat(SUBSTR(POLICYNO, 1, 1) , 'U' , SUBSTR(POLICYNO, 3)) AS POLICYNO             
                  ,UNDERWRITEENDDATE   
                  ,STARTDATE           
                  ,ENDDATE  
                  ,'1' as   policy_stat         
                  ,'N' as   INITSTAT
            FROM tmp_policy_status_t2 where policy_stat='1'
            """)
    //5混合单虚拟商业
    val df_policy_split5 = spark.sql("""SELECT              
                   VEHICLE_ID   
                  ,QUOTE_ROUNT        
                  ,QUOTATIONNO         
                  ,POLICYNO            
                  ,UNDERWRITEENDDATE   
                  ,STARTDATE           
                  ,ENDDATE
                  ,'2'  as policy_stat        
                  ,INITSTAT
            FROM tmp_policy_status_t2 where policy_stat='3'
            """)
    //6混合单虚拟交强
    val df_policy_split6 = spark.sql("""SELECT 
                   VEHICLE_ID
                  ,QUOTE_ROUNT
                  ,QUOTATIONNO
                  ,concat(SUBSTR(POLICYNO, 1, 1) , 'U' , SUBSTR(POLICYNO, 3)) AS POLICYNO            
                  ,UNDERWRITEENDDATE   
                  ,STARTDATE           
                  ,ENDDATE
                  ,'1'  as policy_stat        
                  ,INITSTAT
            FROM tmp_policy_status_t2 where policy_stat='3'
            """)

    val df_policy_split7 = df_policy_split1.union(df_policy_split2).union(df_policy_split3).union(df_policy_split4).union(df_policy_split5).union(df_policy_split6)
    df_policy_split7.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_policy_split")
    spark.sql("refresh table tmp_policy_split")

    spark.sql("uncache table tmp_policy_status_t2")

    //计算到期保单
    val df_pd1 = spark.sql("""select
        t.vehicle_id
        ,concat_ws('|',collect_set(concat_ws('#',t.policyno,substr(t.startdate,1,10)))) as policydate 
      from tmp_policy_split t 
      group by t.vehicle_id""")

    df_pd1.createOrReplaceTempView("df_pd1")
    spark.udf.register("compute_last_pol", compute_last_pol _)
    val df_pd2 = spark.sql("""select vehicle_id, split(arrstring,'\\|') as policyorder
        from (select vehicle_id,compute_last_pol(policydate) as policyorder from df_pd1) t
        lateral view explode(t.policyorder) adtable as arrstring""")

    df_pd2.createOrReplaceTempView("df_pd2")

    val df_pd3 = spark.sql("select vehicle_id,policyorder[0] as policyno,policyorder[1] as orderid from df_pd2")
    df_pd3.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_policy_order")
    spark.sql("refresh table tmp_policy_order")

    val df_pol_order_stat = spark.sql("""select
                                      t1.*
                                     ,t2.orderid
                               from tmp_policy_split t1
                               left join tmp_policy_order t2
                                 on t1.policyno=t2.policyno
                           """)
    df_pol_order_stat.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_pol_order_stat")
    spark.sql("refresh table tmp_pol_order_stat")

    val df_policy_bph = spark.sql("""select t1.VEHICLE_ID   ,T1.QUOTE_ROUNT         
                 ,t1.QUOTATIONNO          
                 ,t1.POLICYNO             
                 ,t1.UNDERWRITEENDDATE    
                 ,t1.STARTDATE            
                 ,t1.ENDDATE              
                 ,t1.INITSTAT             
                 ,t1.policy_stat
                 ,t2.policyno          as expiry_policyno         
                 ,t2.underwriteenddate as expiry_underwriteenddate
                 ,t2.startdate         as expiry_startdate        
                 ,t2.enddate           as expiry_enddate          
                 ,t2.initstat          as expiry_initstat
                 ,t2.policy_stat       as expiry_policy_stat         
              from tmp_pol_order_stat t1
                left join tmp_pol_order_stat t2 
                 on t1.vehicle_id=t2.vehicle_id
                and cast(t1.orderid as int) = cast(t2.orderid as int)+1
                and t1.policy_stat=t2.policy_stat
       """)

    df_policy_bph.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_policy_bph")
    spark.sql("refresh table tmp_policy_bph")

    val df_cpi_main_car_list10 = spark.sql("""select                        
      T1.QUOTE_ROUNT                              
      ,T1.QUOTATIONNO                              
      ,T1.PROPOSALNO                               
      ,T1.RENEW_FLAG                               
      ,T1.COMCODE                                  
      ,T1.QUOTE_TIME                               
      ,T1.BUSINESSNATURE                           
      ,T1.BUSINESSNATURE2                          
      ,T1.CHN_CODE                                 
      ,T1.STARTDATE                                
      ,T1.ENDDATE                                  
      ,ROUND(T1.SUMPREMIUM,4)  as  SUMPREMIUM                   
      ,T1.RISKCODE                                 
      ,T1.VEHICLE_ID                               
      ,T1.FRAMENO                                  
      ,T1.LICENSENO                                
      ,T1.BRANDNAME                                
      ,T1.factoryName                                
      ,T1.LICENSECOLORCODE                         
      ,T1.CARKINDCODE                              
      ,T1.HKFLAG                                   
      ,T1.HKLICENSENO                              
      ,T1.ENGINENO                                 
      ,T1.ENROLLDATE                               
      ,ROUND(T1.USEYEARS,4)   as USEYEARS                   
      ,T1.USENATURECODE                            
      ,ROUND(T1.PURCHASEPRICE,4)    as PURCHASEPRICE               
      ,ROUND(T1.SEATCOUNT,4)          as SEATCOUNT             
      ,ROUND(T1.TONCOUNT,4)         as TONCOUNT                  
      ,ROUND(T1.POWERSCALE,4)       as POWERSCALE               
      ,ROUND(T1.EXHAUSTSCALE,4)     as EXHAUSTSCALE               
      ,T1.FUELTYPE                                 
      , T1.AUTOINSURANCESCORE 
      ,T1.BUSI_QUOTE_FLAG                          
      ,T1.TRA_QUOTE_FLAG                           
      ,T1.DEMANDNO                                 
      ,ROUND(T1.EFFECTIVETRAFFICVIOLATION,4)     as EFFECTIVETRAFFICVIOLATION  
      ,ROUND(T1.SERIOUSTRAFFICVIOLATION,4)        as SERIOUSTRAFFICVIOLATION 
      ,T1.CLASS_TYPE                               
      ,ROUND(T1.CUSTNO,4)      as CUSTNO                    
      ,T1.INSUREDNAME                              
      ,T1.INSUREDSEX                               
      ,T1.INSUREDAGE                               
      ,T1.PROFITRATE_NCD                           
      ,T1.PROFITRATE_VIOLATION                     
      ,ROUND(T1.AMOUNT,4)        as AMOUNT                  
      ,ROUND(T1.PREMIUM,4)       as PREMIUM                   
      ,ROUND(T1.BENCHMARKPREMIUM,4)      as BENCHMARKPREMIUM          
      ,ROUND(T1.BUSI_PREMIUM,4)          as BUSI_PREMIUM          
      ,ROUND(T1.BUSI_BENCHMARKPREMIUM,4)   as BUSI_BENCHMARKPREMIUM        
      ,T1.INSURANCE_CODE                           
      ,T1.DISCOUNT_RANGE                           
      ,ROUND(T1.NET_FEE,4)                as NET_FEE         
      ,ROUND(T1.NET_FEE_PREMIUM,4)       as NET_FEE_PREMIUM          
      ,T1.QTY_PROPOSALNO                           
      ,T1.UNDERWRITEENDDATE                        
      ,t2.policyno 
      ,t1.INTG_DATASOURCE
      from tmp_cpi_main_car_list9 t1 
      left join tmp_quotano_policy2 t2 
       on t1.quotationno=t2.quotationno """)
    df_cpi_main_car_list10.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list10")
    spark.sql("refresh table tmp_cpi_main_car_list10")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'关联出保单号'  as step_name "
      + ",'tmp_cpi_main_car_list10' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")
    //续存年限字段
    v_begin_time = getnow
    val df_filter_main_car_list10 = spark.sql("select vehicle_id,quote_rount,policyno from tmp_cpi_main_car_list10").filter("nvl(policyno,'')!=''")

    df_filter_main_car_list10.createOrReplaceTempView("df_filter_main_car_list10")
    spark.sql("cache table df_filter_main_car_list10")

    val df_pol_inner_quote = spark.sql("""select distinct t1.vehicle_id,t1.quote_rount,t3.policyno from df_filter_main_car_list10  T1
                 INNER JOIN CCIC_SOURCE.t03_PrpCitemCar T3
                    ON t1.vehicle_id=t3.eaif_id
                   and t1.policyno=t3.policyno
                   and t3.intg_datastate<>'D'
              """)
    val df_pol_minus_quote = spark.sql("""select t3.vehicle_id
                                          ,t4.quote_rount
                                          ,t3.policyno
                     from (select  distinct t1.eaif_id as vehicle_id
                                             ,t1.policyno 
                             from CCIC_SOURCE.t03_PrpCitemCar t1
                                inner join df_filter_main_car_list10 t2
                                        on t1.eaif_id=t2.vehicle_id
                                        and t1.intg_datastate<>'D') t3
                             left join tmp_cpi_main_car_list10 t4
                                    on t3.vehicle_id=t4.vehicle_id
                                   and t3.policyno=t4.policyno
                    where t4.policyno is null
      """)

    val df_list_car_veh = df_pol_inner_quote.union(df_pol_minus_quote)

    df_list_car_veh.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_list_car_pol")
    spark.sql("refresh table tmp_list_car_pol")

    spark.sql("uncache table df_filter_main_car_list10")

    val df_pol_prpcmain = spark.sql("""select t1.vehicle_id
                    ,nvl(min(t1.quote_rount) over(partition by t1.vehicle_id order by t2.STARTDATE desc)
                        ,max(t1.quote_rount) over(partition by t1.vehicle_id order by t2.STARTDATE)) as quote_rount
                    ,t1.POLICYNO 
                    ,substr(T2.STARTDATE,1,10) as  STARTDATE    --起保日期
                    ,substr(T2.ENDDATE,1,10)   as  ENDDATE      --终保日期
                 from tmp_list_car_pol T1
                 INNER JOIN CCIC_SOURCE.t03_prpcmain T2
                    ON T1.POLICYNO = T2.POLICYNO and t2.intg_datastate<>'D' and datediff(t2.enddate,t2.startdate)>183""")
    df_pol_prpcmain.createOrReplaceTempView("df_pol_prpcmain")

    val df_cpi_main_car_list10_1 = spark.sql("""SELECT    
             VEHICLE_ID 
             ,quote_rount  
             ,POLICYNO     --保单号
             ,STARTDATE   --起保日期
             ,ENDDATE     --终保日期
             ,(CASE WHEN datediff(START_DATE , ENDDATE) > 30 THEN 1        
                   ELSE 0                                       --1为超过30天的数据
              END) AS FLAG                                      --是否30天内判断标识
             ,rn
        FROM  (select 
                    t.vehicle_id
                    ,cast(t.quote_rount as string)
                    ,t.POLICYNO 
                    ,t.STARTDATE    --起保日期
                    ,t.ENDDATE      --终保日期
                    ,LAG(substr(T.STARTDATE,1,10)) OVER(PARTITION BY T.VEHICLE_ID ORDER BY substr(T.STARTDATE,1,10) DESC) as START_DATE    
                    ,ROW_NUMBER() OVER(PARTITION BY T.vehicle_id  ,quote_rount     ORDER BY T.STARTDATE DESC) RN        
          from df_pol_prpcmain t) t """)

    df_cpi_main_car_list10_1.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list10_1")
    spark.sql("refresh table tmp_cpi_main_car_list10_1")

    val df_cpi_main_car_list10_2 = spark.sql("""select vehicle_id,quote_rount, startdate ,enddate ,policyno
            ,sum(lg) over(partition by vehicle_id order by startdate) as yearrount
       from (select vehicle_id,quote_rount, startdate ,enddate ,policyno
             ,nvl((lag(FLAG) over(partition by vehicle_id order by startdate)),0) as lg
      from TMP_CPI_MAIN_CAR_LIST10_1   )  t    
       """)
    df_cpi_main_car_list10_2.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list10_2")
    spark.sql("refresh table tmp_cpi_main_car_list10_2")

    val df_quoterount_grp = spark.sql(""" select vehicle_id
            ,quote_rount
            ,yearrount
            ,cast(datediff( max(enddate), min(startdate)) as string)  AS RENEWDAY
      from tmp_cpi_main_car_list10_2
      group by vehicle_id,quote_rount,yearrount
      """)
    df_quoterount_grp.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_quoterount_grp")
    spark.sql("refresh table tmp_quoterount_grp")

    //一个报价轮有多个连续的续存，取最新的一个
    val df_quoterount_yearrount_max = spark.sql("""select m.* from (select t.*,
            row_number() over(partition by vehicle_id,quote_rount order by yearrount desc ) as ro
            from tmp_quoterount_grp t ) m where m.ro=1 
       """).drop("ro")
    df_quoterount_yearrount_max.createOrReplaceTempView("df_quoterount_yearrount_max")

    val df_quoterount_years = spark.sql("""select vehicle_id
               ,quote_rount
               ,cast(floor(RENEWDAY/365) as int) + (case when ( RENEWDAY % 365 ) < 183 then 0 else 1 end) as renewyears
             from  (select vehicle_id
               ,quote_rount
               ,sum(RENEWDAY) over(partition by vehicle_id  order by quote_rount) as RENEWDAY
              from df_quoterount_yearrount_max ) t
       """)

    df_quoterount_years.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_quoterount_years")
    spark.sql("refresh table tmp_quoterount_years")

    //    spark.sql("""INSERT OVERWRITE TABLE tmp_cpi_main_car_list10_3
    //      SELECT VEHICLE_ID,quote_rount,
    //      case when MIN_BEGINDATE='2999-12-31' then 0
    //            else cast(floor(datediff( MAX_ENDDATE, MIN_BEGINDATE)/ 365.25)+1 as int) end AS RENEWYEARS
    //      FROM (SELECT T1.VEHICLE_ID   ,t1.quote_rount
    //            ,MIN(CASE WHEN NVL(T2.NEAR_STARTDATE,'') !='' AND  T1.STARTDATE>T2.NEAR_STARTDATE  --关联上 大于最后一次超过30天的起保日期
    //                        THEN T1.STARTDATE
    //                     WHEN NVL(T2.VEHICLE_ID,'')=''                                             --关联不上 说明一直有连续续存
    //                        THEN T1.STARTDATE
    //                     ELSE '2999-12-31' END            )           AS   MIN_BEGINDATE           --其他直接取最大时间min时不会取到
    //            ,MAX(CASE WHEN NVL(T2.NEAR_STARTDATE,'') !='' AND  T1.STARTDATE>T2.NEAR_STARTDATE  --关联上 大于最后一次超过30天的终保日期
    //                     THEN T1.ENDDATE
    //                     WHEN NVL(T2.VEHICLE_ID,'')=''                                             --关联不上 说明一直有连续续存
    //                        THEN T1.ENDDATE
    //                     ELSE '1900-01-01' END )                      AS   MAX_ENDDATE             --其他直接取最小时间max时不会取到
    //            FROM TMP_CPI_MAIN_CAR_LIST10_1 t1
    //            LEFT JOIN TMP_CPI_MAIN_CAR_LIST10_2 T2
    //                   ON T1.VEHICLE_ID=T2.VEHICLE_ID
    //                   --and t1.quote_rount=t2.quote_rount
    //                   GROUP BY T1.VEHICLE_ID,t1.quote_rount) T                                                  """)

    spark.sql("""INSERT OVERWRITE TABLE TMP_BUSI_PAY_CNT 
         SELECT DISTINCT T.QUOTATIONNO    ,t.DEMANDNO                                                    
           ,cast((CASE WHEN NVL(T.DEMANDNO,'')='' and nvl(T.LOSSTIME,'')='' and nvl(T.CLAIMNO,'')='' THEN 0     
           ELSE PRIOR_COUNT END) as int)                    AS PRIOR_COUNT                      --出险次数
          FROM (SELECT                                                                  
             T1.QUOTATIONNO                                                         
             ,T2.DEMANDNO                                                           
             ,T2.LOSSTIME                                                           
             ,T2.CLAIMNO                                                            
             ,COUNT(CONCAT(T1.QUOTATIONNO,T2.DEMANDNO,T2.LOSSTIME,T2.CLAIMNO))      
                   OVER (PARTITION BY T1.QUOTATIONNO) AS PRIOR_COUNT                            --出险次数
      FROM TMP_CPI_MAIN_CAR_LIST10 T1                                               
      LEFT JOIN (select * from CCIC_SOURCE.T06_PRPCIINSDEMANDPAY where intg_datastate != 'D')  T2 ON T1.DEMANDNO=T2.DEMANDNO   and nvl(t1.DEMANDNO,'')!=''
      and (T2.BUSIORBZ='2' OR T2.BUSIORBZ IS NULL) 
                AND T2.INTG_DATASTATE<>'D'                                         
      WHERE SUBSTR(T1.QUOTATIONNO,1,3) IN('QBI','QDE')) T """)

    spark.sql("INSERT OVERWRITE TABLE TMP_BUSI_PAY_CNT2 SELECT                                                                     "
      + "                 T1.QUOTATIONNO,                                           "
      + "                 T1.DEMANDNO,                                              "
      + "                 T3.PROFITRATEREASON,                                      "
      + "                 T3.NONADJUSTREASONCODE                                    "
      + "          FROM TMP_CPI_MAIN_CAR_LIST10                T1                   "
      + "          INNER JOIN CCIC_SOURCE.T06_PRPCIINSUREDEMAND    T2                "
      + "          ON T1.DEMANDNO=T2.DEMANDNO                                       "
      + "          AND (T2.POLICYRECTYPE IN('2','4')OR T2.POLICYRECTYPE IS NULL)    "
      + "          AND T2.INTG_DATASTATE<>'D'                                       "
      + "          INNER JOIN CCIC_SOURCE.T06_PRPCIINSDEMANDPROFIT T3                "
      + "          ON T1.DEMANDNO=T3.DEMANDNO                                       "
      + "          AND T3.PROFITCODE='13'                                           "
      + "          AND T3.INTG_DATASTATE<>'D'                                       ")

    spark.sql("INSERT OVERWRITE TABLE  TMP_BUSI_PAY_CNT3 SELECT   T1.QUOTATIONNO,       "
      + "    (CASE WHEN RISKCODE  IN ('DDC','DDG') THEN                            "
      + "          (CASE WHEN SUBSTR(T1.COMCODE,1,4) IN ('1101','3502')  THEN       "
      + "                (CASE WHEN T2.PROFITRATEREASON IN ('B42','B43','B44') THEN '连续3年没有发生赔款'   "
      + "                      WHEN T2.PROFITRATEREASON='B45' THEN '连续2年没有发生赔款'                    "
      + "                      WHEN T2.PROFITRATEREASON='B46' THEN '上年没有发生赔款'                   "
      + "                      WHEN T2.PROFITRATEREASON='B47' THEN '上年发生1次赔款'                   "
      + "                      WHEN T2.PROFITRATEREASON='B48' THEN '上年发生2次赔款'                   "
      + "                      WHEN T2.PROFITRATEREASON='B49' THEN '上年发生3次赔款'                   "
      + "                      WHEN T2.PROFITRATEREASON='B50' THEN '上年发生4次赔款'                   "
      + "                      WHEN T2.PROFITRATEREASON IN ('B51','B52','B53','B54','B55','B56','B57') THEN '上年发生5次及以上赔款'   "
      + "                 ELSE (CASE WHEN T2.NONADJUSTREASONCODE IN ('01', '06') THEN '过户车' "
      + "                            WHEN T2.NONADJUSTREASONCODE IN ('02', '08') THEN '脱保车' "
      + "                       ELSE (CASE WHEN (T1.STARTDATE-T1.ENROLLDATE+1)/365<=0.75  THEN '新车' "
      + "                             ELSE '首年投保'  "
      + "                             END)   "
      + "                       END)          "
      + "                 END)               "
      + "           ELSE   (CASE WHEN T2.PROFITRATEREASON='B31' AND PRIOR_COUNT=1  THEN '上年发生1次赔款'    "
      + "                        WHEN T2.PROFITRATEREASON='B13' THEN '连续3年没有发生赔款'                     "
      + "                        WHEN T2.PROFITRATEREASON='B12' THEN '连续2年没有发生赔款'                     "
      + "                        WHEN T2.PROFITRATEREASON='B11' THEN '上年没有发生赔款'                       "
      + "                        WHEN T2.PROFITRATEREASON='B32' THEN '上年发生2次赔款'                       "
      + "                        WHEN T2.PROFITRATEREASON='B33' THEN '上年发生3次赔款'                       "
      + "                        WHEN T2.PROFITRATEREASON='B34' THEN '上年发生4次赔款'                       "
      + "                        WHEN T2.PROFITRATEREASON='B35' THEN '上年发生5次及以上赔款'                    "
      + "                   ELSE (CASE WHEN T2.NONADJUSTREASONCODE IN ('01', '06') THEN '过户车'         "
      + "                              WHEN T2.NONADJUSTREASONCODE IN ('02', '08') THEN '脱保车'         "
      + "                         ELSE (CASE WHEN (T1.STARTDATE-T1.ENROLLDATE+1)/365<=0.75  THEN '新车' "
      + "                               ELSE '首年投保'                                                   "
      + "                               END)                                                          "
      + "                         END)                                                                "
      + "                   END)                                                                      "
      + "            END)                                                                             "
      + "     END) AS NCDCLASS_SYX                                                                    "
      + "FROM TMP_CPI_MAIN_CAR_LIST10              T1                                                 "
      + "JOIN TMP_BUSI_PAY_CNT2              T2                                                       "
      + "ON T1.QUOTATIONNO=T2.QUOTATIONNO                                                             "
      + "JOIN TMP_BUSI_PAY_CNT              T3                                                       "
      + "ON T1.QUOTATIONNO=T3.QUOTATIONNO  ")

    val df_cpi_main_car_list11 = spark.sql("""SELECT                                  
            T1.* 
            ,CASE WHEN T1.INTG_DATASOURCE!='98' THEN
                CASE WHEN T1.USENATURECODE = '85' AND T1.CARKINDCODE IN ('A0', 'B0') THEN
                 'A家庭自用车'
                WHEN T1.USENATURECODE IN ('83', '84') AND
                     T1.CARKINDCODE IN ('A0', 'B0') THEN
                 'B非营业客车'
                WHEN T1.USENATURECODE IN ('83', '84', '85') AND
                     T1.CARKINDCODE IN ('H0', 'I0', 'I1', 'L0', 'ZZ') THEN
                 'C非营业货车'
                WHEN T1.USENATURECODE IN
                     ('82', '86', '87', '88', '89', '90', '91', '92', '93', '99') AND
                     T1.CARKINDCODE IN ('H0', 'I0', 'I1', 'ZZ') THEN
                 'D营业货车'
                WHEN T1.USENATURECODE IN ('91', '93', '99') AND
                     T1.CARKINDCODE IN ('L0') THEN
                 'D营业货车'
                WHEN T1.USENATURECODE IN
                     ('82', '86', '87', '88', '89', '90', '91', '92', '93', '99') AND
                     T1.CARKINDCODE IN ('A0', 'B0') THEN
                 'E营业客车'
                WHEN T1.USENATURECODE IN ('82', '86', '87', '88', '89', '90', '92') AND
                     CARKINDCODE IN ('L0') THEN
                 'E营业客车'
                WHEN T1.CARKINDCODE IN ('M0', 'M1', '31', '32') THEN
                 'F摩托车'
                WHEN T1.CARKINDCODE IN ('J0', 'J1', 'N0') THEN
                 'G拖拉机'
                WHEN T1.CARKINDCODE IN ('G0') AND
                     T1.USENATURECODE IN ('83', '84', '85') THEN
                 'H非营业挂车'
                WHEN T1.CARKINDCODE IN ('G0') AND
                     T1.USENATURECODE IN
                     ('82', '86', '87', '88', '89', '90', '91', '92', '93', '99') THEN
                 'I营业挂车'
                ELSE
                 'J特种车'
               END
            ELSE
               CASE
                WHEN T1.USENATURECODE = '85' AND T1.CARKINDCODE IN ('11') THEN
                 'A家庭自用车'
                WHEN T1.USENATURECODE IN ('240','250','200','220','230','84') AND
                     T1.CARKINDCODE IN ('11') THEN
                 'B非营业客车'
                WHEN T1.USENATURECODE IN ('240','250','200','220','230','84', '85') AND
                     T1.CARKINDCODE IN ('21','22','94','11') THEN
                 'C非营业货车'
                WHEN T1.USENATURECODE IN
                     ('101','102','103','104','106') AND
                     T1.CARKINDCODE IN ('21','22','94') THEN
                 'D营业货车'
                WHEN T1.USENATURECODE IN ('104','106') AND
                     T1.CARKINDCODE IN ('11','21') THEN
                 'D营业货车'
                WHEN T1.USENATURECODE IN
                     ('101','102','103','104','106') AND
                     T1.CARKINDCODE IN ('11') THEN
                 'E营业客车'
                WHEN T1.USENATURECODE IN ('101','102','103','104','106') AND
                     CARKINDCODE IN ('11','21') THEN
                 'E营业客车'
                WHEN T1.CARKINDCODE IN ('51', '52', '53') THEN
                 'F摩托车'
                WHEN T1.CARKINDCODE IN ('61', '62', '63', '64') THEN
                 'G拖拉机'
                WHEN T1.CARKINDCODE IN ('31') AND
                     T1.USENATURECODE IN ( '84', '85','200','220','230','240','250') THEN
                 'H非营业挂车'
                WHEN T1.CARKINDCODE IN ('31') AND
                     T1.USENATURECODE IN
                     ('101', '102', '103', '104', '106') THEN
                 'I营业挂车'
                ELSE
                 'J特种车'
               END
            END  AS USENATURENAME --使用性质名称                                    
            ,T2.NCDCLASS_SYX                       
            ,T5.RENEWYEARS 
            ,CASE WHEN T1.RENEW_FLAG = '新车' THEN
               '新车 - 尚未投保过'
              WHEN T1.RENEW_FLAG = '首年投保或转保'   AND
                   ADD_MONTHS(T4.ENDDATE, 12) < '""" + v_endDay + """' THEN
               '旧车 - 曾经在大地有过投保，至少上1年不在大地'
              WHEN T1.RENEW_FLAG = '首年投保或转保' THEN
               '旧车 - 首次在大地投保，以前在其他公司'
              WHEN T1.RENEW_FLAG = '续保' AND 1 <= T5.RENEWYEARS AND T5.RENEWYEARS < 2 THEN
               '旧车 - 已有1年承保期'
              WHEN T1.RENEW_FLAG = '续保' AND 2 <= T5.RENEWYEARS AND T5.RENEWYEARS < 3 THEN
               '旧车 - 连续2年承保'
              WHEN T1.RENEW_FLAG = '续保' AND 3 <= T5.RENEWYEARS AND T5.RENEWYEARS < 4 THEN
               '旧车 - 连续3年承保'
              WHEN T1.RENEW_FLAG = '续保' AND 4 <= T5.RENEWYEARS THEN
               '旧车 - 连续4年及以上承保'
            END                  AS VEHICLE_STATUS                        
          FROM TMP_CPI_MAIN_CAR_LIST10 T1         
          LEFT JOIN TMP_BUSI_PAY_CNT3 T2          
             ON T1.QUOTATIONNO=T2.QUOTATIONNO        
          LEFT JOIN tmp_quoterount_years T5  
             ON T1.VEHICLE_ID=T5.VEHICLE_ID 
             and t1.quote_rount=t5.quote_rount      
          LEFT JOIN TMP_CPI_MAIN_CAR_LIST10_1 T4  
             ON T1.VEHICLE_ID=T4.VEHICLE_ID    
             and t1.quote_rount=t4.quote_rount and T4.RN = 1 """)
    df_cpi_main_car_list11.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list11")
    spark.sql("refresh table tmp_cpi_main_car_list11")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'关联续存年限和商业险出险次数解析，生成出所有需要计算的清单数据'  as step_name "
      + ",'tmp_cpi_main_car_list11' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    //每轮最近6个月的是否是单交强
    val df_six_months_grp_max = spark.sql("""select
           vehicle_id
           ,quote_rount
           ,max(quote_time) as max_quote_time 
         from tmp_cpi_main_car_list11 
         group by vehicle_id,quote_rount
    """)
    df_six_months_grp_max.createTempView("df_six_months_grp_max")

    val df_six_months_tra_flag = spark.sql("""
           select
           t1.QUOTATIONNO
           ,case when substr(t1.quotationno,1,3) IN('QCI','QDF') then 0 else 1 end  as only_tra_flag
           from tmp_cpi_main_car_list11 t1
           inner join df_six_months_grp_max t2
              on t1.vehicle_id = t2.vehicle_id
             and t1.quote_rount = t2.quote_rount
             and t1.quote_time >= date_add(t2.max_quote_time,-183)
           """)
    df_six_months_tra_flag.createTempView("df_six_months_tra_flag")

    v_begin_time = getnow
    spark.sql("insert overwrite table tmp_cpi_main_car_grp "
      + "select "
      + "  vehicle_id "
      + "  ,quote_rount "
      + "  ,max(RENEWYEARS)                     as        renewyears      "
      + "  ,max(case when substr(t1.quotationno,1,3) IN('QCI','QDF') then quote_time else '' end )       as last_tra_quote_time "
      + "  ,max(case when substr(t1.quotationno,1,3) IN('QBI','QDE') then quote_time else '' end )       as last_busi_quote_time "
      + "  ,case when min(case when substr(t1.quotationno,1,3) IN('QCI','QDF') then quote_time else '2999-12-31' end )='2999-12-31' then null "
      + "                   else   min(case when substr(t1.quotationno,1,3) IN('QCI','QDF') then quote_time else '2999-12-31' end ) end   as early_tra_quote_time "
      + "  ,case when min(case when substr(t1.quotationno,1,3) IN('QBI','QDE') then quote_time else '2999-12-31' end )='2999-12-31' then null "
      + "                   else   min(case when substr(t1.quotationno,1,3) IN('QBI','QDE') then quote_time else '2999-12-31' end ) end   as early_busi_quote_time "
      + "  ,max(NET_FEE)                                                                    as max_net_fee "
      + "  ,nvl(min(NET_FEE),0)                                                             as min_net_fee "
      + "  ,nvl(cast(substr(max(case when nvl(amount,'')!='' then concat(quote_time,AMOUNT) else '0' end),11) as double),0)  as three_premium "
      + "  ,cast(count(qty_proposalno) as STRING)                                           as qty_proposalno "
      + "  ,cast(count(distinct policyno) as STRING)                                        as qty_rount_policy "
      + "  ,sum(busi_quote_flag)                                                             as busi_quote_cnt "
      + "  ,sum(tra_quote_flag)                                                              as tra_quote_cnt "
      + "  ,cast(count(distinct chn_code) as STRING)                                          as qty_chn_code "
      + "  ,cast(count(distinct comcode) as STRING)                                          as qty_com "
      + "  ,concat_ws('|',collect_set(distinct  case when net_fee_premium=0 then null else cast(net_fee_premium as string) end)) as net_fee_premium "
      + "  ,concat_ws('|',collect_set(distinct  case when premium=0 then null else cast(premium as string) end )               )  as premium "
      + "  ,concat_ws('|',collect_set(distinct  case when benchmarkpremium=0 then null else cast(benchmarkpremium as string) end) ) as benchmarkpremium "
      + "  ,substr(max(case when substr(t1.quotationno,1,3) IN('QBI','QDE') then concat(quote_time,policyno) else null end ),11)  as busi_policyno"
      + "  ,substr(max(case when substr(t1.quotationno,1,3) IN('QCI','QDF') then concat(quote_time,policyno) else null end ),11)  as tra_policyno"
      + "  ,case when sum(nvl(t2.only_tra_flag,0))=0 then '07' else null end  as only_tra_flag "
      + "from tmp_cpi_main_car_list11 t1 "
      + "   left join    df_six_months_tra_flag t2         "
      + "     on t1.QUOTATIONNO=t2.QUOTATIONNO "
      + "group by vehicle_id,quote_rount distribute by vehicle_id,quote_rount")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'按车辆id和报价轮次的聚合'  as step_name "
      + ",'tmp_cpi_main_car_grp' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow

    spark.sql("insert overwrite table tmp_cpi_main_car_full "
      + "select   t.vehicle_id ,t.VEHICLE_STATUS "
      + "  ,t.quote_rount "
      + "  ,t.RENEW_FLAG                "
      + "  ,t.frameno                   "
      + "  ,t.LicenseNo                 "
      + "  ,t.brandname                 "
      + "  ,t.factoryName                 "
      + "  ,t.class_type                "
      + "  ,t.licensecolorcode          "
      + "  ,t.carkindcode               "
      + "  ,t.hkflag                    "
      + "  ,t.hklicenseno               "
      + "  ,t.engineno                  "
      + "  ,t.enrolldate                "
      + "  ,t.useyears                  "
      + "  ,t.usenaturecode             "
      + "  ,t.USENATURENAME             "
      + "  ,t.purchaseprice             "
      + "  ,t.seatcount                 "
      + "  ,t.toncount                  "
      + "  ,t.powerscale                "
      + "  ,t.exhaustscale              "
      + "  ,t.fueltype                  "
      + ", t.AUTOINSURANCESCORE "
      + "  ,t.custno                    "
      + "  ,t.insuredname               "
      + "  ,t.insuredsex                "
      + "  ,t.insuredage                "
      + "  ,t.effectivetrafficviolation "
      + "  ,t.serioustrafficviolation   "
      + "  ,t.last_chn_code "
      + "  ,t.comcode                     "
      + "  ,t.insurance_code              "
      + "  ,t.discount_range              "
      + " from ( "
      + "select   vehicle_id ,VEHICLE_STATUS"
      + "  ,quote_rount "
      + "  ,row_number() over(distribute by vehicle_id,quote_rount sort by quote_time desc) as ro "
      + "  ,RENEW_FLAG                "
      + "  ,frameno                   "
      + "  ,LicenseNo                 "
      + "  ,brandname                 "
      + "  ,factoryName                 "
      + "  ,class_type                "
      + "  ,licensecolorcode          "
      + "  ,carkindcode               "
      + "  ,hkflag                    "
      + "  ,hklicenseno               "
      + "  ,engineno                  "
      + "  ,enrolldate                "
      + "  ,useyears                  "
      + "  ,usenaturecode             "
      + "  ,USENATURENAME             "
      + "  ,purchaseprice             "
      + "  ,seatcount                 "
      + "  ,toncount                  "
      + "  ,powerscale                "
      + "  ,exhaustscale              "
      + "  ,fueltype                  "
      + ", AUTOINSURANCESCORE "
      + "  ,custno                    "
      + "  ,insuredname               "
      + "  ,insuredsex                "
      + "  ,insuredage                "
      + "  ,effectivetrafficviolation "
      + "  ,serioustrafficviolation   "
      + "  ,chn_code   as  last_chn_code "
      + "  ,comcode                     "
      + "  ,insurance_code              "
      + "  ,discount_range              "
      + "from tmp_cpi_main_car_list11   "
      + ") t where t.ro=1              distribute by vehicle_id,quote_rount ")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'每轮最近一次报价情况'  as step_name "
      + ",'tmp_cpi_main_car_full' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow
    spark.sql("insert overwrite table tmp_cpi_main_car_busi "
      + " select "
      + "  vehicle_id "
      + "  ,quote_rount "
      + "  ,busi_BUSINESSNATURE "
      + "  ,busi_BUSINESSNATURE2 "
      + "  ,busi_startdate "
      + "  ,busi_enddate "
      + "  ,profitrate_ncd "
      + "  ,profitrate_violation "
      + "  ,busi_premium "
      + "  ,busi_BenchMarkPremium "
      + "  ,busi_net_fee "
      + "  ,ncdclass_syx "
      + " from ( "
      + "select                                          "
      + "  vehicle_id "
      + "  ,quote_rount "
      + "  ,row_number() over(distribute by vehicle_id,quote_rount sort by quote_time desc) as ro "
      + "  ,BUSINESSNATURE             as   busi_BUSINESSNATURE "
      + "  ,BUSINESSNATURE2            as   busi_BUSINESSNATURE2 "
      + "  ,startdate                  as   busi_startdate "
      + "  ,enddate                    as   busi_enddate "
      + "  ,profitrate_ncd "
      + "  ,profitrate_violation "
      + "  ,busi_premium               as   busi_premium "
      + "  ,busi_benchmarkpremium      as   busi_BenchMarkPremium "
      + "  ,net_fee                    as   busi_net_fee "
      + "  ,ncdclass_syx               as   ncdclass_syx "
      + "from tmp_cpi_main_car_list11 "
      + "where substr(quotationno,1,3) IN('QBI','QDE') "
      + ") t where t.ro=1 distribute by vehicle_id,quote_rount")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'每轮最近一次商业险报价情况'  as step_name "
      + ",'tmp_cpi_main_car_busi' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow

    spark.sql("insert overwrite table tmp_cpi_main_car_tra "
      + " select "
      + "  vehicle_id "
      + "  ,quote_rount "
      + "  ,tra_BUSINESSNATURE "
      + "  ,tra_BUSINESSNATURE2 "
      + "  ,tra_startdate "
      + "  ,tra_enddate "
      + " from ( "
      + "select "
      + "  vehicle_id "
      + "  ,quote_rount "
      + "  ,row_number() over(distribute by vehicle_id,quote_rount sort by quote_time desc) as ro "
      + "  ,BUSINESSNATURE             as   tra_BUSINESSNATURE "
      + "  ,BUSINESSNATURE2            as   tra_BUSINESSNATURE2 "
      + "  ,startdate                  as   tra_startdate "
      + "  ,enddate                    as   tra_enddate "
      + " from tmp_cpi_main_car_list11 "
      + " where substr(quotationno,1,3) IN('QCI','QDF') "
      + ") t where t.ro=1 distribute by vehicle_id,quote_rount")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'每轮最近一次交强险报价情况'  as step_name "
      + ",'tmp_cpi_main_car_tra' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow

    spark.sql("insert overwrite table tmp_cpi_main_car_pol "
      + "select "
      + "  vehicle_id "
      + "  ,quote_rount "
      + "  ,ProposalNo "
      + "  ,underwriteenddate "
      + " from (  "
      + "select          "
      + "  vehicle_id "
      + "  ,quote_rount "
      + "  ,row_number() over(distribute by vehicle_id,quote_rount sort by underwriteenddate desc) as ro "
      + "  ,ProposalNo "
      + "  ,underwriteenddate "
      + "from tmp_cpi_main_car_list11 "
      + ") t where t.ro=1 distribute by vehicle_id,quote_rount")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'每轮最近一次核保完成时的报价情况'  as step_name "
      + ",'tmp_cpi_main_car_pol' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow

    val df_cpi_main_car_list12 = spark.sql("select     "
      + "    t1.vehicle_id                  "
      + "  , t1.quote_rount                 "
      + "  , t2.vehicle_status              "
      + "  , t1.renewyears                  "
      + "  , t1.last_tra_quote_time         "
      + "  , t1.last_busi_quote_time        "
      + "  , t1.early_tra_quote_time        "
      + "  , t1.early_busi_quote_time       "
      + "  , t1.max_net_fee                 "
      + "  , t1.min_net_fee                 "
      + "  , t1.three_premium               "
      + "  , t1.qty_proposalno              "
      + "  , t1.qty_rount_policy            "
      + "  , t1.busi_quote_cnt              "
      + "  , t1.tra_quote_cnt               "
      + "  , t1.qty_chn_code                "
      + "  , t1.qty_com                     "
      + "  , t1.net_fee_premium             "
      + "  , t1.premium                     "
      + "  , t1.benchmarkpremium            "
      + "  , t1.busi_policyno               "
      + "  , t1.tra_policyno                "
      + "  , t2.renew_flag                  "
      + "  , t2.frameno                     "
      + "  , t2.licenseno                   "
      + "  , t2.brandname                   "
      + "  , t2.factoryName                   "
      + "  , t2.class_type                  "
      + "  , t2.licensecolorcode            "
      + "  , t2.carkindcode                 "
      + "  , t2.hkflag                      "
      + "  , t2.hklicenseno                 "
      + "  , t2.engineno                    "
      + "  , t2.enrolldate                  "
      + "  , t2.useyears                    "
      + "  , t2.usenaturecode               "
      + "  , t2.USENATURENAME               "
      + "  , t2.purchaseprice               "
      + "  , t2.seatcount                   "
      + "  , t2.toncount                    "
      + "  , t2.powerscale                  "
      + "  , t2.exhaustscale                "
      + "  , t2.fueltype                    "
      + "  , t2.AUTOINSURANCESCORE          "
      + "  , t2.custno                      "
      + "  , t2.insuredname                 "
      + "  , t2.insuredsex                  "
      + "  , t2.insuredage                  "
      + "  , t2.effectivetrafficviolation   "
      + "  , t2.serioustrafficviolation     "
      + "  , t2.last_chn_code               "
      + "  , t2.comcode                     "
      + "  , nvl(t1.only_tra_flag,t2.insurance_code) as         insurance_code      "
      + "  , t2.discount_range              "
      + "from tmp_cpi_main_car_grp t1 "
      + "left join tmp_cpi_main_car_full t2 "
      + "on concat(t1.vehicle_id,t1.quote_rount)=concat(t2.vehicle_id,t2.quote_rount)"
      + "distribute by vehicle_id,quote_rount")

    df_cpi_main_car_list12.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list12")
    spark.sql("refresh table tmp_cpi_main_car_list12")

    val df_cpi_main_car_list13 = spark.sql("select     "
      + "    t1.vehicle_id                  "
      + "  , t1.quote_rount                 "
      + "  , t1.vehicle_status              "
      + "  , t1.renewyears                  "
      + "  , t1.last_tra_quote_time         "
      + "  , t1.last_busi_quote_time        "
      + "  , t1.early_tra_quote_time        "
      + "  , t1.early_busi_quote_time       "
      + "  , t1.max_net_fee                 "
      + "  , t1.min_net_fee                 "
      + "  , t1.three_premium               "
      + "  , t1.qty_proposalno              "
      + "  , t1.qty_rount_policy            "
      + "  , t1.busi_quote_cnt              "
      + "  , t1.tra_quote_cnt               "
      + "  , t1.qty_chn_code                "
      + "  , t1.qty_com                     "
      + "  , t1.net_fee_premium             "
      + "  , t1.premium                     "
      + "  , t1.benchmarkpremium            "
      + "  , t1.renew_flag                  "
      + "  , t1.frameno                     "
      + "  , t1.licenseno                   "
      + "  , t1.brandname                   "
      + "  , t1.factoryName                   "
      + "  , t1.class_type                  "
      + "  , t1.licensecolorcode            "
      + "  , t1.carkindcode                 "
      + "  , t1.hkflag                      "
      + "  , t1.hklicenseno                 "
      + "  , t1.engineno                    "
      + "  , t1.enrolldate                  "
      + "  , t1.useyears                    "
      + "  , t1.usenaturecode               "
      + "  , t1.USENATURENAME               "
      + "  , t1.purchaseprice               "
      + "  , t1.seatcount                   "
      + "  , t1.toncount                    "
      + "  , t1.powerscale                  "
      + "  , t1.exhaustscale                "
      + "  , t1.fueltype                    "
      + ", t1.AUTOINSURANCESCORE "
      + "  , t1.custno                      "
      + "  , t1.insuredname                 "
      + "  , t1.insuredsex                  "
      + "  , t1.insuredage                  "
      + "  , t1.effectivetrafficviolation   "
      + "  , t1.serioustrafficviolation     "
      + "  , t1.last_chn_code               "
      + "  , t1.comcode                     "
      + "  , t1.insurance_code              "
      + "  , t1.discount_range              "
      + "  , t3.busi_businessnature         "
      + "  , t3.busi_businessnature2        "
      + "  , t3.busi_startdate              "
      + "  , t3.busi_enddate                "
      + "  , t3.profitrate_ncd              "
      + "  , t3.profitrate_violation        "
      + "  , t3.busi_premium                "
      + "  , t3.busi_benchmarkpremium       "
      + "  , t3.busi_net_fee                "
      + "  , t1.busi_policyno               "
      + "  , t1.tra_policyno                "
      + "  , t3.ncdclass_syx                "
      + "from tmp_cpi_main_car_list12 t1 "
      + "left join tmp_cpi_main_car_busi t3 "
      + "on concat(t1.vehicle_id,t1.quote_rount)=concat(t3.vehicle_id,t3.quote_rount)"
      + "distribute by vehicle_id,quote_rount")

    df_cpi_main_car_list13.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list13")
    spark.sql("refresh table tmp_cpi_main_car_list13")

    val df_cpi_main_car_list14 = spark.sql("select     "
      + "    t1.vehicle_id                  "
      + "  , t1.quote_rount                 "
      + "  , t1.vehicle_status              "
      + "  , t1.renewyears                  "
      + "  , t1.last_tra_quote_time         "
      + "  , t1.last_busi_quote_time        "
      + "  , t1.early_tra_quote_time        "
      + "  , t1.early_busi_quote_time       "
      + "  , t1.max_net_fee                 "
      + "  , t1.min_net_fee                 "
      + "  , t1.three_premium               "
      + "  , t1.qty_proposalno              "
      + "  , t1.qty_rount_policy            "
      + "  , t1.busi_quote_cnt              "
      + "  , t1.tra_quote_cnt               "
      + "  , t1.qty_chn_code                "
      + "  , t1.qty_com                     "
      + "  , t1.net_fee_premium             "
      + "  , t1.premium                     "
      + "  , t1.benchmarkpremium            "
      + "  , t1.renew_flag                  "
      + "  , t1.frameno                     "
      + "  , t1.licenseno                   "
      + "  , t1.brandname                   "
      + "  , t1.factoryName                   "
      + "  , t1.class_type                  "
      + "  , t1.licensecolorcode            "
      + "  , t1.carkindcode                 "
      + "  , t1.hkflag                      "
      + "  , t1.hklicenseno                 "
      + "  , t1.engineno                    "
      + "  , t1.enrolldate                  "
      + "  , t1.useyears                    "
      + "  , t1.usenaturecode               "
      + "  , t1.USENATURENAME               "
      + "  , t1.purchaseprice               "
      + "  , t1.seatcount                   "
      + "  , t1.toncount                    "
      + "  , t1.powerscale                  "
      + "  , t1.exhaustscale                "
      + "  , t1.fueltype                    "
      + "  , t1.AUTOINSURANCESCORE          "
      + "  , t1.custno                      "
      + "  , t1.insuredname                 "
      + "  , t1.insuredsex                  "
      + "  , t1.insuredage                  "
      + "  , t1.effectivetrafficviolation   "
      + "  , t1.serioustrafficviolation     "
      + "  , t1.last_chn_code               "
      + "  , t1.comcode                     "
      + "  , t1.insurance_code              "
      + "  , t1.discount_range              "
      + "  , t1.busi_businessnature         "
      + "  , t1.busi_businessnature2        "
      + "  , t1.busi_startdate              "
      + "  , t1.busi_enddate                "
      + "  , t1.profitrate_ncd              "
      + "  , t1.profitrate_violation        "
      + "  , t1.busi_premium                "
      + "  , t1.busi_benchmarkpremium       "
      + "  , t1.busi_net_fee                "
      + "  , t1.busi_policyno               "
      + "  , t1.ncdclass_syx                "
      + "  , t4.tra_businessnature          "
      + "  , t4.tra_businessnature2         "
      + "  , t4.tra_startdate               "
      + "  , t4.tra_enddate                 "
      + "  , t1.tra_policyno                "
      + "  , t5.ProposalNo                  "
      + "  , t5.underwriteenddate           "
      + "from tmp_cpi_main_car_list13 t1 "
      + "left join tmp_cpi_main_car_tra t4 "
      + "on concat(t1.vehicle_id,t1.quote_rount)=concat(t4.vehicle_id,t4.quote_rount)"
      + "left join tmp_cpi_main_car_pol t5 "
      + "on concat(t1.vehicle_id,t1.quote_rount)=concat(t5.vehicle_id,t5.quote_rount)"
      + "distribute by vehicle_id")

    df_cpi_main_car_list14.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list14")
    spark.sql("refresh table tmp_cpi_main_car_list14")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "      
      + ",'聚合计算结束后按车辆id、报价轮次拼成宽表'  as step_name "
      + ",'tmp_cpi_main_car_list14' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow

    val df_cpi_main_car_list15 = spark.sql("select "
      + "t1.vehicle_id    "
      + ",t1.quote_rount    "
      + ",t1.vehicle_status              "
      + ",t1.renew_flag    "
      + ",t1.renewyears    "
      + ",t1.frameno    "
      + ",t1.licenseno                             "
      + ",t1.brandname                             "
      + ",t1.factoryName                             "
      + ",t1.class_type                            "
      + ",t1.licensecolorcode                      "
      + ",t1.carkindcode                           "
      + ",t1.hkflag                                "
      + ",t1.hklicenseno                           "
      + ",t1.engineno                              "
      + ",t1.enrolldate                            "
      + ",t1.useyears                              "
      + ",t1.usenaturecode                         "
      + ",t1.USENATURENAME                         "
      + ",t1.purchaseprice                         "
      + ",t1.seatcount                             "
      + ",t1.toncount                              "
      + ",t1.powerscale                            "
      + ",t1.exhaustscale                          "
      + ",t1.fueltype                              "
      + ",t1.underwriteenddate                     "
      + ",t1.custno                                "
      + ",t1.insuredname                           "
      + ",t1.insuredsex                            "
      + ",t1.insuredage                            "
      + ",t1.early_busi_quote_time    "
      + ",t1.early_tra_quote_time    "
      + ",t1.last_busi_quote_time    "
      + ",t1.last_tra_quote_time    "
      + ",t1.busi_businessnature    "
      + ",t1.tra_businessnature    "
      + ",t1.busi_businessnature2    "
      + ",t1.tra_businessnature2    "
      + ",t1.busi_startdate    "
      + ",t1.tra_startdate    "
      + ",t1.busi_enddate     "
      + ",t1.tra_enddate    "
      + ", t1.AUTOINSURANCESCORE "
      + ",t1.ncdclass_syx    "
      + ",t1.profitrate_ncd    "
      + ",t1.profitrate_violation    "
      + ",t1.effectivetrafficviolation    "
      + ",t1.serioustrafficviolation    "
      + ",t1.busi_premium    "
      + ",t1.busi_benchmarkpremium     "
      + ",t1.busi_net_fee     "
      + ",t1.max_net_fee    "
      + ",t1.min_net_fee    "
      + ",t1.net_fee_premium    "
      + ",t1.premium    "
      + ",t1.benchmarkpremium     "
      + ",t1.three_premium    "
      + ",t1.qty_proposalno    "
      + ",t1.ProposalNo    "
      + ",t1.qty_rount_policy    "
      + ",t1.busi_quote_cnt    "
      + ",t1.tra_quote_cnt     "
      + ",t1.last_chn_code    "
      + ",t1.comcode    "
      + ",t1.insurance_code    "
      + ",t1.discount_range     "
      + ",t1.qty_chn_code    "
      + ",t1.qty_com    "
      + ",t2.custno                       as prv_custno                             "
      + ",t2.insuredname                  as prv_insuredname                        "
      + ",t2.insuredsex                   as prv_insuredsex                         "
      + ",t2.insuredage                   as prv_insuredage                         "
      + ",t2.early_busi_quote_time        as prv_early_busi_quote_time         "
      + ",t2.early_tra_quote_time         as prv_early_tra_quote_time          "
      + ",t2.last_busi_quote_time         as prv_last_busi_quote_time          "
      + ",t2.last_tra_quote_time          as prv_last_tra_quote_time           "
      + ",t2.busi_businessnature          as prv_busi_businessnature           "
      + ",t2.tra_businessnature           as prv_tra_businessnature            "
      + ",t2.busi_businessnature2         as prv_busi_businessnature2          "
      + ",t2.tra_businessnature2          as prv_tra_businessnature2           "
      + ",t2.busi_startdate               as prv_busi_startdate                "
      + ",t2.tra_startdate                as prv_tra_startdate                 "
      + ",t2.busi_enddate                 as prv_busi_enddate                  "
      + ",t2.tra_enddate                  as prv_tra_enddate                   "
      + ",t2.AUTOINSURANCESCORE           as prv_autoinsurancescore  "
      + ",t2.ncdclass_syx                 as prv_ncdclass_syx                  "
      + ",t2.profitrate_ncd               as prv_profitrate_ncd                "
      + ",t2.profitrate_violation         as prv_profitrate_violation          "
      + ",t2.effectivetrafficviolation    as prv_effectivetrafficviolation     "
      + ",t2.serioustrafficviolation      as prv_serioustrafficviolation       "
      + ",t2.busi_premium                 as prv_busi_premium                  "
      + ",t2.busi_benchmarkpremium        as prv_busi_benchmarkpremium         "
      + ",t2.busi_net_fee                 as prv_busi_net_fee                  "
      + ",t2.max_net_fee                  as prv_max_net_fee                   "
      + ",t2.min_net_fee                  as prv_min_net_fee                   "
      + ",t2.net_fee_premium              as prv_net_fee_premium               "
      + ",t2.premium                      as prv_premium                       "
      + ",t2.benchmarkpremium             as prv_benchmarkpremium              "
      + ",t2.three_premium                as prv_three_premium                 "
      + ",t2.qty_proposalno               as prv_qty_proposalno                "
      + ",t2.ProposalNo                   as prv_ProposalNo                    "
      + ",t2.qty_rount_policy             as prv_qty_rount_policy              "
      + ",t2.busi_policyno                as prv_busi_policyno                 "
      + ",t2.tra_policyno                 as prv_tra_policyno                  "
      + ",t2.busi_quote_cnt               as prv_busi_quote_cnt                "
      + ",t2.tra_quote_cnt                as prv_tra_quote_cnt                 "
      + ",t2.last_chn_code                as prv_last_chn_code                 "
      + ",t2.comcode                      as prv_comcode                       "
      + ",t2.insurance_code               as prv_insurance_code                "
      + ",t2.discount_range               as prv_discount_range                "
      + ",t2.qty_chn_code                 as prv_qty_chn_code                  "
      + ",t2.qty_com                      as prv_qty_com                       "
      + "from tmp_cpi_main_car_list14 t1    "
      + "left join tmp_cpi_main_car_list14 t2    "
      + "on concat(t1.vehicle_id,(cast(t1.quote_rount as int)-1))=concat(t2.vehicle_id,t2.quote_rount)")

    df_cpi_main_car_list15.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list15")
    spark.sql("refresh table tmp_cpi_main_car_list15")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'自关联,拉平上一次报价'  as step_name "
      + ",'tmp_cpi_main_car_list15' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow

    //近6个月的数据
    val df_cpi_main_car_list11_1 = spark.sql("""SELECT                                  
          T1.*                                   
          ,case when substr(quotationno,1,3) IN('QCI','QDF') then 0 else 1 end  as only_tra_flag     
          --,row_number() over(distribute by t1.vehicle_id,t1.quote_rount sort by quote_rount desc) as rn 
         FROM TMP_CPI_MAIN_CAR_LIST11 T1   
         where t1.quote_time>date_add('""" + v_workdate + """',-183)    """)
    df_cpi_main_car_list11_1.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_cpi_main_car_list11_1")
    spark.sql("refresh table tmp_cpi_main_car_list11_1")

    spark.sql("cache table tmp_cpi_main_car_list11_1")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'对tmp_cpi_main_car_list11表取最近6个月的清单数据'  as step_name "
      + ",'tmp_cpi_main_car_list11_1' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    v_begin_time = getnow

    spark.sql("""insert overwrite table tmp_cpi_main_car_grp_6 
      select 
        vehicle_id 
        ,quote_rount 
        ,max(case when substr(quotationno,1,3) IN('QCI','QDF') then quote_time else '' end )       as last_tra_quote_time 
        ,max(case when substr(quotationno,1,3) IN('QBI','QDE') then quote_time else '' end )       as last_busi_quote_time 
        ,case when min(case when substr(t1.quotationno,1,3) IN('QCI','QDF') then quote_time else '2999-12-31' end )='2999-12-31' then null 
                         else   min(case when substr(quotationno,1,3) IN('QCI','QDF') then quote_time else '2999-12-31' end ) end   as early_tra_quote_time 
        ,case when min(case when substr(t1.quotationno,1,3) IN('QBI','QDE') then quote_time else '2999-12-31' end )='2999-12-31' then null 
                         else   min(case when substr(quotationno,1,3) IN('QBI','QDE') then quote_time else '2999-12-31' end ) end   as early_busi_quote_time 
        ,max(NET_FEE)                                                          as max_net_fee 
        ,nvl(min(NET_FEE),0)                                                   as min_net_fee 
        ,nvl(cast(substr(max(case when nvl(amount,'')!='' then concat(quote_time,AMOUNT) else '0' end),11) as double),0)  as three_premium 
        ,cast(count(qty_proposalno) as STRING)                                           as qty_proposalno 
        ,cast(count(distinct policyno) as STRING)                                        as qty_rount_policy 
        ,sum(busi_quote_flag)                                                             as busi_quote_cnt 
        ,sum(tra_quote_flag)                                                              as tra_quote_cnt 
        ,cast(count(distinct chn_code) as STRING)                                          as qty_chn_code 
        ,cast(count(distinct comcode) as STRING)                                          as qty_com 
        ,concat_ws('|',collect_set(distinct  case when net_fee_premium=0 then null else cast(net_fee_premium as string) end)) as net_fee_premium 
        ,concat_ws('|',collect_set(distinct  case when premium=0 then null else cast(premium as string) end )               )  as premium 
        ,concat_ws('|',collect_set(distinct  case when benchmarkpremium=0 then null else cast(benchmarkpremium as string) end) ) as benchmarkpremium 
        ,substr(max(case when substr(quotationno,1,3) IN('QBI','QDE') then concat(quote_time,policyno) else null end ),11)  as busi_policyno
        ,substr(max(case when substr(quotationno,1,3) IN('QCI','QDF') then concat(quote_time,policyno) else null end ),11)  as tra_policyno
        ,case when sum(only_tra_flag)=0 then '07' else null end  as only_tra_flag 
      from tmp_cpi_main_car_list11_1 t1
      group by vehicle_id ,quote_rount""")

    spark.sql("insert overwrite table tmp_cpi_main_car_full_6 "
      + "select   t.vehicle_id "
      + " ,t.quote_rount "
      + "  ,t.custno                    "
      + "  ,t.insuredname               "
      + "  ,t.insuredsex                "
      + "  ,t.insuredage                "
      + "  ,t.effectivetrafficviolation "
      + "  ,t.serioustrafficviolation   "
      + "  ,t.last_chn_code "
      + "  ,t.comcode                     "
      + "  ,t.insurance_code              "
      + "  ,t.discount_range              "
      + ",  t.AUTOINSURANCESCORE "
      + " from ( "
      + "select   vehicle_id "
      + " ,quote_rount "
      + "  ,row_number() over(distribute by vehicle_id ,quote_rount sort by quote_time desc) as ro "
      + "  ,custno                    "
      + "  ,insuredname               "
      + "  ,insuredsex                "
      + "  ,insuredage                "
      + "  ,effectivetrafficviolation "
      + "  ,serioustrafficviolation   "
      + "  ,chn_code   as  last_chn_code "
      + "  ,comcode                     "
      + "  ,insurance_code              "
      + "  ,discount_range              "
      + "  ,autoinsurancescore "
      + "from tmp_cpi_main_car_list11_1   "
      + ") t where t.ro=1              distribute by vehicle_id ")

    spark.sql("insert overwrite table tmp_cpi_main_car_busi_6 "
      + " select "
      + "  vehicle_id "
      + "  ,quote_rount "
      + "  ,busi_BUSINESSNATURE "
      + "  ,busi_BUSINESSNATURE2 "
      + "  ,busi_startdate "
      + "  ,busi_enddate "
      + "  ,profitrate_ncd "
      + "  ,profitrate_violation "
      + "  ,busi_premium "
      + "  ,busi_BenchMarkPremium "
      + "  ,busi_net_fee "
      + "  ,ncdclass_syx "
      + " from ( "
      + "select                                          "
      + "  vehicle_id "
      + " ,quote_rount"
      + "  ,row_number() over(distribute by vehicle_id ,quote_rount sort by quote_time desc) as ro "
      + "  ,BUSINESSNATURE             as   busi_BUSINESSNATURE "
      + "  ,BUSINESSNATURE2            as   busi_BUSINESSNATURE2 "
      + "  ,startdate                  as   busi_startdate "
      + "  ,enddate                    as   busi_enddate "
      + "  ,profitrate_ncd "
      + "  ,profitrate_violation "
      + "  ,busi_premium               as   busi_premium "
      + "  ,busi_benchmarkpremium      as   busi_BenchMarkPremium "
      + "  ,net_fee                    as   busi_net_fee "
      + "  ,ncdclass_syx               as   ncdclass_syx "
      + "from tmp_cpi_main_car_list11_1 "
      + "where substr(quotationno,1,3) IN('QBI','QDE') "
      + ") t where t.ro=1 distribute by vehicle_id")

    spark.sql("insert overwrite table tmp_cpi_main_car_tra_6 "
      + " select "
      + "  vehicle_id "
      + "  ,quote_rount "
      + "  ,tra_BUSINESSNATURE "
      + "  ,tra_BUSINESSNATURE2 "
      + "  ,tra_startdate "
      + "  ,tra_enddate "
      + " from ( "
      + "select "
      + "  vehicle_id "
      + "  ,quote_rount "
      + "  ,row_number() over(distribute by vehicle_id ,quote_rount sort by quote_time desc) as ro "
      + "  ,BUSINESSNATURE             as   tra_BUSINESSNATURE "
      + "  ,BUSINESSNATURE2            as   tra_BUSINESSNATURE2 "
      + "  ,startdate                  as   tra_startdate "
      + "  ,enddate                    as   tra_enddate "
      + " from tmp_cpi_main_car_list11_1 "
      + " where substr(quotationno,1,3) IN('QCI','QDF') "
      + ") t where t.ro=1 distribute by vehicle_id")

    spark.sql("insert overwrite table tmp_cpi_main_car_pol_6 "
      + "select "
      + "  vehicle_id "
      + "  ,quote_rount "
      + "  ,ProposalNo "
      + " from (  "
      + "select          "
      + "  vehicle_id "
      + "  ,quote_rount "
      + "  ,row_number() over(distribute by vehicle_id sort by underwriteenddate desc) as ro "
      + "  ,ProposalNo "
      + "from tmp_cpi_main_car_list11_1 "
      + ") t where t.ro=1 distribute by vehicle_id")

    spark.sql("uncache table tmp_cpi_main_car_list11_1")

    val df_near6_agg1 = spark.sql("select     "
      + "    t1.vehicle_id                  "
      + "  , t1.quote_rount                 "
      + "  , t1.last_tra_quote_time         "
      + "  , t1.last_busi_quote_time        "
      + "  , t1.early_tra_quote_time        "
      + "  , t1.early_busi_quote_time       "
      + "  , t1.max_net_fee                 "
      + "  , t1.min_net_fee                 "
      + "  , t1.three_premium "
      + "  , t1.qty_proposalno              "
      + "  , t1.qty_rount_policy            "
      + "  , t1.busi_quote_cnt              "
      + "  , t1.tra_quote_cnt               "
      + "  , t1.qty_chn_code                "
      + "  , t1.qty_com                     "
      + "  , t1.net_fee_premium             "
      + "  , t1.premium                     "
      + "  , t1.benchmarkpremium            "
      + "  , t1.busi_policyno               "
      + "  , t1.tra_policyno                "
      + "  , t2.custno                      "
      + "  , t2.insuredname                 "
      + "  , t2.insuredsex                  "
      + "  , t2.insuredage                  "
      + "  , t2.effectivetrafficviolation   "
      + "  , t2.serioustrafficviolation     "
      + "  , t2.last_chn_code               "
      + "  , t2.comcode                     "
      + "  , nvl(t1.only_tra_flag,t2.insurance_code) as        insurance_code       "
      + "  , t2.discount_range              "
      + "  , t2.autoinsurancescore          "
      + "from tmp_cpi_main_car_grp_6 t1 "
      + "left join tmp_cpi_main_car_full_6 t2 "
      + "on t1.vehicle_id=t2.vehicle_id "
      + "and t1.quote_rount=t2.quote_rount ")

    df_near6_agg1.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_near6_agg1")

    spark.sql("insert overwrite table tmp_near6_agg2 select     "
      + "    t1.vehicle_id                  "
      + "  , t1.quote_rount                 "
      + "  , t1.last_tra_quote_time         "
      + "  , t1.last_busi_quote_time        "
      + "  , t1.early_tra_quote_time        "
      + "  , t1.early_busi_quote_time       "
      + "  , t1.max_net_fee                 "
      + "  , t1.min_net_fee                 "
      + "  , t1.three_premium               "
      + "  , t1.qty_proposalno              "
      + "  , t1.qty_rount_policy            "
      + "  , t1.busi_quote_cnt              "
      + "  , t1.tra_quote_cnt               "
      + "  , t1.qty_chn_code                "
      + "  , t1.qty_com                     "
      + "  , t1.net_fee_premium             "
      + "  , t1.premium                     "
      + "  , t1.benchmarkpremium            "
      + "  , t1.custno                      "
      + "  , t1.insuredname                 "
      + "  , t1.insuredsex                  "
      + "  , t1.insuredage                  "
      + "  , t1.effectivetrafficviolation   "
      + "  , t1.serioustrafficviolation     "
      + "  , t1.last_chn_code               "
      + "  , t1.comcode                     "
      + "  , t1.insurance_code              "
      + "  , t1.discount_range              "
      + ", t1.autoinsurancescore "
      + "  , t3.busi_businessnature         "
      + "  , t3.busi_businessnature2        "
      + "  , t3.busi_startdate              "
      + "  , t3.busi_enddate                "
      + "  , t3.profitrate_ncd              "
      + "  , t3.profitrate_violation        "
      + "  , t3.busi_premium                "
      + "  , t3.busi_benchmarkpremium       "
      + "  , t3.busi_net_fee                "
      + "  , t1.busi_policyno               "
      + "  , t1.tra_policyno               "
      + "  , t3.ncdclass_syx                "
      + "from tmp_near6_agg1 t1 "
      + "left join tmp_cpi_main_car_busi_6 t3 "
      + "on t1.vehicle_id=t3.vehicle_id "
      + "and t1.quote_rount=t3.quote_rount ")

    spark.sql("insert overwrite table tmp_near6_agg3 select     "
      + "    t1.vehicle_id                  "
      + "  , t1.quote_rount                 "
      + "  , t1.last_tra_quote_time         "
      + "  , t1.last_busi_quote_time        "
      + "  , t1.early_tra_quote_time        "
      + "  , t1.early_busi_quote_time       "
      + "  , t1.max_net_fee                 "
      + "  , t1.min_net_fee                 "
      + "  , t1.three_premium               "
      + "  , t1.qty_proposalno              "
      + "  , t1.qty_rount_policy            "
      + "  , t1.busi_quote_cnt              "
      + "  , t1.tra_quote_cnt               "
      + "  , t1.qty_chn_code                "
      + "  , t1.qty_com                     "
      + "  , t1.net_fee_premium             "
      + "  , t1.premium                     "
      + "  , t1.benchmarkpremium            "
      + "  , t1.custno                      "
      + "  , t1.insuredname                 "
      + "  , t1.insuredsex                  "
      + "  , t1.insuredage                  "
      + "  , t1.effectivetrafficviolation   "
      + "  , t1.serioustrafficviolation     "
      + "  , t1.last_chn_code               "
      + "  , t1.comcode                     "
      + "  , t1.insurance_code              "
      + "  , t1.discount_range              "
      + ", t1.autoinsurancescore "
      + "  , t1.busi_businessnature         "
      + "  , t1.busi_businessnature2        "
      + "  , t1.busi_startdate              "
      + "  , t1.busi_enddate                "
      + "  , t1.profitrate_ncd              "
      + "  , t1.profitrate_violation        "
      + "  , t1.busi_premium                "
      + "  , t1.busi_benchmarkpremium       "
      + "  , t1.busi_net_fee                "
      + "  , t1.busi_policyno               "
      + "  , t1.ncdclass_syx                "
      + "  , t4.tra_businessnature          "
      + "  , t4.tra_businessnature2         "
      + "  , t4.tra_startdate               "
      + "  , t4.tra_enddate                 "
      + "  , t1.tra_policyno                "
      + "  , t5.ProposalNo                  "
      + "from tmp_near6_agg2 t1 "
      + "left join tmp_cpi_main_car_tra_6 t4 "
      + "on t1.vehicle_id=t4.vehicle_id "
      + "and t1.quote_rount=t4.quote_rount "
      + "left join tmp_cpi_main_car_pol_6 t5 "
      + "on t1.vehicle_id=t5.vehicle_id "
      + "and t1.quote_rount=t5.quote_rount "
      + "distribute by vehicle_id")

    //df_near6_agg3.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_near6_agg3")

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'对6个月的清单数据汇总并按车牌拉平'  as step_name "
      + ",'tmp_near6_agg3' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    val df_dm_vehicle_quotation = spark.sql("""select t1.vehicle_id     
        ,t1.quote_rount        
        ,t1.vehicle_status     
        ,t1.renew_flag         
        ,t1.renewyears         
        ,t1.frameno            
        ,t1.licenseno          
        ,t1.brandname          
        ,t1.factoryName        
        ,t1.class_type         
        ,t1.licensecolorcode   
        ,t1.carkindcode        
        ,t1.hkflag             
        ,t1.hklicenseno        
        ,t1.engineno           
        ,t1.enrolldate         
        ,t1.useyears           
        ,t1.usenaturecode      
        ,t1. usenaturename     
        ,t1.purchaseprice      
        ,t1.seatcount          
        ,t1.toncount           
        ,t1.powerscale         
        ,t1.exhaustscale       
        ,t1.fueltype           
        ,t3.POLICYNO             
        ,t3.UNDERWRITEENDDATE    
        ,t3.STARTDATE            
        ,t3.ENDDATE              
        ,t3.INITSTAT             
        ,t3.expiry_policyno             
        ,t3.expiry_underwriteenddate    
        ,t3.expiry_startdate            
        ,t3.expiry_enddate              
        ,t3.expiry_initstat             
        ,t1.custno             
        ,t1.insuredname        
        ,t1.insuredsex         
        ,t1.insuredage         
        ,t1.early_busi_quote_time  
        ,t1.early_tra_quote_time   
        ,t1.last_busi_quote_time   
        ,t1.last_tra_quote_time    
        ,t1.busi_businessnature    
        ,t1.tra_businessnature     
        ,t1.busi_businessnature2   
        ,t1.tra_businessnature2    
        ,t1.busi_startdate         
        ,t1.tra_startdate          
        ,t1.busi_enddate           
        ,t1.tra_enddate            
        ,t1.autoinsurancescore    
        ,t1.ncdclass_syx           
        ,t1.profitrate_ncd         
        ,t1.profitrate_violation   
        ,t1.effectivetrafficviolation  
        ,t1.serioustrafficviolation    
        ,t1.busi_premium           
        ,t1.busi_benchmarkpremium  
        ,t1.busi_net_fee           
        ,t1.max_net_fee            
        ,t1.min_net_fee            
        ,t1.net_fee_premium       
        ,t1.premium               
        ,t1.benchmarkpremium      
        ,t1.three_premium          
        ,t1.qty_proposalno         
        ,t1.ProposalNo             
        ,t1.qty_rount_policy       
        ,t1.busi_quote_cnt         
        ,t1.tra_quote_cnt          
        ,t1.last_chn_code          
        ,t1.comcode                
        ,t1.insurance_code         
        ,t1.discount_range         
        ,t1.qty_chn_code           
        ,t1.qty_com                
        ,t1.prv_custno             
        ,t1.prv_insuredname        
        ,t1.prv_insuredsex         
        ,t1.prv_insuredage         
        ,t1.prv_early_busi_quote_time  
        ,t1.prv_early_tra_quote_time   
        ,t1.prv_last_busi_quote_time   
        ,t1.prv_last_tra_quote_time    
        ,t1.prv_busi_businessnature    
        ,t1.prv_tra_businessnature     
        ,t1.prv_busi_businessnature2   
        ,t1.prv_tra_businessnature2    
        ,t1.prv_busi_startdate         
        ,t1.prv_tra_startdate          
        ,t1.prv_busi_enddate           
        ,t1.prv_tra_enddate            
        ,t1.prv_autoinsurancescore    
        ,t1.prv_ncdclass_syx           
        ,t1.prv_profitrate_ncd         
        ,t1.prv_profitrate_violation   
        ,t1.prv_effectivetrafficviolation  
        ,t1.prv_serioustrafficviolation    
        ,t1.prv_busi_premium           
        ,t1.prv_busi_benchmarkpremium  
        ,t1.prv_busi_net_fee           
        ,t1.prv_max_net_fee            
        ,t1.prv_min_net_fee            
        ,t1.prv_net_fee_premium       
        ,t1.prv_premium               
        ,t1.prv_benchmarkpremium      
        ,t1.prv_three_premium          
        ,t1.prv_qty_proposalno         
        ,t1.prv_ProposalNo             
        ,t1.prv_qty_rount_policy  
        ,t1.prv_busi_policyno     
        ,t1.prv_tra_policyno      
        ,t1.prv_busi_quote_cnt    
        ,t1.prv_tra_quote_cnt     
        ,t1.prv_last_chn_code     
        ,t1.prv_comcode           
        ,t1.prv_insurance_code    
        ,t1.prv_discount_range    
        ,t1.prv_qty_chn_code      
        ,t1.prv_qty_com           
       ,t2.custno                                as near6_custno                          
       ,t2.insuredname                          as near6_insuredname                     
       ,t2.insuredsex                           as near6_insuredsex                      
       ,t2.insuredage                           as near6_insuredage                      
       ,t2.early_busi_quote_time                as near6_early_busi_quote_time           
       ,t2.early_tra_quote_time                 as near6_early_tra_quote_time            
       ,t2.last_busi_quote_time                 as near6_last_busi_quote_time            
       ,t2.last_tra_quote_time                  as near6_last_tra_quote_time             
       ,t2.busi_businessnature                  as near6_busi_businessnature             
       ,t2.tra_businessnature                   as near6_tra_businessnature              
       ,t2.busi_businessnature2                 as near6_busi_businessnature2            
       ,t2.tra_businessnature2                  as near6_tra_businessnature2             
       ,t2.busi_startdate                       as near6_busi_startdate                  
       ,t2.tra_startdate                        as near6_tra_startdate                   
       ,t2.busi_enddate                         as near6_busi_enddate                    
       ,t2.tra_enddate                          as near6_tra_enddate                     
       ,t2.autoinsurancescore                   as near6_autoinsurancescore
       ,t2.ncdclass_syx                         as near6_ncdclass_syx                    
       ,t2.profitrate_ncd                       as near6_profitrate_ncd                  
       ,t2.profitrate_violation                 as near6_profitrate_violation            
       ,t2.effectivetrafficviolation            as near6_effectivetrafficviolation       
       ,t2.serioustrafficviolation              as near6_serioustrafficviolation         
       ,t2.busi_premium                         as near6_busi_premium                    
       ,t2.busi_benchmarkpremium                as near6_busi_benchmarkpremium           
       ,t2.busi_net_fee                         as near6_busi_net_fee                    
       ,t2.max_net_fee                          as near6_max_net_fee                     
       ,t2.min_net_fee                          as near6_min_net_fee                     
       ,t2.net_fee_premium                      as near6_net_fee_premium                 
       ,t2.premium                              as near6_premium                         
       ,t2.benchmarkpremium                     as near6_benchmarkpremium                
       ,t2.three_premium                        as near6_three_premium                   
       ,t2.qty_proposalno                       as near6_qty_proposalno                  
       ,t2.ProposalNo                           as near6_ProposalNo                      
       ,t2.qty_rount_policy                     as near6_qty_rount_policy                
       ,t2.busi_policyno                        as near6_busi_policyno                   
       ,t2.tra_policyno                         as near6_tra_policyno                    
       ,t2.busi_quote_cnt                       as near6_busi_quote_cnt                  
       ,t2.tra_quote_cnt                        as near6_tra_quote_cnt                   
       ,t2.last_chn_code                        as near6_last_chn_code                   
       ,t2.comcode                              as near6_comcode                         
       ,t2.insurance_code                       as near6_insurance_code                  
       ,t2.discount_range                       as near6_discount_range                  
       ,t2.qty_chn_code                         as near6_qty_chn_code                    
       ,t2.qty_com                              as near6_qty_com                         
      from tmp_cpi_main_car_list15 t1 
      left join tmp_near6_agg3 t2 
      on t1.vehicle_id=t2.vehicle_id 
      and t1.quote_rount=t2.quote_rount 
      left join tmp_policy_bph t3 
       on t1.vehicle_id=t3.vehicle_id    
       and t1.quote_rount=t3.quote_rount 
      """)

    df_dm_vehicle_quotation.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/ccic_car.db/tmp_dm_vehicle_quotation")
    spark.sql("refresh table tmp_dm_vehicle_quotation")

    //case class(name:String,age:Long)
    val v_condition = spark.table("dm_vehicle_quotation").filter("workdate<'" + v_workdate + "'").take(5)

    spark.sql("alter table dm_vehicle_quotation  drop if exists partition  (workdate='" + v_workdate + "')")

    if (v_condition.isEmpty) {
      spark.sql("alter table dm_vehicle_quotation  add if not exists partition  (workdate='" + v_workdate + "')")

      spark.sql("insert overwrite table dm_vehicle_quotation  partition  (workdate='" + v_workdate + "')"
        + " select t.* from TMP_DM_VEHICLE_QUOTATION t")
    } else {
//      val max_workdate = spark.read.table("dm_vehicle_quotation").filter("workdate<'" + v_workdate + "'").sort(col("workdate").desc).first().getAs[String]("workdate")
      val max_workdate=spark.sql("select max(workdate) as workdate from dm_vehicle_quotation where workdate<'"+v_workdate+"'").first().getAs[String]("workdate")
      val ds_oldfull = spark.read.table("dm_vehicle_quotation").filter("workdate='" + max_workdate + "'")
      //ds_oldfull.createTempView("oldfull")

      val ds_DM_VEHICLE_QUOTATION = spark.sql("select t.*,'" + v_workdate + "' as workdate from TMP_DM_VEHICLE_QUOTATION t").filter("1=1")
      
      val dsunion = ds_DM_VEHICLE_QUOTATION.union(ds_oldfull)
      dsunion.createOrReplaceTempView("full_add_union")
      val dfunion1 = spark.sql("select vehicle_id, quote_rount, policyno, expiry_policyno, workdate from full_add_union")

      val rddunion1= dfunion1.rdd.map { line => (line.getAs[String]("vehicle_id") + "|" + line.getAs[String]("quote_rount") + "|" + line.getAs[String]("policyno") + "|" + line.getAs[String]("expiry_policyno"), line) }
      val reduceRdd = rddunion1.reduceByKey((x, y) => if (x.getAs[String]("workdate") > y.getAs[String]("workdate")) x else y).map(x => x._2)

      //.map{ line => (line.getAs[String]("vehicle_id")+"|"+line.getAs[String]("quote_rount"),line) }

      val dfrdd = spark.createDataFrame(reduceRdd, dfunion1.schema)
      dfrdd.createOrReplaceTempView("dfrdd")

      spark.sql("alter table dm_vehicle_quotation  add if not exists partition  (workdate='" + v_workdate + "')")

      spark.sql("""insert overwrite table dm_vehicle_quotation  partition  (workdate='""" + v_workdate + """') 
        select t2.vehicle_id  
        ,t2.quote_rount
        ,t2.vehicle_status
        ,t2.renew_flag
        ,t2.renewyears
        ,t2.frameno
        ,t2.licenseno
        ,t2.brandname
        ,t2.factoryname
        ,t2.class_type
        ,t2.licensecolorcode
        ,t2.carkindcode
        ,t2.hkflag
        ,t2.hklicenseno
        ,t2.engineno
        ,t2.enrolldate
        ,t2.useyears
        ,t2.usenaturecode
        ,t2.usenaturename
        ,t2.purchaseprice
        ,t2.seatcount
        ,t2.toncount
        ,t2.powerscale
        ,t2.exhaustscale
        ,t2.fueltype
        ,t2.policyno
        ,t2.underwriteenddate
        ,t2.startdate
        ,t2.enddate
        ,t2.initstat
        ,t2.expiry_policyno
        ,t2.expiry_underwriteenddate
        ,t2.expiry_startdate
        ,t2.expiry_enddate
        ,t2.expiry_initstat
        ,t2.custno
        ,t2.insuredname
        ,t2.insuredsex
        ,t2.insuredage
        ,t2.early_busi_quote_time
        ,t2.early_tra_quote_time
        ,t2.last_busi_quote_time
        ,t2.last_tra_quote_time
        ,t2.busi_businessnature
        ,t2.tra_businessnature
        ,t2.busi_businessnature2
        ,t2.tra_businessnature2
        ,t2.busi_startdate
        ,t2.tra_startdate
        ,t2.busi_enddate
        ,t2.tra_enddate
        ,t2.autoinsurancescore
        ,t2.ncdclass_syx
        ,t2.profitrate_ncd
        ,t2.profitrate_violation
        ,t2.effectivetrafficviolation
        ,t2.serioustrafficviolation
        ,t2.busi_premium
        ,t2.busi_benchmarkpremium
        ,t2.busi_net_fee
        ,t2.max_net_fee
        ,t2.min_net_fee
        ,t2.net_fee_premium
        ,t2.premium
        ,t2.benchmarkpremium
        ,t2.three_premium
        ,t2.qty_proposalno
        ,t2.proposalno
        ,t2.qty_rount_policy
        ,t2.busi_quote_cnt
        ,t2.tra_quote_cnt
        ,t2.last_chn_code
        ,t2.comcode
        ,t2.insurance_code
        ,t2.discount_range
        ,t2.qty_chn_code
        ,t2.qty_com
        ,t2.prv_custno
        ,t2.prv_insuredname
        ,t2.prv_insuredsex
        ,t2.prv_insuredage
        ,t2.prv_early_busi_quote_time
        ,t2.prv_early_tra_quote_time
        ,t2.prv_last_busi_quote_time
        ,t2.prv_last_tra_quote_time
        ,t2.prv_busi_businessnature
        ,t2.prv_tra_businessnature
        ,t2.prv_busi_businessnature2
        ,t2.prv_tra_businessnature2
        ,t2.prv_busi_startdate
        ,t2.prv_tra_startdate
        ,t2.prv_busi_enddate
        ,t2.prv_tra_enddate
        ,t2.prv_autoinsurancescore
        ,t2.prv_ncdclass_syx
        ,t2.prv_profitrate_ncd
        ,t2.prv_profitrate_violation
        ,t2.prv_effectivetrafficviolation
        ,t2.prv_serioustrafficviolation
        ,t2.prv_busi_premium
        ,t2.prv_busi_benchmarkpremium
        ,t2.prv_busi_net_fee
        ,t2.prv_max_net_fee
        ,t2.prv_min_net_fee
        ,t2.prv_net_fee_premium
        ,t2.prv_premium
        ,t2.prv_benchmarkpremium
        ,t2.prv_three_premium
        ,t2.prv_qty_proposalno
        ,t2.prv_proposalno
        ,t2.prv_qty_rount_policy
        ,t2.prv_busi_policyno
        ,t2.prv_tra_policyno
        ,t2.prv_busi_quote_cnt
        ,t2.prv_tra_quote_cnt
        ,t2.prv_last_chn_code
        ,t2.prv_comcode
        ,t2.prv_insurance_code
        ,t2.prv_discount_range
        ,t2.prv_qty_chn_code
        ,t2.prv_qty_com
        ,t2.near6_custno
        ,t2.near6_insuredname
        ,t2.near6_insuredsex
        ,t2.near6_insuredage
        ,t2.near6_early_busi_quote_time
        ,t2.near6_early_tra_quote_time
        ,t2.near6_last_busi_quote_time
        ,t2.near6_last_tra_quote_time
        ,t2.near6_busi_businessnature
        ,t2.near6_tra_businessnature
        ,t2.near6_busi_businessnature2
        ,t2.near6_tra_businessnature2
        ,t2.near6_busi_startdate
        ,t2.near6_tra_startdate
        ,t2.near6_busi_enddate
        ,t2.near6_tra_enddate
        ,t2.near6_autoinsurancescore
        ,t2.near6_ncdclass_syx
        ,t2.near6_profitrate_ncd
        ,t2.near6_profitrate_violation
        ,t2.near6_effectivetrafficviolation
        ,t2.near6_serioustrafficviolation
        ,t2.near6_busi_premium
        ,t2.near6_busi_benchmarkpremium
        ,t2.near6_busi_net_fee
        ,t2.near6_max_net_fee
        ,t2.near6_min_net_fee
        ,t2.near6_net_fee_premium
        ,t2.near6_premium
        ,t2.near6_benchmarkpremium
        ,t2.near6_three_premium
        ,t2.near6_qty_proposalno
        ,t2.near6_proposalno
        ,t2.near6_qty_rount_policy
        ,t2.near6_busi_policyno
        ,t2.near6_tra_policyno
        ,t2.near6_busi_quote_cnt
        ,t2.near6_tra_quote_cnt
        ,t2.near6_last_chn_code
        ,t2.near6_comcode
        ,t2.near6_insurance_code
        ,t2.near6_discount_range
        ,t2.near6_qty_chn_code
        ,t2.near6_qty_com   
        from dfrdd t1 inner join full_add_union t2 
        on concat(t1.vehicle_id,t1.quote_rount,t1.workdate,nvl(t1.policyno,'AAA'),nvl(t1.expiry_policyno,'BBB'))
          =concat(t2.vehicle_id,t2.quote_rount,t2.workdate,nvl(t2.policyno,'AAA'),nvl(t2.expiry_policyno,'BBB'))
        """)
    }

    val weekdate = addDay(v_workdate, -7)

    val lastdate = getLastDay(weekdate)

    //每日删除7天前的数据，保留每月月底的数据

    if (weekdate != lastdate) {
      spark.sql("alter table dm_vehicle_quotation  drop if exists partition  (workdate='" + weekdate + "')")
    }

    spark.sql("insert into table vehicle_quono_proc_log "
      + "select "
      + "'" + v_workdate + "' as workdate "
      + ",'" + batch_id + "' as batch_id "
      + ",'tmp_cpi_main_car_list15拉平近6个月的指标并插入结果表'  as step_name "
      + ",'dm_vehicle_quotation' as table_name "
      + ",'" + v_begin_time + "' as begin_time "
      + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  as end_time")

    spark.stop()
  }
}