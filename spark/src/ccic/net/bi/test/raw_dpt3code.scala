package ccic.net.bi.test

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.SaveMode

object raw_dpt3code {
   def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)
    val spark=SparkSession.builder().appName("raw_dpt3code").enableHiveSupport().getOrCreate()
    
    //加载数据源
    val path="/user/hive/warehouse/ccic_source.db/t05_prpdcompany/all/"
    val t05_prpdcompany=spark.read.load(path)
    // 加工基础数据
    val raw1_dptcode=t05_prpdcompany.select("COMCODE","COMCNAME","UPPERCOMCODE")
    //将raw1_dptcode落地为parquet物理表
    raw1_dptcode.write.mode(SaveMode.Overwrite).saveAsTable("ccic_car.raw1_dptcode")
    //将raw_dpt1 dpt2 dpt3 dpt4 dpt5 注册为临时表
    spark.sql("select '31000000' as dpt1").createTempView("raw_dpt1")
    spark.sql("""select 
                 a.dpt1, 
                 b.comcode as dpt2,
                 b.comcname as dpt2_name
              from raw_dpt1 a
              left join ccic_car.raw1_dptcode b
               on a.dpt1=b.UPPERCOMCODE""").createTempView("raw_dpt2")
      spark.sql("""select 
a.dpt1,
a.dpt2,
a.dpt2_name,
b.comcode as dpt3,
b.comcname as dpt3_name
from (select  dpt1,dpt2,dpt2_name from raw_dpt2 where dpt2 <> '31000000') a
left join ccic_car.raw1_dptcode b
on a.dpt2=b.UPPERCOMCODE""").createTempView("raw_dpt3")
spark.sql("""select a.dpt1,
a.dpt2,
a.dpt2_name,
a.dpt3,
a.dpt3_name,
b.comcode as dpt4,
b.comcname as dpt4_name
from raw_dpt3 a
left join ccic_car.raw1_dptcode b
on a.dpt3=b.UPPERCOMCODE""").createTempView("raw_dpt4")
spark.sql("""select
a.dpt1,
a.dpt2,
a.dpt2_name,
a.dpt3,
a.dpt3_name,
a.dpt4,
a.dpt4_name,
b.comcode as dpt5,
b.comcname as dpt5_name
from raw_dpt4 as a
left join ccic_car.raw1_dptcode as b
on a.dpt4=b.UPPERCOMCODE""").createTempView("raw_dpt5")
//raw_dpt5落地为parquet物理表
spark.sql("""select
a.dpt1,
a.dpt2,
a.dpt2_name,
a.dpt3,
a.dpt3_name,
a.dpt4,
a.dpt4_name,
a.dpt5,
a.dpt5_name,
b.comcode as dpt6,
b.comcname as dpt6_name
from raw_dpt5 as a
left join ccic_car.raw1_dptcode as b
on a.dpt5=b.UPPERCOMCODE""").write.mode(SaveMode.Overwrite).saveAsTable("ccic_car.raw_dpt6")

//raw_dpt2code_tmp (2 3 4 5 6)注册为临时表 
//raw_dpt2code (2 3 4 5 6 )落地为parquet物理表
spark.sql("""select
distinct dpt2
from ccic_car.raw_dpt6
where dpt2 is not null""").createTempView("raw_dpt2code_tmp")
spark.sql("""select
dpt2 as dptcode,
dpt2 as dptcode3,
3 as level
from raw_dpt2code_tmp	""").write.mode(SaveMode.Overwrite).saveAsTable("ccic_car.raw_dpt2code")

spark.sql("""select distinct dpt3
from ccic_car.raw_dpt6
where dpt3 is not null""").createTempView("raw_dpt3code_tmp")
spark.sql("""select 
dpt3 as dptcode,
dpt3 as dptcode3,
3 as level
from raw_dpt3code_tmp""").write.mode(SaveMode.Overwrite).saveAsTable("ccic_car.raw_dpt3code")


spark.sql("""select distinct dpt4, dpt3
from ccic_car.raw_dpt6
where dpt4 is not null	""").createTempView("raw_dpt4code_tmp")
spark.sql("""select
dpt4 as dptcode,
dpt3 as dptcode3,
'4' as level
from raw_dpt4code_tmp""").write.mode(SaveMode.Overwrite).saveAsTable("ccic_car.raw_dpt4code")

spark.sql("""select distinct dpt5,dpt3
from ccic_car.raw_dpt6
where dpt5 is not null""").createTempView("raw_dpt5code_tmp")
spark.sql("""select
dpt5 as dptcode, 
dpt3 as dptcode3,
'5' as level
from raw_dpt5code_tmp""").write.mode(SaveMode.Overwrite).saveAsTable("ccic_car.raw_dpt5code")

spark.sql("""select distinct dpt6, dpt3
from ccic_car.raw_dpt6
where dpt6 is not null""").createTempView("raw_dpt6code_tmp")
spark.sql("""select
dpt6 as dptcode,
dpt3 as dptcode3,
'6' as level""").write.mode(SaveMode.Overwrite).saveAsTable("ccic_car.raw_dpt6code")

//最终生成raw_dpt3_code目标表
spark.sql(""" with raw_dpt3_code_tmp as (
  select dptcode,dptcode3 from ccic_car.raw_dpt2code
union all
select dptcode,dptcode3 from  ccic_car.raw_dpt3code
union all
select dptcode,dptcode3 from  ccic_car.raw_dpt4code
union all
select dptcode,dptcode3 from  ccic_car.raw_dpt5code
union all
select dptcode,dptcode3 from  ccic_car.raw_dpt6code )
insert overwrite table ccic_car.raw_dpt3_code
select a.dptcode3,a.dptcode as comcode, b.comcname as dptname3
from raw_dpt3_code_tmp as a
left join ccic_car.raw1_dptcode as b
on a.dptcode3=b.comcode
order by comcode
  """)
  }
}