package ccic.net.bi.udf

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.spark.sql.SparkSession
import net.ccic.sparkprocess.hive.GetEarned

//import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession

class define_ccic_udf{
  def init(sparkSession: SparkSession) = define_ccic_udf.init(sparkSession)
  def registerUDF() = define_ccic_udf.registerUDF()
}

object define_ccic_udf{
   var spark:SparkSession = null
  def init(sparkSession: SparkSession) {
    spark = sparkSession
  }
  def  registerUDF() {
    //spark.udf.register("getnow", getnow())
    spark.udf.register("getnow", getnow _)
    spark.udf.register("addThreeMonth", addThreeMonth _)
    //spark.udf.register("addThreeMonth", addThreeMonth(_:String,_:Int))
    spark.udf.register("addDay", addDay(_: String, _: Int))
    spark.udf.register("getLastDay", getLastDay(_: String))
    spark.udf.register("addMonth", addMonth(_: String, _: Int))
    spark.udf.register("Full2Half", Full2Half(_: String))
    spark.udf.register("stringToAscii", stringToAscii(_: String))
    spark.udf.register("stringToOraAscII", stringToOraAscII(_: String))
    spark.udf.register("string2Unicode", string2Unicode(_: String))
    spark.udf.register("rpad", rpad(_: String, _: Int, _: String))
    spark.udf.register("lpad", lpad(_: String, _: Int, _: String))
    spark.udf.register("vinistrue", vinistrue(_: String))
    spark.udf.register("CreateVehicleID", CreateVehicleID(_: String, _: String))
    spark.udf.register("vehicle_classification", vehicle_classification(_: String))
    spark.udf.register("diff_date", diff_date(_: String, _: String))
    spark.udf.register("compute_quote", compute_quote(_: String))
    spark.udf.register("compute_last_pol", compute_last_pol(_: String))
    spark.udf.register("compute_years_grp", compute_years_grp(_: String))
    spark.udf.register("chexi_judge",chexi_judge(_:String))
    //spark.udf.register("get_test",new GetEarned().evaluate(_:String, _:String, _:String, _:String, _:String, _:String, _:String, _:String))
    spark.udf.register("get_test",get_test(_:String,_:String,_:String,_:String,_:String,_:String,_:String,_:String))
  }
  //val makeDt = udf(addThreeMonth(_:String,_:Int))
  // spark.udf.register("getnow", (getnow: String) =>{
  
  def get_test(p_begindate: String, p_enddate: String, v_startdate: String, v_enddate: String, v_validdate: String, v_earnedtype: String, v_uwdate: String, v_edrtype: String):Double={
   
   new GetEarned()
   .evaluate(p_begindate,p_enddate,v_startdate,v_enddate,v_validdate,v_earnedtype, v_uwdate, v_edrtype);
  }
    
  
  def getnow(): String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var now: Date = new Date()
    return sdf.format(now)
  }

  def addThreeMonth(v_dt: String, v_month: Int): String = {
    //def addThreeMonth(v_dt: String, v_month: Int) {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    var date = sdf.parse(v_dt)
    val cl: Calendar = Calendar.getInstance
    cl.setTime(date)
    cl.add(Calendar.MONTH, v_month)
    cl.set(Calendar.DAY_OF_MONTH, 1)
    date = cl.getTime()
    return sdf.format(date)
    //val vo1 = sdf.format(date)
  }

  def addDay(v_dt: String, v_day: Int): String = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    var date = sdf.parse(v_dt)

    val cl: Calendar = Calendar.getInstance
    cl.setTime(date)
    cl.add(Calendar.DATE, v_day)
    date = cl.getTime()
    return sdf.format(date)
  }

  def getLastDay(v_dt: String): String = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    var date = sdf.parse(v_dt)

    val cl: Calendar = Calendar.getInstance
    cl.setTime(date)
    cl.set(Calendar.DAY_OF_MONTH, cl.getActualMaximum(Calendar.DAY_OF_MONTH))
    date = cl.getTime()
    return sdf.format(date)
  }

  def addMonth(v_dt: String, v_month: Int): String = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    var date = sdf.parse(v_dt)

    val cl: Calendar = Calendar.getInstance
    cl.setTime(date)
    cl.add(Calendar.MONTH, v_month)
    date = cl.getTime()
    return sdf.format(date)
  }

  //全角转半角
  def Full2Half(str: String): String = {
    val strArr = str.toCharArray()
    for (i <- 0 until strArr.length) {
      if (strArr(i) == '\u3000') {
        strArr(i) = ' '
      } else if (strArr(i) > '\uFF00' && strArr(i) < '\uFF5F') {
        strArr(i) = (strArr(i) - 65248).toChar
      }
    }
    return new String(strArr)
  }

  def stringToAscii(value: String): String = {
    var sbu: StringBuffer = new StringBuffer();
    var chars = value.toCharArray()
    for (i <- 0 until chars.length) {
      sbu.append(chars(i).toInt);
    }
    return sbu.toString()
  }

  def stringToOraAscII(v_str: String): String = {
    var v_ascii: String = ""
    if (v_str == "澳") {
      v_ascii = "45252"
    } else if (v_str == "北") {
      v_ascii = "45489"
    } else if (v_str == "丙") {
      v_ascii = "45563"
    } else if (v_str == "藏") {
      v_ascii = "45784"
    } else if (v_str == "厂") {
      v_ascii = "45991"
    } else if (v_str == "辰") {
      v_ascii = "46013"
    } else if (v_str == "成") {
      v_ascii = "46025"
    } else if (v_str == "川") {
      v_ascii = "46248"
    } else if (v_str == "鄂") {
      v_ascii = "46837"
    } else if (v_str == "甘") {
      v_ascii = "47306"
    } else if (v_str == "赣") {
      v_ascii = "47315"
    } else if (v_str == "港") {
      v_ascii = "47323"
    } else if (v_str == "庚") {
      v_ascii = "47357"
    } else if (v_str == "挂") {
      v_ascii = "47570"
    } else if (v_str == "广") {
      v_ascii = "47587"
    } else if (v_str == "桂") {
      v_ascii = "47600"
    } else if (v_str == "贵") {
      v_ascii = "47603"
    } else if (v_str == "海") {
      v_ascii = "47779"
    } else if (v_str == "黑") {
      v_ascii = "47834"
    } else if (v_str == "沪") {
      v_ascii = "48038"
    } else if (v_str == "吉") {
      v_ascii = "48298"
    } else if (v_str == "己") {
      v_ascii = "48314"
    } else if (v_str == "冀") {
      v_ascii = "48317"
    } else if (v_str == "济") {
      v_ascii = "48323"
    } else if (v_str == "甲") {
      v_ascii = "48343"
    } else if (v_str == "教") {
      v_ascii = "48588"
    } else if (v_str == "津") {
      v_ascii = "48626"
    } else if (v_str == "晋") {
      v_ascii = "48634"
    } else if (v_str == "京") {
      v_ascii = "48809"
    } else if (v_str == "警") {
      v_ascii = "48815"
    } else if (v_str == "军") {
      v_ascii = "48892"
    } else if (v_str == "空") {
      v_ascii = "49109"
    } else if (v_str == "兰") {
      v_ascii = "49340"
    } else if (v_str == "辽") {
      v_ascii = "49609"
    } else if (v_str == "鲁") {
      v_ascii = "49843"
    } else if (v_str == "蒙") {
      v_ascii = "50121"
    } else if (v_str == "闽") {
      v_ascii = "50166"
    } else if (v_str == "南") {
      v_ascii = "50383"
    } else if (v_str == "宁") {
      v_ascii = "50430"
    } else if (v_str == "青") {
      v_ascii = "51168"
    } else if (v_str == "琼") {
      v_ascii = "51181"
    } else if (v_str == "壬") {
      v_ascii = "51401"
    } else if (v_str == "赛") {
      v_ascii = "51452"
    } else if (v_str == "陕") {
      v_ascii = "51650"
    } else if (v_str == "申") {
      v_ascii = "51690"
    } else if (v_str == "沈") {
      v_ascii = "51698"
    } else if (v_str == "使") {
      v_ascii = "51897"
    } else if (v_str == "试") {
      v_ascii = "51924"
    } else if (v_str == "苏") {
      v_ascii = "52181"
    } else if (v_str == "皖") {
      v_ascii = "52718"
    } else if (v_str == "未") {
      v_ascii = "52916"
    } else if (v_str == "午") {
      v_ascii = "52967"
    } else if (v_str == "湘") {
      v_ascii = "53222"
    } else if (v_str == "辛") {
      v_ascii = "53441"
    } else if (v_str == "新") {
      v_ascii = "53442"
    } else if (v_str == "戌") {
      v_ascii = "53479"
    } else if (v_str == "学") {
      v_ascii = "53671"
    } else if (v_str == "乙") {
      v_ascii = "53970"
    } else if (v_str == "寅") {
      v_ascii = "54010"
    } else if (v_str == "渝") {
      v_ascii = "54245"
    } else if (v_str == "豫") {
      v_ascii = "54437"
    } else if (v_str == "粤") {
      v_ascii = "54465"
    } else if (v_str == "云") {
      v_ascii = "54470"
    } else if (v_str == "浙") {
      v_ascii = "54755"
    } else v_ascii = ""

    return v_ascii
  }

  def string2Unicode(value: String): String = {
    val unicode = new StringBuffer();
    for (i <- 0 until value.length) {
      // 取出每一个字符
      val c = value.charAt(i);
      if (c.toInt <= 128 && c.toInt >= 0) {
        unicode.append(c);
      } else {
        // 转换为unicode
        unicode.append("\\" + Integer.toHexString(c).toUpperCase());
      }
    }
    return unicode.toString();
  }

  //右补字符串函数
  def rpad(str: String, num: Int, insert: String): String = {
    var v_result: String = str
    for (i <- 0 until num - str.length) {
      v_result = v_result + insert
    }
    return v_result
  }

  //左补字符串函数
  def lpad(str: String, num: Int, insert: String): String = {
    var v_result: String = str
    for (i <- 0 until num - str.length) {
      v_result = insert + v_result
    }
    return v_result
  }

  //车架是否准确
  def vinistrue(ABC: String): Boolean = {
    val isfirstABC = "^[A-Z]+".r.findFirstIn(ABC).getOrElse("")

    val isfirstNUM = "^[0-9]{1,1}[A-Z]+".r.findFirstIn(ABC).getOrElse("")
    var istrue: Boolean = true
    if (!isfirstABC.isEmpty() || !isfirstNUM.isEmpty())
      istrue = true
    else istrue = false
    return istrue
  }

  //生成车辆id
  def CreateVehicleID(vin: String, vehicle_license: String): String = {
    var vehicle_id: String = " "
    //1、按照车架生成车辆id
    var vin_id: String = " "

    var vin_flag: Int = 0

    //全角转半角，去末尾空格、替换空格、转大写
    var v_vin: String = Full2Half(vin).trim().replaceAll("(\0|\\s*|\r|\n)", "").toUpperCase().replaceAll("[^A-Z0-9-]", "")
    if (v_vin.length == 17 && vinistrue(v_vin)) {
      vin_id = "VN" + v_vin;
      vin_flag = 0;
    } else {
      vin_flag = 1;
      vin_id = (if (v_vin.isEmpty()) "X" else v_vin);
      vin_id = string2Unicode(vin_id).replace("\\", "");
      vin_id = ("NV" + lpad(vin_id, 17, "X") + vin_id).substring(0, 19);
    }

    //2、按照车牌生成车辆id
    var vehicle_license_id: String = " "
    var vehicle_license_flag: Int = 0
    var v_vehicle_license: String = Full2Half(vehicle_license).trim().replaceAll("(\0|\\s*|\r|\n)", "").toUpperCase()

    if (v_vehicle_license.contains("河北")) {
      v_vehicle_license = v_vehicle_license.replaceAll("河北", "冀")
    } else if (v_vehicle_license.contains("山西")) {
      v_vehicle_license = v_vehicle_license.replaceAll("山西", "晋")
    } else if (v_vehicle_license.contains("上海")) {
      v_vehicle_license = v_vehicle_license.replaceAll("上海", "沪")
    } else if (v_vehicle_license.contains("安徽")) {
      v_vehicle_license = v_vehicle_license.replaceAll("安徽", "皖")
    } else if (v_vehicle_license.contains("福建")) {
      v_vehicle_license = v_vehicle_license.replaceAll("福建", "闽")
    } else if (v_vehicle_license.contains("江西")) {
      v_vehicle_license = v_vehicle_license.replaceAll("江西", "赣")
    } else if (v_vehicle_license.contains("山东")) {
      v_vehicle_license = v_vehicle_license.replaceAll("山东", "鲁")
    } else if (v_vehicle_license.contains("河南")) {
      v_vehicle_license = v_vehicle_license.replaceAll("河南", "豫")
    } else if (v_vehicle_license.contains("湖北")) {
      v_vehicle_license = v_vehicle_license.replaceAll("湖北", "鄂")
    } else if (v_vehicle_license.contains("湖南")) {
      v_vehicle_license = v_vehicle_license.replaceAll("湖南", "湘")
    } else if (v_vehicle_license.contains("广东")) {
      v_vehicle_license = v_vehicle_license.replaceAll("广东", "粤")
    } else if (v_vehicle_license.contains("广西")) {
      v_vehicle_license = v_vehicle_license.replaceAll("广西", "桂")
    } else if (v_vehicle_license.contains("海南")) {
      v_vehicle_license = v_vehicle_license.replaceAll("海南", "琼")
    } else if (v_vehicle_license.contains("重庆")) {
      v_vehicle_license = v_vehicle_license.replaceAll("重庆", "渝")
    } else {
      v_vehicle_license = v_vehicle_license
    }

    if (v_vehicle_license.contains("赛")) {
      v_vehicle_license = "赛" + v_vehicle_license.replaceAll("[赛试学教使厂内编警港澳挂]+", "")
    } else if (v_vehicle_license.contains("使")) {
      v_vehicle_license = "使" + v_vehicle_license.replaceAll("[赛试学教使厂内编警港澳挂]+", "")
    } else if (v_vehicle_license.contains("试")) {
      v_vehicle_license = v_vehicle_license.replaceAll("[赛试学教使厂内编警港澳挂]+", "试")
    } else if (v_vehicle_license.contains("学")) {
      v_vehicle_license = v_vehicle_license.replaceAll("[赛试学教使厂内编警港澳挂]+", "学")
    } else if (v_vehicle_license.contains("教")) {
      v_vehicle_license = v_vehicle_license.replaceAll("[赛试学教使厂内编警港澳挂]+", "教")
    } else if (v_vehicle_license.contains("警")) {
      v_vehicle_license = v_vehicle_license.replaceAll("[赛试学教使厂内编警港澳挂]+", "警")
    } else if (v_vehicle_license.contains("挂")) {
      v_vehicle_license = v_vehicle_license.replaceAll("[赛试学教使厂内编警港澳挂]+", "挂")
    } else if (v_vehicle_license.contains("厂") || v_vehicle_license.contains("内编")) {
      v_vehicle_license = v_vehicle_license.replaceAll("[赛试学教使厂内编警港澳挂]+", "厂")
    } else if (v_vehicle_license.contains("粤") && v_vehicle_license.contains("港")) {
      v_vehicle_license = v_vehicle_license.replaceAll("[赛试学教使厂内编警港澳挂]+", "港")
    } else if (v_vehicle_license.contains("粤") && v_vehicle_license.contains("澳")) {
      v_vehicle_license = v_vehicle_license.replaceAll("[赛试学教使厂内编警港澳挂]+", "澳")
    } else v_vehicle_license = v_vehicle_license

    v_vehicle_license = v_vehicle_license.toUpperCase.replaceAll("[^赛试学教使厂警港挂京津冀晋蒙辽吉黑沪浙苏皖闽赣鲁豫鄂湘粤桂琼渝川贵云陕甘新宁青藏甲乙丙庚午未己辛壬寅辰戌申军空海北沈兰济南广成A-Z0-9]+", "")
    //新能源车:车牌为8位，第1位为31个省市缩写任意一个汉字，第2位为字母，第3至8位为数字或者字母组合。
    if (v_vehicle_license.length == 8 && "^[京津冀晋蒙辽吉黑沪浙苏皖闽赣鲁豫鄂湘粤桂琼渝川贵云陕甘新宁青藏]{1}[A-Z]{1}[0-9A-Z]{6}".r.findFirstIn(v_vehicle_license).getOrElse("") != "") {
      vehicle_license_flag = 0
      vehicle_license_id = "10" + lpad(stringToOraAscII(v_vehicle_license.substring(0, 1)), 6, "0").substring(0, 6) + v_vehicle_license.substring(1, 8)
    } //全长7位，第一位为31个省市缩写，第二位为字母,之后五位必须出现一次数字
    else if (v_vehicle_license.length == 7 && "^[京津冀晋蒙辽吉黑沪浙苏皖闽赣鲁豫鄂湘粤桂琼渝川贵云陕甘新宁青藏]{1}[A-Z]{1}(?=[0-9A-Z]*\\d).{5}$".r.findFirstIn(v_vehicle_license).getOrElse("") != "" && "[试学教警挂]$".r.findFirstIn(v_vehicle_license).getOrElse("") == "") {
      vehicle_license_flag = 0
      vehicle_license_id = "11" + lpad(stringToOraAscII(v_vehicle_license.substring(0, 1)), 6, "0").substring(0, 6) + v_vehicle_license.substring(1, 7)
    } //全长8位，第一位为粤，第二位为字母,之后五位必须出现一次数字,第8位为'港'或'澳'
    else if (v_vehicle_license.length == 8 && "^[粤]{1}[A-Z]{1}(?=[0-9A-Z]*\\d).{5}[港澳]{1}$".r.findFirstIn(v_vehicle_license).getOrElse("") != "") {
      vehicle_license_flag = 0
      vehicle_license_id = "21" + lpad(stringToOraAscII(v_vehicle_license.substring(0, 1)), 6, "0").substring(0, 6) + v_vehicle_license.substring(1, 7) + lpad(stringToOraAscII(v_vehicle_license.substring(7, 8)), 6, "0").substring(0, 6)
    } //全长8位，第一位为31个省市缩写，第二位为字母,之后五位必须出现一次数字,第8位为'厂'
    else if (v_vehicle_license.length == 8 && "^[京津冀晋蒙辽吉黑沪浙苏皖闽赣鲁豫鄂湘粤桂琼渝川贵云陕甘新宁青藏]{1}[A-Z]{1}(?=[0-9A-Z]*\\d).{5}[厂]{1}$".r.findFirstIn(v_vehicle_license).getOrElse("") != "") {
      vehicle_license_flag = 0
      vehicle_license_id = "31" + lpad(stringToOraAscII(v_vehicle_license.substring(0, 1)), 6, "0").substring(0, 6) + v_vehicle_license.substring(1, 7) + lpad(stringToOraAscII(v_vehicle_license.substring(7, 8)), 6, "0").substring(0, 6)
    } //全长7位，第一位为31个省市缩写，第二位为字母,之后4位必须出现一次数字,第7位为'试','学','教','警','挂'
    else if (v_vehicle_license.length == 7 && "^[京津冀晋蒙辽吉黑沪浙苏皖闽赣鲁豫鄂湘粤桂琼渝川贵云陕甘新宁青藏]{1}[A-Z]{1}(?=[0-9A-Z]*\\d).{4}[试学教警挂]{1}$".r.findFirstIn(v_vehicle_license).getOrElse("") != "") {
      vehicle_license_flag = 0
      vehicle_license_id = "32" + lpad(stringToOraAscII(v_vehicle_license.substring(0, 1)), 6, "0").substring(0, 6) + v_vehicle_license.substring(1, 6) + lpad(stringToOraAscII(v_vehicle_license.substring(6, 7)), 6, "0").substring(0, 6)
    } //全长7位，第一位为赛使，其余六位为数字或字母
    else if (v_vehicle_license.length == 7 && "^[赛使]{1}[0-9A-Z]{6}".r.findFirstIn(v_vehicle_license).getOrElse("") != "") {
      vehicle_license_flag = 0
      vehicle_license_id = "41" + lpad(stringToOraAscII(v_vehicle_license.substring(0, 1)), 6, "0").substring(0, 6) + v_vehicle_license.substring(1, 7)
    } //全长6-12位，第一位为"WJ" "Wj" "wJ" "wj"
    else if (v_vehicle_license.length >= 6 && v_vehicle_license.length <= 12 && v_vehicle_license.substring(0, 2).toUpperCase() == "WJ") {
      vehicle_license_flag = 0
      vehicle_license_id = "51" + rpad(string2Unicode(v_vehicle_license).replace("\\", ""), 18, "W").substring(0, 18)
    } //全长6-12位，第一位为"甲乙丙庚午未己辛壬寅辰戌申军空海北沈兰济南广成"
    else if (v_vehicle_license.length >= 6 && v_vehicle_license.length <= 12 && "^[甲乙丙庚午未己辛壬寅辰戌申军空海北沈兰济南广成]{1}".r.findFirstIn(v_vehicle_license).getOrElse("") != "") {
      vehicle_license_flag = 0
      vehicle_license_id = "52" + lpad(stringToOraAscII(v_vehicle_license.substring(0, 1)), 6, "0").substring(0, 6) + rpad(string2Unicode(v_vehicle_license.substring(1, v_vehicle_license.length)).replace("\\", ""), 12, "J").substring(0, 12)
    } else {
      vehicle_license_flag = 1
      vehicle_license_id = ("97" + rpad(string2Unicode(if (v_vehicle_license.isEmpty()) "X" else v_vehicle_license).replace("\\", ""), 18, "X"))
      var v_length: Int = if (vehicle_license_id.length > 20) 20 else vehicle_license_id.length
      vehicle_license_id = vehicle_license_id.substring(0, v_length)
    }
    //生成唯一码值
    if (vin_flag == 0) {
      vehicle_id = vin_id
    } else if (vehicle_license_flag == 0) {
      vehicle_id = vehicle_license_id
    } else if (!vin_id.isEmpty()) {
      vehicle_id = vin_id + (if (vehicle_license_id.isEmpty()) "97" else vehicle_license_id)
    } else if (!vehicle_license_id.isEmpty()) {
      vehicle_id = vehicle_license_id
    } else {
      vehicle_id = "98NVL"
    }

    return vehicle_id
  }

  def vehicle_classification(family_name: String): String = {
    var vehicle_classcode: String = ""
    val c1_list: List[String] = List("Yeti", "帕杰罗", "东风悦达起亚KX5", "奇骏", "长安CS75", "昂科威", "东风悦达起亚KX3", "北斗星", "TRAX创酷", "长城M2", "北京现代ix25", "奔腾X80", "帕杰罗PAJERO", "大众TRANSPORTER", "宝骏560", "哈弗H6", "宝骏730", "朗境", "风行S500", "哈弗H7")
    val c2_list: List[String] = List("奥迪Q5", "途安", "明锐", "波罗", "长城M4", "哈弗H1", "瑞虎5", "艾力绅", "秀尔", "高尔夫", "帅客", "瑞风S3", "宝马X4", "思威CR-V", "大通V80", "海马S5", "天津一汽丰田RAV4", "东风风神H30", "锐界", "传祺GS4", "锐腾", "朗行", "速腾", "东风雪铁龙C3-XR", "锋驭", "逸致", "帕萨特", "欧陆CONTINENTAL", "速派", "郑州日产NV200", "长安CS15")
    val c3_list: List[String] = List("奥拓", "SPARK乐驰", "景逸", "天语", "奥德赛", "逍客", "长安马自达CX-5", "传祺GS5", "胜达SANTA FE", "一汽奥迪Q3", "长安CS35", "斯巴鲁XV", "瑞风S5", "君威", "启辰R50", "瑞虎3", "熊猫", "昊锐", "中华V5", "骏派D60", "凌渡", "瑞鹰", "荣威750", "途胜", "蓝鸟", "启辰T70", "骐达", "东风悦达起亚K4", "风行", "维特拉", "哈弗H2", "比亚迪F0", "傲虎OUTBACK", "凯越", "安德拉ANTARA", "哈弗H6 COUPE", "森林人FORESTER", "全球鹰GX7", "北汽威旺306", "长城C30", "海马S7", "荣威550", "普力马", "猎豹CS10", "瑞虎", "昕动", "雅力士", "东风雪铁龙C2", "英朗", "陆风X5", "赛拉图", "东风风神AX7", "东风标致307", "优优", "昕锐", "欧力威", "捷达", "炫丽", "炫威XR-V", "一汽奥迪Q5", "宏光", "威志", "君越", "景程", "菲翔", "大迈X5", "骊威", "雷凌", "格锐GRAND SANTA FE", "爱唯欧", "一汽马自达8")
    val c4_list: List[String] = List("优6 SUV", "伊兰特", "雅绅特", "博越", "名爵MG 3", "奔奔", "比亚迪S7", "致尚XT", "索兰托SORENTO", "索纳塔", "中华V3", "驭胜", "陆风X8", "骑士", "雅阁", "东风小康", "陆风X7", "艾瑞泽7", "雨燕", "丘比特", "利亚纳", "风景爱尔法", "福美来", "天籁", "远舰/K5", "威朗", "荣威350", "东风悦达起亚K3", "东风标致408", "宝马X3", "东风雪铁龙C5", "锐欧", "夏利", "迈腾", "谛艾仕DS 6", "福克斯", "致悦", "比亚迪S6", "东风雪铁龙C4L", "翼搏", "皇冠", "沃尔沃XC60", "飞度", "科鲁兹", "凯宴CAYENNE", "名爵MG 6", "朗动", "福瑞迪", "轩逸", "奥迪Q7", "长安CX20", "自由客PATRIOT", "哈弗H3", "欧尚", "迈锐宝", "东风悦达起亚K2", "酷威JCUV", "赛欧", "逸动", "神行者FREELANDER", "缤智", "宝马X5", "海星", "歌诗图", "牧马人WRANGLER", "风云", "瑞纳", "金牛星", "奇瑞E3", "奇瑞QQ/QQ3", "开瑞K50", "沃尔沃XC90", "哈弗H9", "雷克萨斯RX系", "东风风神S30", "佳乐CARENS", "颐达", "自由光", "一汽奥迪A6", "嘉年华", "乐风", "福睿斯", "幻速S3", "库伯COOPER", "奇瑞A5", "花冠/卡罗拉", "长安马自达2", "野马T70", "幻速S2", "东风标致508", "柯兰多KORAND", "欧诺", "奥迪A1", "睿骋", "哈弗H5")
    val c6_list: List[String] = List("奇瑞A3", "东南DX7", "极光", "东风标致2008", "荣威W5", "讴歌MDX", "威驰", "北汽绅宝D50", "博瑞GC9", "悦翔V7", "宝马X6", "北汽威旺M20", "一汽奥迪A3", "帝豪", "传祺GA3S视界", "瑞风S2", "迈凯MACAN", "宝马7系", "力帆X60", "幻速H2", "福瑞达M50", "探险者EXPLORER", "众泰T600", "中华H330", "沃尔沃V60", "宝利格", "领动", "古思特GHOST", "雷克萨斯GX系", "宝马6系")
    val c7_list: List[String] = List("自由光CHEROKEE", "英伦SC6", "总裁QUATTROPORTE", "LANNIA 蓝鸟", "纳5", "夏朗SHARAN", "东风标致301", "北京奔驰GLA级", "上海通用凯迪拉克XTS", "荣威360", "沃尔沃S60L", "沃尔沃V40", "上海通用凯迪拉克ATS-L", "奥迪A7", "雷克萨斯NX系", "比亚迪G5", "英菲尼迪JX系列(QX60)", "帝豪GS", "英菲尼迪M系列(Q70)", "野马F99", "发现神行", "奔驰G级", "北京奔驰GLC级", "奔驰SLK级", "宝马Z4", "奔驰GLE", "奔腾B90", "野马MUSTANG", "奥迪A4", "东风英菲尼迪Q50L", "凯迪拉克ATS", "劳恩斯ROHENS", "英菲尼迪QX系列(QX80)", "奔驰CLA级", "英致G3", "奥迪A6", "林肯MKZ", "奔驰GLA级", "法拉利458", "沃尔沃S40", "英菲尼迪ESQ")
    if (c1_list.contains(family_name)) {
      vehicle_classcode = "C1"
    } else if (c2_list.contains(family_name)) {
      vehicle_classcode = "C2"
    } else if (c3_list.contains(family_name)) {
      vehicle_classcode = "C3"
    } else if (c4_list.contains(family_name)) {
      vehicle_classcode = "C4"
    } else if (c6_list.contains(family_name)) {
      vehicle_classcode = "C6"
    } else if (c7_list.contains(family_name)) {
      vehicle_classcode = "C7"
    } else {
      vehicle_classcode = "C5"
    }
    return vehicle_classcode
  }

  def diff_date(date1: String, date2: String): Int = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val b = sdf.parse(date1).getTime - sdf.parse(date2).getTime
    val num = (b / (1000 * 3600 * 24)).toInt
    return num
  }

  def compute_quote(quo_time_string: String): Array[(String)] = {

    val bbb = quo_time_string.split("\\|")
    val arrlen = bbb.length
    var v_id = ""
    var v_time = ""
    var amap: Map[String, String] = Map()

    for (i <- 0 until arrlen) {
      println(bbb(i))
      val ccc = bbb(i).mkString.split("#")
      for (j <- 0 until ccc.length) {
        if (j % 2 == 0) {
          v_id = ccc(j)
        } else {
          v_time = ccc(j)
        }
        amap += (v_id -> v_time)

      }
    }
    var aaatuple = amap.toSeq.sortBy(_._2)
    var v_string = ""
    var quote: Int = 1
    var v_init_value = aaatuple(0)._2

    for (i <- 0 until aaatuple.length) {
      val v_value = aaatuple(i)._2
      var v_key = aaatuple(i)._1
      quote = if (diff_date(v_value, v_init_value) > 183) quote + 1 else quote
      v_string = v_string + v_key + "|" + quote + ","
      v_init_value = v_value
    }
    v_string = v_string.substring(0, v_string.length - 1)
    return v_string.split(",")
  }


  def compute_last_pol(policydate: String): Array[(String)] = {

    val bbb = policydate.split("\\|")
    val arrlen = bbb.length
    var v_id = ""
    var v_time = ""
    var amap: Map[String, String] = Map()

    for (i <- 0 until arrlen) {
      println(bbb(i))
      val ccc = bbb(i).mkString.split("#")
      for (j <- 0 until ccc.length) {
        if (j % 2 == 0) {
          v_id = ccc(j)
        } else {
          v_time = ccc(j)
        }
        amap += (v_id -> v_time)

      }
    }
    var aaatuple = amap.toSeq.sortBy(_._2)
    var v_string = ""
    var quote: Int = 1
    var v_init_value = aaatuple(0)._2

    for (i <- 0 until aaatuple.length) {
      val v_value = aaatuple(i)._2
      var v_key = aaatuple(i)._1
      quote = if (diff_date(v_value, v_init_value) < 395 && diff_date(v_value, v_init_value) >= 365) {
        quote + 1
      } else if (diff_date(v_value, v_init_value) > 395) {
        quote + 100
      } else {
        quote
      }
      v_string = v_string + v_key + "|" + quote + ","
      v_init_value = v_value
    }
    v_string = v_string.substring(0, v_string.length - 1)
    return v_string.split(",")
  }

  def compute_years_grp(quoteflag: String): Array[(String)] = {

    val bbb = quoteflag.split("\\|")
    val arrlen = bbb.length
    var v_id = ""
    var v_time = ""
    var amap: Map[String, String] = Map()

    for (i <- 0 until arrlen) {
      println(bbb(i))
      val ccc = bbb(i).mkString.split("#")
      for (j <- 0 until ccc.length) {
        if (j % 2 == 0) {
          v_id = ccc(j)
        } else {
          v_time = ccc(j)
        }
        amap += (v_id -> v_time)

      }
    }
    var aaatuple = amap.toSeq.sortBy(_._2)
    var v_string = ""
    var quote: Int = 1
    var v_init_value = aaatuple(0)._2

    for (i <- 0 until aaatuple.length) {
      val v_value = aaatuple(i)._2
      var v_key = aaatuple(i)._1
      quote = if (diff_date(v_value, v_init_value) < 395 && diff_date(v_value, v_init_value) >= 365) {
        quote + 1
      } else if (diff_date(v_value, v_init_value) > 395) {
        quote + 100
      } else {
        quote
      }
      v_string = v_string + v_key + "|" + quote + ","
      v_init_value = v_value
    }

    v_string = v_string.substring(0, v_string.length - 1)
    return v_string.split(",")
  }
  
   //判断车系类别数据
  def chexi_judge(family_name: String): String = {
    var class_code: String = ""
    val c1_list: List[String] = List("Yeti", "帕杰罗", "东风悦达起亚KX5", "奇骏", "长安CS75", "昂科威", "东风悦达起亚KX3", "北斗星", "TRAX创酷", "长城M2", "北京现代ix25", "奔腾X80", "帕杰罗PAJERO", "大众TRANSPORTER", "宝骏560", "哈弗H6", "宝骏730", "朗境", "风行S500", "哈弗H7")
    val c2_list: List[String] = List("奥迪Q5", "途安", "明锐", "波罗", "长城M4", "哈弗H1", "瑞虎5", "艾力绅", "秀尔", "高尔夫", "帅客", "瑞风S3", "宝马X4", "思威CR-V", "大通V80", "海马S5", "天津一汽丰田RAV4", "东风风神H30", "锐界", "传祺GS4", "锐腾", "朗行", "速腾", "东风雪铁龙C3-XR", "锋驭", "逸致", "帕萨特", "欧陆CONTINENTAL", "速派", "郑州日产NV200", "长安CS15")
    val c3_list: List[String] = List("奥拓", "SPARK乐驰", "景逸", "天语", "奥德赛", "逍客", "长安马自达CX-5", "传祺GS5", "胜达SANTA FE", "一汽奥迪Q3", "长安CS35", "斯巴鲁XV", "瑞风S5", "君威", "启辰R50", "瑞虎3", "熊猫", "昊锐", "中华V5", "骏派D60", "凌渡", "瑞鹰", "荣威750", "途胜", "蓝鸟", "启辰T70", "骐达", "东风悦达起亚K4", "风行", "维特拉", "哈弗H2", "比亚迪F0", "傲虎OUTBACK", "凯越", "安德拉ANTARA", "哈弗H6 COUPE", "森林人FORESTER", "全球鹰GX7", "北汽威旺306", "长城C30", "海马S7", "荣威550", "普力马", "猎豹CS10", "瑞虎", "昕动", "雅力士", "东风雪铁龙C2", "英朗", "陆风X5", "赛拉图", "东风风神AX7", "东风标致307", "优优", "昕锐", "欧力威", "捷达", "炫丽", "炫威XR-V", "一汽奥迪Q5", "宏光", "威志", "君越", "景程", "菲翔", "大迈X5", "骊威", "雷凌", "格锐GRAND SANTA FE", "爱唯欧", "一汽马自达8")
    val c4_list: List[String] = List("优6 SUV", "伊兰特", "雅绅特", "博越", "名爵MG 3", "奔奔", "比亚迪S7", "致尚XT", "索兰托SORENTO", "索纳塔", "中华V3", "驭胜", "陆风X8", "骑士", "雅阁", "东风小康", "陆风X7", "艾瑞泽7", "雨燕", "丘比特", "利亚纳", "风景爱尔法", "福美来", "天籁", "远舰/K5", "威朗", "荣威350", "东风悦达起亚K3", "东风标致408", "宝马X3", "东风雪铁龙C5", "锐欧", "夏利", "迈腾", "谛艾仕DS 6", "福克斯", "致悦", "比亚迪S6", "东风雪铁龙C4L", "翼搏", "皇冠", "沃尔沃XC60", "飞度", "科鲁兹", "凯宴CAYENNE", "名爵MG 6", "朗动", "福瑞迪", "轩逸", "奥迪Q7", "长安CX20", "自由客PATRIOT", "哈弗H3", "欧尚", "迈锐宝", "东风悦达起亚K2", "酷威JCUV", "赛欧", "逸动", "神行者FREELANDER", "缤智", "宝马X5", "海星", "歌诗图", "牧马人WRANGLER", "风云", "瑞纳", "金牛星", "奇瑞E3", "奇瑞QQ/QQ3", "开瑞K50", "沃尔沃XC90", "哈弗H9", "雷克萨斯RX系", "东风风神S30", "佳乐CARENS", "颐达", "自由光", "一汽奥迪A6", "嘉年华", "乐风", "福睿斯", "幻速S3", "库伯COOPER", "奇瑞A5", "花冠/卡罗拉", "长安马自达2", "野马T70", "幻速S2", "东风标致508", "柯兰多KORAND", "欧诺", "奥迪A1", "睿骋", "哈弗H5")
    val c6_list: List[String] = List("奇瑞A3", "东南DX7", "极光", "东风标致2008", "荣威W5", "讴歌MDX", "威驰", "北汽绅宝D50", "博瑞GC9", "悦翔V7", "宝马X6", "北汽威旺M20", "一汽奥迪A3", "帝豪", "传祺GA3S视界", "瑞风S2", "迈凯MACAN", "宝马7系", "力帆X60", "幻速H2", "福瑞达M50", "探险者EXPLORER", "众泰T600", "中华H330", "沃尔沃V60", "宝利格", "领动", "古思特GHOST", "雷克萨斯GX系", "宝马6系")
    val c7_list: List[String] = List("自由光CHEROKEE", "英伦SC6", "总裁QUATTROPORTE", "LANNIA 蓝鸟", "纳5", "夏朗SHARAN", "东风标致301", "北京奔驰GLA级", "上海通用凯迪拉克XTS", "荣威360", "沃尔沃S60L", "沃尔沃V40", "上海通用凯迪拉克ATS-L", "奥迪A7", "雷克萨斯NX系", "比亚迪G5", "英菲尼迪JX系列(QX60)", "帝豪GS", "英菲尼迪M系列(Q70)", "野马F99", "发现神行", "奔驰G级", "北京奔驰GLC级", "奔驰SLK级", "宝马Z4", "奔驰GLE", "奔腾B90", "野马MUSTANG", "奥迪A4", "东风英菲尼迪Q50L", "凯迪拉克ATS", "劳恩斯ROHENS", "英菲尼迪QX系列(QX80)", "奔驰CLA级", "英致G3", "奥迪A6", "林肯MKZ", "奔驰GLA级", "法拉利458", "沃尔沃S40", "英菲尼迪ESQ")
    if (c1_list.contains(family_name)) {
      class_code = "C1"
    } else if (c2_list.contains(family_name)) {
      class_code = "C2"
    } else if (c3_list.contains(family_name)) {
      class_code = "C3"
    } else if (c4_list.contains(family_name)) {
      class_code = "C4"
    } else if (c6_list.contains(family_name)) {
      class_code = "C6"
    } else if (c7_list.contains(family_name)) {
      class_code = "C7"
    } else {
      class_code = "C5"
    }
    return class_code
  } 
  
  
  
}
