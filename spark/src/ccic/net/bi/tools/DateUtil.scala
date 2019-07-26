package net.ccic.datasource

import java.text.SimpleDateFormat
import java.util.Date

  object DateUtil {
    def getDateTimeNow(): String = {
      val now: Date = new Date()
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yy/MM/dd HH:mm:ss")
      val date = dateFormat.format(now)
      return date + " 程序输出:"
    }
  }

