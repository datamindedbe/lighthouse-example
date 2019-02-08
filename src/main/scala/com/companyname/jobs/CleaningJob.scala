package com.companyname.jobs
import be.dataminded.lighthouse.config.{LighthouseConfiguration, LighthouseConfigurationParser}
import be.dataminded.lighthouse.spark.SparkApplication
import com.companyname.lake.CompanyDataLake
import com.companyname.lake.DataUIDs._

object CleaningJob extends SparkApplication {
  val parser = new LighthouseConfigurationParser()

  parser.parse(args, LighthouseConfiguration()) match {
    case Some(config) => process(new CompanyDataLake(config.localDate))
    case None => System.exit(1)
  }


  def process(lake: CompanyDataLake) = {
    //process all your raw data like this

    val data = lake.getDataLink(Raw.InputSourceFirst.inputSourceFirstDataFirst).read()

    //clean the data
    //adjust column types, set null values etc

    lake(Clean.firstCleanSource).write(data)

  }

}
