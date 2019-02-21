package com.companyname.jobs
import be.dataminded.lighthouse.config.{LighthouseConfiguration, LighthouseConfigurationParser}
import be.dataminded.lighthouse.spark.SparkApplication
import com.companyname.lake.TypedCompanyDataLake

object CleaningJob extends SparkApplication {
  val parser = new LighthouseConfigurationParser()

  parser.parse(args, LighthouseConfiguration()) match {
    case Some(config) => process(TypedCompanyDataLake(config))
    case None => System.exit(1)
  }


  def process(lake: TypedCompanyDataLake) = {
    //read in your clean data, produce some project data
    val first = lake.raw.firstData.readTyped()

    val second = lake.raw.secondData.readTyped()

    //do the transformations

    lake.clean.firstCleanSource.writeTyped(second)
  }

}
