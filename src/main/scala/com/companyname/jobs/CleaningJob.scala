package com.companyname.jobs
import be.dataminded.lighthouse.config.{LighthouseConfiguration, LighthouseConfigurationParser}
import be.dataminded.lighthouse.spark.SparkApplication
import com.companyname.lake.TypedCompanyDataLake
import com.companyname.models.{CaseClassOne, CaseClassOneCleaned}
import org.apache.spark.sql.Dataset

object CleaningJob extends SparkApplication {
  import spark.implicits._

  val parser = new LighthouseConfigurationParser()

  parser.parse(args, LighthouseConfiguration()) match {
    case Some(config) => process(TypedCompanyDataLake(config))
    case None => System.exit(1)
  }

  def process(lake: TypedCompanyDataLake): Unit = {
    //read in your clean data, produce some project data
    val first = lake.raw.firstData.readTyped()

    val second = lake.raw.secondData.readTyped()

    //do the transformations
    val firstCleaned = cleanFirst(first)

    lake.clean.firstCleanSource.writeTyped(firstCleaned)
  }

  def cleanFirst(ds: Dataset[CaseClassOne]) = {
    //do the actual cleaning, this is just a small example
    ds.map(d => CaseClassOneCleaned(
      name = d.name,
      address = d.address,
      age = Some(d.age.toInt)
    ))
  }



}
