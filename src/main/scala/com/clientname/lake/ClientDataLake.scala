package com.clientname.lake

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import be.dataminded.lighthouse.datalake._
import be.dataminded.lighthouse.spark._
import com.clientname.lake.ClientDataUIDs._
import org.apache.spark.sql.SaveMode

class ClientDataLake(val localDate: LocalDate) extends Datalake {

  //the name argument here must exactly match the environment you are passing from airflow via the -e argument
  environment("PROD") { refs => buildEnvironment(refs, new ClientProdEnvironment) }
  environment("TEST") { refs => buildEnvironment(refs, new ClientTestEnvironment) }

  private def buildEnvironment(refs: EnvironmentBuilder, environment: ClientEnvironment) = {
    //define functions to calculate the path to your data.
    //separate your data into raw/src, project

    def getPath(folder: String) = {
      val dateString = localDate.format(DateTimeFormatter.ofPattern("yyyy/MM/dd/"))
      s"s3://${environment.bucketName}/$folder/$dateString/"
    }

    def getRawPath(folder: String) = {
      getPath("raw/" + folder)
    }

    def getCleanPath(folder: String) = {
      getPath("clean/" + folder)
    }

    def getProjectPath(folder: String) = {
      getPath("project/" + folder)
    }

    //define all your data sources like this:
    refs += Raw.InputSourceFirst.inputSourceFirstDataFirst -> new FileSystemDataLink(
      getRawPath("inputSourceFirst/dataFirst"),
      Parquet
    )

    refs += Raw.InputSourceFirst.inputSourceFirstDataSecond -> new FileSystemDataLink(
      getRawPath("inputSourceFirst/dataSecond"),
      Orc
    )

    //output to clean
    refs += Clean.firstCleanSource -> new FileSystemDataLink(
      getCleanPath("inputSourceFirst/dataSecond"),
      Orc,
      SaveMode.Overwrite
    )

  }
}
