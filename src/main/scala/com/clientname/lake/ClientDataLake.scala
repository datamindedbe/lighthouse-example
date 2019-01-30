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

    def getPath(folder: String) = s"s3://${environment.bucketName}/$folder"

    def getRawPath(folder: String) = {
      getPath("raw/" + folder)
    }

    def getCleanPath(folder: String) = {
      getPath("clean/" + folder)
    }

    def getProjectPath(folder: String) = {
      getPath("project/" + folder)
    }

    def snapshotDataLinkParquet(path: String) = new SnapshotDataLink(
      new FileSystemDataLink(getRawPath(path), Parquet),
      localDate
    )

    //define all your data sources like this:
    refs += Raw.InputSourceFirst.inputSourceFirstDataFirst -> snapshotDataLinkParquet("inputSourceFirst/dataFirst")

    refs += Raw.InputSourceFirst.inputSourceFirstDataFirst -> snapshotDataLinkParquet("inputSourceFirst/dataFirst")

    //one does not need to use helper functions
    refs += Raw.InputSourceFirst.inputSourceFirstDataSecond -> new SnapshotDataLink(
      new FileSystemDataLink(getRawPath("inputSourceFirst/dataSecond"),Orc),
      localDate
    )

    //output to clean
    refs += Clean.firstCleanSource -> new SnapshotDataLink(
      new FileSystemDataLink(getCleanPath("inputSourceFirst/dataSecond"),Orc,SaveMode.Overwrite),
      localDate
    )

  }
}
