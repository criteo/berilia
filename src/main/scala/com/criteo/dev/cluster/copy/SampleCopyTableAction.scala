package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster._
import org.slf4j.LoggerFactory

/**
  * Copies data to target via Hive sampling.
  */
class SampleCopyTableAction(conf: Map[String, String], source: Node, target: Node, sampleProb: Double) {

  private val logger = LoggerFactory.getLogger(classOf[SampleCopyTableAction])

  def copy(tableInfo: TableInfo): Unit = {
    val location = tableInfo.ddl.location.get
    val partitions = tableInfo.partitions

    //this algorithm first creates a sampled temp table on the source cluster.
    //Note that if target is S3, we could have made an external table on S3 and skipped the temp-sample table.
    //But we ran into issues with some versions of Hive doing this.
    val sampleDb = GeneralUtilities.getConfStrict(conf, CopyConstants.sampleDb, GeneralConstants.sourceProps)
    val sampleTable = CopyUtilities.getTempTableName(tableInfo.ddl.table)

    logger.info(s"Sampling " + partitions.length + " partitions from " +
      tableInfo.database + "." + tableInfo.ddl.table)
    createSampleTable(tableInfo, sampleDb, sampleTable)

    //Copy the sampled data to final destination.
    logger.info(s"Copying " + partitions.length + " partitions from " +
      sampleDb + "." + sampleTable)

    val targetLocation = CopyUtilities.toRelative(CopyUtilities.getCommonLocation(location, partitions))
    copyToDest(sampleDb, sampleTable, targetLocation)

    //finished copying, delete the temp table.
    val deleteTempTableAction = new SshHiveAction(source)
    val tableToDelete = s"$sampleDb.$sampleTable"
    require(tableToDelete.contains(CopyConstants.tempTableHint)) //paranoid check not to delete the wrong table.
    deleteTempTableAction.add(s"drop table $tableToDelete")
    deleteTempTableAction.run()
  }


  //copy over the sample data to S3 using distcp.
  private def copyToDest(sampleDb: String, sampleTable: String, targetLocation: String): Unit = {
    val getTempMetadata = new GetMetadataAction(conf, source, throttle = false)
    val tempTableInfo = getTempMetadata(s"$sampleDb.$sampleTable")
    val tempLocation = tempTableInfo.ddl.location.get
    val tempLocationCommon = CopyUtilities.getCommonLocation(tempLocation, tempTableInfo.partitions)
    val tempLocations: Array[String] = {
      if (tempTableInfo.partitions.isEmpty) Array(tempLocation)
      else tempTableInfo.partitions.map(_.location)
    }

    val copyFileAction = CopyFileActionFactory.getCopyFileAction(conf, source, target)
    copyFileAction(tempLocations, tempLocationCommon, targetLocation)
  }

  //create table with sampled data
  private def createSampleTable(sourceTableInfo: TableInfo, sampleDb: String, sampleTable: String): String = {
    val sampleTableInfo: TableInfo = sourceTableInfo.copy(
      database = sampleDb,
      ddl = sourceTableInfo.ddl.copy(database = Some(sampleDb), table = sampleTable, location = None))

    val createAction = new SshHiveAction(source)
    createAction.add(s"use $sampleDb")
    val ddl = sampleTableInfo.ddl
    createAction.add(ddl.format)
    createAction.run

    fireBeforeSample(conf, sourceTableInfo, sampleTableInfo)

    val insertAction = new SshHiveAction(source)
    insertAction.add(s"use $sampleDb")
    insertAction.add("set hive.exec.dynamic.partition=true")
    insertAction.add("set hive.exec.dynamic.partition.mode=nonstrict")

    val query = new StringBuilder(s"insert into table $sampleTable")
    if (sourceTableInfo.partitions.length > 0) {
      query.append(" partition ")
      query.append(CopyUtilities.partitionColumns(sourceTableInfo.partitions(0)))
    }

    query.append(s" select * from ${sourceTableInfo.database}.${sourceTableInfo.ddl.table} ")
    if (sourceTableInfo.partitions.length > 0 || sampleProb < 1) {
      query.append("where ")

      if (sampleProb < 1) {
        query.append(s"rand() <= $sampleProb ")
      }

      if (sampleProb < 1 && sourceTableInfo.partitions.length > 0) {
        query.append("and ")
      }

      //partition filters
      if (sourceTableInfo.partitions.length > 0) {
        val partFilters = sourceTableInfo.partitions.map(p => s"(${CopyUtilities.partitionSpecFilter(p.partSpec)})")
        val partFilter = partFilters.mkString("(", " or ", ")")
        query.append(partFilter)
      }
    }
    insertAction.add(query.toString)
    insertAction.run()
  }

  def fireBeforeSample(conf: Map[String, String], tableInfo: TableInfo, sampleTableInfo: TableInfo) = {
    val listeners = GeneralUtilities.getNonEmptyConf(conf, CopyConstants.sampleListeners)
    if (listeners.isDefined) {
      listeners.get.split(",").map(_.trim()).foreach(l => {
        val clazz = this.getClass.getClassLoader.loadClass(l)
        val listener = clazz.newInstance().asInstanceOf[SampleTableListener]
        val copyFileAction = CopyFileActionFactory.getCopyFileAction(conf, source, target)
        listener.onBeforeSample(tableInfo, sampleTableInfo, source)
      })
    }
  }
}
