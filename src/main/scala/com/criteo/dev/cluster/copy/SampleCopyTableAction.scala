package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.command.{ShellHiveAction, SshHiveAction}
import com.criteo.dev.cluster.config.GlobalConfig
import org.slf4j.LoggerFactory

/**
  * Copies data to target via Hive sampling.
  */
class SampleCopyTableAction(config: GlobalConfig, conf: Map[String, String], source: Node, target: Node, sampleProb: Double) {

  private val logger = LoggerFactory.getLogger(classOf[SampleCopyTableAction])

  def copy(tableInfo: TableInfo): TableInfo = {
    val location = tableInfo.ddl.location.get
    val partitions = tableInfo.partitions

    //this algorithm first creates a sampled temp table on the source cluster.
    //Note that if target is S3, we could have made an external table on S3 and skipped the temp-sample table.
    //But we ran into issues with some versions of Hive doing this.
    val sampleDb = GeneralUtilities.getConfStrict(conf, CopyConstants.sampleDb, GeneralConstants.sourceProps)
    val sampleTable = CopyUtilities.getTempTableName(tableInfo.ddl.table)

    logger.info(s"Sampling " + partitions.length + " partitions from " +
      tableInfo.database + "." + tableInfo.ddl.table)
    createSampleTable(tableInfo, sampleDb, sampleTable, config.source.isLocalScheme)

    //Copy the sampled data to final destination.
    logger.info(s"Copying " + partitions.length + " partitions from " +
      sampleDb + "." + sampleTable)

    val targetLocation = CopyUtilities.toRelative(CopyUtilities.getCommonLocation(location, partitions))
    val res = copyToDest(tableInfo, sampleDb, sampleTable, targetLocation)

    //finished copying, delete the temp table.
    val deleteTempTableAction = if (config.source.isLocalScheme) new ShellHiveAction() else new SshHiveAction(source)
    val tableToDelete = s"$sampleDb.$sampleTable"
    require(tableToDelete.contains(CopyConstants.tempTableHint)) //paranoid check not to delete the wrong table.
    deleteTempTableAction.add(s"drop table $tableToDelete")
    deleteTempTableAction.run()

    res
  }


  //copy over the sample data to the dev cluster
  private def copyToDest(sourceTable: TableInfo,
                         sampleDb: String,
                         sampleTable: String,
                         targetLocation: String): TableInfo = {
    val getTempMetadata = new GetMetadataAction(config, conf, source, throttle = false)
    val tempTableInfo = getTempMetadata(s"$sampleDb.$sampleTable")
    val tempLocation = tempTableInfo.ddl.location.get
    val tempLocationCommon = CopyUtilities.getCommonLocation(tempLocation, tempTableInfo.partitions)
    val tempLocations: Array[String] = {
      if (tempTableInfo.partitions.isEmpty) Array(tempLocation)
      else tempTableInfo.partitions.map(_.location)
    }

    val copyFileAction = CopyFileActionFactory.getCopyFileAction(conf, source, target)
    copyFileAction(tempLocations, tempLocationCommon, targetLocation)

    //here we have unfortunately lost the original source table folder structure (after sampling it).
    //so the target table will be slightly different than the original source, and instead be like the sample result.
    val destTablePath = CopyUtilities.toRelative(sourceTable.ddl.location.get)
    val destParts = tempTableInfo.partitions.map(p => {
      p.copy(
        location = s"$destTablePath/${CopyUtilities.getPartPath(p.location, tempTableInfo.ddl.location.get, includeBase = false)}")
    })
    sourceTable.copy(partitions = destParts)
  }

  //create table with sampled data
  private def createSampleTable(sourceTableInfo: TableInfo, sampleDb: String, sampleTable: String, isLocalScheme: Boolean): String = {
    val descDbAction = if (isLocalScheme) new ShellHiveAction else new SshHiveAction(source)
    descDbAction.add(s"describe database $sampleDb")
    val result = descDbAction.run

    //keep the same relative paths
    val sampleTableInfo: TableInfo = sourceTableInfo.copy(
      database = sampleDb,
      ddl = sourceTableInfo.ddl.copy(
        database = Some(sampleDb),
        table = sampleTable,
        isExternal = false,
        location = None))

    //create sample table
    val createAction = if (isLocalScheme) new ShellHiveAction else new SshHiveAction(source)
    createAction.add(s"use $sampleDb")
    val ddl = sampleTableInfo.ddl
    createAction.add(ddl.format)
    createAction.run

    //fire listeners.
    fireBeforeSample(conf, sourceTableInfo, sampleTableInfo, config.source.isLocalScheme)

    val insertAction = if (isLocalScheme) new ShellHiveAction else new SshHiveAction(source)
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
        val partFilters = sourceTableInfo.partitions.map(p => s"(${CopyUtilities.partitionSpecFilter(p.partSpec,
          sourceTableInfo.ddl.partitionedBy)})")
        val partFilter = partFilters.mkString("(", " or ", ")")
        query.append(partFilter)
      }
    }
    insertAction.add(query.toString)
    insertAction.run()
  }

  def fireBeforeSample(conf: Map[String, String], tableInfo: TableInfo, sampleTableInfo: TableInfo, isLocalScheme: Boolean) = {
    val listeners = GeneralUtilities.getNonEmptyConf(conf, CopyConstants.sampleListeners)
    if (listeners.isDefined) {
      listeners.get.split(",").map(_.trim()).foreach(l => {
        val clazz = this.getClass.getClassLoader.loadClass(l)
        val listener = clazz.newInstance().asInstanceOf[SampleTableListener]
        listener.onBeforeSample(tableInfo, sampleTableInfo, source, isLocalScheme)
      })
    }
  }
}
