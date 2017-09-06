#!/usr/bin/env python

import time, os, datetime, sys, logging, logging.handlers, shutil, ConfigParser, io
import arcpy

from collections import OrderedDict
from ConfigParser import RawConfigParser

########################## functions ##############################

def rarDatabase():
    os.system("RAR.vbs 1")
    time.sleep(600)

## if you have Network Attached Storage use this
##   os.system("NAS.bat 1")

def getDatabaseItemCount(workspace):
    log = logging.getLogger("script_log")
    """returns the item count in provided database"""
    arcpy.env.workspace = workspace
    feature_classes = []
    log.info("Compiling a list of items in {0} and getting count.".format(workspace))
    for dirpath, dirnames, filenames in arcpy.da.Walk(workspace,datatype="Any",type="Any"):
        for filename in filenames:
            feature_classes.append(os.path.join(dirpath, filename))
    log.info("There are a total of {0} items in the database".format(len(feature_classes)))
    return feature_classes, len(feature_classes)

def replicateDatabase(dbConnection, targetGDB):
    log = logging.getLogger("script_log")
    startTime = time.time()

    if arcpy.Exists(dbConnection):
        featSDE,cntSDE = getDatabaseItemCount(dbConnection)
        log.info("Geodatabase being copied: %s -- Feature Count: %s" %(dbConnection, cntSDE))
        if arcpy.Exists(targetGDB):
            featGDB,cntGDB = getDatabaseItemCount(targetGDB)
            log.info("Old Target Geodatabase: %s -- Feature Count: %s" %(targetGDB, cntGDB))
            try:
                shutil.rmtree(targetGDB)
                log.info("Deleted Old %s" %(os.path.split(targetGDB)[-1]))
            except Exception as e:
                log.info(e)

        GDB_Path, GDB_Name = os.path.split(targetGDB)
        log.info("Now Creating New %s" %(GDB_Name))
        arcpy.CreateFileGDB_management(GDB_Path, GDB_Name)

        arcpy.env.workspace = dbConnection
############### datasets #################
        datasetList = (config.get('settings', 'sdeName_') + '.' + config.get('datasets', 'datasetList'))
        datasetList = datasetList.split()
############### tables #################

## i can't get values from config, need help
        tables = [u'TABLE_1', u'TABLE_2', u'TABLE_3']


        allDbData = datasetList + tables

        for sourcePath in allDbData:
            targetName = sourcePath.split('.')[-1]
            targetPath = os.path.join(targetGDB, targetName)
            if not arcpy.Exists(targetPath):
                try:
                    log.info("Atempting to Copy %s to %s" %(targetName, targetPath))
                    arcpy.Copy_management(sourcePath, targetPath)
                    log.info("Finished copying %s to %s" %(targetName, targetPath))
                except Exception as e:
                    log.info("Unable to copy %s to %s" %(targetName, targetPath))
                    log.info(e)
            else:
                log.info("%s already exists....skipping....." %(targetName))

        featGDB,cntGDB = getDatabaseItemCount(targetGDB)
        log.info("Completed replication of %s -- Feature Count: %s" %(dbConnection, cntGDB))

    else:
        log.info("{0} does not exist or is not supported! \
        Please check the database path and try again.".format(dbConnection))

#####################################################################################

def formatTime(x):
    minutes, seconds_rem = divmod(x, 60)
    if minutes >= 60:
        hours, minutes_rem = divmod(minutes, 60)
        return "%02d:%02d:%02d" % (hours, minutes_rem, seconds_rem)
    else:
        minutes, seconds_rem = divmod(x, 60)
        return "00:%02d:%02d" % (minutes, seconds_rem)

if __name__ == "__main__":
    startTime = time.time()
    now = datetime.datetime.now()

    ############################### variables #################################

    with open("config.ini") as f:
        sample_config = f.read()
    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.readfp(io.BytesIO(sample_config))





    logPath = config.get('settings', 'logPath_')
    databaseConnection = os.path.join('Database Connections\\', config.get('settings', 'sdeFile_'))
    targetGDB = config.get('settings', 'gdbPath_')
    targetName = os.path.join(config.get('settings', 'gdbPath_'), config.get('settings', 'sdeName_') + (now.strftime("_%Y-%m-%d_%H-%M.gdb")))

    ############################### logging items ###################################

    logName = os.path.join(logPath,(now.strftime("%Y-%m-%d_%H-%M.log")))

    log = logging.getLogger("script_log")
    log.setLevel(logging.INFO)

    h1 = logging.FileHandler(logName)
    h2 = logging.StreamHandler()

    f = logging.Formatter("[%(levelname)s] [%(asctime)s] [%(lineno)d] - %(message)s",'%m/%d/%Y %I:%M:%S %p')

    h1.setFormatter(f)
    h2.setFormatter(f)

    h1.setLevel(logging.INFO)
    h2.setLevel(logging.INFO)

    log.addHandler(h1)
    log.addHandler(h2)

    log.info('Script: {0}'.format(os.path.basename(sys.argv[0])))

    try:
        ########################## function calls ######################################

        replicateDatabase(databaseConnection, targetName)

        ################################################################################
    except Exception, e:
        log.exception(e)

    totalTime = formatTime((time.time() - startTime))
    log.info('--------------------------------------------------')
    log.info("Script Completed After: {0}".format(totalTime))
    log.info('--------------------------------------------------')
    time.sleep(120)
    rarDatabase()
