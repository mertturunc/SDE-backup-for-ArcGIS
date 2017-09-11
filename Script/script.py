#!/usr/bin/env python

import time, os, datetime, sys, logging, logging.handlers, shutil, ConfigParser, io, errno, stat, getpass, rarfile
import arcpy

from collections import OrderedDict
from ConfigParser import RawConfigParser

########################## functions ##############################
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

        try:
            datasetList = [arcpy.Describe(a).name for a in arcpy.ListDatasets()]
        except Exception, e:
            datasetList = []
            log.info(e)

        try:
            featureClasses = [arcpy.Describe(a).name for a in arcpy.ListFeatureClasses()]
        except Exception, e:
            featureClasses = []
            log.info(e)

        for i in range(len(tables)):
            tables[tables.index(tables[i])] = sdeNames_ + '.' + tables[i]

        print tables


        allDbData = tables

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
def zip_folder(folder_path, output_path):

    parent_folder = os.path.dirname(folder_path)

    contents = os.walk(folder_path)
    try:
        zip_file = zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED)
        for root, folders, files in contents:

            for folder_name in folders:
                absolute_path = os.path.join(root, folder_name)
                relative_path = absolute_path.replace(parent_folder + '\\',
                                                      '')
                print "Adding '%s' to archive." % absolute_path
                zip_file.write(absolute_path, relative_path)
            for file_name in files:
                absolute_path = os.path.join(root, file_name)
                relative_path = absolute_path.replace(parent_folder + '\\',
                                                      '')
                print "Adding '%s' to archive." % absolute_path
                zip_file.write(absolute_path, relative_path)
        print "'%s' created successfully." % output_path
    except IOError, message:
        print message
        sys.exit(1)
    except OSError, message:
        print message
        sys.exit(1)
    except zipfile.BadZipfile, message:
        print message
        sys.exit(1)
    finally:
        zip_file.close()



def formatTime(x):
    minutes, seconds_rem = divmod(x, 60)
    if minutes >= 60:
        hours, minutes_rem = divmod(minutes, 60)
        return "%02d:%02d:%02d" % (hours, minutes_rem, seconds_rem)
    else:
        minutes, seconds_rem = divmod(x, 60)
        return "00:%02d:%02d" % (minutes, seconds_rem)

class MultiOrderedDict(OrderedDict):
    def __setitem__(self, key, value):
        if isinstance(value, list) and key in self:
            self[key].extend(value)
        else:
            super(OrderedDict, self).__setitem__(key, value)



if __name__ == "__main__":
    startTime = time.time()
    now = datetime.datetime.now()

    config = ConfigParser.RawConfigParser(dict_type=MultiOrderedDict)
    config.read(['config.ini'])

    username = getpass.getuser()

    sdeFiles = config.get('settings', 'sdeFiles')
    print "Preparing backup for % i item\n" % len(sdeFiles)
    sdeNames = config.get('settings', 'sdeNames')
    mainFolder = config.get('settings', 'mainFolder')
    mainFolder = ''.join(mainFolder)
    global table_list
    tables = config.get('settings','tables')
    sdeMainName = config.get('settings','mainName')
    nasMainFolder = config.get('settings','nasMainFolder')
    nasMainFolder = ''.join(nasMainFolder)

    for i in range(len(sdeFiles)):

        databaseConnection = os.path.join('Database Connections\\', sdeFiles[i])
        targetGDB = mainFolder + '\\' + sdeMainName[i] + '\\' + sdeNames[i]
        logPath = os.path.join(targetGDB, '\\_LOG')
        gdbName = sdeNames[i] + (now.strftime("_%d.%m.%Y.gdb"))
        targetName = os.path.join(targetGDB, gdbName)
        sdeNames_ = sdeNames[i]

        try:
            os.makedirs(logPath)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        try:
            os.makedirs(targetGDB)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
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

        print "\nRunning script for % s\n" % sdeNames[i]

        replicateDatabase(databaseConnection, targetName)

        target_folder = targetName
        zip_path =  targetGDB + ".zip"
        nasPath = nasMainFolder + '\\' + sdeMainName[i] + '\\' + sdeNames[i] + '\\'
        try:
            os.makedirs(nasPath)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        desktop_folder = nasPath + gdbName + ".zip"

        print "\nRunning archive script for % s\n" % sdeNames[i]

        zip_folder(target_folder,
                      desktop_folder)

        print "\nArchive progress is completed.\n"
