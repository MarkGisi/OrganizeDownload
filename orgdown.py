#!/usr/bin/env python
import os
## import sys
import argparse
from pathlib import Path
from tabulate import tabulate ## pip install tabulate
from csv import reader
import tarfile
from timeit import default_timer as timer
import time
from datetime import timedelta

_ONE_GIG_       = 1073741824
_DOWNLOAD_DIR_    = '__DOWNLOAD'
_CSV_FILE_        = '_FILE_DATA.csv'
_Of_str         = '_of_'

_FILENAME_        = 0
_FILE_SIZE_       = 1
_FILE_LOCATION_   = 2

def loadDownloadPkgs (dir):
    ## make download dir if it doesn't exist
    download_dir = dir + '/' + _DOWNLOAD_DIR_
    if not os.path.exists(download_dir):
        ## create directory
        print("Creating directory: '" + download_dir + "'")
        os.makedirs(download_dir)

    files = os.listdir(dir)
    files.sort()
    file_count = 0
    # table_rows = []
    # list_title = ['name', 'version', 'size', 'filename']
    csv_file = download_dir + '/' + _CSV_FILE_

    ## If csv file exists delete to create new one
    if os.path.isfile (csv_file):
        try:
            os.remove(csv_file)
        except PermissionError:
            print ("   ** ERROR - can't access:")
            print ("        ", csv_file)
            print ("      File being used by another program")
            exit(0)
        except Error as e:
            print ("unhandled error occured")
            print (e)
            exit(0)

    print ()
    print("---------------------------------------------------")
    print ("  Loading DB with file data (size, location)...")
    print("---------------------------------------------------")
    ## time loading
    start_time = timer()
    total_size = 0
    file_handle = open(csv_file, "a")  # append mode
    for file in files:
        if os.path.isdir(file):
            continue
        file_count += 1
        file_full_path = dir + '/' + file
        file_size = os.path.getsize(file_full_path)
        total_size += file_size
        csv_row = file + ',' +  str(file_size) + ',' + file_full_path +  '\n'
        print ( '   ' + str(file_count) + ': ' +  file + ' (' +  str(file_size) + ')')
        file_handle.write(csv_row)
    file_handle.close()
    end_time = timer()
    print("---------------------------------------------------")
    print ("     # Files:", str(file_count))
    print ("     # Bytes:", str(total_size))
    print ("        Time:", timedelta(seconds= end_time - start_time))
    print("---------------------------------------------------")
    print()

def GroupFilesinBuckets (csv_file, num_bytes):
 ## Group the files (one per row) into their respective archive buckets. 
    row_count = 0
    master_list = []
    with open(csv_file, 'r', encoding="utf8") as read_obj:
        csv_reader = reader(read_obj)
        # Iterate over each row in the csv using reader object
        row_count = 0
        total = 0
        i = 0
        list = []
        for csv_row in csv_reader:
            row_count +=1
            file_size = int(csv_row[_FILE_SIZE_])
            if (total + file_size) > num_bytes and total > 0:
                ### Start a new download file (list)
                master_list.append(list)
                list = []
                total = 0
            total += file_size
            list.append (csv_row)
        ## add last set of files to master. 
        if len (list) > 0:
            master_list.append(list)

    return master_list


def createDownloads (content_dir, archive_dir, filename_string, num_bytes):

    working_dir = content_dir + '/' + _DOWNLOAD_DIR_
    ## read directory data
    csv_file = working_dir + '/' + _CSV_FILE_

    if os.path.isfile(csv_file) == False:
        print ("  *** ERROR: csv file does not exist:", csv_file)
        print ("  ***        Make sure to run the '-l' (--load) option first.")
        return
    
    ## clean out any previous existing .gz files e.g., WRCP-1_of_5.tar.gz,  WRCP-1_of_5.tar.gz
    files = os.listdir(working_dir)
    ## Check to make sure they are ok to delete existing achives
    if len (files) > 0:
        answer = input("DELETE previous .tar.gz programs??? [Yes/No]")
        if answer.lower() != 'yes' and answer.lower() != 'y':
                exit()
    for file in files:
        filename, file_extension = os.path.splitext(file)
        ## make sure previously created file by this program before removing
        if os.path.isfile(working_dir + '/'+ file) and file_extension == '.gz' and _Of_str in filename:
            ## remove file
            os.remove(working_dir + '/'+ file)

    master_list = GroupFilesinBuckets (csv_file, num_bytes)

    if not os.path.isdir(archive_dir):
        ## create directory
        print("Creating directory: '" + archive_dir + "'")
        os.makedirs(archive_dir)

    if len(master_list) == 0:
        print ("  There are no files to archive")
        return
    lapse_time_list = []  ## keep track of time to archive each bucket to compute the average
    for i in range(len(master_list)):
        ## create archive file name: e.g., WRCP-1_of_5.tar.gz,  WRCP-2_of_5.tar.gz.
        archive_filename = filename_string + "-" + str(i+1+8) + _Of_str + str(len(master_list)+8) + ".tar.gz" 
        archive_tar_gz_file = archive_dir + "/"+  _DOWNLOAD_DIR_ + "/" + archive_filename

        ## create directory
        print()
        print("Creating archive: " + archive_filename + " [%s]"%len(master_list[i]))
        print("----------------------------------------------------")
        tar = tarfile.open(archive_tar_gz_file, "w:gz")
        ## Report current time:
        t = time.localtime() 
        print("Currrent Time:", time.strftime("%H:%M:%S", t))
        print("-----------------------")
        ## time tar file creation
        start_time = timer()
        for k in range (len(master_list[i])):
            print ("   %s: adding:"%(k+1), master_list[i][k][_FILENAME_])
            tar.add(master_list[i][k][_FILE_LOCATION_])
        tar.close()
        print ()
        end_time = timer()
        lapse_time = end_time - start_time
        lapse_time_list.append (lapse_time)
        print("          Time:", timedelta(seconds= lapse_time))
        lapse_times_count = len(lapse_time_list)
        ## Computer average time to prepare archive 
        sum = 0
        for i in lapse_time_list: sum += i
        print ("   Avg Time(%s): %s"%(lapse_times_count, timedelta(seconds= sum/lapse_times_count) ))
        print ()
    return

def predictBuckets (csv_file, max_bytes):
    master_list = GroupFilesinBuckets (csv_file, max_bytes)

    num_buckets = len(master_list)

    if len(master_list) == 0:
        print ("  There are no files to archive")
        return
    buckets = []
    for i in range(len(master_list)):
        bucket_size = 0
        bucket_files = 0
        for k in range (len(master_list[i])):
            bucket_size += int(master_list[i][k][_FILE_SIZE_])
            bucket_files += 1
        buckets.append (['File %s of %s'%(i+1+8, num_buckets+8), bucket_files, bucket_size])
    
    print()
    print(tabulate(buckets, headers=['Files (.tar.gz)', '# files','# bytes'], numalign="center", stralign="center", tablefmt="presto"))
    print()
    return

def Main ():
    parser = argparse.ArgumentParser()

    parser.add_argument ("-c", "--create", nargs="+",  help="Create download archives - parms: (content_dir)",\
                default=[])

    parser.add_argument ("-e", "--examples", help="Examples of how to use this utility",\
                action="store_true", default=False)

    parser.add_argument ("-d", "--display",  help="Display disclosures in Directory - pass in directory")

    parser.add_argument ("-l", "--load", help="Load disclosures in Directory - pass in directory",\
                default="")
    
    parser.add_argument ("-m", "--max_bytes",  help="Max number of bytes per download file",\
                default=_ONE_GIG_)
    
    parser.add_argument ("-n", "--name",  help="File name prefix. e.g., 'WRCP-21.07' Default is 'download_file'",\
                default="download_file")
    parser.add_argument ("-p", "--predict", help="Calculate how many download files for a given max number of bytes.",\
                default="")

    parser.add_argument ("-t", "--test", nargs="+", help="test fucntionality",\
                default=[])

    parser.add_argument ("-v", "--verbose", help="verbose mode - display progress",\
                 action="store_true", default=False)

    args = parser.parse_args()
    VERBOSE_MODE    = args.verbose


    if args.examples == True:
        print ("Examples:")
        print (' You will need to run the utility first with the -l (load) option to create a db of files and data. Then')
        print (' run a second time with the -c (create) option. You can also run with both at the same time. See examples below.')
        print ('    python create_download.py -l "\productline\2.1\source_code"')
        print ('    python create_download.py -p "\productline\2.1\source_code"')
        print ('    python create_download.py -c "\productline\2.1\source_code"  -m 200000 -n WRCP-21.07')
        print ('    python create_download.py -l "\productline\2.1\source_code" -c same -m 500000 -n WRCP-21.07')
        exit()

    if args.predict != "":
        working_dir = args.predict + '/' + _DOWNLOAD_DIR_
        ## read directory data
        csv_file = working_dir + '/' + _CSV_FILE_
        if not os.path.exists(csv_file):
            print ("   * ERROR: csv file does not exist:")
            print ("        %s"%csv_file)
            print ("     You need to preform the -l option first to generate the csv file")
        else:
            master_list = predictBuckets (csv_file, int(args.max_bytes))
        exit()


    if args.load != "":
        if args.load == 'same' and len (args.create) != 0:
            dir = args.create[0]
        else:
            dir = args.load
        if os.path.isdir (dir) == True:
            loadDownloadPkgs (dir)
        else:
            print ("  *** ERROR: -l option expects a directory")
            print ("      '%s' is NOT a directory"%dir)
            exit()

    if len (args.create) != 0:
        if len (args.create) == 1:
            ## first two are content_dir, out_put_dir
            if args.create[0] == 'same' and args.load != "":
                dir = args.load
            else:
                dir = args.create[0]
            createDownloads (dir, dir, args.name, int(args.max_bytes))
        else:
            print ("  *** ERROR: incorrect number of arguments passed for option -c")
            exit()

   
    ## ----------------- Main End ----------------------------


if __name__ == '__main__':
    Main()