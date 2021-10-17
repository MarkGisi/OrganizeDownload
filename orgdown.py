#!/usr/bin/env python
import os
## import sys
import argparse
from pathlib import Path
from re import VERBOSE
from typing import Any
from tabulate import tabulate ## pip install tabulate
from csv import reader
import tarfile
from timeit import default_timer as timer
import time
from datetime import timedelta
from hurry.filesize import size, si  ## pretty print byte sizes (G=gig, M=meg, KB=kilobyte) # pip install hurry.filesize

_ONE_KB_            = 1024
_ONE_MEG_           = 1048576
_ONE_GIG_           = 1073741824
_BYTE_UNITS_        = _ONE_MEG_
_UNIT_DISPLAY_      = 'MB'

_DOWNLOAD_DIR_      = '__DOWNLOAD'
_CSV_FILE_          = '_FILE_DATA.csv'
_Of_str             = '_of_'

_FILENAME_          = 0
_FILE_SIZE_         = 1
_FILE_LOCATION_     = 2

_VERBOSE_MODE_      = False


def formatTime (time):
    time_components = str (time).split(':') ##  ['0', '00', '00.204294']
    seconds_microseconds = time_components[2].split('.') ##  ['00', '204294']
    if seconds_microseconds[0] == '00':
        seconds_str = '01'
    else:
        seconds_str = seconds_microseconds[0]

    return '{0}:{1}:{2}'.format (time_components[0], time_components[1], seconds_str)

def getByteSizeWithUnits (bytes, units, **options):
    size = bytes/units
    #### print (":", size)
    if size <= .005:
        result = round (size + .005, 3)
        ####print ("=", size, result)
    elif size <= .05:
        result = round (size + .05, 2)
        ####print ("=", size, result)
    elif size <= .5:
         result = round (size, 1)
    elif size <= 1.5:
        result = round (size, 1)
        ####print ("=", size, result)
    else:
        result = round (size)
        ####print ("=", size, result)

    if 'comma' in options:
        if options ['comma'] == True:
            result_str = f'{result:,}'
        else:
            result_str = str(result)
    
    if 'disply_units' in options:
         if options ['disply_units'] == True: 
             result_str = "{0} {1}".format(result_str, _UNIT_DISPLAY_)

    return result_str

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
        except Any as e:
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
        file_full_path = dir + '/' + file
        if os.path.isdir(file_full_path):
            continue
        file_count += 1
        file_size = os.path.getsize(file_full_path)
        total_size += file_size
        csv_row = file + ',' +  str(file_size) + ',' + file_full_path +  '\n'
        ####file_size_str = "(%s %s)"%(str(round (file_size/_BYTE_UNITS_,1)), _UNIT_DISPLAY_)
        file_size_str = '[{0}]'.format (getByteSizeWithUnits (file_size, _BYTE_UNITS_, comma=True, disply_units=True))
        ####file_size_str = str ( round (file_size /_BYTE_UNITS_, 1) ) + _UNIT_DISPLAY_ 
        ####print ( '   ' + str(file_count) + ': ' +  file + ' (' +  file_size_str%s + ')')
        print ( '   ' + str(file_count) + ': ' +  file + ' ' + file_size_str)
        file_handle.write(csv_row)
    file_handle.close()
    end_time = timer()
    total_size_str = getByteSizeWithUnits (total_size, _BYTE_UNITS_, comma=True, disply_units=True)
    time_taken_str = formatTime (timedelta(seconds= end_time - start_time))

    ####print ("tyep:", type(time_taken))
    print("---------------------------------------------------")
    print ("     # Files:", str(file_count))
    print ("        Size:", total_size_str)
    print ("        Time:", time_taken_str)
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
        print ()
        print ("  *** ERROR: csv file does not exist:", csv_file)
        print ("             Make sure to run the '-l' (--load) option first.")
        return
    
    ## clean out any previous existing .gz files e.g., WRCP-1_of_5.tar.gz,  WRCP-1_of_5.tar.gz
    files = os.listdir(working_dir)
    tar_gz_files = []
    for file in files:
        filename, file_extension = os.path.splitext(file)
        ## make sure existing .tar.gz file was created by this program before removing
        if os.path.isfile(working_dir + '/'+ file) and file_extension == '.gz' and _Of_str in filename:
            tar_gz_files.append (working_dir + '/'+ file)

    ## Check to make sure they are ok to delete existing achives
    if len (tar_gz_files) > 0:
        print()
        print ("   *** Do you want to DELETE previous created .tar.gz files in directory: ")
        print ("          ", working_dir)
        answer = input("       To DELETE please responded 'Yes': [Y/N] ")
        if answer.lower() == 'yes' or answer.lower() == 'y':
            print()
            for file in tar_gz_files:
                ## remove file
                print ("   *** Deleting:", os.path.basename(file))
                os.remove(file)
        else:
            exit()

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
        archive_filename = filename_string + "-" + str(i+1) + _Of_str + str(len(master_list)) + ".tar.gz" 
        archive_tar_gz_file = archive_dir + "/"+  _DOWNLOAD_DIR_ + "/" + archive_filename

        ## create directory
        print()
        print("Creating archive: " + archive_filename + " [%s]"%len(master_list[i]))
        print("----------------------------------------------------")
        tar = tarfile.open(archive_tar_gz_file, "w:gz")
        ## Report current time:
        
        if _VERBOSE_MODE_:
            t = time.localtime()
            print("Starting Time:", time.strftime("%H:%M:%S", t))
            print("-----------------------")
        ## time tar file creation
        start_time = timer()
        for k in range (len(master_list[i])):
            ####file_size = int ( int (master_list[i][k][_FILE_SIZE_]) / _BYTE_UNITS_ )
            file_size = int (master_list[i][k][_FILE_SIZE_])
            ##file_size_str = f'({file_size:,}'  + '%s)'%_UNIT_DISPLAY_ ## add ',' e.g., 1000 --> 1,000
            file_size_str = '[{0}]'.format (getByteSizeWithUnits (file_size, _BYTE_UNITS_, comma=True, disply_units=True))
            print ("   {0}: adding: {1} {2}".format(k+1, master_list[i][k][_FILENAME_], file_size_str))

            ####print ("   %s: adding:"%(k+1), master_list[i][k][_FILENAME_], file_size_str)
            tar.add(master_list[i][k][_FILE_LOCATION_])
        tar.close()
        print ()
        if _VERBOSE_MODE_:
            end_time = timer()
            lapse_time = end_time - start_time
            lapse_time_list.append (lapse_time)
            print("          Time:", formatTime (timedelta(seconds= lapse_time)))
            ####  timedelta(seconds= lapse_time).replace('.','')) ## remove microseconds
            lapse_times_count = len(lapse_time_list)
            ## Computer average time to prepare archive 
            sum = 0
            for i in lapse_time_list: sum += i
            print ("   Avg Time({0}): {1}".format (lapse_times_count, formatTime (timedelta(seconds= sum/lapse_times_count))))
            
            ### timedelta(seconds= sum/lapse_times_count).replace('.','')  )) .replace('.','') ## remove microseconds
            print ()
    return

def predictBuckets (csv_file, max_bytes):
    master_list = GroupFilesinBuckets (csv_file, max_bytes)

    num_buckets = len(master_list)

    if len(master_list) == 0:
        print ("  There are no files to archive")
        return
    buckets = []
    total_files = 0
    total_bytes = 0
    for i in range(len(master_list)):
        bucket_size = 0
        bucket_files = 0
        for k in range (len(master_list[i])):
            
            bucket_files += 1
            bucket_size += int(master_list[i][k][_FILE_SIZE_])
        the_size = int (bucket_size/_BYTE_UNITS_)
        ####the_size_str = f'{the_size:,}' ## add ',' e.g., 1000 --> 1,000
        the_size_str = getByteSizeWithUnits (bucket_size, _BYTE_UNITS_, comma=True, disply_units=True)
        ###the_size_str = '%s'%the_size
        ###the_size_str = f'{the_size_str:>5}'
        ##the_size_str = '{0: >10}'.format('%sM'%the_size)
        bucket_files_str = f'{bucket_files:,}'
        buckets.append (['File %s of %s'%(i+1, num_buckets), bucket_files_str, the_size_str])
        total_files += bucket_files
        total_bytes += bucket_size
    buckets.append (['----------------', '----------------', '----------------'])

    ####total_bytes_str = f'{int (total_bytes/_BYTE_UNITS_):,}' ## convert to byte units and add ',' e.g., 1000 --> 1,000
    total_files_str = f'{total_files:,}'
    total_bytes_str = getByteSizeWithUnits (total_bytes, _BYTE_UNITS_, comma=True, disply_units=True)
    buckets.append (['Archives: %s'%num_buckets, total_files_str , total_bytes_str])
    
    print()
    print(tabulate(buckets, headers=['Files (.tar.gz)', '# files','Size'], numalign="center",  stralign="center", tablefmt="presto"))
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

    parser.add_argument ("-u", "--display_units",  help="Display units: 'bytes (or b), 'kb', 'mb', 'gb'",\
                default='mb')

    parser.add_argument ("-v", "--verbose", help="verbose mode - display progress",\
                 action="store_true", default=False)

    args = parser.parse_args()

    global _VERBOSE_MODE_  ## need to declare global so assignment is accessable outside of def Main() 
    _VERBOSE_MODE_ = args.verbose

    global _BYTE_UNITS_
    global _UNIT_DISPLAY_
    units = args.display_units.lower() 
    if units == 'bytes' or units == 'b':
        _BYTE_UNITS_ = 1
        _UNIT_DISPLAY_ = 'Bytes'
    elif units == 'kb' or units == 'k':
        _BYTE_UNITS_ = _ONE_KB_
        _UNIT_DISPLAY_ = 'KB'
    elif units == 'mb' or units == 'm':
        _BYTE_UNITS_ = _ONE_MEG_
        _UNIT_DISPLAY_ = 'MB'
    elif units == 'gb' or units == 'g':
        _BYTE_UNITS_ = _ONE_GIG_
        _UNIT_DISPLAY_ = 'GB'
    else:
        print ()
        print ("  *** WARNING {0} is not a valid diplay unit type.".format(args.display_units))
        print ("      Options: 'bytes' (or 'b'), 'kb', 'mb' and 'gb' ")
        print ("      The system will default to 'mb'")
        input ("  Enter any key to continue")
    # not_done = True
    # while not_done:
    #     a = input ("bytes? : ")
    #     if a.lower() == 'q':
    #         not_done = False
    #     else:
    #         print ("|{}|".format (getByteSizeWithUnits (int(a), _BYTE_UNITS_, comma=True, disply_units=False)))
    # exit()
    # file_count = 0
    # ## file_handle = open('SourceCodeReceived.txt', "a")  # append mode
    # dir = './Disclosures'
    # with open('Disclosures.txt') as f:
    #     lines = [line.rstrip() for line in f]

    #     for file in lines:
    #         if os.path.isdir(file):
    #             continue
    #         file_count += 1
    #         cmd = 'touch "%s"/%s'%(dir, file)
    #         output = os.popen(cmd)
    #         a_list = output.readlines()
    #     exit()

    if args.examples == True:
        print ("Examples:")
        print (' You will need to run the utility first with the -l (load) option to create a db of files and data. Then')
        print (' run a second time with the -c (create) option. You can also run with both at the same time. See examples below.')
        print ('    python orgdown.py -l "\productline\2.1\source_code"')
        print ('    python orgdown.py -p "\productline\2.1\source_code"')
        print ('    python orgdown.py -c "\productline\2.1\source_code"  -m 200000 -n WRCP-21.07')
        print ('    python orgdown.py -l "\productline\2.1\source_code" -c same -m 500000 -n WRCP-21.07')
        print ('    python orgdown.py -v -u kb -p "\productline\2.1\source_code" -m 1450000000')
        exit()

    if args.predict != "":
        working_dir = args.predict + '/' + _DOWNLOAD_DIR_
        ## read directory data
        csv_file = working_dir + '/' + _CSV_FILE_
        if not os.path.exists(csv_file):
            print()
            print ("  *** ERROR: csv file does not exist:")
            print ("        %s"%csv_file)
            print ("      You need to preform the '-l' option first to generate the csv file")
            print()
        else:
            master_list = predictBuckets (csv_file, int(args.max_bytes.replace(',', '')))
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
            createDownloads (dir, dir, args.name, int(args.max_bytes.replace(',', '')))
        else:
            print()
            print ("  *** ERROR: incorrect number of arguments passed for option -c")
            print()
            exit()

   
    ## ----------------- Main End ----------------------------


if __name__ == '__main__':
    Main()