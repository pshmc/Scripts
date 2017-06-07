library(RPostgreSQL)
library(dplyr)
library(tidyr)
library(data.table)
library(stringr)
library(stringi)
library(readr)
library(lubridate)
library(magrittr)
rm(list=ls()); gc(reset = TRUE)
setwd("/data/source")

  # Connecting to the POSTGRES dev Database
pgsrc <- 
  read.csv( 'creds.csv', stringsAsFactors = FALSE) %$%  
  src_postgres(
    host = 'datascience'
    , dbname = 'dev'
    , port = 5432
    , user = username
    , password = password
    , options="-c search_path=public"
  )

## postgres 
drv <- dbDriver("PostgreSQL")
# creates a connection to the postgres database
# note that "con" will be used later in each connection to the database
con <- read.csv( 'creds.csv', stringsAsFactors = FALSE ) %$%  
  dbConnect(drv, dbname = "dev",
                 host = "datascience", port = 5432,
                 user = username, password = password)

# helper function to check a folder for a file pattern via sftp
check_folder <- function( directory, file_pattern ) {
    fread( 'creds.csv' ) %$%
    paste0( ' -k -l sftp://' , username , ':' , password , "@datascience.psu.edu/", directory, "/" ) %>%
    system2( 'curl' , . , stdout = T ) %>% str_subset( file_pattern )
}


#### Consolidated Report ###################################################

yr <- year(Sys.Date() - 30) # get the year for last month date.  Report name should end with this
mon <- sprintf("%02d", month(Sys.Date() - 30))  # get the month for last month date.  Report name should end with this

## Reading from data/drop file (sftp)
drop_file <- check_folder("data/drop", paste(c("data.consolidated.(.*)", yr, mon, "csv"), collapse = "."))

## Reading from data/source file (sftp)
source_file <- check_folder("data/source", paste(c("data.consolidated.(.*)", yr, mon, "csv"), collapse = "."))

  # Historical Data
{
  
# old_reports <- fread( '/projects/creds.csv') %$%
#     paste0( 'curl -k sftp://' , username , ':' , password , "@datascience.psu.edu/data/source/" , files )
#   
# consolidated_list <- lapply(old_reports, fread)
# consolidated_list <- Map(function(dat) dat[, Comments := NULL], consolidated_list)
# 
# for( i in seq_along(consolidated_list) ) {
#   setnames(consolidated_list[[i]], names(consolidated_list[[i]]), c("Department", "CostCenterName", "Metric", "Acutal", "Budget",
#                                                     "Variance", "VariancePercent", "YTDAcutal", "YTDBudget", "YTDVariance", "YTDVariancePercent",
#                                                     "AnnualBudget", "RemainingBudget"))
# }
# 
# for(i in seq_along(consolidated_list)) {
#   consolidated_list[[i]][, Month := ymd(paste0(gsub("data.consolidated.report.(.*).csv$","\\1", files[i]),".01"))]
# }
# 
#   consolidated <- rbindlist(consolidated_list, fill = TRUE)
#   dbWriteTable(con, "ConsolidatedReportTable", consolidated)
  
}

  # Ongoing Data

if( length(source_file) == 0 ) {
  
  if( length(drop_file) != 0 ) {
    
    # moves drop file to local(source) folder to then be moved to source in next step
    fread( 'creds.csv') %$%
      paste0( '-k -O sftp://' , username , ':' , password ,  "@datascience.psu.edu/data/drop/"  , drop_file ) %>%
      system2('curl', .)
  
    # reads in report from local (source) folder
    consolidated <- fread(drop_file) 
    
    # drops comment field
    consolidated[, Comments := NULL]
    
    # set common names
    setnames(consolidated, names(consolidated), c("Department", "CostCenterName", "Metric", "Actual", "Budget", 
                                                  "Variance", "VariancePercent", "YTDAcutal", "YTDBudget", 
                                                  "YTDVariance", "YTDVariancePercent", "AnnualBudget", 
                                                  "RemainingBudget"))
    
    # includeds month field from report title
    consolidated[, Month := ymd(paste0(gsub("data.consolidated.report.(.*).csv$","\\1", drop_file),".01"))]
    
    # appends data to the Consolidated Report Table in database
    dbWriteTable(consolidated, "ConsolidatedReportTable", consolidated, append = TRUE)  
  
  }
}


#### Early-In Table ###########################################################

yr <- year(Sys.Date() - 30) # get the year for last month date.  Report name should end with this
mon <- sprintf("%02d", month(Sys.Date() - 30))  # get the month for last month date.  Report name should end with this

## Reading from data/drop file (sftp)
drop_file <- check_folder("data/drop", paste(c("data.earlyin(.*)", yr, mon, "csv"), collapse = "."))

## Reading from data/source file (sftp)
source_file <- check_folder("data/source", paste(c("data.earlyin(.*)", yr, mon, "csv"), collapse = "."))

  # Historical Data
{
  
  ## old reports before HR switched reporting systems
  
#   old_report_fileds <- c("EMPNUM", "EMPNAME", "Exception", "Type", "EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM",
#                         "MinutesEarly",	"CostCenter",	"CostCenterName",	"Position")
# 
#   old_reports <- c("data.earlyin.mg.2013.04.csv", "data.earlyin.mg.2013.05.csv",
#                    "data.earlyin.nursing.2012.12.csv", "data.earlyin.nursing.2013.04.csv",
#                    "data.earlyin.nursing.2013.05.csv")
# 
#   old_reports <- fread( '/projects/creds.csv') %$%
#     paste0( 'curl -k sftp://' , username , ':' , password , "@datascience.psu.edu/data/source/" , old_reports )
# 
# 
#   old_report_list <- lapply(old_reports, fread)
#   for( i  in seq_along(old_report_list) ) old_report_list[[i]][, c("ORGPATHTXT") := NULL]
#   for( i in seq_along(old_report_list) ) {
#     setnames(old_report_list[[i]], names(old_report_list[[i]])[1:10], old_report_fileds)
#   }
# 
#   old_report_dat <- rbindlist(old_report_list, fill = TRUE)
#   setnames(old_report_dat, "PAYRULENAME", "EmpType")
#   old_report_dat[, paste0(c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM"), "_UTC") := lapply(.SD, mdy_hm, tz = "America/New_York"), .SDcols = c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM")]
#   old_report_dat[, paste0(c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM"), "_UTC") := lapply(.SD, with_tz, tz = "UTC"), .SDcols = c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM")]
#   old_report_dat[, `:=` (EXCEPTIONSTARTDTM = NULL,
#                          EXCEPTIONSENDDTM = NULL,
#                          EMPNUM = as.character(EMPNUM),
#                          CostCenter = as.character(CostCenter))]
#   old_report_dat <- unique(old_report_dat)
# 
# 
#   ## new reporting system reports
# 
# new_reports <-
#     fread( '/projects/creds.csv' ) %$%
#     paste0( ' -k -l sftp://' , username , ':' , password , "@datascience.psu.edu/data/source/" ) %>%
#     system2( 'curl' , . , stdout = T ) %>% str_subset( "data.early." )
# old_reports <- c("data.earlyin.mg.2013.04.csv", "data.earlyin.mg.2013.05.csv",
#                    "data.earlyin.nursing.2012.12.csv", "data.earlyin.nursing.2013.04.csv",
#                    "data.earlyin.nursing.2013.05.csv")
# new_reports <- new_reports[!{new_reports %in% old_reports}]
# new_reports <- fread( '/projects/creds.csv') %$%
#     paste0( 'curl -k sftp://' , username , ':' , password , "@datascience.psu.edu/data/source/" , new_reports )
# 
# early_list <- lapply(new_reports, fread)
# for( i  in seq_along(early_list) ) early_list[[i]][, ORGPATHTXT := NULL]
# for( i in seq_along(early_list) ) {
#   setnames(early_list[[i]], names(early_list[[i]]), c("EMPNUM", "EMPNAME", "Exception", "Type", "PunchDate",
#                                                       "MinutesEarly", "CostCenter", "CostCenterName", "Position", "EmpType"))
# }
# 
# early <- rbindlist(early_list)
# early[grepl("/", substr(PunchDate, 2, 3)), PunchDate_UTC := mdy_hms(PunchDate, tz = "America/New_York")]
# early[!{grepl("/", substr(PunchDate, 2, 3))}, PunchDate_UTC := ymd_hms(PunchDate, tz = "America/New_York")]
# early[, PunchDate_UTC := with_tz(PunchDate_UTC, tz = "UTC")]
# early[, `:=` (PunchDate = NULL, CostCenter = as.character(CostCenter))]
# early <- unique(early)
# 
# 
# # creates similar names between old and new reports
# old_report_dat[, PunchDate_UTC := NA]
# old_report_dat[, PunchDate_UTC := as.POSIXct(PunchDate_UTC)]
# setcolorder(old_report_dat, c("EMPNUM", "EMPNAME", "Exception", "Type", "PunchDate_UTC",
#                               "MinutesEarly", "CostCenter", "CostCenterName", "Position", "EmpType",
#                               "EXCEPTIONSTARTDTM_UTC", "EXCEPTIONSENDDTM_UTC"))
# 
# early[, paste0(c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM"), "_UTC") := NA]
# early[, paste0(c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM"), "_UTC") := lapply(.SD, as.POSIXct), .SDcols = paste0(c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM"), "_UTC")]
# setcolorder(early, c("EMPNUM", "EMPNAME", "Exception", "Type", "PunchDate_UTC",
#                      "MinutesEarly", "CostCenter", "CostCenterName", "Position", "EmpType",
#                      "EXCEPTIONSTARTDTM_UTC", "EXCEPTIONSENDDTM_UTC"))
# 
# # combines into one report
# out <- rbindlist(list(copy(early), copy(old_report_dat)), use.names = TRUE, fill = TRUE)
# 
# dbWriteTable(con, "EarlyTable", out)

}

if( length(source_file) == 0 ) {
  
  if( length(drop_file) != 0 ) {
    
    # moves drop file to local(source) folder to then be moved to source in next step
    fread( 'creds.csv') %$%
      paste0( '-k -O sftp://' , username , ':' , password ,  "@datascience.psu.edu/data/drop/"  , drop_file ) %>%
      system2('curl', .)
    
    # from local (source) folder
    # reads in new files to a list
    early_list <- lapply(drop_file, fread)
    
    # removes ORGPATH field if it exists
    for( i  in seq_along(early_list) ) early_list[[i]][, ORGPATHTXT := NULL]
    
    # setnames for each file
    for( i in seq_along(early_list) ) {
     setnames(early_list[[i]], names(early_list[[i]]), c("EMPNUM", "EMPNAME", "Exception", "Type", "PunchDate",
                                                        "MinutesEarly", "CostCenter", "CostCenterName", "Position", "EmpType"))
    }
    
    # combines to one dataset
    early <- rbindlist(early_list) %>% unique
    
    # reformts to POSIXct
    early[grepl("/", substr(PunchDate, 2, 2)), PunchDate_UTC := mdy_hms(PunchDate, tz = "America/New_York")]
    
    # reformts to POSIXct
    early[!{grepl("/", substr(PunchDate, 2, 2))}, PunchDate_UTC := ymd_hms(PunchDate, tz = "America/New_York")]
    
    # Converts to UTC
    early[, PunchDate_UTC := with_tz(PunchDate_UTC, tz = "UTC")]
    early[, PunchDate := NULL]
  
    # adds dummy columns for dates from older version of the reports that are in the database
    early[, paste0(c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM"), "_UTC") := NA]
    early[, paste0(c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM"), "_UTC") := lapply(.SD, as.POSIXct), .SDcols = paste0(c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM"), "_UTC")]
    setcolorder(early, c("EMPNUM", "EMPNAME", "Exception", "Type", "PunchDate_UTC",
                       "MinutesEarly", "CostCenter", "CostCenterName", "Position", "EmpType",
                       "EXCEPTIONSTARTDTM_UTC", "EXCEPTIONSENDDTM_UTC"))
  
    dbWriteTable(con, "EarlyTable", early, append = TRUE)
  
  }
}



#### Late-Out Table ###########################################################

## Reading from data/source file (sftp)
yr <- year(Sys.Date() - 30) # get the year for last month date.  Report name should end with this
mon <- sprintf("%02d", month(Sys.Date() - 30))  # get the month for last month date.  Report name should end with this

## Reading from data/drop file (sftp)
drop_file <- check_folder("data/drop", paste(c("data.lateout(.*)", yr, mon, "csv"), collapse = "."))

## Reading from data/source file (sftp)
source_file <- check_folder("data/source", paste(c("data.lateout(.*)", yr, mon, "csv"), collapse = "."))

  # Historical Data
{
  
  ## old reports before HR switched reporting systems
  
#   old_report_fields <- c("EMPNUM", "EMPNAME", "Exception", "Type", "EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM",
#                         "Minuteslate",	"CostCenter",	"CostCenterName",	"Position")
# 
#   old_reports <- c("data.lateout.mg.2013.04.csv", "data.lateout.mg.2013.05.csv",
#                    "data.lateout.nursing.2012.12.csv", "data.lateout.nursing.2013.04.csv",
#                    "data.lateout.nursing.2013.05.csv")
# 
#   old_reports <- fread( '/projects/creds.csv') %$%
#     paste0( 'curl -k sftp://' , username , ':' , password , "@datascience.psu.edu/data/source/" , old_reports )
# 
# 
#   old_report_list <- lapply(old_reports, fread)
#   for( i  in seq_along(old_report_list) ) old_report_list[[i]][, c("ORGPATHTXT") := NULL]
#   for( i in seq_along(old_report_list) ) {
#     setnames(old_report_list[[i]], names(old_report_list[[i]])[1:10], old_report_fields)
#   }
# 
#   old_report_dat <- rbindlist(old_report_list, fill = TRUE)
#   setnames(old_report_dat, "PAYRULENAME", "EmpType")
#   old_report_dat[, paste0(c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM"), "_UTC") := lapply(.SD, mdy_hm, tz = "America/New_York"), .SDcols = c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM")]
#   old_report_dat[, paste0(c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM"), "_UTC") := lapply(.SD, with_tz, tz = "UTC"), .SDcols = c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM")]
#   old_report_dat[, `:=` (EXCEPTIONSTARTDTM = NULL,
#                          EXCEPTIONSENDDTM = NULL,
#                          EMPNUM = as.character(EMPNUM),
#                          CostCenter = as.character(CostCenter))]
#   old_report_dat <- unique(old_report_dat)
# 
# 
#   ## new reporting system reports
# 
# new_reports <-
#     fread( '/projects/creds.csv' ) %$%
#     paste0( ' -k -l sftp://' , username , ':' , password , "@datascience.psu.edu/data/source/" ) %>%
#     system2( 'curl' , . , stdout = T ) %>% str_subset( "data.late." )
# old_reports <- c("data.lateout.mg.2013.04.csv", "data.lateout.mg.2013.05.csv",
#                    "data.lateout.nursing.2012.12.csv", "data.lateout.nursing.2013.04.csv",
#                    "data.lateout.nursing.2013.05.csv")
# new_reports <- new_reports[!{new_reports %in% old_reports}]
# new_reports <- fread( '/projects/creds.csv') %$%
#     paste0( 'curl -k sftp://' , username , ':' , password , "@datascience.psu.edu/data/source/" , new_reports )
# 
# late_list <- lapply(new_reports, fread)
# for( i  in seq_along(late_list) ) late_list[[i]][, ORGPATHTXT := NULL]
# for( i in seq_along(late_list) ) {
#   setnames(late_list[[i]], names(late_list[[i]]), c("EMPNUM", "EMPNAME", "Exception", "Type", "PunchDate",
#                                                       "Minuteslate", "CostCenter", "CostCenterName", "Position", "EmpType"))
# }
# 
# late <- rbindlist(late_list)
# late[grepl("/", substr(PunchDate, 2, 3)), PunchDate_UTC := mdy_hms(PunchDate, tz = "America/New_York")]
# late[!{grepl("/", substr(PunchDate, 2, 3))}, PunchDate_UTC := ymd_hms(PunchDate, tz = "America/New_York")]
# late[, PunchDate_UTC := with_tz(PunchDate_UTC, tz = "UTC")]
# late[, `:=` (PunchDate = NULL, CostCenter = as.character(CostCenter))]
# late <- unique(late)
# 
# 
# # creates similar names between old and new reports
# old_report_dat[, PunchDate_UTC := NA]
# old_report_dat[, PunchDate_UTC := as.POSIXct(PunchDate_UTC)]
# setcolorder(old_report_dat, c("EMPNUM", "EMPNAME", "Exception", "Type", "PunchDate_UTC",
#                               "Minuteslate", "CostCenter", "CostCenterName", "Position", "EmpType",
#                               "EXCEPTIONSTARTDTM_UTC", "EXCEPTIONSENDDTM_UTC"))
# 
# late[, paste0(c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM"), "_UTC") := NA]
# late[, paste0(c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM"), "_UTC") := lapply(.SD, as.POSIXct), .SDcols = paste0(c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM"), "_UTC")]
# setcolorder(late, c("EMPNUM", "EMPNAME", "Exception", "Type", "PunchDate_UTC",
#                      "Minuteslate", "CostCenter", "CostCenterName", "Position", "EmpType",
#                      "EXCEPTIONSTARTDTM_UTC", "EXCEPTIONSENDDTM_UTC"))
# 
# # combines into one report
# out <- rbindlist(list(copy(late), copy(old_report_dat)), use.names = TRUE, fill = TRUE)
# 
# dbWriteTable(con, "LateTable", out)

}

if( length(source_file) == 0 ) {
  
  if( length(drop_file) != 0 ) {
    
    # moves drop file to local(source) folder to then be moved to source in next step
    fread( 'creds.csv') %$%
      paste0( '-k -O sftp://' , username , ':' , password ,  "@datascience.psu.edu/data/drop/"  , drop_file ) %>%
      system2('curl', .)
    
    # from local (source) folder
    # reads in new files to a list
    late_list <- lapply(drop_file, fread)
  
    # removes ORGPATH field if it exists
    for( i  in seq_along(late_list) ) late_list[[i]][, ORGPATHTXT := NULL]
    
    # setnames for each file
    for( i in seq_along(late_list) ) {
      setnames(late_list[[i]], names(late_list[[i]]), c("EMPNUM", "EMPNAME", "Exception", "Type", "PunchDate",
                                                        "Minuteslate", "CostCenter", "CostCenterName", "Position", "EmpType"))
    }

    # combines to one dataset
    late <- rbindlist(late_list)
    
    # reformts to POSIXct
    late[grepl("/", substr(PunchDate, 2, 2)), PunchDate_UTC := mdy_hms(PunchDate, tz = "America/New_York")]
  
    # reformts to POSIXct
    late[!{grepl("/", substr(PunchDate, 2, 2))}, PunchDate_UTC := ymd_hms(PunchDate, tz = "America/New_York")]
    
    # Converts to UTC
    late[, PunchDate_UTC := with_tz(PunchDate_UTC, tz = "UTC")]
    late[, PunchDate := NULL]
  
    # adds dummy columns for dates from older version of the reports that are in the database
    late[, paste0(c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM"), "_UTC") := NA]
    late[, paste0(c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM"), "_UTC") := lapply(.SD, as.POSIXct), .SDcols = paste0(c("EXCEPTIONSTARTDTM", "EXCEPTIONSENDDTM"), "_UTC")]
    setcolorder(late, c("EMPNUM", "EMPNAME", "Exception", "Type", "PunchDate_UTC",
                       "Minuteslate", "CostCenter", "CostCenterName", "Position", "EmpType",
                       "EXCEPTIONSTARTDTM_UTC", "EXCEPTIONSENDDTM_UTC"))
  
    dbWriteTable(con, "LateTable", late, append = TRUE)
  
  }
}


#### Detail Punch Table #########################################################################

# Historical Data
{

# # finding all csv DetailPunchFiles in PRODUserFiles
# files <-
#   fread( '/projects/creds.csv' ) %$%
#   paste0( ' -k -l sftp://' , username , ':' , password , "@datascience.psu.edu/hmcfolders/dstage/PROD/Data/PRODUserFiles/SmartSquare/" ) %>%
#   system2( 'curl' , . , stdout = T ) %>% str_subset( "DetailPunchOutFinal(.*).csv" )
# 
# # Making monthly chuncks of files to be used to read in in chunks then reduced
# file_list <- paste0(c(paste0("DetailPunchOutFinal.2015-", sprintf("%02d", 9:12)),
# paste0("DetailPunchOutFinal.2016-", sprintf("%02d", 1:12)),
# paste0("DetailPunchOutFinal.2017-", sprintf("%02d", 1:month(Sys.Date())))), "(.*).csv")
# 
# # getting full file name with curl command attached
# files <- fread( '/projects/creds.csv') %$%
#      paste0( 'curl -k sftp://' , username , ':' , password , "@datascience.psu.edu/hmcfolders/dstage/PROD/Data/PRODUserFiles/SmartSquare/" , files )
# 
# # chunking in of the files then combing and calling a unique function to get unique punches
# punch <- lapply(file_list, function(f) {
#   lapply(files[files %like% f], function(i){
#     fread(i, col.names = c("EENUM", "Name", "CostCenterIn", "UnitIn", "CostCenterOut", "UnitOut", "Date",
#                               "PunchIn", "PunchOut", "Hours", "PayTypeNum", "PayTypeLong", "PayTypeShort",
#                               "PPEND", "Interval", "JobCode"))
#   }) %>%
#     rbindlist %>%
#     unique
# }) %>%
#   rbindlist %>%
#   unique
# 
# 
#   punch$PunchIn_UTC <- ymd_hm(punch$PunchIn) %>% with_tz(.,tz = "America/New_York") %>% force_tz(.,tzone = "UTC")
#   punch$PunchOut_UTC <- ymd_hm(punch$PunchOut) %>% with_tz(.,tz = "America/New_York") %>% force_tz(.,tzone = "UTC")
#   punch <- punch %>% select(-PunchIn, -PunchOut)
#   
#   dbWriteTable(con, "DetailPunchTable", punch)
 
}

# finding all csv DetailPunchFiles in PRODUserFiles
files <-
  fread( 'creds.csv' ) %$%
  paste0( ' -k -l sftp://' , username , ':' , password , "@datascience.psu.edu/hmcfolders/dstage/PROD/Data/PRODUserFiles/SmartSquare/" ) %>%
  system2( 'curl' , . , stdout = T ) %>% str_subset( paste0("DetailPunchOutFinal.", Sys.Date()) )

# getting full file name with curl command attached
read_file <- fread( 'creds.csv') %$%
  paste0( 'curl -k sftp://' , username , ':' , password , "@datascience.psu.edu/hmcfolders/dstage/PROD/Data/PRODUserFiles/SmartSquare/" , files )

# moves drop file to local(source) folder to then be moved to source in next step
fread( 'creds.csv') %$%
  paste0( '-k -O sftp://' , username , ':' , password ,  "@datascience.psu.edu/hmcfolders/dstage/PROD/Data/PRODUserFiles/SmartSquare/"  , files ) %>%
  system2('curl', .)

# reading in the files then combing and calling a unique function to get unique punches
punch <- fread(read_file, col.names = c("EENUM", "Name", "CostCenterIn", "UnitIn", "CostCenterOut", "UnitOut", "Date",
                           "PunchIn", "PunchOut", "Hours", "PayTypeNum", "PayTypeLong", "PayTypeShort",
                           "PPEND", "Interval", "JobCode")) %>% unique


# updates punch date/time to UTC
punch$PunchIn_UTC <- ymd_hm(punch$PunchIn) %>% with_tz(.,tz = "America/New_York") %>% force_tz(.,tzone = "UTC")
punch$PunchOut_UTC <- ymd_hm(punch$PunchOut) %>% with_tz(.,tz = "America/New_York") %>% force_tz(.,tzone = "UTC")
punch <- punch %>% select(-PunchIn, -PunchOut)

# reads in Table for Detail punch combines new file and finds only unique rows
old <- dplyr::tbl(pgsrc, "DetailPunchTable") %>%
  select(-row.names) %>% 
  collect(n = Inf)

out <- rbind(old, punch) %>% unique

dbWriteTable(con, "DetailPunchTable", out, overwrite = TRUE)
  
  


## Visit Demographics ###############################

## Reading from data/source file (sftp)
yr <- year(Sys.Date()) # get the year for last month date.  Report name should end with this
mon <- sprintf("%02d", month(Sys.Date()))  # get the month for last month date.  Report name should end with this

## Reading from data/drop file (sftp)
drop_file <- check_folder("hmcfolders/clinicalinformatics/statisticians/Drop", paste0("data.visit.demographics", yr, "-", mon))

## Reading from data/source file (sftp)
source_file <- check_folder("data/source", paste0("data.visit.demographics", yr, "-", mon))

  # Historical Data
{
  
# visit <- lapply(files, fread) %>% 
#   rbindlist %>% 
#   unique %>%
#   setnames(., names(.), gsub("[[:space:]]|[[:punct:]]", "", names(.)))
# setnames(visit, c("FinancialNumber", "MRNOrganization", "AgeYearsVisit"), c("OOS", "MRN", "Age"))
# 
# visit[ , Age := as.numeric(Age)]
# visit[ , AdmitDateTime := as.POSIXct(ifelse(is.na(ymd_hms(AdmitDateTime, tz = "EST")), mdy_hms(AdmitDateTime, tz = "EST"), ymd_hms(AdmitDateTime, tz = "EST")), origin = "1970-01-01")]
# visit[ , DischargeDateTime := as.POSIXct(ifelse(is.na(ymd_hms(DischargeDateTime, tz = "EST")), mdy_hms(DischargeDateTime, tz = "EST"), ymd_hms(DischargeDateTime, tz = "EST")), origin = "1970-01-01")]
# setorderv(visit, c("MRN", "AdmitDateTime", "EncounterType"), c(1, 1, -1))
# setkeyv(visit, c("MRN", "AdmitDateTime"))
# dups <- visit[duplicated(visit, by = key(visit)), ] # checks for billing encounters and other duplicate records
# visit <- unique(visit, by = key(visit)) # removes billing encounters and other duplicate records
# 
# visit[, AdmitDateTime_UTC := with_tz(AdmitDateTime, tz = "UTC")]
# visit[, DischargeDateTime_UTC := with_tz(DischargeDateTime, tz = "UTC")]
# visit[, paste(c("AdmitDateTime", "DischargeDateTime")) := NULL]
# 
# dbWriteTable(con, "VisitDemographicsTable", visit)
  
}

if( length(source_file) == 0 ) {
  
  if( length(drop_file) != 0 ) {

    # moves drop file to local(source) folder to then be moved to source in next step
    fread( 'creds.csv') %$%
      paste0( '-k -O sftp://' , username , ':' , password ,  "@datascience.psu.edu/hmcfolders/clinicalinformatics/statisticians/Drop/"  , drop_file ) %>%
      system2('curl', .)
    
    # from local (source) folder
    # reads and combines visit files to one dataset and setnames
    visit <- lapply(drop_file, fread) %>% 
      rbindlist %>% 
      unique %>%
      setnames(., names(.), gsub("[[:space:]]|[[:punct:]]", "", names(.)))
    setnames(visit, c("FinancialNumber", "MRNOrganization", "AgeYearsVisit"), c("OOS", "MRN", "Age"))

    # makes age numeric
    visit[ , Age := as.numeric(Age)]
    
    # formats as POSIX
    visit[ , AdmitDateTime := as.POSIXct(ifelse(is.na(ymd_hms(AdmitDateTime, tz = "America/New_York")), mdy_hms(AdmitDateTime, tz = "EST"), ymd_hms(AdmitDateTime, tz = "EST")), origin = "1970-01-01")]
    visit[ , DischargeDateTime := as.POSIXct(ifelse(is.na(ymd_hms(DischargeDateTime, tz = "America/New_York")), mdy_hms(DischargeDateTime, tz = "EST"), ymd_hms(DischargeDateTime, tz = "EST")), origin = "1970-01-01")]
    setorderv(visit, c("MRN", "AdmitDateTime", "EncounterType"), c(1, 1, -1))
    setkeyv(visit, c("MRN", "AdmitDateTime"))
    dups <- visit[duplicated(visit, by = key(visit)), ] # checks for billing encounters and other duplicate records
    visit <- unique(visit, by = key(visit)) # removes billing encounters and other duplicate records

    # converts to UTC
    visit[, AdmitDateTime_UTC := with_tz(AdmitDateTime, tz = "UTC")]
    visit[, DischargeDateTime_UTC := with_tz(DischargeDateTime, tz = "UTC")]
    visit[, paste(c("AdmitDateTime", "DischargeDateTime")) := NULL]

    dbWriteTable(con, "VisitDemographicsTable", visit, append = TRUE)
  
  }
}



## Diagnosis List ###############################

## Reading from data/source file (sftp)
yr <- year(Sys.Date()) # get the year for last month date.  Report name should end with this
mon <- sprintf("%02d", month(Sys.Date()))  # get the month for last month date.  Report name should end with this

## Reading from data/drop file (sftp)
drop_file <- check_folder("hmcfolders/clinicalinformatics/statisticians/Drop", paste0("data.diagnosis.sequence_", yr, "-", mon))

## Reading from data/source file (sftp)
source_file <- check_folder("data/source", paste0("data.diagnosis.sequence_", yr, "-", mon))


# Historical Data
{

#   files <-
#     fread( '/projects/creds.csv' ) %$%
#     paste0( ' -k -l sftp://' , username , ':' , password , "@datascience.psu.edu/hmcfolders/clinicalinformatics/statisticians/Drop/" ) %>%
#     system2( 'curl' , . , stdout = T ) %>% str_subset( "data.diagnosis" )
#   
#   fread( '/projects/creds.csv') %$%
#     paste0( '-k -O sftp://' , username , ':' , password ,  "@datascience.psu.edu/hmcfolders/clinicalinformatics/statisticians/Drop/"  , files ) %>%
#     system2('curl', .)
#   
#   files %>%
#     lapply( function(x) {
#       system2( 'sed' , paste0( "'s/\\x00//g' -i  " , x ) ) })
# 
#   
# diag <- lapply(files[1:4], fread) %>% 
#     rbindlist %>% 
#     unique %>%
#     setnames(., names(.), gsub("[[:space:]]|[[:punct:]]", "", names(.)))
# setnames(diag2, c("FinancialNumber"), c("OOS"))
# diag[, ProblemFreeText := NULL]
# diag[, DischargeDateTime_UTC := NA]
# diag[, DischargeDateTime_UTC := as.POSIXct(DischargeDateTime_UTC)]
# 
# diag2 <- lapply(files[5:27], fread) %>% 
#   rbindlist %>% 
#   unique %>%
#   setnames(., names(.), gsub("[[:space:]]|[[:punct:]]", "", names(.)))
# setnames(diag2, c("FinancialNumber"), c("OOS"))
# 
# diag2[ , DischargeDateTime := as.POSIXct(ifelse(is.na(ymd_hms(DischargeDateTime, tz = "America/New_York")), mdy_hms(DischargeDateTime, tz = "EST"), ymd_hms(DischargeDateTime, tz = "EST")), origin = "1970-01-01")]
# diag2[, DischargeDateTime_UTC := with_tz(DischargeDateTime, tz = "UTC")]
# diag2[, paste(c("DischargeDateTime")) := NULL]
# 
# out <- rbind(diag, diag2, use.names = TRUE, fill = TRUE)
# 
# dbWriteTable(con, "DiagnosisListTable", out)

}


if( length(source_file) == 0 ) {
  
  if( length(drop_file) != 0 ) {
    
    # moves drop file to local(source) folder to then be moved to source in next step
    fread( 'creds.csv') %$%
      paste0( '-k -O sftp://' , username , ':' , password ,  "@datascience.psu.edu/hmcfolders/clinicalinformatics/statisticians/Drop/"  , drop_file ) %>%
      system2('curl', .)
    
    # from local (source) folder    
    diag <- lapply(drop_file, fread) %>% 
      rbindlist %>% 
      unique %>%
      setnames(., names(.), gsub("[[:space:]]|[[:punct:]]", "", names(.)))
    setnames(diag, c("FinancialNumber"), c("OOS"))

    diag[, DischargeDateTime := as.POSIXct(ifelse(is.na(ymd_hms(DischargeDateTime, tz = "America/New_York")), mdy_hms(DischargeDateTime, tz = "EST"), ymd_hms(DischargeDateTime, tz = "EST")), origin = "1970-01-01")]
    diag[, DischargeDateTime_UTC := with_tz(DischargeDateTime, tz = "UTC")]
    diag[, paste(c("DischargeDateTime")) := NULL]

    dbWriteTable(con, "DiagnosisListTable", diag, append = TRUE)
    
  }
}




## Medication List ###############################

## Reading from data/source file (sftp)
yr <- year(Sys.Date()) # get the year for last month date.  Report name should end with this
mon <- sprintf("%02d", month(Sys.Date()))  # get the month for last month date.  Report name should end with this

## Reading from data/drop file (sftp)
drop_file <- check_folder("hmcfolders/clinicalinformatics/statisticians/Drop", paste0("data.medlist", yr, "-", mon))

## Reading from data/source file (sftp)
source_file <- check_folder("data/source", paste0("data.medlist", yr, "-", mon))


  # Historic Data
{
  # drop_file <- check_folder("hmcfolders/clinicalinformatics/statisticians/Drop", paste0("data.medlist"))
  # 
  # # moves drop file to local folder to then be moved to source in next step
  # fread( '/projects/creds.csv') %$%
  #   paste0( '-k -O sftp://' , username , ':' , password ,  "@datascience.psu.edu/hmcfolders/clinicalinformatics/statisticians/Drop/"  , drop_file ) %>%
  #   system2('curl', .)
  # 
  # # move file to data/source folder
  # fread( '/projects/creds.csv') %$%
  #   paste0( 'curl -k -T ', drop_file,' sftp://' , username , ':' , password , '@datascience.psu.edu/data/source/' , drop_file ) %>%
  #   lapply(., system)
  # 
  # # gets the curl name of the report from the source folder to be fread in and placed in the database
  # medication_file <- fread( '/projects/creds.csv') %$%
  #   paste0( 'curl -k sftp://' , username , ':' , password , "@datascience.psu.edu/data/source/" , drop_file )
  # 
  # med <- lapply(medication_file, fread) %>% 
  #   rbindlist %>% 
  #   unique %>%
  #   setnames(., names(.), gsub("[[:space:]]|[[:punct:]]", "", names(.)))
  # setnames(med, c("FinancialNumber"), c("OOS"))
  # 
  # med[ , DischargeDateTime := as.POSIXct(ifelse(is.na(ymd_hms(DischargeDateTime, tz = "EST")), mdy_hms(DischargeDateTime, tz = "EST"), ymd_hms(DischargeDateTime, tz = "EST")), origin = "1970-01-01")]
  # med[, DischargeDateTime_UTC := with_tz(DischargeDateTime, tz = "UTC")]
  # med[, paste(c("DischargeDateTime")) := NULL]
  # 
  # dbWriteTable(con, "MedicationListTable", med, append = TRUE)
}


if( length(source_file) == 0 ) {
  
  if( length(drop_file) != 0 ) {
    
    # moves drop file to local(source) folder to then be moved to source in next step
    fread( 'creds.csv') %$%
      paste0( '-k -O sftp://' , username , ':' , password ,  "@datascience.psu.edu/hmcfolders/clinicalinformatics/statisticians/Drop/"  , drop_file ) %>%
      system2('curl', .)
    
    # from local (source) folder 
    med <- lapply(drop_file, fread) %>% 
      rbindlist %>% 
      unique %>%
      setnames(., names(.), gsub("[[:space:]]|[[:punct:]]", "", names(.)))
    setnames(med, c("FinancialNumber"), c("OOS"))

    med[ , DischargeDateTime := as.POSIXct(ifelse(is.na(ymd_hms(DischargeDateTime, tz = "America/New_York")), mdy_hms(DischargeDateTime, tz = "EST"), ymd_hms(DischargeDateTime, tz = "EST")), origin = "1970-01-01")]
    med[, DischargeDateTime_UTC := with_tz(DischargeDateTime, tz = "UTC")]
    med[, paste(c("DischargeDateTime")) := NULL]

    dbWriteTable(con, "MedicationListTable", med, append = TRUE)

  }
}




## DRG List ###############################


## Reading from data/source file (sftp)
yr <- year(Sys.Date()) # get the year for last month date.  Report name should end with this
mon <- sprintf("%02d", month(Sys.Date()))  # get the month for last month date.  Report name should end with this

## Reading from data/drop file (sftp)
drop_file <- check_folder("hmcfolders/clinicalinformatics/statisticians/Drop", paste0(c("data.drg", yr, mon, "csv"), collapse = "."))

## Reading from data/source file (sftp)
source_file <- check_folder("data/source", paste0(c("data.drg", yr, mon, "csv"), collapse = "."))


# Historic Data
{
  #     # moves drop file to local folder to then be moved to source in next step
  #     fread( '/projects/creds.csv') %$%
  #       paste0( '-k -O sftp://' , username , ':' , password ,  "@datascience.psu.edu/hmcfolders/clinicalinformatics/statisticians/Drop/"  , drop_file ) %>%
  #       system2('curl', .)
  #     
  #     # move file to data/source folder
  #     fread( '/projects/creds.csv') %$%
  #       paste0( 'curl -k -T ', drop_file,' sftp://' , username , ':' , password , '@datascience.psu.edu/data/source/' , drop_file ) %>%
  #       lapply(., system)
  #     
  #     # gets the curl name of the report from the source folder to be fread in and placed in the database
  #     report_name <- fread( '/projects/creds.csv') %$%
  #       paste0( 'curl -k sftp://' , username , ':' , password , "@datascience.psu.edu/data/source/" , drop_file )
  # 
  # 
  # drg_list <- lapply(drg_file, fread)
  # for( i in 1 : length(drg_list) ) drg_list[[i]][, `APR-DRG Name` := NULL]
  # for( i in 1 : length(drg_list) ) setcolorder(drg_list[[i]], c("Financial Number", "DRG Code", "DRG Weight", "APR-DRG Code", "Discharge Date", "Variable Cost"))
  # drg <- rbindlist(drg_list)
  # setnames(drg, names(drg), c("OOS", "DRGCode", "DRGWeight", "APRDRGCode", "DischargeDateTime", "VariableCost"))
  # drg <- drg[!is.na(OOS)]
  # 
  # drg[, DischargeDateTime := as.POSIXct(ifelse(!is.na(mdy(DischargeDateTime, tz = "EST")), 
  #                                              mdy(DischargeDateTime, tz = "EST"),
  #                                              ifelse(!is.na(mdy_hm(DischargeDateTime, tz = "EST")), 
  #                                                     mdy_hm(DischargeDateTime, tz = "EST"), 
  #                                                     ymd_hms(DischargeDateTime, tz = "EST"))), 
  #                                       origin = "1970-01-01")]
  # drg[, DischargeDateTime_UTC := with_tz(DischargeDateTime, tz = "UTC")]
  # drg[, paste(c("DischargeDateTime")) := NULL]
  # 
  # dbWriteTable(con, "DRGListTable", drg)
}



if( length(source_file) == 0 ) {
  
  if( length(drop_file) != 0 ) {
    
    # moves drop file to local(source) folder to then be moved to source in next step
    fread( 'creds.csv') %$%
      paste0( '-k -O sftp://' , username , ':' , password ,  "@datascience.psu.edu/hmcfolders/clinicalinformatics/statisticians/Drop/"  , drop_file ) %>%
      system2('curl', .)
    
    # from local (source) folder 
    # reads into a list
    drg_list <- lapply(drop_file, fread)

    # removes APR DRG column if it exists
    for( i in 1 : length(drg_list) ) drg_list[[i]][, `APR-DRG Name` := NULL]

    # sets field name order in table
    for( i in 1 : length(drg_list) ) setcolorder(drg_list[[i]], c("Financial Number", "DRG Code", "DRG Weight", "APR-DRG Code", "Discharge Date", "Variable Cost"))

    # combines to one dataset
    drg <- rbindlist(drg_list)

    # sets field names
    setnames(drg, names(drg), c("OOS", "DRGCode", "DRGWeight", "APRDRGCode", "DischargeDateTime", "VariableCost"))

    # removes any NA OOS rows
    drg <- drg[!is.na(OOS)]


    drg[, DischargeDateTime := as.POSIXct(ifelse(!is.na(mdy(DischargeDateTime, tz = "EST")), 
                                             mdy(DischargeDateTime, tz = "EST"),
                                             ifelse(!is.na(mdy_hm(DischargeDateTime, tz = "EST")), 
                                                    mdy_hm(DischargeDateTime, tz = "EST"), 
                                                    ymd_hms(DischargeDateTime, tz = "EST"))), 
                                      origin = "1970-01-01")]

    drg[, DischargeDateTime_UTC := with_tz(DischargeDateTime, tz = "UTC")]
    drg[, paste(c("DischargeDateTime")) := NULL]

    dbWriteTable(con, "DRGListTable", drg, append = TRUE)

  }
}





## HAI List ###############################

## Reading from data/source file (sftp)
yr <- year(Sys.Date()) # get the year for last month date.  Report name should end with this
mon <- sprintf("%02d", month(Sys.Date() - 30))  # get the month for last month date.  Report name should end with this

## Reading from data/drop file (sftp)
drop_file <- check_folder("hmcfolders/clinicalinformatics/statisticians/Drop", paste0("data.hai", yr, "-", mon))

## Reading from data/source file (sftp)
source_file <- check_folder("data/source", paste0(c("data.drg", yr, mon, "csv"), collapse = "."))


# Historic Data
{
# # moves drop file to local(source) folder to then be moved to source in next step
# fread( '/projects/creds.csv') %$%
#   paste0( '-k -O sftp://' , username , ':' , password ,  "@datascience.psu.edu/hmcfolders/clinicalinformatics/statisticians/Drop/"  , drop_file ) %>%
#   system2('curl', .)
# 
# # from local (source) folder 
# hai <- read_csv(drop_file)
# colnames(hai) <- gsub("[[:space:]]|[[:punct:]]", "", colnames(hai))
# hai <- hai[!{is.na(hai$Location)}, ]
# colnames(hai)[colnames(hai) %in% c("PatientID", "FinancialNumber", "HMCService", "Location")] <-  c("MRN", "OOS", "MedicalService", "NurseUnit")
# 
# hai %<>% mutate(AdmitDate = ymd_hms(AdmitDate),
#                 HAIYearMonth = ymd_hms(HAIYearMonth),
#                 EventDate = ymd_hms(EventDate))
# 
# dbWriteTable(con, "HAITable", hai, overwrite = TRUE)
}

if( length(source_file) == 0 ) {
  
  if( length(drop_file) != 0 ) {
    
    # moves drop file to local(source) folder to then be moved to source in next step
    fread( 'creds.csv') %$%
      paste0( '-k -O sftp://' , username , ':' , password ,  "@datascience.psu.edu/hmcfolders/clinicalinformatics/statisticians/Drop/"  , drop_file ) %>%
      system2('curl', .)
    
    # from local (source) folder 
    hai <- read_csv(drop_file)
    colnames(hai) <- gsub("[[:space:]]|[[:punct:]]", "", colnames(hai))
    hai <- hai[!{is.na(hai$Location)}, ]
    colnames(hai)[colnames(hai) %in% c("PatientID", "FinancialNumber", "HMCService", "Location")] <-  c("MRN", "OOS", "MedicalService", "NurseUnit")
    
    hai %<>% mutate(AdmitDate = ymd_hms(AdmitDate),
                    HAIYearMonth = ymd_hms(HAIYearMonth),
                    EventDate = ymd_hms(EventDate))
    
    dbWriteTable(con, "HAITable", hai, overwrite = TRUE)

  }
}



## Service/Location History ###############################


## Reading from data/source file (sftp)
yr <- year(Sys.Date()) # get the year for last month date.  Report name should end with this
mon <- sprintf("%02d", month(Sys.Date()))  # get the month for last month date.  Report name should end with this

## Reading from data/drop file (sftp)
drop_file <- check_folder("hmcfolders/clinicalinformatics/statisticians/Drop", paste0("data.service", yr, "-", mon))

## Reading from data/source file (sftp)
source_file <- check_folder("data/source", paste0(c("data.drg", yr, mon, "csv"), collapse = "."))


# Historic Data
{
  
# service_file <- check_folder("hmcfolders/clinicalinformatics/statisticians/Drop", "data.service")
#   
# service_file <- fread( '/projects/creds.csv') %$%
#     paste0( 'curl -k sftp://' , username , ':' , password , "@datascience.psu.edu/data/source/" , service_file )
# 
# 
# service <- lapply(service_file, fread) %>% 
#   rbindlist %>% 
#   unique %>%
#   setnames(., names(.), gsub("[[:space:]]|[[:punct:]]", "", names(.)))
# setnames(service, c("FinancialNumber"), c("OOS"))
# date_cols <- c("DischargeDateTime", "HCOELHBeginEffectiveDateTimeCerner", "HCOELHActiveStatusDateTime", 
#                "HCOELHLocationArrivalDateTime", "HCOELHLocationDepartDateTime", "AdmitDateTime")
# service[, paste(date_cols) := lapply(.SD, ymd_hms, tz = "EST"), .SDcols = date_cols]
# service[, paste0(date_cols, "_UTC") := lapply(.SD, with_tz, tz = "UTC"), .SDcols = date_cols]
# service[, paste(date_cols) := NULL]
# setorderv(service, c("OOS", "HCOELHBeginEffectiveDateTimeCerner_UTC"))
# 
# dbWriteTable(con, "ServiceHistoryTable", service, overwrite = TRUE)

}



if( length(source_file) == 0 ) {
  
  if( length(drop_file) != 0 ) {
    
    # moves drop file to local(source) folder to then be moved to source in next step
    fread( 'creds.csv') %$%
      paste0( '-k -O sftp://' , username , ':' , password ,  "@datascience.psu.edu/hmcfolders/clinicalinformatics/statisticians/Drop/"  , drop_file ) %>%
      system2('curl', .)
    
    # from local (source) folder 
    service <- lapply(drop_file, fread) %>% 
      rbindlist %>% 
      unique %>%
      setnames(., names(.), gsub("[[:space:]]|[[:punct:]]", "", names(.)))
    setnames(service, c("FinancialNumber"), c("OOS"))
    date_cols <- c("DischargeDateTime", "HCOELHBeginEffectiveDateTimeCerner", "HCOELHActiveStatusDateTime", 
                   "HCOELHLocationArrivalDateTime", "HCOELHLocationDepartDateTime", "AdmitDateTime")
    service[, paste(date_cols) := lapply(.SD, ymd_hms, tz = "EST"), .SDcols = date_cols]
    service[, paste0(date_cols, "_UTC") := lapply(.SD, with_tz, tz = "UTC"), .SDcols = date_cols]
    service[, paste(date_cols) := NULL]
    setorderv(service, c("OOS", "HCOELHBeginEffectiveDateTimeCerner_UTC"))

    dbWriteTable(con, "ServiceHistoryTable", service, append = TRUE)

  }
}


dbDisconnect(con)