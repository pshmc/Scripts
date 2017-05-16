#' NRC Best Benchmarch Search for Heatmap
#' @author Kirk Gosik kdgosik@gmail.com
#' @LastUpdate 05/16/2017
#' @return Writes a csv file of the 
#' @import data.table 1.9.6
#' @import lubridate 1.6.0
#' @import maggrittr 1.5
#' @details
#' This script is used for reading in the Magnet Submission report data saved as CSV files.  The script will
#' read in the data files and combine them into a single dataset.  It will perform some transformations on the
#' data in order to count the optimal benchmark and categories to choose.  The dataset is then split into a 
#' list based off of each combination of 4 categories.  It then calculates the outperformance of each benchmark
#' for each unit and then ranks the overall performance.  It then selects the best performance combination and
#' benchmarks that were used for each unit and writes to a csv file.  This csv file then can be subsequently saved
#' as and xlsx file and a pivot table can be created to make the quarterly heatmap. 
#' 
#' In the pivot table:
#' Report Filter: Choose (filter this to TRUE)
#' Column Labels: fldCategory
#' Row Labels: Unit, fldQuestion, fldBenchmark
#' Values: Sum of Outperform



library(data.table)
library(lubridate)
library(magrittr)
rm(list = ls()); gc(reset = TRUE)

cfg.units <- c("3RD FLOOR SOUTH ADDITION", "3RD FLOOR WOMENS HEALTH", "4TH FLOOR SURGERY", "5TH FLOOR ACUTE CARE",
               "6TH FLOOR MED", "HEART & VASC PROGRESSIVE CARE", "MED IMC (6TH FLOOR)", "MICU","NEUROSCIENCES CRITICAL CARE UNIT",
               "PENN STATE HERSHEY CANCER INST-I/P UNIT", "PSHHVI CRITICAL CARE UNIT", "PSHMC Emergency Adult Overall",
               "PSHMC Emergency Pediatrics Overall", "SURGICAL ICU", "SURGICAL IMC", "7TH FLOOR NEONATAL ICU",
               "PEDIATRIC ACUTE", "PEDIATRIC ONCOLOGY", "PICU", "PIMCU", "PSHMC CGCAHPS Adult Overall", "PSHMC CGCAHPS Peds Overall")


Year <- year(Sys.Date() - 90) # getting the year of the of the quarter before the current date
Month <- sprintf("%02d", 3 * ceiling(month(Sys.Date() - 90) / 3)) # getting the starting month of the quarter before the current date
file_pattern <- paste0(c("data.nrc(.*)", Year, Month, "csv"), collapse = ".")
data_folder <- "../../Drop"

  ## reading in all benchmark csv files and combining to one dataset
dat <- lapply(dir(data_folder, pattern = file_pattern, full.names = TRUE), function( fl ) {
  tmp <- fread(fl)
  row_end <- which(tmp$fldCategory == "") - 1
  if(length(row_end) == 0) tmp
  else tmp[1 : row_end, ]
}) %>% rbindlist

  ## filtering to only magnet units
dat <- dat[!{fldUnit %like% "^xx|^_|^PSH |^PSHMG"}, ]
dat <- dat[fldUnit %in% cfg.units, ]

  ## striping out n size, line breaks and percent signs
dat[, fldScore := gsub("µ*\n(.*)","", fldScore)]
dat[, fldBenchmarkScore := gsub("µ*\n(.*)","", fldBenchmarkScore)]
dat[, fldAboveBenchmark := gsub("%", "", fldAboveBenchmark)]
dat[, fldAboveBenchmark := as.numeric(fldAboveBenchmark)/100]

  ## setting non-sensical benchmark performance to 0
dat[!{fldUnit %like% "PED|Ped|PICU|PIMCU|NEONATAL"} & fldBenchmark %like% "Children's Hosp Avg", fldAboveBenchmark := 0]
dat[!{fldUnit %like% "Emergency"} & fldBenchmark %like% "ER Volume", fldAboveBenchmark := 0]

  # creating missing unit, category, benchmark combinations to be consistent
missing_combos <- outer(unique(dat$fldUnit), outer(unique(dat$fldCategory), unique(dat$fldBenchmark), paste, sep = ";"), paste, sep = ";") %>%
  as.vector %>%
  strsplit(., ";") %>%
  do.call(rbind, .) %>% 
  as.data.table

setnames(missing_combos, names(missing_combos), c("fldUnit", "fldCategory", "fldBenchmark"))
setkeyv(missing_combos,c("fldUnit", "fldCategory", "fldBenchmark"))


  # identifies the top question by Unit, Category and Benchmark for Score Type of Mean
  # it then chains another data.table to reduce to the line of data that has the top question 
  # by Unit, Category and Benchmark
out_mean <- dat[fldScoreType == "Mean",                # filters to just Mean score type
                .SD[which.max(fldAboveBenchmark), ],   # keeps max row of each by statement combination
                by = .(fldUnit, fldCategory, fldBenchmark)][, .(fldAboveBenchmark = mean(fldAboveBenchmark)), 
                                                           by = .(fldUnit, fldCategory, fldQuestion, fldBenchmark)]

  # merging missing categories
  # sets question to 'No Question' and Outperformance to 0
setkeyv(out_mean, c("fldUnit", "fldCategory", "fldBenchmark"))
out_mean <- out_mean[missing_combos]
out_mean[is.na(fldQuestion), fldQuestion := "No Question"]
out_mean[is.na(fldAboveBenchmark), fldAboveBenchmark := 0]


  # creating nested list giving benchmarks scores by Unit and Categories, 
  # then casts dataset making each category a column
units <- unique(dat$fldUnit)
benchmarks <- unique(dat$fldBenchmark)
out_list <- lapply(units, function( unit ) {
  
  lapply(benchmarks, function( bench ) {
    
    dcast(out_mean[fldUnit %in% unit &
                     fldBenchmark %in% bench, ], fldUnit ~ fldCategory, max, value.var = "fldAboveBenchmark")
    
  })
  
})

  # naming each part of the lists
names(out_list) <- units
for( unit in units ) { 
  names(out_list[[unit]]) <- benchmarks 
}

  # Every combo of 4 categories
combos <- combn(unique(dat$fldCategory), 4)

  # calculating a score list containing the unit, the number categories outperformed, 
  # the best benchmark for that unit, and the categories looked at for that iteration
score_list <- list()
for( i in 1 : ncol(combos) ) {
  
  out <- matrix(0, nrow = length(units), ncol = 4)
  for(j in 1 : length(units) ) {
    
    tmp <- NULL
    for( k in 1 : length(benchmarks) ) {
      tmp[k] <- sum(out_list[[j]][[k]][,.SD, .SDcols = combos[,i]] > 0.5)
    }
    
    out[j, ] <- c(units[j], max(tmp), benchmarks[which.max(tmp)], paste0(combos[,i], collapse = "|"))
    
  }
  out <- as.data.frame(out)
  colnames(out) <- c("Unit", "CategoriesOutperform", "BestBenchmark", "Categories")
  out$CategoriesOutperform <- as.numeric(out$CategoriesOutperform)
  score_list[[i]] <- out

}

  # calculates all scores and picks the best
scores <- sapply(1 : ncol(combos), function(i) mean(score_list[[i]]$CategoriesOutperform >= 3))
pick <- which.max(scores)

#   # overwrites above and picks same has final designation
# combo_want <- paste0(c("Careful Listening|Patient Education|Responsiveness|Safety"), collapse = "|")
# pick <- Position(function(x) x$Categories == combo_want, score_list)

  # creates a data.table of picked score_list data
merge_dat <- as.data.table(score_list[[pick]])
setkeyv(merge_dat, "Unit")
setkeyv(out_mean, "fldUnit")

  # merges score_list data with output data
out_mean <- merge_dat[out_mean]
out_mean[, Outperform := round(fldAboveBenchmark * 8, 0)]
out_mean[Unit == "PSHMC CGCAHPS Adult Overall", BestBenchmark := "NRC Average"]
out_mean[Unit == "PSHMC CGCAHPS Peds Overall", BestBenchmark := "Children's Hosp Avg"]
out_mean[, Choose := fldBenchmark == BestBenchmark]

write.csv(out_mean, paste0("NRC_Heatmap_Update_", Sys.Date(), ".csv"), row.names = FALSE)