library(fs)
library(data.table)
library(mlr3verse)
library(mlr3batchmark)
library(batchtools)
library(arrow)
library(dplyr)
library(PerformanceAnalytics)
library(AzureStor)
library(future.apply)
library(matrixStats)
library(finautoml)


# SETUP ------------------------------------------------------------------
# creds
blob_key = Sys.getenv("BLOBKEY")
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)


# IMPORNAT PREDICTORS -----------------------------------------------------
# Get data from padobran
# scp -r padobran:/home/jmaric/peadml/gausscov_f1 /home/sn/data/strategies/pead/

# Import gausscov f1 data
files = list.files("/home/sn/data/strategies/pead/gausscov_f1", full.names = TRUE)
length(files)
tasks_ = gsub("gausscov_f1-|-\\d+\\.rds", "", basename(files))
tasks_unique = unique(tasks_)
gausscov_tasks = list()
for (i in seq_along(tasks_unique)) {
  task_ = tasks_unique[i]
  print(task_)
  files_ = files[which(tasks_ == task_)]
  gausscov_tasks[[i]] = unlist(lapply(files, function(x) {
    names(head(sort(readRDS(x), decreasing = TRUE), 5))
  }))
}
names(gausscov_tasks) = tasks_unique
best_vars = lapply(seq_along(gausscov_tasks), function(i) {
  x = gausscov_tasks[[i]]
  cbind.data.frame(task = names(gausscov_tasks)[i], 
                   as.data.table(head(sort(table(x), decreasing = TRUE), 10)))
})
best_vars = rbindlist(best_vars)

# Inspect best variables
tasks_unique
best_vars[task == "task_retStand5"]
best_vars[task == "task_retStand22"]
best_vars[task == "task_retStand44"]
best_vars[task == "task_retStand66"]
best_vars[task == "task_ret5Excess"]
best_vars[task == "task_ret22Excess"]
best_vars[task == "task_ret44Excess"]
best_vars[task == "task_ret66Excess"]
best_vars[task == "task_ret5Excess"]
best_vars[task == "task_ret22Excess"]
best_vars[task == "task_ret44Excess"]
best_vars[task == "task_ret66Excess"]
best_vars[task == "task_retExcessStand5"]
best_vars[task == "task_retExcessStand22"]
best_vars[task == "task_retExcessStand44"]
best_vars[task == "task_retExcessStand66"]
best_vars[, sum(N), by = x]


# PREDICTIONS -------------------------------------------------------------
# Get data from padobran
dir_save = "experiment"
dir_path = path("/home/sn/data/strategies/pead/", dir_save)
if (!dir_exists(dir_path)) dir_create(dir_path)
# dir_delete(dir_path)
# scp -r padobran:/home/jmaric/peadml/experiments_pead/algorithms /home/sn/data/strategies/pead/experiment
# scp -r padobran:/home/jmaric/peadml/experiments_pead/exports /home/sn/data/strategies/pead/experiment
# scp -r padobran:/home/jmaric/peadml/experiments_pead/problems /home/sn/data/strategies/pead/experiment
# scp -r padobran:/home/jmaric/peadml/experiments_pead/results /home/sn/data/strategies/pead/experiment
# scp -r padobran:/home/jmaric/peadml/experiments_pead/updates /home/sn/data/strategies/pead/experiment
# scp padobran:/home/jmaric/peadml/experiments_pead/registry.rds /home/sn/data/strategies/pead/experiment/registry.rds
# Doesnt work: # scp -r padobran:/home/jmaric/pread/experiments_pre/{algorithms,exports,problems,results,updates} /home/sn/data/strategies/pread/experiment

# load registry
reg = loadRegistry(dir_path, work.dir=dir_path)

# used memory
reg$status[!is.na(mem.used)]
reg$status[, max(mem.used, na.rm = TRUE)]

# done jobs
results_files = fs::path_ext_remove(fs::path_file(dir_ls(fs::path(dir_path, "results"))))
ids_done = findDone(reg=reg)
ids_done = ids_done[job.id %in% results_files]
ids_notdone = findNotDone(reg=reg)

# Errors in logs - NO ERRORS !

# import already saved predictions
# fs::dir_ls("predictions")
# predictions = readRDS("predictions/predictions-20231025215620.rds")

# # Try get results
# x = mlr3batchmark::reduceResultsBatchmark(
#   ids = 1:5,
#   store_backends = FALSE,
#   reg = reg
# )
# format(object.size(x), units = "auto")

# get results
tabs = batchtools::getJobTable(ids_done, reg = reg)[
  , c("job.id", "job.name", "repl", "prob.pars", "algo.pars"), with = FALSE]
predictions_meta = cbind.data.frame(
  id = tabs[, job.id],
  task = vapply(tabs$prob.pars, `[[`, character(1L), "task_id"),
  learner = gsub(".*regr.|.tuned", "", vapply(tabs$algo.pars, `[[`, character(1L), "learner_id")),
  cv = gsub("custom_|_.*", "", vapply(tabs$prob.pars, `[[`, character(1L), "resampling_id")),
  fold = gsub("custom_\\d+_", "", vapply(tabs$prob.pars, `[[`, character(1L), "resampling_id"))
)
predictions_l = lapply(unlist(ids_done), function(id_) {
  # id_ = 10035
  x = tryCatch({readRDS(fs::path(dir_path, "results", id_, ext = "rds"))},
               error = function(e) NULL)
  if (is.null(x)) {
    print(id_)
    return(NULL)
  }
  x["id"] = id_
  x
})
predictions = lapply(predictions_l, function(x) {
  cbind.data.frame(
    id = x$id,
    row_ids = x$prediction$test$row_ids,
    truth = x$prediction$test$truth,
    response = x$prediction$test$response
  )
})
predictions = rbindlist(predictions)
predictions = merge(predictions_meta, predictions, by = "id")
predictions = as.data.table(predictions)

# import tasks
tasks_files = dir_ls(fs::path(dir_path, "problems"))
tasks = lapply(tasks_files, readRDS)
names(tasks) = lapply(tasks, function(t) t$data$id)
tasks
tasks[[1]]$data$feature_names[grep("eps", tasks[[1]]$data$feature_names, ignore.case = TRUE)]

# backends
predictions[, unique(task)]
get_backend = function(task_name = "task_ret5") {
  # task_name = "task_ret5"
  task_ = tasks[names(tasks) == task_name][[1]]
  back_ = task_$data$backend
  cols_ = setdiff(back_$colnames, task_$data$feature_names)
  back_$data(rows = back_$rownames, cols = cols_)
  back_ = back_$data(rows = back_$rownames, cols = cols_)
  return(back_)
}
id_cols = c("symbol", "date", "month", "..row_id", "epsDiff", "nincr",
            "nincr2y", "nincr3y", paste0("ret_", c("5", "22", "44", "66")))
task_ret5_ = get_backend()
task_ret22_ = get_backend("task_ret22")
task_ret44_ = get_backend("task_ret44")
task_ret66_ = get_backend("task_ret66")
backend = Reduce(function(x, y) merge(x, y, by = c("symbol", "date", "..row_id"), all.x = TRUE, all.y = FALSE),
                 list(task_ret5_, task_ret22_, task_ret44_, task_ret66_))
setnames(backend, "..row_id", "row_ids")

# measures
mlr_measures$add("linex", finautoml::Linex)
mlr_measures$add("adjloss2", finautoml::AdjLoss2)

# merge backs and predictions
predictions = backend[predictions, on = c("row_ids")]
predictions[, date := as.Date(date)]
setnames(predictions,
         c("task_names", "learner_names", "cv_names"),
         c("task", "learner", "cv"),
         skip_absent = TRUE)


# PREDICTIONS RESULTS -----------------------------------------------------
# remove dupliactes - keep firt
predictions = unique(predictions, by = c("row_ids", "date", "task", "learner", "cv"))

# predictions
sign01 = function(x) {
  ifelse(x > 0, 1, 0)
}
predictions[, `:=`(
  truth_sign = as.factor(sign01(truth)),
  response_sign = as.factor(sign01(response))
)]

# Remove na value
predictions_dt = na.omit(predictions)

# number of predictions by task and cv
unique(predictions_dt, by = c("task", "learner", "cv", "row_ids"))[, .N, by = c("task")]
unique(predictions_dt, by = c("task", "learner", "cv", "row_ids"))[, .N, by = c("task", "cv")]

# Classification measures across ids
measures = function(t, res) {
  list(acc   = mlr3measures::acc(t, res),
       fbeta = mlr3measures::fbeta(t, res, positive = "1"),
       tpr   = mlr3measures::tpr(t, res, positive = "1"),
       tnr   = mlr3measures::tnr(t, res, positive = "1"))
}
predictions_dt[, measures(truth_sign, response_sign), by = c("cv")]
predictions_dt[, measures(truth_sign, response_sign), by = c("task")]
predictions_dt[, measures(truth_sign, response_sign), by = c("learner")]
predictions_dt[, measures(truth_sign, response_sign), by = c("cv", "task")]
predictions_dt[, measures(truth_sign, response_sign), by = c("cv", "learner")]
predictions_dt[, measures(truth_sign, response_sign), by = c("task", "learner")][order(fbeta)][tnr > 0.4][tpr > 0.4]
# predictions[, measures(truth_sign, response_sign), by = c("cv", "task", "learner")][order(V1)]

# create truth factor
predictions_dt[, truth_sign := as.factor(sign(truth))]

# prediction to wide format
predictions_wide = dcast(
  predictions_dt,
  task + symbol + date + truth + truth_sign ~ learner,
  value.var = "response"
)

# ensambles
cols = colnames(predictions_wide)
cols = cols[which(cols == "glmnet"):ncol(predictions_wide)]
p = predictions_wide[, ..cols]
pm = as.matrix(p)
predictions_wide = cbind(predictions_wide, mean_resp = rowMeans(p, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, median_resp = rowMedians(pm, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, sum_resp = rowSums2(pm, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, iqrs_resp = rowIQRs(pm, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, sd_resp = rowMads(pm, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, q9_resp = rowQuantiles(pm, probs = 0.9, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, max_resp = rowMaxs(pm, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, min_resp = rowMins(pm, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, all_buy = rowAlls(pm >= 0, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, all_sell = rowAlls(pm < 0, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, sum_buy = rowSums2(pm >= 0, na.rm = TRUE))
predictions_wide = cbind(predictions_wide, sum_sell = rowSums2(pm < 0, na.rm = TRUE))
predictions_wide = na.omit(predictions_wide)

# results by ensamble statistics for classification measures
calculate_measures = function(t, res) {
  is_na_ = which(is.na(t))
  if (length(is_na_) > 0) {
    res = res[-is_na_]
    t = t[-is_na_]
  }
  list(
    acc       = mlr3measures::acc(t, res),
    fbeta     = mlr3measures::fbeta(t, res, positive = "1"),
    tpr       = mlr3measures::tpr(t, res, positive = "1"),
    precision = mlr3measures::precision(t, res, positive = "1"),
    tnr       = mlr3measures::tnr(t, res, positive = "1"),
    npv       = mlr3measures::npv(t, res, positive = "1")
  )
}
cols_ens = c("task", "symbol", "date", "truth", "mean_resp", "truth_sign",
             "median_resp", "sum_resp", "q9_resp", "max_resp", "min_resp")
predictions_wide_ens = predictions_wide[, ..cols_ens]
predictions_wide_ens = na.omit(predictions_wide_ens)
predictions_wide_ens[, truth_sign := as.factor(sign01(as.integer(as.character(truth_sign))))]
# predictions_wide_ens[, truth := as.factor(sign01(as.integer(as.character(truth))))]

predictions_wide_ens[, calculate_measures(truth_sign, as.factor(sign01(mean_resp))), by = task]
predictions_wide_ens[, calculate_measures(truth_sign, as.factor(sign01(median_resp))), by = task]
predictions_wide_ens[, calculate_measures(truth_sign, as.factor(sign01(sum_resp))), by = task]
predictions_wide_ens[, calculate_measures(truth_sign, as.factor(sign01(max_resp + min_resp))), by = task]
predictions_wide_ens[, calculate_measures(truth_sign, as.factor(sign01(q9_resp))), by = task]

# Performance by returns
cols = colnames(predictions_wide)
cols = cols[which(cols == "earth"):which(cols == "sum_resp")]
cols = c("task", "symbol", "date", "truth", cols)
predictions_long = melt(na.omit(predictions_wide[, ..cols]), id.vars = c("task", "symbol", "date", "truth"))
setorder(predictions_long, task, variable, date)
predictions_long = predictions_long[value > 0]
predictions_long[, weights := 1 / nrow(.SD), by = c("task", "variable", "date")]
portfolios = predictions_long[, sum(truth * weights), by = c("task", "variable", "date")]
portfolios[, mean(V1), by = .(task, variable)][order(V1)]
portfolios = dcast(portfolios[, .(task, variable, date, value = V1)], date + task ~ variable)
charts.PerformanceSummary(as.xts.data.table(portfolios[task == "task_ret22", .SD, .SDcols = -c("task")]))
charts.PerformanceSummary(as.xts.data.table(portfolios)["2020/2023"])
charts.PerformanceSummary(as.xts.data.table(portfolios)["2024"])
SharpeRatio.annualized(as.xts.data.table(portfolios)["2020/"])
SharpeRatio(as.xts.data.table(portfolios))

# Save to azure for QC backtest
cont = storage_container(BLOBENDPOINT, "qc-backtest")
file_name_ =  paste0("pead_qc.csv")
qc_data = unique(predictions_wide, by = c("task", "symbol", "date"))
# qc_data = na.omit(qc_data)
setorder(qc_data, task, date)
qc_data[, .(min_date = min(date), max_date = max(date))]
data.table::last(qc_data[, .(task, symbol, date, truth, ranger)], 100)
storage_write_csv(qc_data, cont, file_name_)


# SYSTEMIC RISK -----------------------------------------------------------
# import SPY data
spy = open_dataset('/home/sn/lean/data/stocks_daily.csv', format = 'csv') %>% 
  dplyr::filter(Symbol == 'spy') %>% 
  collect()
setDT(spy)
spy = spy[, .(date = Date, close = `Adj Close`)]
spy[, returns := close / shift(close) - 1]
spy = na.omit(spy)
plot(spy[, close])

# systemic risk
predictions_wide[, unique(task)]
indicator = predictions_wide[task == "task_retStand66", .(
  indicator = mean(mean_resp, na.rm = TRUE),
  indicator_sd = sd(mean_resp, na.rm = TRUE),
  indicator_q1 = quantile(mean_resp, probs = 0.01, na.rm = TRUE)
),
by = date][order(date)]
cols = colnames(indicator)[2:ncol(indicator)]
indicator[, (cols) := lapply(.SD, nafill, type = "locf"), .SDcols = cols]
indicator[, `:=`(
  indicator_ema = TTR::EMA(indicator, 5, na.rm = TRUE),
  indicator_sd_ema = TTR::EMA(indicator_sd, 5, na.rm = TRUE),
  indicator_q1_ema = TTR::EMA(indicator_q1, 5, na.rm = TRUE)
)]
indicator = na.omit(indicator)
# plot(as.xts.data.table(indicator)[, 4])
# plot(as.xts.data.table(indicator)[, 5])
# plot(as.xts.data.table(indicator)[, 6])

# create backtest data
backtest_data =  merge(spy, indicator, by = "date", all.x = TRUE, all.y = FALSE)
min_date = indicator[, min(date)]
backtest_data = backtest_data[date > min_date]
max_date = indicator[, max(date)]
backtest_data = backtest_data[date < max_date]
cols = colnames(backtest_data)[4:ncol(backtest_data)]
backtest_data[, (cols) := lapply(.SD, nafill, type = "locf"), .SDcols = cols]
backtest_data[, signal := 1]
backtest_data[shift(indicator_ema) < 0, signal := 0]
# backtest_data[shift(indicator_sd_ema) < 4, signal := 0]
backtest_data_xts = as.xts.data.table(backtest_data[, .(date, benchmark = returns, strategy = ifelse(signal == 0, 0, returns * signal * 1))])
charts.PerformanceSummary(backtest_data_xts)
# backtest performance
Performance <- function(x) {
  cumRetx = Return.cumulative(x)
  annRetx = Return.annualized(x, scale=252)
  sharpex = SharpeRatio.annualized(x, scale=252)
  winpctx = length(x[x > 0])/length(x[x != 0])
  annSDx = sd.annualized(x, scale=252)
  
  DDs <- findDrawdowns(x)
  maxDDx = min(DDs$return)
  # maxLx = max(DDs$length)
  
  Perf = c(cumRetx, annRetx, sharpex, winpctx, annSDx, maxDDx) # , maxLx)
  names(Perf) = c("Cumulative Return", "Annual Return","Annualized Sharpe Ratio",
                  "Win %", "Annualized Volatility", "Maximum Drawdown") # "Max Length Drawdown")
  return(Perf)
}
Performance(backtest_data_xts[, 1])
Performance(backtest_data_xts[, 2])

# analyse indicator
library(forecast)
ndiffs(as.xts.data.table(indicator)[, 1])
plot(diff(as.xts.data.table(indicator)[, 1]))
