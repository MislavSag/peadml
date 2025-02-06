library(mlr3)

#' @title Logistic Weighted Return-to-Risk (Flexible Long or Long/Short)
#'
#' @description
#' This measure uses logistic transforms to create portfolio weights from predictions,
#' then computes the ratio of portfolio return over its standard deviation.
#'
#' Modes:
#' - "long": only invests in positive predictions
#' - "longshort": invests long in positives, short in negatives
#'
#' @format R6 class inheriting from mlr3::MeasureRegr
#' @export
LogisticWeightedReturnToRisk = R6::R6Class(
  "LogisticWeightedReturnToRisk",
  inherit = mlr3::MeasureRegr,
  public = list(
    
    #' @description
    #' Initialize the measure.
    #' @param alpha (`numeric(1)`): Steepness parameter for logistic transform.
    #' @param eps (`numeric(1)`): Small epsilon for numerical stability.
    #' @param mode (`character(1)`): One of c("long", "longshort").
    initialize = function(alpha = 1, eps = 1e-15, mode = "long") {
      # Store private fields
      private$.alpha = alpha
      private$.eps   = eps
      private$.mode  = match.arg(mode, c("long", "longshort"))
      
      # Call the superclass constructor
      super$initialize(
        id = "logistic_weighted_return_to_risk",
        predict_type = "response",
        # This measure is typically "higher is better"
        minimize = FALSE,
        range = c(-Inf, Inf)
      )
    }
  ),
  
  private = list(
    
    # The main scoring function
    .score = function(prediction, ...) {
      # We'll define two helper functions:
      
      # 1) long_only()
      long_only = function(truth, response, alpha, eps) {
        # Zero out non-positive predictions
        wpos = ifelse(response > 0, 1 / (1 + exp(-alpha * response)), 0)
        sum_pos = sum(wpos)
        if (sum_pos < eps) {
          # No valid weights => 0 measure
          return(0)
        }
        wpos = wpos / sum_pos
        # Weighted portfolio return
        port_return = sum(wpos * truth)
        # Weighted stdev
        var_p = sum(wpos * (truth - port_return)^2)
        sd_p = sqrt(var_p)
        # Return ratio
        if (sd_p < eps) {
          if (port_return > 0) return(Inf)
          else if (port_return < 0) return(-Inf)
          else return(0)
        }
        return(port_return / sd_p)
      }
      
      # 2) long_short()
      long_short = function(truth, response, alpha, eps) {
        # Positive => logistic => long weights
        wpos = ifelse(response > 0, 1 / (1 + exp(-alpha * response)), 0)
        # Negative => logistic on abs => short weights
        wneg = ifelse(response < 0, 1 / (1 + exp(-alpha * abs(response))), 0)
        
        # Normalize each side to sum to 1 (if they are nonzero)
        sum_pos = sum(wpos)
        sum_neg = sum(wneg)
        if (sum_pos > eps) wpos = wpos / sum_pos
        if (sum_neg > eps) wneg = wneg / sum_neg
        
        w_final = wpos - wneg   # net-zero weights
        
        # Portfolio return
        port_return = sum(w_final * truth)
        
        # --- Fix is here: weighted variance with absolute weights
        var_p = sum(abs(w_final) * (truth - port_return)^2)
        sd_p = sqrt(var_p)
        
        # Avoid division by zero
        if (sd_p < eps) {
          if (port_return > 0) {
            return(Inf)
          } else if (port_return < 0) {
            return(-Inf)
          } else {
            return(0)
          }
        }
        return(port_return / sd_p)
      }
      
      truth    = prediction$truth
      response = prediction$response
      
      # Depending on mode, call one or the other
      out = switch(
        private$.mode,
        "long"      = long_only(truth, response, private$.alpha, private$.eps),
        "longshort" = long_short(truth, response, private$.alpha, private$.eps)
      )
      return(out)
    },
    
    # Private fields
    .alpha = NULL,
    .eps   = NULL,
    .mode  = NULL
  )
)


# # Suppose we have 6 stocks with the following "true" realized returns:
# true_returns = c(0.02, -0.015, 0.03, 0.01, -0.02, 0.045)
# 
# # And our model predicted:
# pred_returns = c(0.03, -0.01, 0.025, -0.002, -0.03, 0.04)
# 
# # Make a PredictionRegr object by hand (bypassing a formal task/learner):
# prediction = mlr3::PredictionRegr$new(
#   row_ids   = seq_along(true_returns),
#   truth     = true_returns,
#   response  = pred_returns
# )
# 
# # Instantiate our custom measure, for example "long" mode:
# measure_long = LogisticWeightedReturnToRisk2$new(
#   alpha = 2,
#   mode = "long"
# )
# 
# # Evaluate measure:
# score_long = measure_long$score(prediction)
# cat("Score (long-only) =", score_long, "\n")
# 
# # Instantiate our custom measure, "longshort" mode:
# measure_ls = LogisticWeightedReturnToRisk2$new(
#   alpha = 2,
#   mode = "longshort"
# )
# 
# # Evaluate measure:
# score_ls = measure_ls$score(prediction)
# cat("Score (long–short) =", score_ls, "\n")
