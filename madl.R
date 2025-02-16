#' @title Mean Absolute Directional Loss (MADL) https://arxiv.org/pdf/2309.10546
#'
#' @description
#' This measure implements the Mean Absolute Directional Loss (MADL) for evaluating forecasting models
#' in algorithmic trading strategies. For each observation, it computes:
#'
#' \deqn{\text{loss}_i = -\operatorname{sign}(R_i \times \hat{R}_i) \times |R_i|,}
#'
#' and returns the mean loss over all observations.
#'
#' Lower values are better since the loss function is designed to be minimized.
#'
#' @format R6 class inheriting from mlr3::MeasureRegr.
#' @export
MeanAbsoluteDirectionalLoss = R6::R6Class(
  "MeanAbsoluteDirectionalLoss",
  inherit = mlr3::MeasureRegr,
  public = list(
    
    #' @description
    #' Initialize the MADL measure.
    initialize = function() {
      super$initialize(
        id = "mean_absolute_directional_loss",
        predict_type = "response",
        minimize = TRUE,
        range = c(-Inf, Inf)
      )
    }
  ),
  
  private = list(
    
    # The main scoring function
    .score = function(prediction, ...) {
      truth    = prediction$truth
      response = prediction$response
      
      # Compute the loss for each observation:
      # loss_i = (-1) * sign(truth * response) * abs(truth)
      loss_terms = (-1) * sign(truth * response) * abs(truth)
      
      # Compute the mean loss (MADL)
      madl = mean(loss_terms)
      return(madl)
    }
  )
)
# 
# ## Example usage:
# # Suppose we have 6 stocks with the following "true" realized returns:
# true_returns = c(0.02, -0.015, 0.03, 0.01, -0.02, 0.045)
# 
# # And our model predicted:
# pred_returns = c(0.03, -0.01, 0.025, -0.002, -0.03, 0.04)
# 
# # Create a PredictionRegr object (bypassing a formal task/learner)
# prediction = mlr3::PredictionRegr$new(
#   row_ids  = seq_along(true_returns),
#   truth    = true_returns,
#   response = pred_returns
# )
# 
# # Instantiate our custom MADL measure
# madl_measure = MeanAbsoluteDirectionalLoss$new()
# 
# # Evaluate the measure
# score = madl_measure$score(prediction)
# cat("MADL Loss:", score, "\n")
