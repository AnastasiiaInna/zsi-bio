source.dir <- "~/zsi-bio/"
setwd(source.dir)

if(!require(ggplot2)) {
  install.packages("ggplot2"); require(ggplot2)}

if(!require(shiny)) {
  install.packages("shiny"); require(shiny)}

if(!require(flexclust)){
  install.packages("flexclust"); require("flexclust")
}

if(!require(dplyr)){
  install.packages("dplyr"); require("dplyr")
}

if(!require(plyr)){
  install.packages("plyr"); require("plyr")
}

if(!require(tidyr)){
  install.packages("tidyr"); require("tidyr")
}

if(!require(fpc)){
  install.packages("fpc"); require("fpc")
}

if(!require(clusterCrit)){
  install.packages("clusterCrit"); require("clusterCrit")
}

plotMap <- function (data){
  ui <- fluidPage(
    fluidRow(
      column(width = 12, class = "well",
             h4("Brush and double-click to zoom"),
             plotOutput("plot1", height = 900,
                        dblclick = "plot1_dblclick",
                        brush = brushOpts(
                          id = "plot1_brush",
                          resetOnNew = TRUE
                        )
             )
      )
    )
  )
  
  server <- function(input, output) {
    
    # -------------------------------------------------------------------
    # Single zoomable plot (on left)
    ranges <- reactiveValues(x = NULL, y = NULL)
    
    output$plot1 <- renderPlot({
      ggplot(data, aes(`PC1`, `PC2`, color = prediction)) +
        geom_point() +
        # geom_point(aes(size = 1), shape = 21) +
        geom_text(aes(label = Region), hjust=0, vjust=0) +
        coord_cartesian(xlim = ranges$x, ylim = ranges$y)
    })
    
    # When a double-click happens, check if there's a brush on the plot.
    # If so, zoom to the brush bounds; if not, reset the zoom.
    observeEvent(input$plot1_dblclick, {
      brush <- input$plot1_brush
      if (!is.null(brush)) {
        ranges$x <- c(brush$xmin, brush$xmax)
        ranges$y <- c(brush$ymin, brush$ymax)
        
      } else {
        ranges$x <- NULL
        ranges$y <- NULL
      }
    })
  }
  
  shinyApp(ui, server)
}

purity <- function(conf.matr, start.col = 1){
  corr <- 0
  for(i in start.col:ncol(conf.matr))
    corr <- corr + max(conf.matr[, i])  
  accuracy <- corr / sum(conf.matr) * 100
  return(accuracy)
}

# randIndex <- function(prediction, true.labels){
#   tp <- 0
#   tn <- 0
#   for(i in 1:(length(prediction) - 1))
#     for (j in (i + 1):length(prediction)){
#       if(i != j & prediction[i] == prediction[j] & true.labels[i] == true.labels[j])
#         tp <- tp + 1
#       if(i !=j & prediction[i] != prediction[j] & true.labels[i] != true.labels[j])
#         tn <- tn + 1
#     }
#   ri <- (tp + tn) / (length(prediction) * (length(prediction) - 1))
#   print(tp)
#   print(tn)
#   return(ri)
# }


isClustering <- 1
filename <- "class_chrMT_svm.csv" 
filename <- "cluster_kmeans_chr22.csv"
source.filename <- "~/IdeaProjects/Results/"
data.filename <- paste0(source.filename, filename)
data <- read.table(data.filename, sep = ",", header = T)

if (isClustering)  {
  data <- data[, c("Region", "SampleId", "features", "label", "Predict")]
}
data <- data[, c("Region", "pcaFeatures", "label", "prediction")]
colnames(data) <- c("Region", "features", "label", "prediction")

# --- feature convert --- #
out <- strsplit(as.character(data$features), ",")
features <- as.data.frame(do.call(rbind, out))
features[, 1] <- gsub("\\[|\\]", "", features[, 1])
features[, ncol(features)] <- gsub("\\[|\\]", "", features[, ncol(features)])
features <- as.data.frame(apply(features, 2, FUN = as.numeric))
pc.vector <- rep("PC", ncol(features))
col.names <- paste0(pc.vector, 1:ncol(features))
colnames(features) <- col.names
data <- cbind(data %>% select(-features), features)

summary(data)

if (isClustering)  {
  data["prediction"] <- data$Predict
}

data$Region <- as.factor(data$Region)
data$prediction <- as.factor(data$prediction)

target <- data$Region
prediction <- as.numeric(as.character(data$prediction))
individuals <- data$SampleID

summary(data)

(conf.matr <- table(target, prediction))

plotMap(data)
library(cluster)
clusplot(features, prediction, main='2D representation of the Cluster solution',
         color = TRUE, shade = TRUE, labels = 2, lines = 0)


# --- Clustering --- #

(conf.matr <- table(target, prediction))
purity(conf.matr) # purity
randIndex(prediction, target, correct = FALSE) # RI - from the package flexclust
calinhara(features, prediction) # CH index - from fpc
intCriteria(as.matrix(features), as.integer(prediction), c("C_index","Calinski_Harabasz","Dunn"))
extCriteria(as.integer(data$label), as.integer(prediction), "all")


# --- Classification --- #
(conf.matr <- table(data$label, target))
# target.labels <- (data[!duplicated(data[, c("Region", "label")]), c("Region", "label")])
# prediciton.labels <- (data[, c("prediction", "label")])
# prediction.targets <- join(prediciton.labels, target.labels, "inner",by = "label")
(conf.matr <- table(data$label, prediction))

n <- sum(conf.matr) # number of instances
nc <- nrow(conf.matr) # number of classes
diag <- diag(conf.matr) # number of correctly classified instances per class 
rowsums <- apply(conf.matr, 1, sum) # number of instances per class
colsums <- apply(conf.matr, 2, sum) # number of predictions per class
p <- rowsums / n # distribution of instances over the actual classes
q <- colsums / n # distribution of instances over the predicted classes

(accuracy <- round(sum(diag) / n * 100, 2))

# per-class metrics
precision <- round(diag / colsums * 100, 2)
recall <- round(diag / rowsums * 100, 2)
f1 <- round(2 * precision * recall / (precision + recall) , 2)
(per.class.metrics <- data.frame(precision, recall, f1) )

# macro-averaged metrics

macro.precision <- round(mean(precision), 2)
macro.recall <- round(mean(recall), 2)
macro.f1 <- round(mean(f1), 2)

(aveg.metrics <- data.frame(macro.precision, macro.recall, macro.f1))

# one-vs-all

oneVsAll <-function(i){
  v = c(conf.matr[i, i],
        rowsums[i] - conf.matr[i,i],
        colsums[i] - conf.matr[i,i],
        n - rowsums[i] - colsums[i] + conf.matr[i, i]);
  return(matrix(v, nrow = 2, byrow = T))}

(one.vs.all.matr <- lapply(1 : nc, oneVsAll ))

conf.matr.2.by.2 <- matrix(0, nrow = 2, ncol = 2)
for (i in 1 : nc){
  conf.matr.2.by.2 <- conf.matr.2.by.2 + one.vs.all.matr[[i]]}
(conf.matr.2.by.2)

# average accuracy
(avg.accuracy <- round(sum(diag(conf.matr.2.by.2)) / sum(conf.matr.2.by.2) * 100, 2))

# micro avg preciosion, recall, f1
(micro.prf <- round((diag(conf.matr.2.by.2) / apply(conf.matr.2.by.2, 1, sum))[1] * 100, 2))

# Evaluation on Highly Imbalanced Datasets 
# Majority-class Metrics 
mc.index <- which(rowsums == max(rowsums))[1] # majority-class index
mc.accuracy <- as.numeric(p[mc.index]) 
mc.recall <- 0 * p
mc.recall[mc.index] <- 1
mc.precision <- 0 * p 
mc.precision[mc.index] <- p[mc.index]
mc.f1 <- 0 * p 
mc.f1[mc.index] <- 2 * mc.precision[mc.index] / (mc.precision[mc.index] + 1)

mc.index

(mc.netrics <- round(data.frame(mc.recall, mc.precision, mc.f1) * 100, 2))
(target.labels <- (data[!duplicated(data[, c("Region", "label")]), c("Region", "label")]))

# random-guess metrics

(rg.accuracy <- 1 / nc * 100)
rg.precision <- p
rg.recall <- 0 * p + 1 / nc
rg.f1 <- 2 * p / (nc * p + 1)

(rg.metrics <- round(data.frame(rg.recall, rg.precision, rg.f1) * 100, 2))

# Kappa measure
exp.accuracy <- sum(p * q)
(kappa <- round((accuracy - exp.accuracy) / (1 - exp.accuracy),2))
