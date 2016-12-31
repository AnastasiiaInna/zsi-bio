source.dir <- "/home/anastasiia/IdeaProjects/"
setwd(source.dir)

if(!require(ggplot2)) {
  install.packages("ggplot2"); require(ggplot2)}

if(!require(shiny)) {
  install.packages("shiny"); require(shiny)}


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
      ggplot(data, aes(`PC1`, `PC2`, color = Predict)) +
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

purity <- function(conf.matr, start.col = 1)
{
  corr <- 0;
  for(i in start.col:ncol(conf.matr))
  {
    corr <- corr + max(conf.matr[, i])  
  }
  accuracy <- corr / sum(conf.matr) * 100
  return(accuracy)
}


# panel <- read.table("~/1000genomes/ALL.panel")
# summary(panel)

filename <- "gmm_chr22.csv/part-00000"
data.filename <- paste0(source.dir, filename)

data <- read.table(data.filename, sep = ",", header = T)
summary(data)

data$Region <- as.factor(data$Region)
data$Predict <- as.factor(data$Predict)

target <- data$Region
prediction <- as.numeric(as.character(data$Predict))
individuals <- data$SampleID

summary(data)

(conf.matr <- table(target, prediction))

plotMap(data)
purity(conf.matr)
  




