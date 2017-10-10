csv.data <- read.csv("~/Dropbox/BETACAR.csv")
library(car)
library(rgl)

scatterplot(Id - Base1lvl|type, boxplot=FALSE,span=0.75,csv.data=Id)