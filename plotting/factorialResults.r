require(ggplot2)

args <- commandArgs(trailingOnly = TRUE)

prefix <- args[1]
dir <- args[2]

latency <- read.table(paste("../", dir, "/", prefix, "_Latency", sep=""))
colnames(latency)[1] <- "ServerId"
colnames(latency)[2] <- "Timestamp"
colnames(latency)[3] <- "Latency"

print(c(prefix, quantile(latency$Latency,c(0.5,0.95, 0.99))))
