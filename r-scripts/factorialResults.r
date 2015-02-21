args <- commandArgs(trailingOnly = TRUE)

dir <- args[1]
prefix <- args[2]

latency <- read.table(paste("../", dir, "/", prefix, "_Latency", sep=""))
colnames(latency)[1] <- "ServerId"
colnames(latency)[2] <- "Timestamp"
colnames(latency)[3] <- "Latency"

subset <- latency[latency$Timestamp > 2000,]

options(width=10000)
print(c(dir, mean(subset$Latency), quantile(subset$Latency,c(0.5,0.95, 0.99, 0.999) ) ) )

write(c(dir, mean(subset$Latency), quantile(subset$Latency,c(0.5,0.95, 0.99, 0.999) ) )
	       , file = paste("../", dir, "/", "results", sep=""))
