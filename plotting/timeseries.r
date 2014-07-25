require(ggplot2)

args <- commandArgs(trailingOnly = TRUE)

prefix <- args[1]

latency <- read.table(paste("../logs/", prefix, "_Latency", sep=""))
colnames(latency)[1] <- "ServerId"
colnames(latency)[2] <- "Timestamp"
colnames(latency)[3] <- "Latency"

act.mon <- read.table(paste("../logs/", prefix, "_ActMon", sep=""))
colnames(act.mon)[1] <- "ServerId"
colnames(act.mon)[2] <- "Timestamp"
colnames(act.mon)[3] <- "ActiveRequests"

wait.mon <- read.table(paste("../logs/", prefix, "_WaitMon", sep=""))
colnames(wait.mon)[1] <- "ServerId"
colnames(wait.mon)[2] <- "Timestamp"
colnames(wait.mon)[3] <- "WaitingRequests"

pending.requests <- read.table(paste("../logs/", prefix, "_PendingRequests", sep=""))
colnames(pending.requests)[1] <- "ClientId"
colnames(pending.requests)[2] <- "Timestamp"
colnames(pending.requests)[3] <- "PendingRequests"

latency.samples <- read.table(paste("../logs/", prefix, "_LatencyTracker", sep=""))
colnames(latency.samples)[1] <- "ClientId"
colnames(latency.samples)[2] <- "Timestamp"
colnames(latency.samples)[3] <- "ServerId"
colnames(latency.samples)[4] <- "LatencySample"

p1 <- ggplot(latency) + 
	  geom_point(aes(y=Latency, x=Timestamp), size=4) + 
	  facet_grid(ServerId ~ .) +
	  ggtitle(paste(prefix, "Latency")) +
	  theme(text = element_text(size=15), 
	  		axis.text = element_text(size=20))
ggsave(p1, file=paste(prefix, "_latency.pdf", sep=""), width=15)

p1 <- ggplot(act.mon) + 
	  geom_line(aes(y=ActiveRequests, x=Timestamp), size=2) + 
	  facet_grid(ServerId ~ .) +
	  ggtitle(paste(prefix, "Act")) +
	  theme(text = element_text(size=15), 
	  		axis.text = element_text(size=20))
ggsave(p1, file=paste(prefix, "_act.mon.pdf", sep=""), width=15)

p1 <- ggplot(wait.mon) + 
	  geom_line(aes(y=WaitingRequests, x=Timestamp), size=2) + 
	  facet_grid(ServerId ~ .) +
	  ggtitle(paste(prefix, "Wait")) +
	  theme(text = element_text(size=15), 
	  		axis.text = element_text(size=20))
ggsave(p1, file=paste(prefix, "_wait.mon.pdf", sep=""), width=15)

p1 <- ggplot(pending.requests) + 
	  geom_point(aes(y=PendingRequests, x=Timestamp), size=4) + 
	  facet_grid(ClientId ~ .) +
	  ggtitle(paste(prefix, "Pending")) +
	  theme(text = element_text(size=15), 
	  		axis.text = element_text(size=20))
ggsave(p1, file=paste(prefix, "_pending.requests.pdf", sep=""), width=15)

p1 <- ggplot(latency.samples) + 
	  geom_point(aes(y=LatencySample, x=Timestamp), size=4) + 
	  facet_grid(ServerId ~ ClientId) +
	  ggtitle(paste(prefix, "Latency Samples")) +
	  theme(text = element_text(size=15), 
	  		axis.text = element_text(size=20))
ggsave(p1, file=paste(prefix, "_latency.samples.pdf", sep=""), width=15)
