require(ggplot2)

args <- commandArgs(trailingOnly = TRUE)

prefix <- args[1]

latency <- read.table(paste("../logs/", prefix, "_Latency", sep=""))
colnames(latency)[1] <- "ServerId"
colnames(latency)[2] <- "Timestamp"
colnames(latency)[3] <- "Latency"

# latency <- latency[10:NROW(latency),]
print(summary(latency[latency$Timestamp > 200,]))

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
colnames(pending.requests)[3] <- "ServerId"
colnames(pending.requests)[4] <- "PendingRequests"

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

print(quantile(latency$Latency,c(0.5,0.99)))

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
	  facet_grid(ServerId ~ ClientId) +
	  ggtitle(paste(prefix, "Pending")) +
	  theme(text = element_text(size=15), 
	  		axis.text = element_text(size=20))
ggsave(p1, file=paste(prefix, "_pending.requests.pdf", sep=""), width=15)

p1 <- ggplot(latency.samples) + 
	  geom_point(aes(y=LatencySample, x=Timestamp, colour=ClientId), size=2) + 
	  facet_grid(ServerId ~ .) +
	  ggtitle(paste(prefix, "Latency Samples")) +
	  theme(text = element_text(size=15), 
	  		axis.text = element_text(size=20))
ggsave(p1, file=paste(prefix, "_latency.samples.pdf", sep=""), width=15)


rate <- read.table(paste("../logs/", prefix, "_Rate", sep=""))
colnames(rate)[1] <- "ClientId"
colnames(rate)[2] <- "Timestamp"
colnames(rate)[3] <- "ServerId"
colnames(rate)[4] <- "Rate"


p1 <- ggplot(rate) + 
	  geom_line(aes(y=Rate, x=Timestamp, colour=ClientId), size=1) + 
	  geom_point(aes(y=Rate, x=Timestamp, colour=ClientId), size=2) + 
	  facet_grid(ServerId ~ ClientId) +
	  ggtitle(paste(prefix, "rate")) +
	  theme(text = element_text(size=15), 
	  		axis.text = element_text(size=20))
ggsave(p1, file=paste(prefix, "_rate.pdf", sep=""), height=30, width=50, limitsize=FALSE)



tokens <- read.table(paste("../logs/", prefix, "_Tokens", sep=""))
colnames(tokens)[1] <- "ClientId"
colnames(tokens)[2] <- "Timestamp"
colnames(tokens)[3] <- "ServerId"
colnames(tokens)[4] <- "Tokens"


p1 <- ggplot(tokens) + 
	  geom_line(aes(y=Tokens, x=Timestamp, colour=ClientId), size=1) + 
	  geom_point(aes(y=Tokens, x=Timestamp, colour=ClientId), size=2) + 
	  facet_grid(ServerId ~ ClientId) +
	  ggtitle(paste(prefix, "tokens")) +
	  theme(text = element_text(size=15), 
	  		axis.text = element_text(size=20))
ggsave(p1, file=paste(prefix, "_tokens.pdf", sep=""), height=30, width=50, limitsize=FALSE)
