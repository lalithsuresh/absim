require(ggplot2)

args <- commandArgs(trailingOnly = TRUE)

latency <- read.table(paste("../logs/", "test", "_Latency", sep=""))
colnames(latency)[1] <- "ServerId"
colnames(latency)[2] <- "Timestamp"
colnames(latency)[3] <- "Latency"

act.mon <- read.table(paste("../logs/", "test", "_ActMon", sep=""))
colnames(act.mon)[1] <- "ServerId"
colnames(act.mon)[2] <- "Timestamp"
colnames(act.mon)[3] <- "ActiveRequests"

wait.mon <- read.table(paste("../logs/", "test", "_WaitMon", sep=""))
colnames(wait.mon)[1] <- "ServerId"
colnames(wait.mon)[2] <- "Timestamp"
colnames(wait.mon)[3] <- "WaitingRequests"

pending.requests <- read.table(paste("../logs/", "test", "_PendingRequests", sep=""))
colnames(pending.requests)[1] <- "ClientId"
colnames(pending.requests)[2] <- "Timestamp"
colnames(pending.requests)[3] <- "PendingRequests"

p1 <- ggplot(latency) + 
	  geom_line(aes(y=Latency, x=Timestamp), size=2) + 
	  facet_grid(ServerId ~ .) +
	  theme(text = element_text(size=15), 
	  		axis.text = element_text(size=20))
ggsave(p1, file="latency.pdf", width=15)

p1 <- ggplot(act.mon) + 
	  geom_line(aes(y=ActiveRequests, x=Timestamp), size=2) + 
	  facet_grid(ServerId ~ .) +
	  theme(text = element_text(size=15), 
	  		axis.text = element_text(size=20))
ggsave(p1, file="act.mon.pdf", width=15)

p1 <- ggplot(wait.mon) + 
	  geom_line(aes(y=WaitingRequests, x=Timestamp), size=2) + 
	  facet_grid(ServerId ~ .) +
	  theme(text = element_text(size=15), 
	  		axis.text = element_text(size=20))
ggsave(p1, file="wait.mon.pdf", width=15)

p1 <- ggplot(pending.requests) + 
	  geom_line(aes(y=PendingRequests, x=Timestamp), size=2) + 
	  facet_grid(ClientId ~ .) +
	  theme(text = element_text(size=15), 
	  		axis.text = element_text(size=20))
ggsave(p1, file="pending.requests.pdf", width=15)
