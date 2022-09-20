suppressPackageStartupMessages(library(ggplot2))
suppressPackageStartupMessages(library(readr))

## root dir is sim-<name>
setwd("../")

## load data
files = list.files(path = "./data/", pattern = "*.csv")
raw = data.frame()
for (file in files) {
  dat = suppressMessages(read_csv(file = paste0("./data/",file),col_names = TRUE))
  raw = rbind(raw,dat)
}

# completed jobs/ms
p1 = ggplot(data = raw, aes(x = a, y = completedJobPs/1000, group = algo, colour = algo )) +
  geom_line() + xlab("U") + ylab("completed transactions/ms") + theme_bw() +
  theme(text = element_text(size = 20)) + labs(color="protocol:") 
ggsave("./graphics/completed-jobs-per-ms.pdf", p1, width = 8, height = 6, device = "pdf")

# lost jobs/ms
p2 = ggplot(data = raw, aes(x = a, y = lostJobsPs/1000, group = algo, colour = algo )) +
  geom_line() + xlab("a") + ylab("lost jobs/ms") + theme_bw()
ggsave("./graphics/lost-jobs-per-ms.png", p2, width = 8, height = 6,device = "png")

# lost jobs/failure
p3 = ggplot(data = raw, aes(x = a, y = lostJobsPf, group = algo, colour = algo )) +
  geom_line() + xlab("a") + ylab("lost jobs/failure") + theme_bw()
ggsave("./graphics/lost-jobs-per-failure.png", p3, width = 8, height = 6,device = "png")

# average operational commit groups/failure
p4 = ggplot(data = raw, aes(x = a, y = avOpCommitGroupsPf, group = algo, colour = algo )) +
  geom_line() + xlab("a") + ylab("average operational commit groups/failure") + theme_bw()
ggsave("./graphics/av-op-commit-groups-per-failure.png", p4, width = 8, height = 6,device = "png")

# simulation time
p5 = ggplot(data = raw, aes(x = a, y = realTime, group = algo, colour = algo )) +
  geom_line() + xlab("a") + ylab("real simulation time (secs)") + theme_bw()
ggsave("./graphics/real-simulation-time.png", p5, width = 8, height = 6,device = "png")

# proportion of lost jobs saved
a = raw$a[which(raw$algo == "single")]
totalSavedJobs = raw$totalLostJobs[which(raw$algo == "single")] - raw$totalLostJobs[which(raw$algo == "multi")]
prop = totalSavedJobs/raw$totalLostJobs[which(raw$algo == "single")]
dat = data.frame(a,totalSavedJobs,prop)
p6 = ggplot(data = dat, aes(x = a, y = prop)) +
  geom_line() +  xlab("a") + ylab("multi-commit prop. jobs saved") + theme_bw()
ggsave("./graphics/prop-jobs-saved.png", p6, width = 8, height = 6,device = "png")

raw$avRespTime =raw$avRespTime * 1000
single = raw[which(raw$algo == "single"),]
multi = raw[which(raw$algo == "multi"),]

a = seq(40,120,by=10)
lb = c(c(35.10,40.15,45.21,50.28,55.36,60.45,65.55,70.66,75.78))
ub=c(214.67,203.34,208.85,220.35,234.70,250.64,267.56,285.15,303.21)
single <- single[order(single$a),]
single = single[-1,]
multi <- multi[order(multi$a),]
multi = multi[-1,]

U = rep(a,4)
resp = c(lb,ub,single$avRespTime,multi$avRespTime)
p = c(rep("Wd",9),rep("Wu",9),rep("single",9),rep("multi",9))
isi = data.frame(U,resp,p)
isi
p7 = ggplot(data = isi, aes(x = U, y = resp, group = p, colour = p )) +
  geom_line() +  
  xlab("U") + 
  ylab("average response time (ms)") + 
  theme_bw() +
  theme(text = element_text(size = 20)) + labs(color="") 

ggsave("./graphics/av-response-time.pdf", p7, width = 8, height = 6,device = "pdf")




dat = suppressMessages(read_csv(file = "./data/sim-finite-queue-epoch-single-30-queue.csv",col_names = F))

size=t(dat[,3:length(dat)])
epoch = seq(0,length(dat)-3,by=1)
d = data.frame(epoch,size)
p1 = ggplot(data = d, aes(x = epoch, y = size )) +
  geom_line() + xlab("epoch") + ylab("queue size") + theme_bw()
p1
