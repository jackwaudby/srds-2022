suppressPackageStartupMessages(library(ggplot2))
suppressPackageStartupMessages(library(readr))

setwd("../")

# jack's data
files = list.files(path = "./data/", pattern = "*.csv")
raw = data.frame()
for (file in files) {
  dat = suppressMessages(read_csv(file = paste0("./data/",file),col_names = TRUE))
  raw = rbind(raw,dat)
}

# completed jobs/ms
p1 = ggplot(data = raw, aes(x = a, y = completedJobPs/1000, group = algo, colour = algo )) +
  geom_line() + xlab("a") + ylab("completed jobs/ms") + theme_bw()
p1
ggsave("./graphics/completed-jobs-per-ms.png", p1, width = 8, height = 6, device = "png")

# lost jobs/ms
p2 = ggplot(data = raw, aes(x = a, y = lostJobsPs/1000, group = algo, colour = algo )) +
  geom_line() + xlab("a") + ylab("lost jobs/ms") + theme_bw()
p2
ggsave("./graphics/lost-jobs-per-ms.png", p2, width = 8, height = 6,device = "png")

# lost jobs/failure
p3 = ggplot(data = raw, aes(x = a, y = lostJobsPf, group = algo, colour = algo )) +
  geom_line() + xlab("a") + ylab("lost jobs/failure") + theme_bw()
p3
ggsave("./graphics/lost-jobs-per-failure.png", p3, width = 8, height = 6,device = "png")

# average operational commit groups/failure
p4 = ggplot(data = raw, aes(x = a, y = avOpCommitGroupsPf, group = algo, colour = algo )) +
  geom_line() + xlab("a") + ylab("average operational commit groups/failure") + theme_bw()
p4
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



