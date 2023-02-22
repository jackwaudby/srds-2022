suppressPackageStartupMessages(library(ggplot2))
suppressPackageStartupMessages(library(readr))

raw = read_csv(file = "scripts/results.csv",col_names = TRUE)

p1 = ggplot(data = raw, aes(x = k, y = completedJobPs/2)) +
  geom_line() + xlab("proportion of long running transactions") + ylab("completed txn/s") + theme_bw()
p1
ggsave("test.png", p1, width = 8, height = 6, device = "png")


dev.off()
getwd()
