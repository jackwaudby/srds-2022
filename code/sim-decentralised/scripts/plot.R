suppressPackageStartupMessages(library(ggplot2))
suppressPackageStartupMessages(library(readr))

raw = read_csv(file = "results.csv",col_names = TRUE,show_col_types = FALSE)

p1 = ggplot(data = raw, aes(x = propLongTransactions, y = throughput)) +
  geom_line() + xlab("proportion of long running transactions") + ylab("throughput") + theme_bw()

ggsave("decentralised-thpt.png", p1, width = 8, height = 6, device = "png")