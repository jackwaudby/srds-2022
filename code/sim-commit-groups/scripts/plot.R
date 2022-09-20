suppressPackageStartupMessages(library(ggplot2))
suppressPackageStartupMessages(library(readr))
dat = suppressMessages(read_csv(file = "results.csv",col_names = TRUE))

p1 = ggplot(data = dat, aes(x = propDist, y = avCommitGroupsPerEpoch)) +
  geom_line() +
  geom_hline(yintercept=1, linetype="dashed", color = "black") +
  geom_vline(xintercept=0.1095, linetype="dashed", color = "red") +
  xlab("proportion of distributed transactions") +
  ylab("commit groups") +
  theme_bw() +
  theme(text = element_text(size = 20))

ggsave("./graphics/tpc-c.pdf", p1, width = 8, height = 6, device = "pdf")
