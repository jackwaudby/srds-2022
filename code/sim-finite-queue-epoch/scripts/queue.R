failure = 157

q = suppressMessages(read_csv(file = "../../sim-finite-queue-epoch/queue.csv",col_names = F))
size=t(q[,3:length(q)])
epoch = seq(0,length(q)-3,by=1)
queueData = data.frame(epoch,size)

f = which(queueData$epoch == failure)
lf = f - 2
uf = f + 2
queueData[lf:uf,]

p1 = ggplot(data = queueData, aes(x = epoch, y = size )) +
  geom_line() + geom_vline(xintercept = f, linetype="dotted", color = "blue") + 
  xlab("epoch") + ylab("queue size") + theme_bw() 

rt = suppressMessages(read_csv(file = "../perEpochRespTime.csv",col_names = F))
rt = t(rt)
epoch = seq(0,length(rt)-1,by=1)
respData = data.frame(epoch,rt[,1])

f = which(respData$epoch == failure)
lf = f - 2
uf = f + 2
respData[lf:uf,]

p2 = ggplot(data = respData, aes(x = epoch, y = rt )) +
  geom_line() + geom_vline(xintercept = f, linetype="dotted", color = "blue") + 
  xlab("epoch") + ylab("av latency") + theme_bw() 

dat = suppressMessages(read_csv(file = "../perEpochArrivals.csv",col_names = F))
dat = t(dat)
epoch = seq(0,length(dat)-1,by=1)
arrData = data.frame(epoch,dat[,1])

f = which(arrData$epoch == failure)
lf = f - 2
uf = f + 2
arrData[lf:uf,]

p3 = ggplot(data = arrData, aes(x = epoch, y = dat )) +
  geom_line() + geom_vline(xintercept = f, linetype="dotted", color = "blue") + 
  xlab("epoch") + ylab("arrivals") + theme_bw() 
    
combined <- p1 + p2 + p3 & theme(legend.position = "top",text = element_text(size = 16)) 
g = combined + plot_layout(guides = "collect") 
g




