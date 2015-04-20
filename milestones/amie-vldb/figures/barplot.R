source("~/workspace/mf/tools/mfplot.R")
library(rg)

rg.setDefaultOptions("pdf")
rg.options(width.mm=80*1.6, height.mm=37*1.6, ps=5, legend.ps=5, mar=c(3,3,0.7,0.7)+0.1, bty="o", axis.lwd=1, embedFonts=F)




		    # onlyOutput +MRL  +QWR 	+PR
yago2 	<- c( 3.23*60,	29.03,	38.16 ,	2.98*60) 
yago2.width 	<- c(0.5,0.5,0.5, 0.5)
yago2.space 	<- c(1,0,0,0)



yago2c 		<- c( 38.8,	8.9,	12.04, 36.48) 
yago2c.width	<- c(0.5,0.5,0.5, 0.5) 
yago2c.space 	<- c(1,0,0,0)

 
tooLong <- 5000
 
yago2s 		<- c( 24*60,	67,	72,	24*60) 
yago2s.width 	<- c(0.5,0.5,0.5, 0.5)
yago2s.space 	<- c(1,0,0,0)


 
db2 		<- c( 24*60,	63,	NA,	24*60) 
db2.width 	<- c(0.5,0.5,0.5, 0.5)
db2.space 	<- c(1,0,0,0)

db3 		<- c( 24,	6.16,	7.55,	15.1) 
db3.width 	<- c(0.5,0.5,0.5, 0.5)
db3.space 	<- c(1,0,0,0)

wd 		<- c( 24*60,	74,	664,	NA) 
wd.width 	<- c(0.5,0.5,0.5, 0.5)
wd.space 	<- c(1,0,0,0)

bars <- c(yago2,yago2c,yago2s,db2, db3, wd)

col=c(  "gray70", "gray45", "gray20", "black")
rg.startplot("individual.pdf")

# par(cex=1.45)

x <- rg.barplot(bars, width=c(yago2.width, yago2c.width, yago2s.width,db2.width, db3.width, wd.width),
		space=c(yago2.space, yago2c.space, yago2s.space, db2.space, db3.space, wd.space),
		beside=F,
		ylim=c(1,5000), log="y",
                ylab="Total wall-clock times (log scale)",                 
                col=col, xpd=F
                )
#                 text(2.4,3.5,"22.3x", pos=4, col="black")
#                 text(5.4,7,"13.8x", pos=4, col="black")
#                 text(8.4,120,"2.1x", pos=4, col="black")
#                 text(11.4,12,"2x", pos=4, col="black")
                
axis(1, at=c(1.5, 4, 6.5, 9, 11.5, 14),labels=c("Yago2(s)","Yago2-const(min)","Yago2s(min)","DBPedia 2.0(min)", "DBPedia 3.8(h)", "Wikidata(min)"), cex=1.3)
# axis(1, at=c(1.5, 4.5, 7.5, 10.5, 13.5, 16.5),labels=c("D1","D2","D3","D4", "D5", "D6"))
par(xpd=NA)
legend("topleft", c( "AMIE+(Out)", "AMIE+(Out+MRL)", "AMIE+(Out+QRW)", "AMIE+(Out+PR)"), fill=col, cex=1.2)
text(5.3,30*60,">1d", pos=4, col="black")
text(6.8,30*60,">1d", pos=4, col="black")

text(7.8,30*60,">1d", pos=4, col="black")
text(9.3,30*60,">1d", pos=4, col="black")
text(10.3,30,">1d", pos=4, col="black")

text(12.8,30*60,">1d", pos=4, col="black")
rg.endplot()