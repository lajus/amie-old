 
# source("~/workspace/mf/tools/mfplot.R")

library(rg)

log.axis = function (side, limits, exp.format = FALSE, grid = TRUE, grid.lty = "solid",
    grid.col = "lightgray", grid.lwd = 1, major = TRUE, minor = TRUE, 
    label.minor = FALSE, minor.cex = 0.75, major.cex = 1, ...)
{
    if (!side %in% 1:4)
        stop("Side must be 1, 2, 3, or 4.")
    if ((side == 1 || side == 3) & !par("xlog"))
        stop("x-axis must be on a log scale")
    if ((side == 2 || side == 4) & !par("ylog"))
        stop("y-axis must be on a log scale")
    if (missing(limits)) {
        if (side == 1 || side == 3)
            limits <- par("usr")[1:2]
        else limits <- par("usr")[3:4]
        limits <- 10^limits
    }
    tics <- log.tics(limits, exact10 = FALSE)
    maj <- tics$major.tics
    min <- tics$minor.tics
    min <- min[!min %in% maj]
    min.exp <- log10(min)
    maj.exp <- log10(maj)
    if (major) {
        par(tcl = -0.5)
        if (exp.format)
            axis(side, maj, labels = F, ##parse(text = paste("10^", maj.exp)),
                 cex.axis = major.cex, ...)
        else axis(side, maj, labels = formatC(maj, format = "fg"),
            cex.axis = major.cex, ...)
        if (grid) {
            if (side == 1 || side == 3)
                abline(v = maj, col = grid.col, lty = grid.lty,
                  lwd = grid.lwd)
            else abline(h = maj, col = grid.col, lty = grid.lty,
                lwd = grid.lwd)
        }
    }
    if (minor) {
        par(tcl = -0.25)
        if (label.minor) {
            if (exp.format)
                axis(side, min, labels = F, ## parse(text = paste("10^", min.exp)),
                     cex.axis = minor.cex, ...)
            else axis(side, min, labels = formatC(min, format = "fg"),
                cex.axis = minor.cex, ...)
        }
        else axis(side, min, labels = F, ...)
        if (grid) {
            if (side == 1 || side == 3)
                abline(v = min, col = grid.col, lty = grid.lty,
                  lwd = grid.lwd)
            else abline(h = min, col = grid.col, lty = grid.lty,
                lwd = grid.lwd)
        }
        par(tcl = -0.5)
    }
}

log.tics = function (x, exact10 = TRUE)
{
    x <- as.numeric(x)
    xlim <- range(x, na.rm = TRUE)
    if (any(x <= 0)) {
        warning("Some x data <= zero. Setting to NA.")
        x[x <= 0] <- NA
        ok <- complete.cases(x)
        x <- x[ok]
        xlim <- range(x, na.rm = T)
    }
    x2 <- ifelse(xlim[2] <= 0.1, trunc(log10(xlim[2]) - 1e-04),
        trunc(log10(xlim[2]) + 0.9999))
    x1 <- ifelse(xlim[1] >= 10, trunc(log10(xlim[1])), trunc(log10(xlim[1]) -
        0.9999))
    if (!exact10) {
        xlim[1] <- 10^x1 * trunc(xlim[1]/10^x1)
        xlim[2] <- 10^(x2 - 1) * ceiling(xlim[2]/10^(x2 - 1))
    }
    else {
        xlim <- c(10^x1, 10^x2)
    }
    tics.min <- c()
    tics.maj <- c()
    if (10^x1 >= xlim[1])
        tics.maj <- as.numeric(formatC(10^x1, format = "fg"))
    for (i in x1:(x2 - 1)) {
        if (10^(i + 1) <= xlim[2])
            tics.maj <- c(tics.maj, as.numeric(formatC(10^(i +
                1), format = "fg")))
        f <- ifelse(i == x1, xlim[1]/10^x1, 1)
        e <- ifelse(i == (x2 - 1), xlim[2], 10^(i + 1) - 10^i)
        tics.min <- c(tics.min, seq(f * 10^i, e, by = 10^i))
    }
    list(true.range = range(x), tic.range = range(tics.min),
        minor.tics = tics.min, major.tics = tics.maj)
}

rg.setDefaultOptions("pdf")
rg.options(width.mm=40*1.8, height.mm=37*1.8, ps=7, legend.ps=7, mar=c(3,3,0.7,0.7)+0.1, bty="o", axis.lwd=1, embedFonts=F)

names 		<- c("PCA confidence (rules 1-30)", "Std confidence (rules 1-30)", 	"Std confidence (rules 31-46)"	)
colors 		<- c("black",	"grey40",		"grey60"	)

lineType 	<- c(1, 	1,		1)
linePoint	<- c(20,	18,		17)




########################################################################
## for svd theta Bucket
########################################################################

x_std1 <- c(
1865,
2933,
8091,
9067,
9585,
9911,
9986,
10285,
11075,
38055,
40906,
45131,
46301,
51274,
51745,
51845,
51997,
54853,
55405,
55499,
71761,
72199,
73398,
89670,
89941,
92158,
112354,
112734,
113164,
113439,
125509
)

x_std2<- c(
125509,
125842,
126505,
126860,
127054,
152828,
152935,
153053,
153442,
166562,
179498,
180295,
180541,
181291,
188049
)





std1 <-c(
0.3448275862,
0.3389830508,
0.5505617978,
0.4201680672,
0.3445945946,
0.4011627907,
0.3712871287,
0.4380530973,
0.4110671937,
0.3936170213,
0.3754045307,
0.4201183432,
0.4483695652,
0.4166666667,
0.4198113208,
0.4140969163,
0.4504132231,
0.4522417154,
0.453038674,
0.4310645724,
0.4096185738,
0.412044374,
0.4069591528,
0.3942028986,
0.3888888889,
0.3775100402,
0.3629343629,
0.3511166253,
0.3528708134,
0.3437139562,
0.3382187148
)

std2 <-c(
0.3382187148,
0.3293347874,
0.3329809725,
0.3258196721,
0.3161033797,
0.3098455598,
0.3011257036,
0.2937956204,
0.2918149466,
0.2879444926,
0.2831632653,
0.2810945274,
0.2791262136,
0.2733017378,
0.2685185185
)




x2 <- c(
5158,
18196,
23135,
98714,
99040,
99339,
99439,
100415,
102280,
134771,
165654,
166722,
219076,
232989,
234162,
238329,
249010,
249528,
271411,
272581,
294650,
295202,
295992,
308635,
321571,
326544,
339153,
366133,
380420,
394369
)


pca <- c(
1,
0.5762711864,
0.5280898876,
0.5701754386,
0.6153846154,
0.6140350877,
0.5671641791,
0.4935064935,
0.4769230769,
0.4517241379,
0.4263322884,
0.4183381089,
0.3888888889,
0.4324324324,
0.4233409611,
0.4,
0.3898989899,
0.3702290076,
0.3916211293,
0.4075993092,
0.3980263158,
0.4012539185,
0.3884673748,
0.414244186,
0.4115107914,
0.3997214485,
0.3917112299,
0.3860103627,
0.3728179551,
0.3743781095
)












rg.startplot("std_vs_pca.pdf")


rg.plot(x2, pca, type="o", col=colors[1], ylab="Aggregated Precision", xlab="Aggregated Predictions (beyond the initial KB)", xlim=c(500, 400000),  ylim=c(0.25,1), lty=lineType[1], pch=linePoint[1], xaxt='n')
lines(x_std1, std1, type="o", pch=linePoint[2], lty=lineType[2], col=colors[2])
lines(x_std2, std2, type="o", pch=linePoint[3], lty=lineType[3], col=colors[3])
legend("topright",legend=names, cex=1, col=colors, pch=linePoint, lty=lineType)#, ncol=2) 
axis(1, at=c(0,50000,100000,150000,200000,250000, 300000, 350000, 400000),labels=c("0","50000","100000","150000","200000","250000", "300000", "350000", "400000"), tick=T, lwd=T)

# 
# ################################
# par(fig=c(0,0.8,0,0.8), new=TRUE)
# plot(mtcars$wt, mtcars$mpg, xlab="Car Weight",
#   ylab="Miles Per Gallon")
# par(fig=c(0,0.8,0.25,1), new=TRUE)
# boxplot(mtcars$wt, horizontal=TRUE, axes=FALSE)
# par(fig=c(0.65,1,0,0.8),new=TRUE)
# boxplot(mtcars$mpg, axes=FALSE)
# mtext("Enhanced Scatterplot", side=3, outer=TRUE, line=-3)
# ##############################


rg.endplot()
