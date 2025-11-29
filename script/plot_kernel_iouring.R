suppressPackageStartupMessages({
library(ggplot2)
library(dplyr)
library(sqldf)
library(gridExtra)
library(ggpubr)
library(data.table)
library(stringr)
})



theme_set(theme_bw())
theme_update(
    axis.title.y=element_text(margin=margin(0, r=0, unit='mm')),
    axis.title.x=element_text(margin=margin(0, t=0, unit='mm')),
    axis.text.x=element_text(angle=45, hjust=1),
    legend.key=element_blank(),
    legend.key.size=unit(3.5, 'mm'),
    legend.position="top",
    legend.title=element_text(size=8, face='bold', margin=margin(0, unit='mm')),
    # legend.position=c(0.5, 0.6),
    legend.margin=margin(0, l=-15, b=-3.5, unit='mm'),
    legend.spacing.x = unit(0.5, 'mm'),
    legend.spacing.y = unit(0, 'mm'),
    legend.text = element_text(margin=margin(0), size=8),
    legend.background = element_blank()
)


df <- read.csv("kernel_iouring.csv") %>%
    mutate(
        version=sprintf("%s", str_sub(version, 2)),
    )


print(df)


p <- ggplot(df, aes(x = factor(i), y = commits)) +
    geom_bar(stat = "identity", fill = "steelblue", color='black') +
    #geom_text(
    #    aes(label = version), 
    #    position=position_dodge(width=0.9),
    #    angle=45,
    #    hjust=0,
    #    vjust=-0.6,
    #    size=3,
    #) +
    scale_x_discrete(labels=df$version) +
        labs(
        x = "Kernel",
        y = "#Commits",
        #title = "Commits per Version"
    )




file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
dim=c(160, 60) 
ggsave(file=sprintf("%s.pdf", file), plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", sprintf("%s.pdf", file), sprintf("%s.pdf", file)), wait=T)
#ggsave(file=sprintf("%s.png", file), plot=p, width=dim[1], height=dim[2], units="mm")


