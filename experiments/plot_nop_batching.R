suppressPackageStartupMessages({
library(ggplot2)
library(dplyr)
library(sqldf)
library(stringr)
library(viridis)
library(scales)
library(tidyr)
})


source('./utils.R')

theme_set(theme_bw())
theme_update(
    legend.title = element_blank(),
    legend.position = "top",
    # legend.margin=margin(0, b=-3, l=-10, unit='mm'),
    legend.margin=margin(0, b=-3, unit='mm'),
    legend.spacing.x = unit(0.3, 'mm'),
    legend.spacing.y = unit(0, 'mm'),
    legend.key.size=unit(3.5, 'mm'),
)

options(width=300)


df <- read.csv("data/bench_nop3.csv")


df <- df %>%
    mutate(across(c(reg_ring, reg_fds, reg_bufs, test_file, test_buf, mitigations, perfevent), ~ as.logical(.))) %>%
    group_by(
        node, nr_nops, reg_ring, reg_fds, reg_bufs,
        setup_mode, mode, test_file, test_buf, mitigations, perfevent
    ) %>%
    summarize(
        ops=mean(ops_per_sec),
        cycles=mean(perf_cycles),
        cycles_sd=sd(perf_cycles),
        instructions=mean(perf_instructions),
        branch_misses=mean(perf_branch.misses),
        ipc=mean(perf_IPC),
        .groups = "drop"
    )

df %>%
    select(-reg_ring, -reg_fds, -reg_bufs, -test_file, -test_buf, -mitigations, -perfevent, -mode, -ops) %>%
    print(n=100)



df <- df %>%
    filter(nr_nops <= 256)


rect_df <- df %>%
    slice(1) %>%  # one row per facet needed
    mutate(
           xmin = 32,
           xmax = Inf, #max(df$bs),
           ymin = -Inf,
           ymax = Inf,
           label_x = 32,
           label_y = max(df$cycles),
    )



p <- ggplot(df, aes(y=cycles, x=nr_nops)) +
    geom_line()+
    geom_point()+

    geom_rect(
        data = rect_df,
        aes(xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax),
        inherit.aes = FALSE,
        fill = "green", alpha = 0.3
    ) +
    geom_text(
        data = rect_df,
        aes(x=label_x, y=label_y, label="syscall cost\nis amortized"),
        inherit.aes = FALSE,
        hjust = -0.05, vjust = 0.92,
        size = 3.5, lineheight = 0.9,
        fontface = "italic"
    ) +

    scale_x_continuous(
        name="Batch Size",
        breaks=unique(df$nr_nops),
        limits=c(1, max(df$nr_nops)),
        trans='log2',
    ) +
    scale_y_continuous(
        name="Cycles per OP",
        limits=c(0, NA),
        labels=function(x) sprintf("%d", x),
    ) +
    #guides(fill=guide_legend(title=NULL, nrow=1, byrow=T))+
    theme(
        legend.title=element_text(),
        axis.title.y=element_text(margin=margin(0, r=-1, unit='mm')),
        axis.title.x=element_text(margin=margin(0, unit='mm')),
        #axis.title.x=element_blank(),
        #axis.text.x=element_blank(),
        #axis.ticks.x=element_blank(),
    )


file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
dim=c(140, 60) * 0.7
fname=sprintf("out/%s.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)



