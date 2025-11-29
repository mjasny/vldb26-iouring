suppressPackageStartupMessages({
library(ggplot2)
library(dplyr)
library(sqldf)
library(stringr)
library(viridis)
library(scales)
library(tidyr)
library(patchwork)
library(forcats)
library(slider)
})
source('./utils.R')

theme_set(theme_bw())
theme_update(
    legend.position = "top",
    #axis.title.x=element_blank(),
    legend.margin=margin(0, b=-3, l=-10, unit='mm'),
    legend.key.size=unit(3.5, 'mm'),
    legend.text=element_text(margin=margin(0, l=0.25, unit='mm')),
    #legend.spacing.x = unit(0.3, 'mm'),
    #legend.spacing.y = unit(0, 'mm'),
    #axis.text.x=element_blank(),
    #axis.ticks.x=element_blank(),
    axis.title.y=element_text(hjust=0.60),
)

#options(dplyr.print_max = 1e9)
options(width=300)


STATS_SCALE = 10






df3 <- read.csv('data/bench_shufflev2.csv', comment='#') # scale 10

df3 <- df3 %>%
    mutate(across(c(reg_ring, reg_fds, reg_bufs, send_zc, recv_zc, pin_queues, use_budget, use_hashtable, fairness), ~ as.logical(.))) %>%
    pivot_longer(
        cols = c(recv, sent), 
        names_to = "metric", 
        values_to = "value"
    ) %>% 
    filter(metric == "recv") %>%
    mutate(
        value=value*STATS_SCALE,
        ts=ts/STATS_SCALE,
    ) %>%
    group_by(run, node, num_nodes, use_hashtable, nr_conns, num_workers, tuple_size, send_zc, recv_zc, fairness) %>%
    slice({
        nz <- which(value > 0)
        n_ <- n()
        if (length(nz) == 0) {
            1L                                 # all zeros → keep just the first row
        } else {
            start <- max(min(nz) - 1L, 1L)     # keep one leading zero
            end   <- min(max(nz) + 1L, n_)     # keep one trailing zero
            seq.int(start, end)
        }
    }) %>%
    mutate(
        start_ts=first(ts),
        stop_ts=last(ts),
        ts=ts-first(ts),
    ) %>%
    filter(ts >= 0) %>%
    ungroup()

df3_2 <- df3 %>%
    filter(!send_zc & !use_hashtable) %>%
    filter(nr_conns==1) %>%
    filter(num_nodes==2) %>%
    filter(num_workers==8) %>%
    filter(tuple_size==128*4) %>%
    print() %>%

    assign_labels(
        fairness, flabel,
        c("FALSE"="Default", "TRUE"="Tuned")
    ) %>%
    assign_labels(
        node, name,
        c("fn05"="Node A", "fn06"="Node B", "total"="Total")
    )


df_runtime <- df3_2 %>%
    group_by(flabel) %>%
    summarize(
        runtime=max(ts),
    ) %>%
    mutate(
        label=sprintf("Runtime: %0.1fs", runtime),
        vjust=case_when(
            runtime >= 5 ~ -0.4,
            runtime < 5 ~ 1.4,
        )
    ) %>%
    print()


df_labels <- df3_2 %>%
  group_by(name, flabel) %>%
  filter(ts == 1.5) %>%
  ungroup()

p <- ggplot(df3_2, aes(x=ts, y=value, color=name)) +
    geom_line() + #linewidth=0.35) +

    geom_vline(data=df_runtime, aes(xintercept=runtime), linetype = "dashed") + 
    geom_text(data=df_runtime, aes(x=runtime, label=label, vjust=vjust), color='black', y=0, angle=90, hjust=0)+
    
    geom_text_repel(
        data = df_labels,
        aes(label = name),
        nudge_x = 0.2,
        direction = "y",
        hjust = 0,
        segment.color = NA,
        size = 3
    ) +

    guides(
        #color=guide_legend(title=NULL, nrow=1),
        color="none",
    )+
    scale_x_continuous(
        name="Timestamp [s]",
        labels=function(x) sprintf("%0.1f", x),
    ) + 
    scale_y_continuous(
        name="Per Node Egress Bandwidth",
        #limits=c(0, NA),
        limits=c(0, 2**30 * 18),
        breaks=2**30 * seq(0, 18, by=4),
        label=fmt_bytes(unit='bin_bytes', suffix='/s')
    ) +
    facet_grid(. ~ flabel) #, scales="free_x")


dim=c(140, 54) #* c(1.5, 2)
file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname=sprintf("out/%s.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)
