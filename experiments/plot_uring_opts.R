suppressPackageStartupMessages({
library(ggplot2)
library(dplyr)
library(sqldf)
library(stringr)
library(viridis)
library(scales)
library(tidyr)
library(patchwork)
})
source('./utils.R')

theme_set(theme_bw())
theme_update(
    legend.position = "top",
    axis.title.x=element_blank(),
    legend.margin=margin(0, b=-3, l=0, unit='mm'),
    legend.key.size=unit(3.5, 'mm'),
    legend.text=element_text(margin=margin(0, l=1, unit='mm')),
    #axis.text.x=element_text(angle=45, hjust=1),
    axis.text.x=element_text(),
)

options(width=300)


df <- read.csv('data/bench_buffer_mgr.csv', comment='#')

opt_name <- "io_uring\n(Opt.)"

df_bm <- df %>%
    filter(workload == 'ycsb') %>%
    filter(virt_size != 4294967296) %>% # no in-memory
    mutate(across(c(reg_ring, reg_fds, reg_bufs, nvme_cmds, submit_always, sync_variant, iopoll, libaio), ~ as.logical(.))) %>%
    mutate(
        name=case_when(
            !iopoll & libaio & submit_always ~ "libaio",
            !iopoll & !libaio & setup_mode=="defer" & submit_always & !reg_ring & !reg_fds & !reg_bufs & !nvme_cmds ~ "io_uring", 
            iopoll & setup_mode=="defer" & !submit_always & reg_ring & reg_fds & reg_bufs & nvme_cmds ~ "io_uring*",
            T ~ "???",
        ),
    ) %>%
    filter(name != "???") %>%
    assign_labels(
        name, name_s,
        c(
            "libaio"="libaio",
            "io_uring"="io_uring",
            "io_uring*"=opt_name
        )
    ) %>%
    group_by(name) %>%
    slice(min(which(tps != 0)-1):n()) %>%
    mutate(
        ts=ts-first(ts),
    ) %>%
    slice(n()) %>% # Last value
    ungroup() %>%
    mutate(
        tlabel=sprintf("%0.1f", tps/1e3),
        vjust=ifelse(tps < 100e3, -0.2, 1.2),
    ) %>%
    print()



p <- ggplot(df_bm, aes(x=name_s, y=tps, fill=name_s)) +
    geom_col(position=position_dodge(), color='black', width=0.9) +
    geom_text(
        position=position_dodge(width=0.9),
        aes(label=tlabel, vjust=vjust),
    ) +

    guides(
        fill="none", #guide_legend(title=NULL, nrow=1, byrow=T),
        #shape=guide_legend(title=NULL, nrow=1, byrow=T),
    ) +
    scale_x_discrete() + 
    scale_y_continuous(
        name="Transactions/s [K/s]",
        limits=c(0, NA),
        labels=function(x) sprintf("%0.0f", x/1e3),
        #limit=c(0, 21.5e6),
    ) +
    facet_grid(. ~ "Buffer Manager") 



STATS_SCALE = 10
df <- read.csv('data/bench_shufflev2_3.csv', comment='#') 
df <- df %>%
    mutate(across(c(reg_ring, reg_fds, reg_bufs, send_zc, recv_zc, pin_queues, use_budget, use_hashtable, fairness, use_epoll), ~ as.logical(.))) %>%
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
    group_by(run, node, num_nodes, use_hashtable, nr_conns, num_workers, tuple_size, send_zc, recv_zc, fairness, use_epoll) %>%
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
        ts=ts-first(ts),
    ) %>%
    filter(ts >= 0) %>%
    ungroup()

df_net <- df %>%
    filter(tuple_size==128*2) %>%
    filter(num_workers==8) %>%
    filter(!use_hashtable) %>%
    group_by(run, num_nodes, use_hashtable, nr_conns, num_workers, tuple_size, send_zc, recv_zc, fairness, use_epoll) %>%
    arrange(ts, .by_group=TRUE) %>%
    slice(ceiling(0.20 * dplyr::n()) : floor(0.80 * dplyr::n())) %>%
    summarise(
        sd=sd(value),
        bw=mean(value),
    ) %>%
    ungroup() %>%
    mutate(
        tlabel=sprintf("%0.1f", bw / 2**30),
        name=sprintf("%d%d%d", send_zc, recv_zc, use_epoll),
        vjust=ifelse(bw > 10*2**30, 1.2, -0.2),
    ) %>%
    assign_labels(
        name, name_s,
        c(
          "001"="epoll",
          "000"="io_uring",
          #"101"="epoll+ZC Send",
          #"100"="io_uring+ZC Send",
          "110"=opt_name #io_uring+ZC Recv"
        )
    ) %>%
    filter(!is.na(name_s)) %>%
    print()


p2 <- ggplot(df_net, aes(x=name_s, y=bw, fill=name_s)) +
    geom_col(position=position_dodge(), color='black', width=0.9) +
    geom_text(position=position_dodge(width=0.9), aes(label=tlabel, vjust=vjust)) +
    guides(
        fill="none",
    ) +
    scale_x_discrete() + 
    scale_y_continuous(
        name="Bandwidth [GiB/s]",
        limit=c(0, NA),
        breaks=2**30 * seq(0, 30, by=10),
        labels=unit_format(unit="", scale=1/2**30),
    ) +
    facet_grid(. ~ "Network Shuffle") 



p <- add_bar_arrow(
  p = p,
  data = df_bm,
  x = "name_s",
  y = "tps",
  from = "libaio",
  to = "io_uring",
  label  = "1.06x",
  text_angle=0,
  text_vjust=-1.0,
  fontface='bold',
  curvature=-0.50,
  from_pad=0,
  to_pad=0.48,
)

p <- add_bar_arrow(
  p = p,
  data = df_bm,
  x = "name_s",
  y = "tps",
  from   = "io_uring",
  to     = opt_name,
  label  = "2.05x",
  text_angle=55,
  text_vjust=0.5,
  fontface='bold',
  curvature=-0.28,
  from_pad=0,
  to_pad=0.48,
)

p2 <- add_bar_arrow(
  p = p2,
  data = df_net,
  x = "name_s",
  y = "bw",
  from   = "epoll",
  to     = "io_uring",
  label  = "1.10x",
  text_angle=0,
  text_vjust=-1.0,
  fontface='bold',
  curvature=-0.5,
  from_pad=0,
  to_pad=0.48,
)
p2 <- add_bar_arrow(
  p = p2,
  data = df_net,
  x = "name_s",
  y = "bw",
  from   = "io_uring",
  to     = opt_name,
  label  = "2.31x",
  text_angle=55,
  text_vjust=0.5,
  fontface='bold',
  curvature=-0.28,
  from_pad=0,
  to_pad=0.48,
)


p <- p | p2

dim=c(140, 50) #* c(0.5, 1)
file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname=sprintf("out/%s.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)

