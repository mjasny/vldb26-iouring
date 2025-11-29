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
)

#options(dplyr.print_max = 1e9)
options(width=300)


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






df_bw <- df %>%
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
        flabel=reorder(sprintf("%d Worker%s", num_workers, ifelse(num_workers>1, "s", "")), num_workers),
        #flabel2=node,
        name_code=sprintf("%d%d%d", send_zc, recv_zc, use_epoll),
        hjust=ifelse(bw > 8*2**30, 1.1, -0.1),
    ) %>%
    assign_labels(
        name_code, name,
        c(
          "001"="epoll",
          "000"="io_uring",
          "101"="epoll+ZC Send",
          "100"="io_uring+ZC Send",
          "110"="io_uring+ZC Recv"
        )
    ) %>%
    mutate(
        x = case_when(
            !send_zc & !recv_zc ~ 0,
            send_zc & !recv_zc ~ 1,
            send_zc & recv_zc ~ 1.8,
        ),
    )




df_bw2 <- df_bw %>%
    filter(tuple_size %in% c(16, 64, 128, 1024, 4096, 16384)) %>%
    filter(num_workers < 32 & use_hashtable) %>%
    mutate(
        flabel2=reorder(sprintf("%dB Tuples", tuple_size), tuple_size)
    ) %>%
    print()



# Over tuple-size

df_bw2 <- df_bw %>%
    filter(use_hashtable) %>%
    filter(num_workers %in% c(1, 8, 16)) %>%
    speedup(
        use_epoll & !send_zc & !recv_zc, bw,
        by=c("tuple_size", "num_workers")
    ) %>%
    mutate(
        speedup=1/speedup, # inverse
    ) %>%
    print()


breaks = c(16, 64, 256, 1024, 4096, 16384)
labels = c('16B', '64B', '256B', '1KiB', '4KiB', '16KiB')


df_bw2 %>%
    filter(tuple_size >= 512) %>%
    mutate(
        bw=bw/2**30,
    ) %>%
    select(name, num_workers, tuple_size, bw, speedup) %>%
    print(n=100)


df_bw2 <- df_bw2 %>% filter(!(use_epoll & !send_zc & !recv_zc))

p <- ggplot(df_bw2, aes(x=tuple_size, y=speedup, color=name, shape=name)) +
    geom_line() + 
    geom_point() + 

    guides(
        color=guide_legend(title=NULL, nrow=1),
        fill=guide_legend(title=NULL, nrow=1),
        shape=guide_legend(title=NULL, nrow=1),
    )+
    scale_x_continuous(
        name="Tuple size [log]",
        breaks=breaks,
        label=labels,
        trans="log",
    ) +
    scale_y_continuous(
        name="Speedup vs. Epoll (naive)",
        limits=c(0.9, NA),
        breaks=seq(1.0, 3.0, by=0.5),
        #label=fmt_bytes(unit='bin_bytes', suffix='/s')
    ) +
    theme(
        legend.title=element_text(),
        #axis.title.x=element_blank(),
        axis.text.x=element_text(angle=25, hjust=0.7, margin=margin(0, t=0.4, b=-2.5, unit='mm')),
        #axis.ticks.x=element_blank(),
    ) +  
    facet_grid(. ~ flabel) 


size <- 2.9
p <- annotate_points(
  plot = p,
  data = df_bw2,
  condition = num_workers==1 & tuple_size==16*KiB & use_epoll & send_zc & !recv_zc,
  label = "io_uring: 3.48GiB/s\nepoll: 3.17GiB/s",
  lineheight=0.8,
  size = size,
  color='black',
  nudge_x=-7.0,
  nudge_y=0.45,
  point.padding=0.5,
  hjust=0,
)

p <- annotate_points(
  plot = p,
  data = df_bw2,
  condition = num_workers==8 & tuple_size==512 & use_epoll & send_zc & !recv_zc,
  label = "epoll max.\n(11.2GiB/s)",
  lineheight=0.8,
  size = size,
  color='black',
  nudge_x=-2.0,
  nudge_y=0.35,
  point.padding=0.5,
)

p <- annotate_points(
  plot = p,
  data = df_bw2,
  condition = num_workers==8 & tuple_size==16*KiB & !use_epoll & send_zc & recv_zc,
  label = "io_uring max.\n(27.2GiB/s)",
  lineheight=0.8,
  size = size,
  color='black',
  nudge_x=-4.0,
  nudge_y=-0.10,
  point.padding=0.5,
)

p <- annotate_points(
  plot = p,
  data = df_bw2,
  condition = num_workers==16 & tuple_size==16*KiB & use_epoll & send_zc,
  label = "epoll max.\n(30.5GiB/s)",
  lineheight=0.8,
  size = size,
  color='black',
  nudge_x=-2.5,
  nudge_y=-0.32,
  point.padding=0.5,
)

p <- annotate_points(
  plot = p,
  data = df_bw2,
  condition = num_workers==16 & tuple_size==2048 & !use_epoll & recv_zc,
  label = "Link limit\nreached\n(42.9GiB/s)",
  lineheight=0.8,
  size = size,
  color='black',
  nudge_x=-3.5,
  nudge_y=-0.1,
  point.padding=0.5,
)


dim=c(140, 55) 
file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname=sprintf("out/%s_tuplesize_speedup.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)


