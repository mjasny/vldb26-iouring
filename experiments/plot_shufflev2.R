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
library(tidytable)
library(mmtable2)
library(ggrepel)
library(geomtextpath)
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


df <- read.csv('data/bench_shufflev2_1.csv', comment='#') 
df_mem <- read.csv('data/bench_shufflev2_1_membw.csv', comment='#') 


df <- df %>%
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
    group_by(run, node, num_nodes, use_hashtable, nr_conns, num_workers, tuple_size, send_zc, recv_zc, fairness, membw_key) %>%
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



starts <- df %>%
    group_by(membw_key, node) %>%
    summarise(
        start_ts = first(start_ts),
        stop_ts = last(stop_ts),
        .groups = "drop"
    )


df_mem_adj <- df_mem %>%
    left_join(starts, by = c("membw_key", "node")) %>%
    group_by(membw_key, node) %>%
    mutate(
        time = time - start_ts - 0.09,
        mem_rd = (umc_mem_read_bandwidth) * 1e6,
        mem_wr = (umc_mem_write_bandwidth) * 1e6,
        mem_util = umc_data_bus_utilization,
    ) %>%
    filter(time >= 0) %>%
    filter(time <= stop_ts-start_ts)


df <- full_join(
  df %>% group_by(membw_key, node) %>% mutate(row = row_number()),
  df_mem_adj %>% group_by(membw_key, node) %>% mutate(row = row_number()),
  by = c("membw_key", "node", "row"),
  suffix = c("_left", "_right")
)





df_bw <- df %>%
    filter(!is.na(ts)) %>%
    group_by(num_nodes, tuple_size, num_workers, fairness, send_zc, recv_zc, use_hashtable) %>%
    dplyr::arrange(ts, .by_group=TRUE) %>%
    slice(ceiling(0.20 * dplyr::n()) : floor(0.80 * dplyr::n())) %>%
    summarise(
        bw=mean(value), # / 2**30,
        mem_rd=max(mem_rd, na.rm=TRUE), # / 2**30, 
        mem_wr=max(mem_wr, na.rm=TRUE), # / 2**30,
        mem_util=max(mem_util, na.rm=TRUE),
        stalled=max(stalled),
        .groups = "drop"
    ) %>%
    mutate(
        name_key=sprintf("zc_send=%d zc_recv=%d", send_zc, recv_zc),
        flabel=reorder(sprintf("%d Worker HT=%d", num_workers, use_hashtable), num_workers),
        flabel2=reorder(sprintf("%d Nodes", num_nodes), num_nodes),
        slabel=sprintf("%d", tuple_size),
    ) %>%
    assign_labels(
        name_key, name,
        c("zc_send=0 zc_recv=0"="Default", "zc_send=1 zc_recv=0"="+ZC Send", "zc_send=1 zc_recv=1"="+ZC Recv")
    )



df_bw2 <- df_bw %>%
    filter(fairness & num_nodes==6 & use_hashtable) %>%
    #filter(tuple_size %in% c(16, 64, 128, 1024, 4096, 16384)) %>%
    filter(tuple_size %in% c(64, 1024, 4096)) %>%
    mutate(
        flabel=reorder(sprintf("%dB Tuples", tuple_size), tuple_size)
    )




df_labels <- df_bw2 %>%
  group_by(name, flabel) %>%
  mutate(
    hjust=case_when(
        !use_hashtable & name_key=="zc_send=1 zc_recv=1" ~ 0.75,
        use_hashtable & name_key=="zc_send=1 zc_recv=1" & tuple_size>=4096 ~ 0.75,
        T ~ 0.97,
    ),
    vjust=case_when(
        name_key=="zc_send=0 zc_recv=0" ~ 1.2,
        name_key=="zc_send=1 zc_recv=0" ~ 1.2,
        name_key=="zc_send=1 zc_recv=1" ~ -0.2,
        T ~ 0,
    ),
  ) %>%
  ungroup()


df_bw2 %>%
    mutate(
        bw = bw / 2**30,
        mem_rd = mem_rd / 2**30,
        mem_wr = mem_wr / 2**30,
    ) %>%
    filter(recv_zc) %>%
    select(-send_zc, -recv_zc) %>%
    print(n=100)






p <- ggplot(df_bw2, aes(x=num_workers, y=bw, color=name, shape=name)) + #, shape=slabel)) +
    geom_line() +
    geom_point() +

    geom_hline(yintercept=400*10**9/8, linetype="dashed", color = "black", linewidth=0.3) +
    annotate('text', y=400*10**9/8, x=1.0, hjust=0, vjust=-0.15, 
             size=2.5, color="black", label="Link Limit") + #fontface="bold", 

    geom_textline(
        data=df_labels,
        aes(label=name, hjust=hjust, vjust=vjust),
        size=3,
        text_smoothing=30
    ) +

    guides(
        color='none', #guide_legend(title=NULL, nrow=1),
        shape='none', #guide_legend(title=NULL, nrow=1),
    )+
    scale_x_continuous(
        name="Worker threads",
        breaks=unique(df$num_workers),
        labels=function(x) sprintf("%0.0f", x),
        trans="log",
    ) + 
    scale_y_continuous(
        name="Per Node Egress Bandwidth",
        #limits=c(0, NA),
        limits=c(0, 2**30 * 50),
        breaks=2**30 * seq(0, 50, by=10),
        label=fmt_bytes(unit='bin_bytes', suffix='/s')
    ) +
    theme(
        legend.margin=margin(0, b=-3, l=0, unit='mm'),
    ) +
    facet_grid(. ~ flabel)

dim=c(140, 55) #* c(2, 1) #* c(1, 1.4)
file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname=sprintf("out/%s.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)




# Memory reduction normalized to achieved bw
df_bw <- df_bw %>%
    #filter(use_hashtable) %>%
    mutate(
        mem_bw=mem_rd+mem_wr,
        mem_bw_norm = mem_bw / (bw*2),
    ) %>%
    speedup(
        !send_zc & !recv_zc, mem_bw_norm,
        by=c("flabel", "num_workers", "tuple_size", "num_nodes")
    ) %>%
    assign_labels(
        name_key, name,
        c("zc_send=0 zc_recv=0"="Default", "zc_send=1 zc_recv=0"="+ZC Send", "zc_send=1 zc_recv=1"="+ZC Recv")
    ) %>%
    print()




df_bw <- df_bw %>%
    filter(use_hashtable & num_workers == 32 & num_nodes==6) %>%
    #filter(tuple_size %in% c(128, 1024)) %>%
    filter(tuple_size %in% c(64, 4096)) %>%
    mutate(
        flabel=reorder(sprintf("%dB Tuples", tuple_size), tuple_size),
    )


yhjust = 0.65 # smaller => up
p <- ggplot(df_bw, aes(x=name, y=mem_bw, fill=name)) +
    geom_col(
        position=position_dodge(width=0.8),
        width=1.0,
        color='black'
    ) +
    geom_text(
        aes(label=sprintf("%.1f", mem_bw/2**30)),
        angle=90,
        position=position_dodge(width=0.9),
        hjust=1.1,
        size=3.2,
    ) +
    guides(
        fill='none',
    )+
    scale_x_discrete(
        name=NULL,
        expand = expansion(add=0.8) 
    ) + 
    scale_y_continuous(
        name="System Memory Bandwidth", # ~ Network-BW",
        #limits=c(0, NA),
        limits=c(0, 2**30 * 420),
        breaks=2**30 * seq(0, 400, by=100),
        label=fmt_bytes(unit='bin_bytes', suffix='/s')
    ) +
    theme(
        axis.text.x=element_text(angle=45, hjust=1, margin=margin(0, unit='mm')),
        axis.title.y=element_text(hjust=yhjust),
    ) +
    facet_grid(. ~ flabel)


p2 <- ggplot(df_bw, aes(x=name, y=speedup, fill=name)) +
    geom_col(
        position=position_dodge(width=0.8),
        width=1.0,
        color='black'
    ) +
    guides(
        fill='none',
    )+
    scale_x_discrete(
        name=NULL,
        expand = expansion(add=0.8) 
    ) + 
    scale_y_continuous(
        #position='right',
        name="Mem-BW reduction [norm.]",
        limits=c(0, NA),
        #limits=c(0, 2**30 * 50),
        #breaks=2**30 * seq(0, 50, by=10),
    ) +
    theme(
        axis.text.x=element_text(angle=45, hjust=1, margin=margin(0, unit='mm')),
        axis.title.y=element_text(hjust=yhjust),
    ) +
    facet_grid(. ~ flabel)



p2 <- add_bar_arrow(
  p = p2,
  data = df_bw %>% filter(tuple_size==64),
  x = "name",
  y = "speedup",
  from = "Default",
  to = "+ZC Recv",
  label  = "1.60x",
  text_angle=40,
  text_vjust=-1.6,
  fontface='bold',
  curvature=-0.50,
  curve_angle=65,
  from_pad=0,
  to_pad=0.48,
)
p2 <- add_bar_arrow(
  p = p2,
  data = df_bw %>% filter(tuple_size==4096),
  x = "name",
  y = "speedup",
  from = "Default",
  to = "+ZC Recv",
  label  = "2.17x",
  text_angle=65,
  text_vjust=-1.8,
  fontface='bold',
  curvature=-0.50,
  curve_angle=65,
  from_pad=0,
  to_pad=0.48,
)

df_bw %>% filter(recv_zc) %>% select(tuple_size, speedup) %>% print()


p <- (p | p2) + plot_layout(guides = "collect")
#p <- (p | p2) + plot_layout()

dim=c(140, 57.5) #* c(1, 1.4)
file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname=sprintf("out/%s_mem.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)



