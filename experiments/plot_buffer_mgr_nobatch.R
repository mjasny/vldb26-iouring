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
library(ggpattern)
library(ggh4x)
})
# paru -S r-ggplot2 r-gtable r-viridis
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



df <- read.csv('data/bench_buffer_mgr_nobatch.csv', comment='#')
#df <- read.csv('data/bench_buffer_mgr.csv', comment='#')







df_ycsb <- df %>%
    filter(workload == 'ycsb') %>%
    filter(virt_size != 4294967296) %>% # no in-memory
    mutate(across(c(reg_ring, reg_fds, reg_bufs, nvme_cmds, submit_always, sync_variant, posix_variant, iopoll, libaio), ~ as.logical(.))) %>%
    filter(sync_variant) %>%
    mutate(
        name_short=case_when(
            libaio ~ "libaio", 
            setup_mode=="defer" & !submit_always & !reg_ring & !reg_fds & !reg_bufs & !nvme_cmds & !iopoll ~ "sync", 
            setup_mode=="defer" & !submit_always & reg_ring & reg_fds & !reg_bufs & !nvme_cmds & !iopoll ~ "regfds",
            setup_mode=="defer" & !submit_always & reg_ring & reg_fds & reg_bufs & !nvme_cmds & !iopoll ~ "regbufs",
            setup_mode=="defer" & !submit_always & reg_ring & reg_fds & reg_bufs & nvme_cmds & !iopoll ~ "passthru",
            setup_mode=="defer" & !submit_always & reg_ring & reg_fds & reg_bufs & nvme_cmds & iopoll ~ "iopoll",
            setup_mode=="sqpoll" & !submit_always & reg_ring & reg_fds & reg_bufs & nvme_cmds ~ "sqpoll",
            T ~ "???",
        ),
    ) %>%
    filter(name_short != "???") %>%
    group_by(name_short) %>%
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
    mutate(
        group=case_when(
            #libaio ~ "libaio",
            #libaio ~ "async",
            setup_mode=="sqpoll" ~ "sqpoll",
            sync_variant ~ "sync",
            setup_mode=="defer" ~ "async",
        ),
    ) %>%
    assign_labels(
        name_short, name,
        c(
            "posix"="Posix Sync.", #"posix_batch_evict"="Posix+BatchEvict",
            "libaio"="libaio\nSync.", 
            "sync"="io_uring\nSync.", "batch_evict"="io_uring\n+BatchEvict",
            "libaio_sync_submit"="libaio\n+Fibers",
            "sync_submit"="io_uring\n+Fibers",
            "batch_submit"="+Batch-\nSubmit",
            "regfds"="+RegFDs",
            "regbufs"="+RegBufs", "passthru"="+Passthru",
            "iopoll"="+IOPoll",
            "sqpoll"="+SQPoll"
        )
    ) %>%
    filter(!is.na(name)) %>%
    mutate(xlabel=name) %>%
    select(-iopoll) %>%
    mutate(
        tlabel=sprintf(" %0.1f", tps/1e3),
        vjust=ifelse(tps<10e3, -0.3, 1.2),
    ) %>%
    assign_labels(
        group, flabel,
        c("sync"="Synchronous I/O (Without Batching)", "libaio"="libaio", "async"="Asynchronous I/O", "sqpoll"="+2nd Core")
    ) %>%
    print()




p <- ggplot(df_ycsb, aes(x=name, y=tps)) +
    geom_col(
        position=position_dodge(width=0.88),
        width=1.0,
        aes(fill=xlabel),
        color='black'
    ) +
    geom_text(
        aes(label=tlabel, vjust=vjust, y=tps),
        position=position_dodge(width=0.9),
        size=3.20,
    ) +

    guides(
        color="none",
        fill="none", 
    )+
    scale_x_discrete(
        name=NULL,
        expand = expansion(add=0.7) 
    ) + 
    scale_y_continuous(
        name="Throughput [tx/s]",
        labels=function(x) sprintf("%0.0fK", x/1e3),
        #limits=c(0, NA),
        limits=c(0, 30e3),
        breaks=seq(0, 30e3, by=10e3),
    ) +
    facet_grid(. ~ flabel, scales="free_x", space="free") +
    theme(
        legend.title=element_text(),
        axis.title.x=element_blank(),
        axis.text.x=element_text(angle=35, margin=margin(t=1, unit='mm'), hjust=0.7),
        panel.spacing = unit(0, "mm"),
        #axis.ticks.x=element_blank(),
    )

p <- add_bar_arrow(
  p = p,
  data = df_ycsb, #%>% filter(flabel=="Asynchronous I/O"),
  x = "name",
  y = "tps",
  from = "io_uring\nSync.",
  to = "+IOPoll",
  label  = "1.20x",
  text_angle=4,
  text_vjust=-2.5,
  fontface='bold',
  curvature=-0.30,
  #curve_angle=-60,
  from_pad=0.3,
  to_pad=0.48,
)


dim=c(100, 60) #* c(0.5, 1)
file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname=sprintf("out/%s.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)
