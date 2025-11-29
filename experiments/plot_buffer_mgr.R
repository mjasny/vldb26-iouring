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



df <- read.csv('data/bench_buffer_mgr.csv', comment='#')



df_ycsb <- df %>%
    filter(workload == 'ycsb') %>%
    filter(virt_size != 4294967296) %>% # no in-memory
    mutate(across(c(reg_ring, reg_fds, reg_bufs, nvme_cmds, submit_always, sync_variant, posix_variant, iopoll, libaio), ~ as.logical(.))) %>%
    mutate(
        name_short=case_when(
            libaio & submit_always ~ "libaio_sync_submit",
            libaio & !submit_always ~ "libaio_batch_submit",
            posix_variant & evict_batch == 1 ~ "posix",
            posix_variant & evict_batch > 1 ~ "posix_batch_evict",
            setup_mode=="defer" & sync_variant & evict_batch == 1 & !iopoll ~ "sync",
            setup_mode=="defer" & sync_variant & evict_batch > 1 & !iopoll ~ "batch_evict",
            setup_mode=="defer" & submit_always & !iopoll ~ "sync_submit",
            setup_mode=="defer" & !submit_always & !reg_ring & !reg_fds & !reg_bufs & !nvme_cmds & !iopoll ~ "batch_submit", 
            setup_mode=="defer" & !submit_always & reg_ring & reg_fds & reg_bufs & !nvme_cmds & !iopoll ~ "regbufs",
            setup_mode=="defer" & !submit_always & reg_ring & reg_fds & reg_bufs & nvme_cmds & !iopoll ~ "passthru",
            setup_mode=="defer" & !submit_always & reg_ring & reg_fds & reg_bufs & nvme_cmds & iopoll ~ "iopoll",
            setup_mode=="sqpoll" & !submit_always & reg_ring & reg_fds & reg_bufs & nvme_cmds ~ "sqpoll",
            T ~ "???",
        ),
    ) %>%
    assign_labels(
        name_short, name,
        c(
            "posix"="Posix", "posix_batch_evict"="Posix+BatchEvict",
            "sync"="Sync.", "batch_evict"="+BatchEvict",
            "libaio_sync_submit"="libaio",
            "libaio_batch_submit"="libaio+BatchSubmit",
            "sync_submit"="+Fibers",
            "batch_submit"="+BatchSubmit",
            "regbufs"="+RegBufs", "passthru"="+Pass\nthru",
            "iopoll"="+IOPoll",
            "sqpoll"="+SQPoll"
        )
    ) %>%
    filter(!is.na(name)) %>%
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



df_ycsb2 <- df_ycsb %>%
    mutate(
        xlabel=name,
        group=case_when(
            #libaio ~ "libaio",
            libaio ~ "async",
            sync_variant ~ "sync",
            setup_mode=="defer" ~ "async",
            setup_mode=="sqpoll" ~ "sqpoll",
        ),
    ) %>%
    assign_labels(
        name_short, name,
        c(
            "posix"="Posix Sync.", #"posix_batch_evict"="Posix+BatchEvict",
            "sync"="io_uring\nSync.", "batch_evict"="io_uring\n+BatchEvict",
            "libaio_sync_submit"="libaio\n+Fibers",
            "sync_submit"="io_uring\n+Fibers",
            "batch_submit"="+Batch-\nSubmit",
            "regbufs"="+RegBufs", "passthru"="+Passthru",
            "iopoll"="+IOPoll",
            "sqpoll"="+SQPoll"
        )
    ) %>%
    filter(!is.na(name)) %>%
    select(-iopoll) %>%
    mutate(
        tlabel=sprintf(" %0.1f", tps/1e3),
        vjust=ifelse(tps<100e3, -0.3, 1.2),
    ) %>%
    assign_labels(
        group, flabel,
        c("sync"="Synchronous I/O", "libaio"="libaio", "async"="Asynchronous I/O", "sqpoll"="+2nd Core")
    )




p <- ggplot(df_ycsb2, aes(x=name, y=tps)) +
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
        limits=c(0, 575e3),
        breaks=seq(0, 550e3, by=100e3),
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
  data = df_ycsb2 %>% filter(flabel=="Asynchronous I/O"),
  x = "name",
  y = "tps",
  from = "io_uring\n+Fibers",
  to = "+IOPoll",
  label  = "2.05x",
  text_angle=10,
  text_vjust=-3.3,
  fontface='bold',
  curvature=-0.50,
  curve_angle=-60,
  from_pad=0.3,
  to_pad=0.48,
)





dim=c(140, 60) #* c(0.5, 1)
file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname=sprintf("out/%s.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)



df <- df %>%
    filter(workload == 'tpcc') %>%
    filter(concurrency %in% c(1, 128)) %>%
    mutate(across(c(reg_ring, reg_fds, reg_bufs, nvme_cmds, iopoll, libaio), ~ as.logical(.))) %>%
    mutate(
        name=case_when(
            libaio ~ "libaio",
            setup_mode=="defer" & !reg_ring & !reg_fds & !reg_bufs & !nvme_cmds & concurrency == 1 ~ "Default",
            setup_mode=="defer" & !reg_ring & !reg_fds & !reg_bufs & !nvme_cmds ~ "+Fibers",
            setup_mode=="defer" & reg_ring & reg_fds & reg_bufs & !nvme_cmds ~ "+RegBufs",
            setup_mode=="defer" & reg_ring & reg_fds & reg_bufs & nvme_cmds & !iopoll ~ "+Passthru",
            setup_mode=="defer" & reg_ring & reg_fds & reg_bufs & nvme_cmds & iopoll ~ "+IOPoll",
            setup_mode=="sqpoll" & reg_ring & reg_fds & reg_bufs & nvme_cmds & iopoll ~ "+SQPoll",
            T ~ "???",
        ),
    ) %>%
    print()


df <- rbind(
    read.csv('data/bench_vmcache.csv', comment='#') %>%
        rename(
            tps=tx,
            tpcc_warehouses=datasize,
        ) %>%
        mutate(name="vmcache") %>%
        select(ts, tps, name, tpcc_warehouses)
    ,
    df %>%
        select(ts, tps, name, tpcc_warehouses)
) 

df <- df %>%
    group_by(name, tpcc_warehouses) %>%
    filter(ts > 1) %>%
    mutate(
        ts=ts-first(ts),
    ) %>%
    ungroup() %>%
    assign_labels(
        name, name_s,
        #c("vmcache"="vmcache", "libaio"="libaio-based BM", "+Passthru"="io_uring-based BM", "+SQPoll"="+SQPoll", "+SQPoll+IOPoll"="+IOPoll")
        c("vmcache"="vmcache", "libaio"="libaio", "+Passthru"="io_uring", "+IOPoll"="+IOPoll", "+SQPoll"="+SQPoll")
    ) %>%
    filter(!is.na(name))



flabel1 <- 'TPC-C  -  mostly in-memory'
flabel2 <- 'TPC-C  -  mostly out-of-memory'


df_1 <- df %>%
    filter(tpcc_warehouses==1) %>%
    group_by(name) %>%
    arrange(ts, .by_group=TRUE) %>%
    filter(ts > 190 & ts <= 200) %>%
    summarize(
        tps=mean(tps),
        .groups='drop',
    ) %>%
    mutate(
        tlabel=sprintf("%0.1f", tps/1e3),
        vjust=ifelse(tps < 10e3, -0.2, 1.2),
        flabel=flabel1,
    )



df_100 <- df %>%
    filter(tpcc_warehouses==100) %>%
    group_by(name) %>%
    arrange(ts, .by_group=TRUE) %>%
    slice(ceiling(0.30 * dplyr::n()) : floor(0.70 * dplyr::n())) %>%
    summarize(
        tps=mean(tps),
        .groups='drop',
    ) %>%
    mutate(
        tlabel=sprintf("%0.1f", tps/1e3),
        vjust=ifelse(tps < 10e3, -0.2, 1.2),
        flabel=flabel2,
    )
    


df <- rbind(df_1, df_100) %>%
    assign_labels(
        name, name_s,
        c("vmcache"="vmcache", "libaio"="libaio", "+Fibers"="io_uring", "+RegBufs"="+RegBufs", "+Passthru"="+Passthru", "+IOPoll"="+IOPoll", "+SQPoll"="+SQPoll")
    ) %>%
    filter(!is.na(name_s))


p <- ggplot(df, aes(x=name_s, y=tps, fill=name_s)) +
    geom_col(position=position_dodge(), color='black', width=1) +
    geom_text(
        aes(label=tlabel, vjust=vjust),
        position=position_dodge(width=0.9),
        size=3.50,
    ) +
    guides(
        color="none",#guide_legend(title=NULL, nrow=2),
        fill="none",#guide_legend(title=NULL, nrow=2),
    )+
    scale_x_discrete(
        name=NULL,
        expand = expansion(add=0.8) 
    ) + 
    scale_y_continuous(
        name="Throughput [tx/s]",
        labels=function(x) sprintf("%0.0fK", x/1e3),
        limits=c(0, NA),
        #limits=c(0, 35e3),
        #breaks=seq(0, 550e3, by=100e3),
    ) +
    facet_wrap(. ~ flabel, scales="free_y") +
    theme(
        legend.title=element_text(),
        axis.title.x=element_blank(),
        axis.text.x=element_text(angle=35, hjust=1),
        #axis.text.x=element_blank(),
        #axis.ticks.x=element_blank(),
    )


p <- add_bar_arrow(
  p = p,
  data = df %>% filter(flabel==flabel1),
  x = "name_s",
  y = "tps",
  from = "vmcache",
  to = "io_uring",
  label  = "1.70x",
  text_angle=40,
  text_vjust=-3.0,
  text_hjust=0.8,
  fontface='bold',
  curvature=-0.70,
  curve_angle=-100,
  from_pad=-0.35,
  to_pad=0.20,
)



p <- add_bar_arrow(
  p = p,
  data = df %>% filter(flabel==flabel2),
  x = "name_s",
  y = "tps",
  from = "vmcache",
  to = "io_uring",
  label  = "12.5x",
  text_angle=60,
  text_vjust=-2.7,
  fontface='bold',
  curvature=-0.80,
  curve_angle=-130,
  from_pad=-0.40,
  to_pad=0.48,
)



p <- add_bar_arrow(
  p = p,
  data = df %>% filter(flabel==flabel2),
  x = "name_s",
  y = "tps",
  from = "io_uring",
  to = "+SQPoll",
  label  = "1.55x",
  text_angle=35,
  text_vjust=-2.1,
  text_hjust=1.1,
  fontface='bold',
  curvature=-0.30,
  curve_angle=-70,
  from_pad=0,
  to_pad=0.48,
)


dim=c(140, 55) #* c(0.45, 1)
file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname=sprintf("out/%s_tpcc_bar.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)


