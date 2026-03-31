suppressPackageStartupMessages({
library(ggplot2)
library(dplyr)
library(scales)
})
source("./utils.R")

options(width=300)

theme_set(theme_bw())
theme_update(
    legend.position = "none",
    axis.title.y=element_text(margin=margin(0, r=-0.25, unit = "mm")),
    axis.title.x=element_text(margin=margin(0, t=-0.5, unit = "mm")),
)

read_threads <- 32
write_threads <- 16

df <- read.csv("data/bench_ssd_scaleout.csv")

df <- df %>%
    mutate(across(c(reg_fds, reg_ring, reg_bufs, iopoll, nvme_cmds, mitigations, perfevent, write, libaio, posix), ~ as.logical(.))) %>%
    filter(mitigations & num_ssds == 8) %>%
    filter(iodepth == 512) %>%
    group_by(
        run, num_threads, duration, node, kernel, mitigations, iodepth, mode, num_ssds,
        perfevent, bs, write, reg_fds, reg_ring, reg_bufs, iopoll, nvme_cmds, libaio, posix
    ) %>%
    summarize(
        iops=sum(iops),
        .groups="drop",
    ) %>%
    group_by(
        num_threads, duration, node, kernel, mitigations, iodepth, mode, num_ssds,
        perfevent, bs, write, reg_fds, reg_ring, reg_bufs, iopoll, nvme_cmds, libaio, posix
    ) %>%
    summarize(
        iops=mean(iops),
        .groups="drop",
    ) %>%
    mutate(
        id=sprintf("%d%d%d%d%d%d%d", libaio, reg_ring, reg_fds, reg_bufs, nvme_cmds, iopoll, mode=="sqpoll"),
    ) %>%
    assign_labels(
        id, method,
        c(
            "0110000"="io_uring\nDefault",
            "0111110"="io_uring\n+IOPoll"
        )
    ) %>%
    filter(!is.na(method)) %>%
    mutate(
        flabel=ifelse(write, "Write", "Read"),
        selected_threads=ifelse(write, write_threads, read_threads)
    ) %>%
    filter(num_threads == selected_threads) %>%
    select(flabel, method, iops)

spdk_df <- read.csv(text='\
flabel,method,iops
Read,SPDK,20650000
Write,SPDK,7670000
', comment="#")

plot_df <- bind_rows(df, spdk_df) %>%
    mutate(
        method=factor(method, levels=c("io_uring\nDefault", "io_uring\n+IOPoll", "SPDK")),
        flabel=factor(flabel, levels=c("Read", "Write")),
    )

idx <- c(2, 1, 4, 5, 6, 1)
pal_fun <- hue_pal()
colors <- pal_fun(6)[idx]

fill_values <- c(
    "io_uring\nDefault"=colors[2],
    "io_uring\n+IOPoll"=colors[5],
    "SPDK"="gray70"
)

p <- ggplot(plot_df, aes(x=method, y=iops, fill=method)) +
    geom_col(width=0.7, color="black") +
    geom_text(
        aes(label=sprintf("%.1fM", iops / 1e6)),
        vjust=1.4,
        size=3,
    ) +
    scale_fill_manual(values=fill_values, drop=FALSE) +
    scale_y_continuous(
        name="Throughput [IOPS]",
        labels=function(x) sprintf("%.0fM", x / 1e6),
        expand=expansion(mult=c(0, 0.12)),
    ) +
    scale_x_discrete(name=NULL) +
    facet_wrap(. ~ flabel, scales="free_y") +
    theme(
        strip.background=element_rect(fill="gray90"),
        panel.grid.major.x=element_blank(),
    )

dim <- c(115, 50)
file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname <- sprintf("out/%s.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=TRUE)
