suppressPackageStartupMessages({
library(ggplot2)
library(dplyr)
library(ggh4x)
})
source("./utils.R")

options(width=300)

theme_set(theme_bw())
theme_update(
    legend.position = "top",
    legend.margin=margin(0, b=-3, l=0, unit='mm'),
    legend.spacing.x = unit(3, 'mm'),
    legend.spacing.y = unit(0, 'mm'),
    legend.key.size=unit(3.5, 'mm'),
    legend.key.spacing.x = unit(0.3, 'mm'),
    legend.text=element_text(margin=margin(0, l=0.25, unit='mm')),
    axis.title.y=element_text(margin=margin(0, r=-0.25, unit = "mm")),
    axis.title.x=element_text(margin=margin(0, t=-0.5, unit = "mm")),
)

#selected_num_sockets <- c(8, 16, 32)
selected_num_sockets <- c(32)

df_runs <- read.csv("data/bench_remotescan.csv") %>%
    mutate(across(c(reg_ring, reg_fds, reg_bufs, napi, send_zc, pin_queues), ~ as.logical(.))) %>%
    #filter(!reg_ring & !reg_fds & !reg_bufs & !napi & !send_zc & !pin_queues) %>%
    #filter(pin_queues) %>%
    filter(num_workers <= 32) %>%
    filter(request_size %in% c(4096, 16*1024, 64*1024)) %>%
    mutate(
        throughput_Bps=client_bandwidth_Bps,
        flabel=reorder(sprintf("%d KiB Pages", request_size / 1024), request_size),
        socket_label=factor(
            sprintf("%d sockets", num_sockets),
            levels=sprintf("%d sockets", sort(unique(num_sockets)))
        ),
        name_key=sprintf("%d%d%d%d%d%d%d", backend=="io_uring", reg_ring, reg_fds, reg_bufs, napi, send_zc, pin_queues),
    ) %>%
    assign_labels(
        name_key, name,
        c("0000000"="libaio+epoll",
          "1000000"="io_uring",
          "1110000"="+RegFDs",
          "1111000"="+RegBufs",
          "1111010"="+SendZC",
          "1111011"="+PinQueues"
        )
    )

if (length(selected_num_sockets) > 0) {
    df_runs <- df_runs %>%
        filter(num_sockets %in% selected_num_sockets)
}


df <- df_runs %>%
    group_by(backend, name, flabel, request_size, num_workers, num_sockets, socket_label) %>%
    summarize(
        n_runs=n(),
        throughput_Bps=mean(throughput_Bps),
        throughput_Bps_var=var(throughput_Bps),
        throughput_Bps_sd=sd(throughput_Bps),
        .groups="drop",
    ) %>%
    print(n=1000)

link_limit_df <- data.frame(
    flabel=factor("64 KiB Pages", levels=levels(df$flabel)),
    throughput_Bps=400 * 10^9 / 8
)


p <- ggplot(df, aes(x=num_workers, y=throughput_Bps, color=name, shape=name)) +
    geom_hline(
        data=link_limit_df,
        aes(yintercept=throughput_Bps),
        linetype="dashed",
        color="black",
        linewidth=0.3
    ) +
    geom_text(
        data=link_limit_df,
        aes(x=1, y=throughput_Bps, label="Link Limit"),
        inherit.aes=FALSE,
        hjust=0,
        vjust=-0.15,
        size=2.5,
        color="black"
    ) +

    geom_line(alpha=0.4) +
    geom_point() +
    scale_x_continuous(
        name="Worker threads [log]",
        breaks=2**seq(0, 8, by=1),
        trans="log",
    ) +
    #scale_y_continuous(
    #    name="Throughput",
    #    #limits=c(0, 2**30 * 50),
    #    #breaks=2**30 * seq(0, 50, by=10),
    #    labels=fmt_bytes(unit='bin_bytes', suffix='/s'),
    #) +
    guides(
        color=guide_legend(title=NULL),
        shape=guide_legend(title=NULL),
    ) +
    facet_wrap(. ~ flabel, scales="free_y") +
    ggh4x::facetted_pos_scales(
      y = list(
        scale_y_continuous(
          name="Throughput",
          limits = c(0, 11 * 2^30),
          breaks=2**30 * seq(0, 10, by=2),
          labels = fmt_bytes(unit = "bin_bytes", suffix = "/s")
        ),
        scale_y_continuous(
          name="Throughput",
          limits = c(0, 35 * 2^30),
          breaks=2**30 * seq(0, 35, by=5),
          labels = fmt_bytes(unit = "bin_bytes", suffix = "/s")
        ),
        scale_y_continuous(
          name="Throughput",
          limits = c(0, 50 * 2^30),
          breaks=2**30 * seq(0, 50, by=10),
          labels = fmt_bytes(unit = "bin_bytes", suffix = "/s")
        )
      )
    )


if (length(selected_num_sockets) == 1) {
    p <- p + guides(shape="none")
}

dim <- c(140, 60)
file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname <- sprintf("out/%s.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=TRUE)


df_simple <- df %>%
    filter(name %in% c("libaio+epoll", "io_uring", "+SendZC")) %>%
    assign_labels(
        name, name_simple,
        c(
            "libaio+epoll"="libaio+epoll",
            "io_uring"="io_uring",
            "+SendZC"="io_uring (optimized)"
        )
    )


p <- ggplot(df_simple, aes(x=num_workers, y=throughput_Bps, color=name_simple, shape=name_simple)) +
    geom_hline(
        data=link_limit_df,
        aes(yintercept=throughput_Bps),
        linetype="dashed",
        color="black",
        linewidth=0.3
    ) +
    geom_text(
        data=link_limit_df,
        aes(x=1, y=throughput_Bps, label="Link Limit"),
        inherit.aes=FALSE,
        hjust=0,
        vjust=-0.15,
        size=2.5,
        color="black"
    ) +
    geom_line() +
    geom_point() +
    scale_x_continuous(
        name="Worker threads [log]",
        breaks=2**seq(0, 8, by=1),
        trans="log",
    ) +
    guides(
        color=guide_legend(title=NULL),
        shape=guide_legend(title=NULL),
    ) +
    facet_wrap(. ~ flabel, scales="free_y") +
    ggh4x::facetted_pos_scales(
      y = list(
        scale_y_continuous(
          name="Throughput",
          limits = c(0, 11 * 2^30),
          breaks=2**30 * seq(0, 10, by=2),
          labels = fmt_bytes(unit = "bin_bytes", suffix = "/s")
        ),
        scale_y_continuous(
          name="Throughput",
          limits = c(0, 35 * 2^30),
          breaks=2**30 * seq(0, 35, by=5),
          labels = fmt_bytes(unit = "bin_bytes", suffix = "/s")
        ),
        scale_y_continuous(
          name="Throughput",
          limits = c(0, 50 * 2^30),
          breaks=2**30 * seq(0, 50, by=10),
          labels = fmt_bytes(unit = "bin_bytes", suffix = "/s")
        )
      )
    )

p <- add_gap_arrow(
  p = p,
  data = df_simple,
  condition=request_size==4*1024 & num_workers==32,
  pad_top=0.1,
  pad_bottom=0.1,
  pad_h=-0.3,
  extrapolate=T,
  from = "libaio+epoll",
  to = "io_uring (optimized)",
  label  = "1.25x",
  text_angle=0,
  text_hjust=1.15,
  text_vjust=0.5,
  fontface="bold",
)

p <- add_gap_arrow(
  p = p,
  data = df_simple,
  condition=request_size==16*1024 & num_workers==32,
  pad_top=0.1,
  pad_bottom=0.1,
  pad_h=-0.2,
  extrapolate=T,
  from = "libaio+epoll",
  to = "io_uring (optimized)",
  label  = "1.40x",
  text_angle=0,
  text_hjust=1.15,
  text_vjust=0.5,
  fontface="bold",
)

p <- add_gap_arrow(
  p = p,
  data = df_simple,
  condition=request_size==64*1024 & num_workers==16,
  pad_top=0.2,
  pad_bottom=0.2,
  pad_h=0,
  extrapolate=T,
  from = "libaio+epoll",
  to = "io_uring (optimized)",
  label  = "1.67x",
  text_angle=0,
  text_hjust=1.05,
  text_vjust=0.5,
  fontface="bold",
)



dim <- c(140, 50)
fname <- sprintf("out/%s_simple.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=TRUE)
