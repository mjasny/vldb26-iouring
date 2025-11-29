suppressPackageStartupMessages({
library(ggplot2)
library(dplyr)
library(sqldf)
library(stringr)
library(viridis)
library(scales)
})


source('./utils.R')

theme_set(theme_bw())
theme_update(
    #legend.title = element_blank(),
    legend.position = "top",
    # legend.margin=margin(0, b=-3, l=-10, unit='mm'),
    legend.margin=margin(0, b=-3, unit='mm'),
    #legend.spacing.x = unit(0.3, 'mm'),
    legend.spacing.y = unit(0, 'mm'),
    legend.key.size=unit(3.5, 'mm'),
    legend.text=element_text(margin=margin(0, l=0.25, unit='mm')),
)
options(width=300)



df <- read.csv("data/bench_ssd_bs.csv")
df <- df %>%
    mutate(across(c(reg_fds, reg_ring, reg_bufs, iopoll, nvme_cmds, mitigations, perfevent, write), ~ as.logical(.))) %>%
    group_by(
        duration, node, kernel, mitigations, iodepth, mode, num_ssds,
        perfevent, bs, write, reg_fds, reg_ring, reg_bufs, iopoll, nvme_cmds
    ) %>%
    summarize(
        iops_sd=sd(iops),
        iops=mean(iops),
        cycles=mean(cycles),
        instructions=mean(instructions),
        ipc=mean(ipc),
        l1_misses=mean(l1_misses),
        llc_misses=mean(llc_misses),
        branch_misses=mean(branch_misses),
        ioworker=max(ioworker),
        .groups='drop',
    ) %>%
    rowwise() %>%
    mutate(
        id=sprintf("%d%d%d%d%d", reg_ring, reg_fds, reg_bufs, nvme_cmds, iopoll),
        bps=iops*bs,
    ) %>%
    assign_labels(
        write, flabel2,
        c("FALSE"="Read", "TRUE"="Write")
    ) %>%
    assign_labels(
        id, label,
        c(
          #"00000"="Default",
          #"10000"="+RegRing",
          #"11000"="+RegFDs",
          "11000"="Default/RegRing/RegFDs",
          "11100"="+RegBufs",
          "11110"="+Passthru",
          "11111"="+IOPoll"
        )
    ) %>%
    filter(!is.na(label)) %>%
    mutate(
        bps=iops*bs,
        cycles_per_byte=cycles/bs,
    )

print(df)

breaks <- unique(df$bs)

rect_df <- df %>%
    group_by(flabel2) %>%
    slice(1) %>%  # one row per facet needed
    mutate(
           xmin = 512*1024,
           xmax = Inf, #max(df$bs),
           ymin = 0, #-Inf,
           ymax = Inf,
           label_x = 512*1024,
           label_y = 2,
    )


mask <- c(TRUE, FALSE, FALSE, TRUE, TRUE, TRUE)

pal_fun <- hue_pal() 
colors = pal_fun(6)[mask]

p <- ggplot(df, aes(y=cycles_per_byte, x=bs, color=label, shape=label)) +
    geom_line() +
    geom_point() +
    #geom_text(aes(label=tlabel, hjust=thjust, vjust=tvjust, y=ty), size=2, show.legend=F) +
    
    geom_rect(
        data = rect_df,
        aes(xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax),
        inherit.aes = FALSE,
        fill = "grey70", alpha = 0.3
    ) +
    geom_text(
        data = rect_df,
        aes(x=label_x, y=label_y, label="io-worker\n     active"),
        inherit.aes = FALSE,
        hjust = -0.20, vjust = 0.82,
        size = 3.5, lineheight = 0.8,
        fontface = "italic"
    ) +

    scale_color_manual(values = colors, drop = FALSE) +
    scale_x_continuous(name="Block Size [log]", trans='log', breaks=breaks, label=fmt_bytes(unit='bin_bytes')) +
    scale_y_continuous(name="Cycles/Byte [log]", trans='log10') +#, limits=c(0.0001, NA)) + 
    guides(
        color=guide_legend(title="Optimizations:", nrow=1, byrow=T),
        shape=guide_legend(title="Optimizations:", nrow=1, byrow=T),
    ) +
    facet_grid(flabel2 ~ .) #, scales='free_y')




size=2.9

#p <- annotate_points(
#  plot = p,
#  data = df,
#  condition = bs==64*KiB & !reg_bufs & !write,
#  label = "Registered Ring/FDs\nadd no benefit",
#  lineheight=0.8,
#  size = size,
#  color='black',
#  nudge_x=-0.2,
#  nudge_y=0.6,
#  point.padding=1.5,
#)
#p <- annotate_points(
#  plot = p,
#  data = df,
#  condition = bs==256*KiB & reg_bufs & !nvme_cmds & !write,
#  label = "RegBufs scales better→\nno user-kernel copy",
#  lineheight=0.8,
#  size = size,
#  color='black',
#  nudge_x=0.4,
#  nudge_y=0.80,
#  point.padding=0.8,
#)
p <- add_vert_arrow(
  p = p,
  data = df %>% filter(!write),
  x = "bs",
  y = "cycles_per_byte",
  x_value=450*KiB,
  pad_top=0.3,
  pad_bottom=0.1,
  extrapolate=T,
  from = "Default/RegRing/RegFDs",
  to = "+RegBufs",
  label  = "RegBufs avoids user-kernel\ncopy and scales better",
  text_angle=0,
  text_hjust=0.5,
  text_vjust=-0.9,
  lineheight=0.8,
)
p <- annotate_points(
  plot = p,
  data = df,
  condition = bs==256*KiB & iopoll & !write,
  label = "From here, IPoll saturates\nSSD and busy-spins",
  lineheight=0.8,
  size = size,
  color='black',
  nudge_x=-2.2,
  nudge_y=0.0,
  point.padding=1.0,
)


p <- annotate_points(
  plot = p,
  data = df,
  condition = bs==1024*KiB & !reg_bufs & write,
  label = "Auto-offload to worker-\npool adds overheads",
  lineheight=0.8,
  size = size,
  color='black',
  nudge_x=-1.5,
  nudge_y=0.4,
  point.padding=1.5,
)
p <- annotate_points(
  plot = p,
  data = df,
  condition = bs==128*KiB & iopoll & write,
  label = "From here, IPoll saturates\nSSD and busy-spins",
  lineheight=0.8,
  size = size,
  color='black',
  nudge_x=-1.5,
  nudge_y=-0.30,
  point.padding=1.0,
)
#p <- annotate_points(
#  plot = p,
#  data = df,
#  condition = bs==8*KiB & nvme_cmds & !iopoll & write,
#  label = "NVMe-Passthru by-\npasses storage stack",
#  lineheight=0.8,
#  size = size,
#  color='black',
#  nudge_x=-0.4,
#  nudge_y=-0.6,
#  point.padding=1.5,
#)



file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
dim=c(140, 60) + c(0, 20)
fname=sprintf("out/%s.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)

