suppressPackageStartupMessages({
library(ggplot2)
library(dplyr)
library(sqldf)
library(stringr)
library(viridis)
library(scales)
library(ggrepel)
library(geomtextpath)
library(patchwork)
})
source('./utils.R')


options(width=300)

theme_set(theme_bw())
theme_update(
    #legend.title = element_blank(),
    legend.position = "top",
    # legend.margin=margin(0, b=-3, l=-10, unit='mm'),
    legend.margin=margin(0, b=-3, l=-12, unit='mm'),
    legend.spacing.x = unit(0.3, 'mm'),
    legend.spacing.y = unit(0, 'mm'),
    legend.key.size=unit(3.5, 'mm'),
    legend.text=element_text(margin=margin(0, l=0.25, unit='mm')),
    axis.title.y=element_text(margin=margin(0, r=-0.25, unit = "mm")),
    axis.title.x=element_text(margin=margin(0, t=-0.5, unit = "mm")),
)


df <- read.csv("data/bench_ssd_scaleout.csv") 

df <- df %>%
    mutate(across(c(reg_fds, reg_ring, reg_bufs, iopoll, nvme_cmds, mitigations, perfevent, write, libaio, posix), ~ as.logical(.))) %>%
    filter(mitigations & num_ssds==8) %>%
    filter(iodepth==512) %>%
    group_by(
        run, num_threads, duration, node, kernel, mitigations, iodepth, mode, num_ssds,
        perfevent, bs, write, reg_fds, reg_ring, reg_bufs, iopoll, nvme_cmds, libaio, posix
    ) %>%
    summarize(
        iops=sum(iops),
        .groups='drop',
    ) %>%
    group_by(
        num_threads, duration, node, kernel, mitigations, iodepth, mode, num_ssds,
        perfevent, bs, write, reg_fds, reg_ring, reg_bufs, iopoll, nvme_cmds, libaio, posix
    ) %>%
    summarize(
        iops_sd=sd(iops),
        iops=mean(iops),
        .groups='drop',
    ) %>%
    rowwise() %>%
    print(n=2) %>%
    mutate(
        id=sprintf("%d%d%d%d%d%d%d", libaio, reg_ring, reg_fds, reg_bufs, nvme_cmds, iopoll, mode=='sqpoll'),
    ) %>%
    assign_labels(
        id, label,
        c(
          "1000000"="libaio",
          #"0000000"="io_uring",
          #"0100000"="+RegRing",
          #"0110000"="+RegFDs",
          "0110000"="Default/RegRing/RegFDs",
          "0111000"="+RegBufs",
          "0111100"="+Passthru",
          "0111110"="+IOPoll"#,
          #"0111111"="+SQPoll"
        )
    ) %>%
    filter(!is.na(label)) %>%
    assign_labels(
        write, flabel,
        c("FALSE"="Read", "TRUE"="Write")
    ) %>%
    print()




df_labels <- df %>%
  group_by(label, write) %>%
  mutate(
    hjust=case_when(
        label=="libaio" ~ 0.52,
        label=="+RegBufs" ~ 0.92,
        label=="+Passthru" ~ 0.75,
        label=="+IOPoll" ~ 0.7,
        #!use_hashtable & name_key=="zc_send=1 zc_recv=1" ~ 0.75,
        T ~ 0.97,
    ),
    vjust=case_when(
        label=="libaio" ~ 1.2,
        grepl("Default", label) ~ 1.8,
        #name_key=="zc_send=1 zc_recv=0" ~ 1.2,
        #name_key=="zc_send=1 zc_recv=1" ~ -0.2,
        T ~ -0.2,
    ),
  ) %>%
  ungroup()



print(df, n=1000)



df %>%
    speedup(
        label=="libaio", iops,
        by=c("write", "num_threads")
    ) %>%
    mutate(speedup=1/speedup) %>%
    filter((!write & num_threads==16) | (write & num_threads==4)) %>%
    select(write, num_threads, label, iops, speedup) %>%
    print()




# SSD RandRead: max 2,450K IOPS
# SSD RandWrite: max 310K IOPS


hline_df <- data.frame(
  flabel = c("Read", "Write"),
  yint      = c(21e6, 7.8e6),
  label     = c("SSDs Limit", "SSDs Limit")
)


idx <- c(2, 1, 4, 5, 6, 1)
pal_fun <- hue_pal() 
colors = pal_fun(6)[idx]

p <- ggplot(df, aes(y=iops, x=num_threads, color=label)) +
    geom_hline(
        data = hline_df,
        aes(yintercept = yint),
        linetype = "dashed",
        color = "black",
        linewidth = 0.5,
        show.legend=F,
    ) +
    geom_text(
        data = hline_df,
        aes(x = 1, y = yint, label = label),
        hjust = 0.1, vjust = 1.4,
        color = "black",
        size = 2.5, fontface = "bold",
        show.legend=F,
    ) +


    geom_line() +
    geom_point(aes(shape=label)) +

    #geom_textline(
    #    data=df_labels,
    #    aes(label=label, hjust=hjust, vjust=vjust),
    #    size=3,
    #    text_smoothing=30
    #) +


    scale_x_continuous(
        name="Worker threads [log]",
        breaks=2**seq(0, 8, by=1),
        trans='log',
    ) +

    scale_y_continuous(
        name="Throughput [IOPS]",
        labels=function(x) sprintf("%.0fM", x/1e6),
        #limit=c(0, 21.5e6),
    ) +
    scale_color_manual(values = colors, drop = FALSE) +
    theme(
        #legend.position=c(.22,.65)
    ) +
    guides(
        #color=guide_legend(title=NULL, ncol=1, byrow=T),
        #shape=guide_legend(title=NULL, ncol=1, byrow=T),
        color=guide_legend(title="Method:", nrow=1, byrow=T),
        shape=guide_legend(title="Method:", nrow=1, byrow=T),
    ) +
    facet_wrap(. ~ flabel, scales='free_y') 


p <- add_vert_arrow(
  p = p,
  data = df %>% filter(!write),
  x = "num_threads",
  y = "iops",
  x_value=18,
  pad_top=0.1,
  pad_bottom=0.1,
  extrapolate=T,
  from = "libaio",
  to = "+IOPoll",
  label  = "3.51x",
  text_angle=0,
  text_hjust=1.1,
  text_vjust=0.3,
  lineheight=0.8,
)


p <- add_vert_arrow(
  p = p,
  data = df %>% filter(write),
  x = "num_threads",
  y = "iops",
  x_value=4.7,
  pad_top=0.1,
  pad_bottom=0.1,
  extrapolate=T,
  from = "libaio",
  to = "+IOPoll",
  label  = "3.37x",
  text_angle=0,
  text_hjust=1.1,
  text_vjust=0.5,
  lineheight=0.8,
)


dim=c(140, 55) 
file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname=sprintf("out/%s.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)

