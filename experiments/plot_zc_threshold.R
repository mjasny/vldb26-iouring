suppressPackageStartupMessages({
library(ggplot2)
library(dplyr)
library(sqldf)
library(stringr)
library(viridis)
library(ggrepel)
library(patchwork)
library(purrr)
})


source('./utils.R')



theme_set(theme_bw())
theme_update(
    #legend.title = element_blank(),
    legend.position = "top",
    # legend.margin=margin(0, b=-3, l=-10, unit='mm'),
    legend.margin=margin(0, b=-3, l=-5, unit='mm'),
    #legend.spacing.x = unit(0.3, 'mm'),
    legend.spacing.y = unit(0, 'mm'),
    legend.key.size=unit(3.5, 'mm'),
    axis.title.y=element_text(margin=margin(0, r=-1, unit = "mm")),
)
options(width=300)


df <- read.csv("data/bench_zc_threshold.csv", comment="#") 



col_name <- 'perf_cycles'
#col_name <- 'perf_kcycles'
#col_name <- 'perf_instructions'
#col_name <- 'perf_L1.misses'
#col_name <- 'perf_LLC.misses'
#col_name <- 'perf_branch.misses'
#col_name <- 'perf_task.clock'
#col_name <- 'perf_IPC'
#col_name <- 'perf_CPUs'
#col_name <- 'perf_GHz'
#col_name <- 'bw_avg'
#col_name <- 'bw_max'



df <- df %>%
    #filter(workload == "recv") %>%
    filter(!(flags %in% c("RFB--ZN"))) %>%
    filter(!(flags %in% c("RFN---", "RFBN--", "RFBMN-"))) %>%
    group_by(
        node, num_threads, core_id, connections, size, flags, workload, mtu,
    ) %>%
    summarize(
        bw = mean(bw_avg),
        mean_cycles = median(!!sym(col_name)),
        sd_cycles= sd(!!sym(col_name)),
        error_margin_cycles= 1.96 * sd(!!sym(col_name)) / sqrt(n()),
        .groups = "drop"
    ) %>%
    mutate(
        label=flags,
    ) %>%
    ungroup() %>%
    mutate(
        order=case_when(
            label=="------" ~ 0,
            label=="RF----" ~ 1,
            label=="RF---Z" ~ 2,
            label=="RFB--Z" ~ 3, 

            workload == "recv" & label=="RFB---" ~ 4, 
            workload == "recv" & label=="RFBM--" ~ 5, 
            workload == "recv" & label=='RFN---' ~ 3.5,
            workload == "recv" & label=='RFBN--' ~ 4.5,
            workload == "recv" & label=='RFBMN-' ~ 5.5,
            workload == "recv" & label=='R---Z' ~ 6,

            T ~ -1,
        ),
        label=case_when(
            #label=="------" ~ "Default",
            #label=="RF----" ~ "+RegFDs",
            label=="------" ~ "Default/RegRing/RegFDs",
            label=="RF---Z" ~ "ZeroCopy",
            label=="RFB--Z" ~ "ZeroCopy+RegBufs", 

            workload == "recv" & label=="RFB---" ~ "+RingBufs", 
            workload == "recv" & label=="RFBM--" ~ "+Multishot", 
            workload == "recv" & label=='RFN---' ~ "+RegFDs+Napi",
            workload == "recv" & label=='RFBN--' ~ "+RingBufs+Napi",
            workload == "recv" & label=='RFBMN-' ~ "+Multishot+Napi",
            workload == "recv" & label=='R---Z' ~ "+ZeroCopy",

            label=="RF---ZN" ~ "+ZeroCopyNapi",
            label=="RFB--ZN" ~ "+RegBufsNapi",
            label=="------H" ~ "PlainH",
            label=="RF----H" ~ "+RegFDsH",
            label=="RF---ZH" ~ "+ZeroCopyH",
            label=="RFB--ZH" ~ "+RegBufsH", 
            T ~ NA,
        ),
        label=reorder(label, order),
    ) %>%
    filter(!is.na(label)) %>%

    #filter(label %in% c("Default", "+RingBufs", "+RingBufs+Napi")) %>%
    #filter(label %in% c("Default", "+Multishot", "+Multishot+Napi")) %>%

    
    assign_labels(
        workload, flabel,
        c("send"="Send", "recv"="Receive")
    )

#options(dplyr.print_max = 1e9)
df %>%
    select(workload, connections, size, label, mean_cycles, sd_cycles, bw) %>%
    mutate(
        expected_bw = 3.7e9 / mean_cycles,
        reached_pct = (bw / expected_bw) * 100,
    ) %>%
    filter(connections==16) %>%
    filter(reached_pct <= 95) %>%
    arrange(reached_pct) %>%
    print()



df <- df %>% filter(!(workload=="send" & connections != 16))


#df <- df %>% mutate(label=sprintf("%s %d", label, core_id))


make_plot <- function(df, threshold, thresholdt) {
    df <- df %>%
        group_by(size) %>%
        arrange(-order, .by_group = TRUE) %>%
        mutate(
            ord = row_number(), 
            vjust_fix = -(ord - 1) * 1.2,
        ) %>%
        rowwise() %>% 
        mutate(
            #tlabel=format_size(bw, suffix='/s'),
            tlabel=ifelse(size %in% c(64, 512, 1024, 4*KiB, 64*KiB, 256*KiB, 1024*KiB), sprintf("%6.2f", mean_cycles), NA),
            tlabel=NA,
            tvjust=case_when(
                T ~ vjust_fix,
            ),
            ty=case_when(
                size <= 1024*4 ~ 0.1,
                size > 1024*4 ~ 8,
                T ~ mean_cycles,
            ),
        ) 
    breaks <- unique(df$size)[c(TRUE, FALSE)]

    p <- ggplot(df, aes(y=mean_cycles, x=size, color=label, shape=label)) +
        geom_line() +
        geom_point() +

        #geom_text(aes(label=tlabel, vjust=tvjust, y=ty), size=1.8, show.legend=F) +
        geom_text(aes(label=tlabel, vjust=tvjust, y=ty), size=2.4, show.legend=F) +

        scale_x_continuous(name="Buffer Size [log]", trans='log', breaks=breaks, label=fmt_bytes(unit='bin_bytes')) +
        scale_y_continuous(name="Cycles/Byte [log]", trans='log10', limits=c(0.1, NA)) +#, limits=c(0.0001, NA)) + 


        #scale_y_continuous(name="Cycles/Byte", limits=c(0, NA)) +
        #scale_y_continuous(name="Bandwidth", limits=c(0, NA), label=fmt_bytes(unit='bin_bytes')) +
        guides(
            color=guide_legend(title="Optimizations:", nrow=1, byrow=T),
            shape=guide_legend(title="Optimizations:", nrow=1, byrow=T),
        ) +
        facet_grid(flabel ~ .) #, scales='free_y')

    p <- p + theme(
        plot.margin=margin(0, l=2, unit='mm'),
    )

    p
}

# old at 10KiB
p <- make_plot(df %>% filter(workload=="send")) +
        geom_vline(xintercept=0.75*KiB, linetype="dashed", color="black", linewidth=0.5, alpha=0.6) +
        annotate("text", x=0.75*KiB, y=90, label="ZeroCopy Threshold",
                 hjust=-0.05, vjust="top", fontface="bold", color="black", size=3)


size=2.9
p <- annotate_points(
  plot = p,
  data = df %>% filter(workload=="send"),
  condition = size==512 & label=="ZeroCopy",
  label = "ZeroCopy has\nhigher overhead",
  lineheight=0.8,
  size = size,
  color='black',
  nudge_x=-0.7,
  nudge_y=-0.8,
  point.padding=1.5,
)
p <- annotate_points(
  plot = p,
  data = df %>% filter(workload=="send"),
  condition = size==1024 & label=="ZeroCopy",
  label = "ZeroCopy becomes\nmore efficient",
  lineheight=0.8,
  size = size,
  color='black',
  nudge_x=1.1,
  nudge_y=0.6,
  point.padding=1.5,
)

p <- add_vert_arrow(
  p = p,
  data = df %>% filter(workload=="send"),
  x = "size",
  y = "mean_cycles",
  x_value=800*KiB,
  pad_top=0.3,
  pad_bottom=0.1,
  extrapolate=T,
  from = "Default/RegRing/RegFDs",
  to = "ZeroCopy+RegBufs",
  label  = "2.90x fewer cycles",
  text_angle=0,
  text_hjust=0.75,
  fontface="bold",
  text_vjust=-1.5,
)




p2 <- make_plot(df %>% filter(workload=="recv")) + 
        geom_vline(xintercept=1.6*KiB, linetype="dashed", color="black", linewidth=0.5, alpha=0.6) +
        annotate("text", x=1.6*KiB, y=90, label="ZeroCopy Threshold", hjust=1.05,
                 vjust="top", fontface="bold", color="black", size=3) +
        geom_vline(xintercept=12*KiB, linetype="dashed", color="black", linewidth=0.5, alpha=0.6) +
        annotate("text", x=12*KiB, y=90, label="Multishot Threshold", hjust=-0.05,
                 vjust="top", fontface="bold", color="black", size=3)

p2 <- annotate_points(
  plot = p2,
  data = df %>% filter(workload=="recv"),
  condition = size==16*KiB & label=="+Multishot",
  label = "Normal Recv over-\ntakes Multishot Recv",
  lineheight=0.8,
  size = size,
  color='black',
  nudge_x=1.5,
  nudge_y=0.9,
  point.padding=1.5,
)
p2 <- annotate_points(
  plot = p2,
  data = df %>% filter(workload=="recv"),
  condition = size==2*KiB & label=="+Multishot",
  label = "ZeroCopy Recv over-\ntakes Multishot Recv",
  lineheight=0.8,
  size = size,
  color='black',
  nudge_x=-1.6,
  nudge_y=-0.8,
  point.padding=0.5,
)


p2 <- add_vert_arrow(
  p = p2,
  data = df %>% filter(workload=="recv"),
  x = "size",
  y = "mean_cycles",
  x_value=950*KiB,
  pad_top=0.3,
  pad_bottom=0.1,
  extrapolate=T,
  from = "Default/RegRing/RegFDs",
  to = "+ZeroCopy",
  label  = "3.54x fewer cycles",
  text_angle=0,
  text_hjust=1.02,
  fontface="bold",
  text_vjust=0.8,
)

#p <- (p + p2) + plot_layout(guides = "collect", ncol=1)
p <- (p | plot_spacer() | p2) + plot_layout(widths=c(1, 0.01, 1))
#p <- p2

file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
dim=c(140, 55) * c(2, 1)
fname=sprintf("out/%s.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)

