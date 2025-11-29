suppressPackageStartupMessages({
library(ggplot2)
library(dplyr)
library(sqldf)
library(tidyr)
library(stringr)
library(viridis)
library(patchwork)
library(ggh4x)
})

theme_set(theme_bw())
theme_update(
    #legend.title = element_blank(),
    legend.position = "top",
    legend.margin = margin(0, b = -3, l=-7, unit = "mm"),
    legend.spacing.x = unit(0.3, "mm"),
    legend.spacing.y = unit(0, "mm"),
    legend.key.size = unit(3.5, "mm"),
    legend.key.spacing=unit(0.5, 'mm'),
    legend.title=element_text(margin=margin(0, r=3, unit = "mm")),
    axis.title.x=element_blank(),
    axis.text.x=element_blank(),
    axis.ticks.x=element_blank(),
)
options(width=300)

df <- read.csv("data/bench_sqpoll_wakeup.csv")

df <- df %>%
    filter(group=='200_100') %>%
    select(-min, -p50, -p90, -p95, -p99, -max) %>%
    mutate(
        wakeup=idle_ms < interval_ms,
        flabel=ifelse(do_nops, "NOPs", "SSD Reads"),
        label=ifelse(wakeup, "Wakeup\n(idle+start)", "Awake\n(running)"),
        group=reorder(group, idle_ms),
    ) %>%
    separate_rows(latency, sep = "\\|") %>%        # Split on "|"
    #separate(trace, into = c("ready", "ts", "duration"), sep = "~") %>% # Split on "~"
    group_by(run, node, ssd_id, ops, idle_ms, interval_ms, group, do_nops, mitigations) %>%
    mutate( # strip outliers at start and end of latency numbers
        n = n(),
        lower = ceiling(0.05 * n),
        upper = floor(0.95 * n)
    ) %>%
    slice((lower + 1):upper) %>%
    mutate(
        latency=as.numeric(latency)/1e3, # to us
    ) %>%
    print()

df %>%
    group_by(node, ssd_id, ops, group, do_nops, mitigations, kernel) %>%
    summarise(
        avg_awake  = mean(avg[!wakeup]) / 1e3,   # when wakeup==FALSE
        avg_wakeup = mean(avg[wakeup]) / 1e3,    # when wakeup==TRUE
        #n_awake  = sum(n[!wakeup]),   # when wakeup==FALSE
        #n_wakeup = sum(n[wakeup]),    # when wakeup==TRUE
        .groups = "drop"
    ) %>%
    print()

df_text <- df %>%
    group_by(node, ssd_id, ops, idle_ms, interval_ms, do_nops, mitigations, kernel) %>%
    summarise(
        lat = median(latency),
        .groups = "drop"
    ) %>%
    rowwise() %>%
    mutate(
        wakeup=idle_ms < interval_ms,
        flabel=ifelse(do_nops, "NOPs", "SSD Reads"),
        label=ifelse(wakeup, "Wakeup\n(idle+start)", "Awake\n(running)"),
        text=ifelse(!wakeup && lat < 10, sprintf("%.2fµs", lat), NA),
        y_pos=lat,
    ) %>%
    print()


df_speedup <- df %>%
    group_by(node, ssd_id, ops, idle_ms, interval_ms, do_nops, mitigations, kernel) %>%
    summarise(
        lat = median(latency),
        lat_max = max(latency),
        .groups = "drop"
    ) %>%
    mutate(
        wakeup=idle_ms < interval_ms,
    ) %>%
    group_by(node, ssd_id, ops, do_nops, mitigations, kernel) %>%
    summarise(
        lat_max = lat_max[wakeup],
        lat_wakeup = lat[wakeup],
        lat_awake  = lat[!wakeup],
        diff = lat_wakeup - lat_awake,
        .groups = "drop"
    ) %>%
    mutate(
        flabel=ifelse(do_nops, "NOPs", "SSD Reads"),
        text=sprintf("+%0.1fµs", diff),
        arrow_x=1,
        arrow_xend=1.5,
        arrow_y=lat_awake+ lat_max*0.09,
        arrow_yend=lat_wakeup,
        text_x=0.95,
        text_y=(arrow_yend-arrow_y)*0.75 + arrow_y,
        text_angle=ifelse(do_nops, 60, 45),
    ) %>%
    select(-node, -ssd_id, -ops, -mitigations, -kernel) %>%
    print()


p <- ggplot(df, aes(x = label, y = latency, fill = label)) +
    geom_boxplot(outlier.shape = NA, alpha = 0.7, position = position_dodge(width = 0.75)) +

    geom_text(
        data=df_text, 
        inherit.aes = FALSE,
        aes(x=label, y=y_pos, label=text),
        angle=0,
        position=position_dodge(width=0.9),
        hjust=0.5,
        vjust=-0.2,
        size=3,
    ) +
    geom_curve(data = df_speedup, aes(x = arrow_x, xend = arrow_xend, y = arrow_y, yend = arrow_yend), curvature=-0.20,
                 arrow = arrow(type = "closed", length = unit(1.50, "mm"), angle=30), color = "black", linewidth = 0.6, inherit.aes = FALSE) +
    geom_text(data = df_speedup, aes(x = text_x, y=text_y, label = text, angle=text_angle), size = 3.5, inherit.aes = FALSE) +


    scale_y_continuous(name="Latency [us]", limits=c(0, NA)) + 
    facet_nested(~ flabel, scales="free_y", independent = "y") +
    guides(fill = guide_legend(title = "SQPoll-Thread:", nrow = 1, byrow = TRUE))


file_base <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname <- sprintf("out/%s.pdf", file_base)
dim = c(140, 60) * c(0.60, 1)
ggsave(file = fname, plot = p, device = cairo_pdf, width=dim[1], height=dim[2], units = "mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)
