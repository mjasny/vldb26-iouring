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
source('./utils.R')

theme_set(theme_bw())
theme_update(
    #legend.title = element_blank(),
    legend.position = "top",
    legend.margin = margin(0, b = -3, l=-9, unit = "mm"),
    legend.spacing.x = unit(0.3, "mm"),
    legend.spacing.y = unit(0, "mm"),
    legend.key.size = unit(3.5, "mm"),
    legend.key.spacing=unit(0.5, 'mm'),
    legend.title=element_text(),
    axis.title.x=element_blank(),
    axis.text.x=element_blank(),
    axis.ticks.x=element_blank(),
    axis.title.y=element_text(margin=margin(0, r=-1, unit = "mm")),
)
options(width=300)


df <- read.csv("data/bench_async_tw.csv")

df <- df %>%
    mutate(across(c(mitigations, test_tw, pin_iowq), ~ as.logical(.))) %>%
    filter(node=='fn01') %>%
    filter(setup_mode=='sqpoll') %>%
    separate_rows(latency, sep = "\\|") %>%        # Split on "|"
    group_by(run, node, mitigations, nr_nops, test_tw) %>%
    mutate( # strip outliers at start and end of latency numbers
        n = n(),
        lower = ceiling(0.05 * n),
        upper = floor(0.95 * n)
    ) %>%
    slice((lower + 1):upper) %>%
    mutate(
        latency=as.numeric(latency)/1e3, # to us
    ) %>%
    mutate(
        flabel=setup_mode,
        flabel="NOPs",
    ) %>%
    assign_labels(
        test_tw, label,
        c("FALSE"="Inline\nCompletion", "TRUE"="Async Worker\n(forced)")
    ) %>%
    print()


df_text <- df %>%
    group_by(node, mitigations, nr_nops, test_tw, label) %>%
    summarise(
        lat = median(latency),
        .groups = "drop"
    ) %>%
    rowwise() %>%
    mutate(
        text=ifelse(lat < 1, sprintf("%.2fµs", lat), NA),
        y_pos=lat,
        vjust=ifelse(lat < 1, -0.2, 1.2),
    ) %>%
    print()


df_speedup <- df %>%
    group_by(node, mitigations, nr_nops, test_tw, label) %>%
    summarise(
        lat = median(latency),
        lat_max = max(latency),
        .groups = "drop"
    ) %>%
    group_by(node, mitigations, nr_nops) %>%
    summarise(
        lat_max = lat_max[test_tw],
        lat_tw = lat[test_tw],
        lat_inline  = lat[!test_tw],
        diff = lat_tw - lat_inline,
        .groups = "drop"
    ) %>%
    mutate(
        text=sprintf("+%0.1fµs", diff),
        arrow_x=1,
        arrow_xend=1.5,
        arrow_y=lat_inline+ lat_max*0.09,
        arrow_yend=lat_tw,
        text_x=1.05,
        text_y=(arrow_yend-arrow_y)*0.75 + arrow_y,
        text_angle=60,
    ) %>%
    print()


   
p <- ggplot(df, aes(x = label, y = latency, fill = label)) +
    geom_boxplot(outlier.shape = NA, alpha = 0.7, position = position_dodge(width = 0.75)) +

    geom_text(
        data=df_text, 
        inherit.aes = FALSE,
        aes(x=label, y=y_pos, label=text, vjust=vjust),
        angle=0,
        position=position_dodge(width=0.9),
        hjust=0.5,
        size=3,
    ) +

    geom_curve(data = df_speedup, aes(x = arrow_x, xend = arrow_xend, y = arrow_y, yend = arrow_yend), curvature=-0.20,
                 arrow = arrow(type = "closed", length = unit(1.50, "mm"), angle=30), color = "black", linewidth = 0.6, inherit.aes = FALSE) +
    geom_text(data = df_speedup, aes(x = text_x, y=text_y, label = text, angle=text_angle), size = 3.5, inherit.aes = FALSE) +



    scale_y_continuous(name="Latency [us]", limits=c(0, 10)) + 
    facet_nested(~ flabel, scales="free_y", independent = "y") +
    guides(fill = guide_legend(title=NULL, nrow = 1, byrow = TRUE))


file_base <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname <- sprintf("out/%s.pdf", file_base)
dim = c(140, 60) * c(0.35, 1)
ggsave(file = fname, plot = p, device = cairo_pdf, width=dim[1], height=dim[2], units = "mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)
