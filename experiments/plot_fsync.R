suppressPackageStartupMessages({
library(ggplot2)
library(dplyr)
library(sqldf)
library(stringr)
library(viridis)
library(scales)
library(tidyr)
library(patchwork)
library(ggh4x)
})
source('./utils.R')

theme_set(theme_bw())
theme_update(
    legend.position = "top",
    axis.title.x=element_blank(),
    legend.margin=margin(0, b=-3, l=4, unit='mm'),
    legend.key.size=unit(3.5, 'mm'),
    legend.text=element_text(margin=margin(0, l=0.25, unit='mm')),
    #legend.spacing.x = unit(0.3, 'mm'),
    #legend.spacing.y = unit(0, 'mm'),
    #axis.text.x=element_blank(),
    #axis.ticks.x=element_blank(),
    panel.spacing.x = unit(0, "mm"),

    strip.text.x=element_text(margin=margin(b=1, t=1)),
    axis.text.x=element_text(angle=40, hjust=0.8, lineheight=0.75, margin=margin(t=0, unit='mm')),
)

#options(dplyr.print_max = 1e9)
options(width=300)



df <- read.csv("data/bench_fsync.csv")

df <- df %>%
    mutate(across(c(mitigations, reg_ring, reg_fds, reg_bufs, iopoll, pin_iowq, measure_lat), ~ as.logical(.))) %>%
    filter(measure_lat) %>%
    filter(!(method %in% c('fsync'))) %>%
    filter(!(method %in% c('open_sync', 'write_sync'))) %>%
    group_by(
        duration, node, kernel, mitigations, reg_ring, reg_fds, reg_bufs, iopoll, perfevent, measure_lat, pin_iowq, max_workers,
        setup_mode, method, ssd
    ) %>%
    rename(
        lat_n=lat_samples,
        lat_sd=lat_std
    ) %>%
    summarize(
        #lat_avg=mean(lat_avg),
        #lat_min=min(lat_min),
        #lat_max=max(lat_max),
        total_n = sum(lat_n),
        lat_avg = sum(lat_avg * lat_n) / total_n,
        lat_var = sum(lat_n * (lat_sd^2 + lat_avg^2)) / total_n - lat_avg^2,
        lat_sd  = sqrt(lat_var),
        lat_min = min(lat_min),
        lat_max = max(lat_max),

        ops_avg=mean(ops_avg),
        ops_min=min(ops_min),
        ops_max=max(ops_max),
        ioworker=max(ioworker),
        .groups='drop',
    ) %>%
    rowwise() %>%
    mutate(
        #lat_sd=(lat_max-lat_min)/2,
        across(starts_with("lat_"), ~ .x / 1e3),

        method=ifelse(pin_iowq, sprintf("%s_w=%d", method, max_workers), method),
        flabel=sprintf('1x 4KiB iopoll=%d', iopoll),
        flabel2=setup_mode,
        label=case_when(
            #iops >= 1e6 ~ sprintf("%.2fM", iops/1e6),
            #iops >= 1e3 ~ sprintf("%.2fK", iops/1e3),
            #T ~ sprintf("w=%d", ioworker),
            #T ~ sprintf("%.0f", lat_avg),
            T ~ ifelse(ioworker==0, sprintf("%.2f", lat_avg), sprintf("%.2f\n%d iowq", lat_avg, ioworker)),
        ),
        vjust=case_when(
            #type=='pread' ~ -0.2,
            T ~ 1.2,
        ),
    )

df2 <- df %>%
    filter(node == 'c08' & grepl('3n1', ssd)) %>%
    filter(!(node == 'c08' & method %in% c('none', 'nvme_passthru'))) %>%

    filter(method!='write_dsync') %>%
    filter(!(!iopoll & grepl('fsync_link2', method) & pin_iowq)) %>%
    filter(!(!iopoll & method=='fsync_link_w=0')) %>%
    filter(!(iopoll & method=='open_dsync')) %>%
    filter(setup_mode=='defer') %>%
    mutate(
        flabel="Consumer SSD",
        flabel2=ifelse(iopoll, "IOPOLL", "Default"),
        across(starts_with("lat_"), ~ .x / 1e3), # to ms
        label=sprintf("%.2f", lat_avg),
    ) %>%
    assign_labels(
        method, method,
        c(
          "fsync_link"="Fsync\n(linked)","fsync_link_w=8"="+other\nChiplet", "fsync_link2"="Fsync\n(manual)",
          "nvme_passthru_flush"="Passthru\n+Flush",
           "open_dsync"="OpenSync/\nWriteSync"
        )
    )


rect_df <- df2 %>%
    ungroup() %>%
    filter(!iopoll) %>%
    slice(1) %>%  # one row per facet needed
    mutate(
           xmin = 0.5,
           xmax = 3.5,
           ymin = -Inf,
           ymax = Inf,
           label_x = 0.55,
           label_y = 4000 / 1e3
    )

segment_data <- df2 %>%
    ungroup() %>%
    filter(!iopoll) %>%
    slice(1) %>%  # one row per facet needed
    mutate(
        x=1,
        xend=2,
        y_start=1.9,
        y_end=2,

        text_x=1.5,
        text_y=2.6,
        label='1.05x',
        text_angle=0,
    )


p2 <- ggplot(df2, aes(x=method, y=lat_avg, fill=method)) +
    geom_col(position=position_dodge(), color='black') +
    geom_errorbar(
        aes(
            ymin = lat_avg - lat_sd,
            ymax = lat_avg + lat_sd, 
        ),
        width = 0.2,  # Width of the error bars
        position = position_dodge(width = 0.9)  # Align error bars with the bars
    ) +
    #geom_text(position=position_dodge(width=0.9), aes(label=label, vjust=vjust)) +
    geom_text(position=position_dodge(width=0.9), aes(label=label, vjust=-0.1, y=0)) +

    geom_rect(
        data = rect_df,
        aes(xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax),
        inherit.aes = FALSE,
        fill = "grey70", alpha = 0.3
    ) +
    geom_text(
        data = rect_df,
        aes(x=label_x, y=label_y, label="io-worker active"),
        inherit.aes = FALSE,
        hjust = 0, vjust = 1,
        size = 3.5,
        fontface = "italic"
    ) +

    geom_curve(data = segment_data, aes(x=x, xend=xend, y= y_start, yend = y_end), curvature=-0.71,
                 arrow = arrow(type = "closed",length = unit(1.50, "mm"), angle=30), color = "black", size = 0.8, inherit.aes = FALSE) +
    geom_text(data = segment_data, aes(x = text_x, y = text_y, label = label, angle=text_angle), size = 3.5, inherit.aes = FALSE) +


    guides(
        fill='none', #guide_legend(title=NULL, nrow=2, byrow=T),
        shape='none', #guide_legend(title=NULL, nrow=1, byrow=T),
    ) +
    scale_x_discrete(name="Page Size") + 
    scale_y_continuous(
        name="Latency [ms]",
        limits=c(0, NA),
        #labels=function(x) sprintf("%.1fM", x/1e6),
        #limit=c(0, 21.5e6),
    ) +
    #facet_grid(flabel2 ~ flabel) 
    facet_nested(~ flabel + flabel2, scale='free_x', space='free') +
    theme(
        legend.margin=margin(0, b=-3, l=-10, unit='mm'),
    )




df <- df %>%
    filter(node == 'fn01' & grepl('8n1', ssd)) %>%
    filter(!(node=='fn01' & method == 'nvme_passthru_flush')) %>%
    filter(node=='fn01' & method %in% c('none', 'nvme_passthru')) %>%
    filter(setup_mode=='defer') %>%
    mutate(
        flabel="Enterprise SSD",
        flabel2=ifelse(iopoll, "IOPOLL", "Default"),
        label=sprintf("%.2f", lat_avg),
    ) %>%
    assign_labels(
        method, method,
        c("none"="Write", "nvme_passthru"="Passthru")
    )

p <- ggplot(df, aes(x=method, y=lat_avg, fill=method)) +
    geom_col(position=position_dodge(), color='black') +
    geom_errorbar(
        aes(
            ymin = lat_avg - lat_sd, #- error_margin_iops,
            ymax = lat_avg + lat_sd, # + error_margin_iops
        ),
        width = 0.2,  # Width of the error bars
        position = position_dodge(width = 0.9)  # Align error bars with the bars
    ) +
    #geom_text(position=position_dodge(width=0.9), aes(label=label, vjust=vjust)) +
    geom_text(position=position_dodge(width=0.9), aes(label=label, vjust=-0.1, y=0)) +

    guides(
        fill='none', #guide_legend(title=NULL, nrow=1, byrow=T),
        shape='none', #guide_legend(title=NULL, nrow=1, byrow=T),
    ) +
    scale_x_discrete(name="Page Size") + 
    scale_y_continuous(
        name="Latency [us]",
        limits=c(0, NA),
        #labels=function(x) sprintf("%.1fM", x/1e6),
        #limit=c(0, 21.5e6),
    ) +
    #facet_grid(flabel2 ~ flabel) 
    facet_nested(~ flabel + flabel2, scale='free_x') 


p <- (p2 | p) +
    plot_layout(guides = "collect", widths=c(7, 4.5)) +
    theme(
        plot.margin=margin(0, unit='mm'),
    )

dim=c(140, 55) #* c(0.5, 1)
dim=dim + c(0, 10)
#dim=dim*2
file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname=sprintf("out/%s.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)

