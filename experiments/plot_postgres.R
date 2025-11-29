suppressPackageStartupMessages({
library(ggplot2)
library(dplyr)
library(sqldf)
library(stringr)
library(viridis)
library(scales)
library(tidyr)
library(ggh4x)
})

source('./utils.R')

theme_set(theme_bw())
theme_update(
    #legend.title = element_blank(),
    legend.position = "top",
    # legend.margin=margin(0, b=-3, l=-10, unit='mm'),
    legend.margin=margin(0, b=-3, unit='mm'),
    legend.spacing.x = unit(0.3, 'mm'),
    legend.spacing.y = unit(0, 'mm'),
    legend.key.size=unit(3.5, 'mm'),
    #axis.text.x=element_blank(),
    #axis.ticks.x=element_blank(),
    axis.text.x=element_text(angle=20, hjust=0.7),
)
#options(dplyr.print_max = 1e9)
options(width=300)




fname = "data/bench_postgres.csv"
df <- read.csv(fname)

df <- df %>%
    filter(iodirect=='data+wal' & method=='io_uring') %>%
    mutate(
        w=as.integer(str_extract(tag, "(?<=_)\\d+(?=w)")),
        mode=str_extract(tag, "(?<=mode)\\d+"),
        name=str_extract(tag, "(?<=io_)\\w+"),
        iodepth=as.integer(str_extract(tag, "(?<=w_)\\d+(?=io)")),
        
        mode=case_when( #backwards compatibility
            is.na(mode) & is.na(name) ~ 0,
            is.na(mode) & name=="fix" ~ 7,
            is.na(mode) & name=="fix_coop" ~ 2,
            is.na(mode) & name=="fix_iopoll" ~ 3,
            is.na(mode) & name=="fix_sqpoll" ~ 6,
            T ~ as.integer(mode),
        ),
    ) %>%
    group_by(
        method, iodirect, mode, w, iodepth
    ) %>%
    summarize(
        duration = mean(ms),
        sd_duration = sd(ms),
        .groups = "drop"
    )



df <- df %>%
    speedup(
        mode==0, duration,
        by=c("method", "iodirect", "w", "iodepth")
    ) %>%
    mutate(
        facet_label=sprintf("%d Worker%s", w, ifelse(w>1, "s", "")),
        facet_label2=reorder(sprintf("IO-Depth: %d", iodepth), iodepth),
    ) %>%
    filter(w %in% c(1, 2, 4, 8)) %>%
    filter(iodepth==16)  %>%
    filter(mode %in% c(0,1,2,3,4,5,6,7)) %>%
    assign_labels(
        mode, mode_label,
        c("0"="Baseline", "1"="+RegBufs", "2"="+RegBufs", "7"="+RegBufs", "3"="+IOPoll",
          "4"="+SQPoll", "5"="+SQPoll", "6"="+SQPoll")
    ) %>% 
    print() %>%
    group_by(mode_label, facet_label) %>%
    slice_max(order_by = speedup, n = 1, with_ties = FALSE) %>%
    mutate(
        speedup=(speedup-1)*100,
        label=sprintf("%0.2f%%", speedup),
        label_sec=sprintf("%0.2fs", duration/1e3),
    )



baseline_df <- df %>%
    filter(mode == 0) %>%
    mutate(
        label=sprintf("Baseline: %s", label_sec),
    )

df <- df %>%
    filter(mode != 0)

print(df, n=1000)
print(baseline_df, n=1000)


p <- ggplot(df, aes(x=mode_label, y=speedup, fill=mode_label)) +
    geom_col(position=position_dodge(), color='black') +
    geom_text(position=position_dodge(width=0.9), aes(label=label, vjust=-0.2), size=2.5) +
    geom_text(position=position_dodge(width=0.9), aes(label=label_sec, vjust=1.4), size=2.5) +

    geom_label(data = baseline_df,
        aes(x = 2, y = 0, label = label),
        inherit.aes = FALSE,
        hjust = 0.5, vjust = -0.1, #1.5,
        fontface="bold",
        size = 3, lineheight = 0.9
    ) +

    guides(
        fill='none', #guide_legend(title="Features enabled:", nrow=1, reverse=FALSE),
    )+
    scale_x_discrete(name=NULL) + 
    scale_y_continuous(
        name="Speedup vs. Baseline",
        limits=c(0, 16),
        labels=function(x) sprintf("%d%%", x),
    ) +
    facet_grid(.  ~ facet_label)  

file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
dim=c(140, 55)
fname=sprintf("out/%s.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)

