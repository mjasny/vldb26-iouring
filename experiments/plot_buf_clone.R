suppressPackageStartupMessages({
library(ggplot2)
library(dplyr)
library(sqldf)
library(stringr)
library(viridis)
library(scales)
library(tidyr)
})
source('./utils.R')

theme_set(theme_bw())
theme_update(
    legend.position = "top",
    #axis.title.x=element_blank(),
    legend.margin=margin(0, b=-3, l=0, unit='mm'),
    legend.key.size=unit(3.5, 'mm'),
    legend.text=element_text(margin=margin(0, l=1, unit='mm')),
    #legend.spacing.x = unit(0.3, 'mm'),
    #legend.spacing.y = unit(0, 'mm'),
    #axis.text.x=element_blank(),
    #axis.ticks.x=element_blank(),
)

#options(dplyr.print_max = 1e9)
options(width=300)

fname <- "data/bench_buf_clone.csv"

df <- read.csv(fname) %>%
    mutate(across(c(use_hugepages), ~ as.logical(.))) %>%
    filter(mem_size == 549755813888) %>%
    group_by(
        kernel,node,core_id,perfevent,use_hugepages,mem_size,mode,num_threads,chunk_size
    ) %>%
    summarize(
        outer_duration=mean(outer_duration),
        reg_duration=mean(reg_duration),
        clone_duration=mean(clone_duration),
        .groups='drop',
    ) %>%
    rowwise() %>%
    pivot_longer(
        cols = c(outer_duration, reg_duration, clone_duration), 
        names_to = "metric", 
        values_to = "value"
    ) %>%
    mutate(
        type=case_when(
            !use_hugepages ~ "4KiB",
            use_hugepages ~ "2MiB",
        ),
        value = value / 1e6,
        label=sprintf("%.2f", value),
        flabel=case_when(
            chunk_size >= 2**30 ~ sprintf("Entry-Size: %.0fGiB", chunk_size / 2**30),
            chunk_size >= 2**20 ~ sprintf("Entry-Size: %.0fMiB", chunk_size / 2**20),
            T ~ sprintf("Chunk-Size: %d", chunk_size),
        ),
        flabel=reorder(flabel, chunk_size),
        order=case_when(
            metric == 'reg_duration' ~ 0,
            metric == 'clone_duration' ~ 1,
        ),
        metric=case_when(
            metric == 'clone_duration' ~ "Cloned",
            metric == 'reg_duration' ~ "Registered",
        ),
    ) %>%
    filter(metric != "reg_duration")

print(df)





p <- ggplot(df, aes(x=reorder(type, use_hugepages), y=value, fill=reorder(metric, order), label=label)) +
    geom_col(position=position_dodge(), color='black') +
    geom_text(position=position_dodge(width=0.9), aes(vjust=ifelse(value > 1.9, 1.2, -0.2))) +
    #geom_errorbar(aes(ymax=lat+sd, ymin=lat-sd), width=0.2) +
    #geom_text(position=position_fill(vjust = 0.5)) +

    guides(
        fill=guide_legend(title=NULL, nrow=1, byrow=T),
        shape=guide_legend(title=NULL, nrow=1, byrow=T),
    ) +
    scale_x_discrete(name="Page Size") + 
    scale_y_continuous(
        name="Duration [sec]",
        limits=c(0, NA),
    ) +
    facet_grid(. ~ flabel) 



dim=c(140, 60) #* c(0.8, 1)
file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname=sprintf("out/%s.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)

