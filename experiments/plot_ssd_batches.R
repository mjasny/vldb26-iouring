suppressPackageStartupMessages({
library(ggplot2)
library(dplyr)
library(sqldf)
library(stringr)
library(viridis)
library(scales)
library(tidyr)
library(patchwork)
library(xtable)
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
)
#options(dplyr.print_max = 1e9)
options(width=300)



fname = "data/bench_ssd_batches_latencies.csv"

df <- read.csv(fname)
df <- df %>%
    mutate(
        batch_size=as.integer(sub(".*_(\\d+)$", "\\1", label)),
        label=str_replace(label, '_', ': '),
        worker_l=reorder(sprintf("%d", worker), worker),
    ) %>%
    print()





df <- df %>%
    mutate(
        x=reorder(sprintf("%d", batch_size), batch_size),
    )

df_lat <- df

p <- ggplot(df, aes(x=x, y=lat)) +
    #geom_line() +
    #geom_point() +
    geom_boxplot(outlier.shape = '.', alpha = 0.7, position = position_dodge(width = 0.75)) +
    guides(
        fill=guide_legend(title="Worker:", nrow=1, reverse=FALSE),
    )+
    scale_x_discrete(
        name="Batch Size"
    ) + 
    scale_y_continuous(
        name="Latency [µs]",
        #labels=function(x) sprintf("%0.0fµs", x),
        #limits=c(ymin, ymax),
        limits=c(0, NA),
    ) #+
    #facet_grid(. ~ label, scale='free_x', space='free')
    




fname = "data/bench_ssd_batches_outstanding.csv"

df <- read.csv(fname)
df <- df %>%
    filter(ts < 5000) %>%
    mutate(
        batch_size=as.integer(sub(".*_(\\d+)$", "\\1", label)),
        label=str_replace(label, '_', ': '),
        x=reorder(sprintf("%d", batch_size), batch_size),
    )


df %>%
    group_by(label) %>%
    arrange(ts) %>%
    summarize(ios_ma = mean(ios)) %>%
    print()



p2 <- ggplot(df, aes(x=x, y=ios)) +
    geom_boxplot(outlier.shape = '.', alpha = 0.7, position = position_dodge(width = 0.75)) +
    #geom_line(linewidth=0.35) +
    #geom_point() + 
    guides(
        color='none', #guide_legend(title=NULL),
    )+
    scale_x_discrete(
        name="Batch Size",
    ) + 
    scale_y_continuous(
        name="Inflight IOs",
        limits=c(0, NA),
    ) 



p <- (p | p2) +
    plot_layout(guides = "collect") +
    theme(plot.margin=margin(0, unit='mm'))



file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
dim=c(140, 55) 
fname=sprintf("out/%s_box.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)





lat_stats <- df_lat %>%
    group_by(x) %>%
    summarise(
        lat_mean = mean(lat, na.rm = TRUE),
        lat_sd = sd(lat, na.rm = TRUE),
        )  %>%
    pivot_longer(cols = c(lat_mean, lat_sd),
        names_to = "metric",
        values_to = "value"
    )

io_stats <- df %>%
    group_by(x) %>%
    summarise(
        ios_mean = mean(ios, na.rm = TRUE),
        ios_sd   = sd(ios,  na.rm = TRUE)
    ) %>%
    pivot_longer(cols = c(ios_mean, ios_sd),
        names_to = "metric",
        values_to = "value"
    )

stats_wide <- rbind(lat_stats, io_stats) %>%
  pivot_wider(names_from = x, values_from = value) %>%
  mutate(across(
    -metric,
    ~ ifelse(metric %in% c("ios_sd", "lat_sd"),
             sprintf("±%.2f", .x),
             sprintf("%.2f", .x))
  ))

print(xtable(stats_wide), include.rownames = FALSE)

