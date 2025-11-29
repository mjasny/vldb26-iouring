suppressPackageStartupMessages({
library(ggplot2)
library(dplyr)
library(sqldf)
library(stringr)
library(viridis)
library(patchwork)
library(ggpattern)
library(tidyr)
})
source('./utils.R')

theme_set(theme_bw())
theme_update(
    #legend.title = element_blank(),
    legend.position = "top",
    # legend.margin=margin(0, b=-3, l=-10, unit='mm'),
    legend.margin=margin(0, b=-3, l=-11, unit='mm'),
    legend.title=element_text(size=9),
    legend.spacing.x = unit(13.3, 'mm'),
    legend.spacing.y = unit(0, 'mm'),
    legend.key.size=unit(3.5, 'mm'),
    legend.key.spacing=unit(0.5, 'mm'),
    #axis.title.x=element_blank(),
    axis.title.x=element_text(size=8),
    axis.text.x=element_blank(),
    #axis.ticks.x=element_blank(),
)


df <- read.csv("data/bench_tcpudp_lat.csv") 

head(df)

df <- df %>%
    mutate(
        same_chiplet=(core_id%/%8)==(rx_queue%/%8),
    ) %>%
    mutate(across(c(reg_ring, reg_fds, reg_bufs, napi, tcp), ~ as.logical(.))) %>%
    group_by(
        node, reg_ring, reg_fds, reg_bufs, napi,
        setup_mode, ping_size, resp_delay, tcp, pin_queues, rx_queue, same_chiplet
    ) %>%
    summarize(
        mean_lat = mean(avg) / 1e3,
        pooled_std_dev = sqrt(sum((std^2) * (samples - 1)) / (sum(samples) - 1)), # Pooled standard deviation
        total_samples = sum(samples),
        error_margin_lat = 1.96 * pooled_std_dev / sqrt(total_samples) / 1e3, # Margin of error
        .groups = "drop"
    ) %>%
    mutate(
        label=sprintf("%s%s%s%s",
            ifelse(reg_ring, "R", "-"),
            ifelse(reg_fds, "F", "-"),
            ifelse(reg_bufs, "B", "-"),
            ifelse(napi, "N", "-")
        ),
        x=reorder(sprintf("%d", ping_size), ping_size),
        flabel=sprintf("%s %d", node, resp_delay),
        flabel2=sprintf("%s", setup_mode),
        flabel3=sprintf("tcp: %d", tcp),
        #flabel=ifelse(same_chiplet, "Local Chiplet", "Remote Chiplet"),
    ) %>%
    ungroup()

#options(dplyr.print_max = 1e9)
options(width=300)
print(df)


df <- df %>% 
    mutate(
        order=case_when(
            label=="----" ~ 0,
            label=="R---" ~ 1,
            label=="RF--" ~ 2,
            label=="RFB-" ~ 3,
            label=="RFBN" ~ 4, 
            T ~ -1,
        ),
        label=case_when(
            label=="----" ~ "Default",
            label=="R---" ~ "+RegRing",
            label=="RF--" ~ "+RegFDs",
            label=="RFB-" ~ "+RegBufs",
            label=="RFBN" ~ "+NAPI", 
            T ~ "?",
        ),
        label=reorder(label, order),
    ) %>%
    filter(label != "?") %>% 
    mutate(
        lorder=case_when(
            setup_mode=="default" ~ 0,
            setup_mode=="coop"~ 1,
            setup_mode=="defer" ~ 2,
            setup_mode=="sqpoll" ~ 3,
            T ~ -1,
        ),
        flabel2=reorder(case_when(
            setup_mode=="default" ~ "Baseline",
            setup_mode=="coop" ~  "CoopTR",
            setup_mode=="defer" ~ "DeferTR",
            setup_mode=="sqpoll" ~ "SQPoll",
            T ~ "?",
        ), lorder),
    ) 

df <- df %>% mutate(mean_lat = mean_lat/2) # End-to-End


head(df)

df <- df %>%
    speedup(
        same_chiplet, mean_lat,
        by=c("flabel2", "flabel", "label", "x", "tcp")
    ) %>%
    mutate(
        lat_col=ifelse(same_chiplet, "lat_low", "lat_high"),
    ) %>%

    #select(-same_chiplet) %>%
    pivot_wider(
        names_from = lat_col,
        values_from = mean_lat,
        values_fill = NA_real_,
    )


head(df)

make_plot <- function(df, name) {
    p <- ggplot(df) +
        geom_col_pattern(
            position=position_dodge(width=0.88),
            width=0.79,
            aes(x=x, y=lat_high, color=label, pattern="Remote Chiplet", pattern_color=label),
            fill=NA,
        ) +
        geom_col(
            position=position_dodge(width=0.88), 
            width=0.79,
            aes(x=x, y=lat_low, fill=label),
            color=NA,
        ) +
        #geom_errorbar(
        #    aes(
        #        ymin = mean_lat - error_margin_lat,
        #        ymax = mean_lat + error_margin_lat,
        #    ),
        #    width = 0.2,  # Width of the error bars
        #    position = position_dodge(width = 0.9)  # Align error bars with the bars
        #) +
        geom_text(
            aes(x=x, y=lat_low, fill=label, label=sprintf("%.2fus", lat_low)),
            angle=90,
            position=position_dodge(width=0.9),
            hjust=1.1,
            size=3,
        ) +

        geom_text(
            aes(x=x, y=lat_high, fill=label, label=sprintf("+%.0f%%", (1-speedup)*100)),
            angle=90,
            position=position_dodge(width=0.9),
            hjust=-0.1,
            size=2.3,
        ) +



        guides(
            color="none",
            pattern_colour="none",
            fill=guide_legend(title="Features enabled:", nrow=1, byrow=T),
            #pattern="none",
            pattern = guide_legend(
              title = "", nrow = 1, byrow = TRUE,
              override.aes = list(color="grey70", pattern_color="black", pattern_fill="black") 
            )
        ) +
        scale_pattern_manual(
            values = c("Remote Chiplet" = "circle"),
            breaks = "Remote Chiplet",
            labels = "Remote Chiplet"
        ) +

        scale_x_discrete(name=name) +
        scale_y_continuous(name="Latency [us]", limit=c(0, 21)) +
        #scale_fill_viridis_d(option = "plasma") +
        #guides(fill=guide_legend(title="Features enabled:", nrow=1, byrow=T))+
        theme(
            axis.ticks.x=element_blank(),
        ) +
        facet_grid(. ~ flabel2)
    p
}

make_plot2 <- function(df, name) {
    p <- ggplot(df, aes(x=label, y=lat_low, fill=label)) +
        geom_col(
            position=position_dodge(width=0.8), 
            width=1.0,
            color='black',
        ) +
        geom_text(
            aes(label=sprintf("%.2f", lat_low)),
            angle=90,
            position=position_dodge(width=0.8),
            hjust=1.1,
            size=3,
        ) +

        guides(
            color="none",
            #fill=guide_legend(title="Features enabled:", nrow=1, byrow=T),
            fill="none",
        ) +
        scale_x_discrete(
            name=name,
            expand = expansion(mult = c(0.25, 0.25)) 
        ) +
        scale_y_continuous(
            name="Latency [us]",
            breaks=seq(0, 15, by=5),
            limit=c(0, 15.5)
        ) +
        theme(
            axis.text.x=element_text(angle=45, hjust=1, margin=margin(t=-5, unit='mm')),
            axis.title.x=element_text(size=8, margin=margin(t=5, unit='mm')),
        ) + 
        facet_grid(. ~ flabel2)
    p
}


# with chiplet
#p <- make_plot(df %>% filter(!tcp), "UDP (End-to-End)")
#p2 <- make_plot(df %>% filter(tcp), "TCP (End-to-End)")

p <- make_plot2(df %>% filter(!tcp), "UDP (End-to-End)")
p <- add_bar_arrow(
  p = p,
  data = df %>% filter(!tcp & same_chiplet & setup_mode=="defer"),
  x = "label",
  y = "lat_low",
  from = "+RegFDs",
  to = "+NAPI",
  label  = "-17%",
  text_angle=-15,
  text_vjust=-2.5,
  fontface='bold',
  curvature=-1.00,
  curve_angle=-65,
  from_pad=0.1,
  to_pad=-0.2,
)
p <- add_bar_arrow(
  p = p,
  data = df %>% filter(!tcp & same_chiplet & setup_mode=="sqpoll"),
  x = "label",
  y = "lat_low",
  from = "+RegFDs",
  to = "+NAPI",
  label  = "+14%",
  text_angle=15,
  text_vjust=-1.0,
  fontface='bold',
  curvature=-0.50,
  from_pad=0,
  to_pad=0.48,
)

p2 <- make_plot2(df %>% filter(tcp), "TCP (End-to-End)")
p2 <- add_bar_arrow(
  p = p2,
  data = df %>% filter(tcp & same_chiplet & setup_mode=="defer"),
  x = "label",
  y = "lat_low",
  from = "+RegFDs",
  to = "+NAPI",
  label  = "-11%",
  text_angle=-10,
  text_vjust=-2.0,
  fontface='bold',
  curvature=-0.90,
  curve_angle=-80,
  from_pad=0.1,
  to_pad=-0.2,
)
p2 <- add_bar_arrow(
  p = p2,
  data = df %>% filter(tcp & same_chiplet & setup_mode=="sqpoll"),
  x = "label",
  y = "lat_low",
  from = "+RegFDs",
  to = "+NAPI",
  label  = "+18%",
  text_angle=15,
  text_vjust=-1.0,
  fontface='bold',
  curvature=-0.50,
  from_pad=0,
  to_pad=0.48,
)

p <- (p | p2) + plot_layout(guides = "collect")

file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
dim=c(140, 60) #* c(0.4, 1)
fname=sprintf("out/%s.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)




