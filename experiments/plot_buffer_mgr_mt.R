suppressPackageStartupMessages({
library(ggplot2)
library(dplyr)
library(sqldf)
library(stringr)
library(viridis)
library(scales)
library(tidyr)
library(patchwork)
library(forcats)
library(ggpattern)
library(ggh4x)
})
# paru -S r-ggplot2 r-gtable r-viridis
source('./utils.R')

theme_set(theme_bw())
theme_update(
    legend.position = "top",
    #axis.title.x=element_blank(),
    legend.margin=margin(0, b=-3, l=-10, unit='mm'),
    legend.key.size=unit(3.5, 'mm'),
    legend.text=element_text(margin=margin(0, l=0.25, unit='mm')),
    #legend.spacing.x = unit(0.3, 'mm'),
    #legend.spacing.y = unit(0, 'mm'),
    #axis.text.x=element_blank(),
    #axis.ticks.x=element_blank(),
)

#options(dplyr.print_max = 1e9)
options(width=300)



df <- read.csv('data/bench_buffer_mgr_mt.csv', comment='#')


df_ycsb_base <- df %>%
    filter(workload == 'ycsb') %>%
    mutate(across(c(reg_ring, reg_fds, reg_bufs, nvme_cmds, submit_always, sync_variant, posix_variant, iopoll, libaio), ~ as.logical(.))) %>%
    mutate(
        name_short=case_when(
            libaio & submit_always ~ "libaio_sync_submit",
            libaio & !submit_always ~ "libaio_batch_submit",
            posix_variant & evict_batch == 1 ~ "posix",
            posix_variant & evict_batch > 1 ~ "posix_batch_evict",
            setup_mode=="defer" & sync_variant & evict_batch == 1 & !iopoll ~ "sync",
            setup_mode=="defer" & sync_variant & evict_batch > 1 & !iopoll ~ "batch_evict",
            setup_mode=="defer" & submit_always & !iopoll ~ "sync_submit",
            setup_mode=="defer" & !submit_always & !reg_ring & !reg_fds & !reg_bufs & !nvme_cmds & !iopoll ~ "batch_submit", 
            setup_mode=="defer" & !submit_always & reg_ring & reg_fds & reg_bufs & !nvme_cmds & !iopoll ~ "regbufs",
            setup_mode=="defer" & !submit_always & reg_ring & reg_fds & reg_bufs & nvme_cmds & !iopoll ~ "passthru",
            setup_mode=="defer" & !submit_always & reg_ring & reg_fds & reg_bufs & nvme_cmds & iopoll ~ "iopoll",
            setup_mode=="sqpoll" & !submit_always & reg_ring & reg_fds & reg_bufs & nvme_cmds ~ "sqpoll",
            T ~ "???",
        ),
    )

trim_steady_state <- function(df) {
    first_active <- which(df$tps != 0)[1]
    if (is.na(first_active)) {
        return(df[0, , drop=FALSE])
    }

    df <- df[first_active:nrow(df), , drop=FALSE]

    # Drop one sample at each end when enough points remain to avoid warmup/shutdown artifacts.
    if (nrow(df) >= 5) {
        df <- df[2:(nrow(df)-1), , drop=FALSE]
    }

    df
}



# bar redo



df_ycsb_bar <- df_ycsb_base %>%
    filter(virt_size == 2^30 * 1) %>%
    filter(num_ssds == 8) %>%
    group_by(name_short, num_workers, ycsb_read_ratio, virt_size, ycsb_tuple_count, ycsb_zipf_theta) %>%
    arrange(ts, .by_group=TRUE) %>%
    group_modify(~ {
        .x <- trim_steady_state(.x)
        tibble(
            tps=mean(.x$tps),
            n_ts=nrow(.x)
        )
    }) %>%
    ungroup() %>%
    mutate(
        flabel2=reorder(
            case_when(
                ycsb_zipf_theta==0.0 ~ "Uniform",
                T ~ sprintf("Zipf: %0.1f", ycsb_zipf_theta),
            ), ycsb_read_ratio),
        skew_label=reorder(sprintf("%g", ycsb_zipf_theta), ycsb_zipf_theta),
        virt_size_label=ifelse(
            virt_size %% (2^30) == 0,
            sprintf("%dM/%dGiB", as.integer(ycsb_tuple_count / 1e6), as.integer(virt_size / 2^30)),
            sprintf("%dM/%.1fGiB", as.integer(ycsb_tuple_count / 1e6), virt_size / 2^30)
        ),
        #concurrency_label=reorder(sprintf("%d", concurrency), concurrency),
        #ssd_label=reorder(sprintf("%d SSDs", num_ssds), num_ssds),
        virt_size_label=factor(
            virt_size_label,
            levels=unique(virt_size_label[order(ycsb_tuple_count, virt_size)])
        ),
    ) %>%
    filter(ycsb_read_ratio == 0) %>%
    filter(ycsb_zipf_theta == 0.0) %>%
    filter(num_workers %in% c(1, 32)) %>%
    assign_labels(
        name_short, name,
        c(
            "sync_submit"="io_uring\n+Fibers",
            "batch_submit"="+Batch-\nSubmit",
            "regbufs"="+RegBufs", "passthru"="+Passthru",
            "iopoll"="+IOPoll"
        )
    ) %>%
    mutate(
        xlabel=name,
        tlabel=sprintf("%0.0f", tps/1e3),
        vjust=ifelse(tps<100e3, -0.3, 1.2),
        flabel=reorder(sprintf("YCSB - %d Worker", num_workers), num_workers),
    ) %>%
    print()


p <- ggplot(df_ycsb_bar, aes(x=name, y=tps)) +
    geom_col(
        position=position_dodge(width=0.88),
        width=1.0,
        aes(fill=xlabel),
        color='black'
    ) +
    geom_text(
        aes(label=tlabel, vjust=vjust, y=tps),
        position=position_dodge(width=0.9),
        size=3.20,
    ) +

    guides(
        color="none",
        fill="none", 
    )+
    scale_x_discrete(
        name=NULL,
        expand = expansion(add=0.7) 
    ) +
    #facet_grid(flabel2 ~ flabel, scales="free_x", space="free") +
    ggh4x::facet_grid2(. ~ flabel, scales="free_y", independent="y") +
    ggh4x::facetted_pos_scales(
        y=list(
            scale_y_continuous(
                name="Throughput [tx/s]",
                limits=c(0, NA),
                breaks=seq(0, 300e3, by=100e3),
                labels=function(x) sprintf("%0.0fK", x/1e3),
            ),
            scale_y_continuous(
                name="Throughput [tx/s]",
                labels=function(x) sprintf("%0.1fM", x/1e6),
            )
        )
    ) +
    theme(
        legend.title=element_text(),
        axis.title.x=element_blank(),
        axis.text.x=element_text(angle=35, margin=margin(t=1, unit='mm'), hjust=0.7),
        #panel.spacing = unit(0, "mm"),
        #axis.ticks.x=element_blank(),
    )


p <- add_bar_arrow(
  p = p,
  data = df_ycsb_bar %>% filter(num_workers==1),
  x = "name",
  y = "tps",
  from = "io_uring\n+Fibers",
  to = "+IOPoll",
  label  = "1.98x",
  text_angle=12,
  text_vjust=-0.8,
  fontface='bold',
  curvature=-0.30,
  curve_angle=-70,
  from_pad=0,
  to_pad=0.48,
)

p <- add_bar_arrow(
  p = p,
  data = df_ycsb_bar %>% filter(num_workers==32),
  x = "name",
  y = "tps",
  from = "io_uring\n+Fibers",
  to = "+IOPoll",
  label  = "1.64x",
  text_angle=12,
  text_vjust=-0.8,
  fontface='bold',
  curvature=-0.30,
  curve_angle=-70,
  from_pad=0,
  to_pad=0.48,
)



dim=c(140, 57) 
file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname=sprintf("out/%s_bars.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)




df_ycsb_scaleout <- df_ycsb_base %>%
    filter(virt_size %in% c(2^30 * 4, 2^30 * 1)) %>%
    filter(ycsb_tuple_count == 10e6) %>%
    filter(ycsb_zipf_theta %in% c(0.0, 0.5, 0.80, 0.90, 0.99)) %>%
    #filter(num_workers < 64) %>%
    group_by(num_workers, ycsb_read_ratio, virt_size, ycsb_tuple_count, ycsb_zipf_theta, name_short) %>%
    arrange(ts, .by_group=TRUE) %>%
    group_modify(~ {
        .x <- trim_steady_state(.x)
        tibble(
            tps=mean(.x$tps),
            n_ts=nrow(.x)
        )
    }) %>%
    ungroup() %>%
    mutate(
        in_mem = virt_size == 2^30 * 4,
        flabel=reorder(case_when(
            ycsb_read_ratio == 0 ~ "Write-only",
            ycsb_read_ratio == 50 ~ "50/50 RW Mix",
            ycsb_read_ratio == 100 ~ "Read-only",
            TRUE ~ sprintf("Read %d%%", ycsb_read_ratio)
        ), ycsb_read_ratio),
        flabel2=case_when(
            in_mem ~ 'In-Memory',
            virt_size == 2^30*1 ~ 'Out-of-Memory',
        ),
        theta_label=reorder(
            case_when(
                ycsb_zipf_theta==0.0 ~ "Uniform",
                T ~ sprintf("Zipf: %0.2f", ycsb_zipf_theta),
            ), ycsb_read_ratio),
        virt_size_label=ifelse(
            virt_size %% (2^30) == 0,
            sprintf("%dM/%dGiB", as.integer(ycsb_tuple_count / 1e6), as.integer(virt_size / 2^30)),
            sprintf("%dM/%.1fGiB", as.integer(ycsb_tuple_count / 1e6), virt_size / 2^30)
        ),
        #concurrency_label=reorder(sprintf("%d", concurrency), concurrency),
        #ssd_label=reorder(sprintf("%d SSDs", num_ssds), num_ssds),
        virt_size_label=factor(
            virt_size_label,
            levels=unique(virt_size_label[order(ycsb_tuple_count, virt_size)])
        ),
    ) %>%
    filter(!(!in_mem & name_short != "regbufs")) %>%
    print()





p <- ggplot(df_ycsb_scaleout, aes(x=num_workers, y=tps, color=theta_label, shape=theta_label, group=interaction(ycsb_zipf_theta, virt_size, ycsb_tuple_count))) +
    #p <- ggplot(df_ycsb_scaleout, aes(x=num_workers, y=tps, color=name, shape=concurrency_label, group=interaction(name, concurrency))) +
    #p <- ggplot(df_ycsb_scaleout, aes(x=num_workers, y=tps, color=name, shape=ssd_label, group=interaction(name, num_ssds))) +
    geom_line() +
    geom_point() +
    guides(
        color=guide_legend(title=NULL, nrow=1, byrow=T, order=1),
        shape=guide_legend(title=NULL, nrow=1, byrow=T, order=1),
    ) +
    scale_x_continuous(
        name="Worker threads [log]",
        breaks=sort(unique(df_ycsb_scaleout$num_workers)),
        trans="log",
    ) +
    scale_y_continuous(
        name="Throughput [tx/s]",
        labels=function(x) sprintf("%0.0fM", x/1e6),
        breaks=seq(0, 25e6, by=5e6),
        limits=c(0, NA),
    ) +
    #facet_wrap(. ~ flabel) +
    ggh4x::facet_grid2(flabel2 ~ flabel, scales="free_y") + #, independent="y") +
    theme(
        legend.box="horizontal",
        legend.margin=margin(0, b=-3, l=0, unit='mm'),
        legend.spacing.x=unit(4, 'mm'),
        legend.key.spacing.x=unit(0.2, 'mm')
    )


p <- annotate_point(
  plot = p,
  data = df_ycsb_scaleout,
  condition = in_mem & ycsb_read_ratio==0 & ((ycsb_zipf_theta == 0.99 & num_workers==32) | (ycsb_zipf_theta == 0.8 & num_workers==64)),
  label = "Write contention\n(not io_uring)",
  lineheight=0.8,
  size = 2.5,
  color='black',
  fontface='bold',
  pos_x=0.5,
  pos_y=0.6,
  vjust=-0.2,
  hjust=0.8,
  point.padding=1.5,
)


p <- annotate_point(
  plot = p,
  data = df_ycsb_scaleout,
  condition = in_mem & ycsb_read_ratio==100 & ycsb_zipf_theta == 0.90 & num_workers==32,
  label = "OLC scales\nfor reads",
  lineheight=0.8,
  size = 2.5,
  color='black',
  fontface='bold',
  pos_x=0.3,
  pos_y=0.9,
  vjust=1.2,
  hjust=0.8,
  point.padding=1.5,
)

p <- annotate_point(
  plot = p,
  data = df_ycsb_scaleout,
  condition = !in_mem & ycsb_read_ratio==0 & ((ycsb_zipf_theta == 0.99 & num_workers==32) | (ycsb_zipf_theta == 0.90 & num_workers==64)),
  label = "Throughput limited\nby skew in B-Tree",
  lineheight=0.8,
  size = 2.5,
  color='black',
  fontface='bold',
  override_y_max = 25e6,
  pos_x=0.5,
  pos_y=0.6,
  vjust=-0.2,
  hjust=0.5,
  point.padding=2.5,
)

p <- annotate_point(
  plot = p,
  data = df_ycsb_scaleout,
  condition = !in_mem & ycsb_read_ratio==50 & (ycsb_zipf_theta == 0.90 & num_workers==32),
  label = "Less write contention",
  lineheight=0.8,
  size = 2.5,
  color='black',
  fontface='bold',
  override_y_max = 25e6,
  pos_x=0.5,
  pos_y=0.6,
  vjust=-0.2,
  hjust=0.5,
  point.padding=2.5,
)

p <- annotate_point(
  plot = p,
  data = df_ycsb_scaleout,
  condition = !in_mem & ycsb_read_ratio==100 & ycsb_zipf_theta == 0.50 & num_workers %in% c(16, 64),
  label = "io_uring scales with\nring-per-thread design",
  lineheight=0.8,
  size = 2.5,
  color='black',
  fontface='bold',
  pos_x=0.42,
  pos_y=0.7,
  vjust=-0.2,
  hjust=0.6,
  point.padding=1.5,
)


dim=c(140, 60) * c(1, 1.7)
file <- tools::file_path_sans_ext(sub(".*=", "", commandArgs()[4]))
fname=sprintf("out/%s_skew.pdf", file)
ggsave(file=fname, plot=p, device=cairo_pdf, width=dim[1], height=dim[2], units="mm")
system(sprintf("pdfcrop \"%s\" \"%s\"", fname, fname), wait=T)



