suppressPackageStartupMessages({
library(data.table) # for shift
library(ggrepel)
})

KiB = 1024
MiB = 1024 * KiB
GiB = 1024 * MiB

fmt_bytes <- function(suffix='', scale=1, base=1000, unit=NULL) {
    if (!is.null(unit) && unit == 'bin_bytes'){
        base = 1024;
    }
    get_factors <- function (n) {
        case_when(
            n < base^1 ~ as.double(n),
            n < base^2 ~ n/base^1,
            n < base^3 ~ n/base^2,
            n < base^4 ~ n/base^3,
            n < base^5 ~ n/base^4,
            n < base^6 ~ n/base^5,
            TRUE ~ as.double(-1)
        )
    }
    get_units <- function(n) {
        if (!is.null(unit)) {
            if (unit == 'bytes') {
                case_when(
                    n < base^1 ~ 'B',
                    n < base^2 ~ 'K',
                    n < base^3 ~ 'M',
                    n < base^4 ~ 'G',
                    n < base^5 ~ 'T',
                    n < base^6 ~ 'P',
                    TRUE ~ '???'
                )
            } else if (unit == 'bits') {
                case_when(
                    n < base^1 ~ 'B',
                    n < base^2 ~ 'Kb',
                    n < base^3 ~ 'Mb',
                    n < base^4 ~ 'Gb',
                    n < base^5 ~ 'Tb',
                    n < base^6 ~ 'Pb',
                    TRUE ~ '???'
                )
            } else if (unit == 'bin_bytes') {
                case_when(
                    n < base^1 ~ 'B',
                    n < base^2 ~ 'KiB',
                    n < base^3 ~ 'MiB',
                    n < base^4 ~ 'GiB',
                    n < base^5 ~ 'TiB',
                    n < base^6 ~ 'PiB',
                    TRUE ~ '???'
                )
            }
        } else {
            case_when(
                n < base^1 ~ '',
                n < base^2 ~ 'K',
                n < base^3 ~ 'M',
                n < base^4 ~ 'B',
                n < base^5 ~ 'T',
                n < base^6 ~ 'P',
                TRUE ~ '???'
            )
        }
    }
    addUnits <- function(n) {
        n <- n * scale
        digits <- integer(length(n))
        factors <- get_factors(n)
        units <- get_units(n)
        while (TRUE) {
            rounded <- round(factors, digits)
            need_prec <- as.logical((shift(rounded, 1)==rounded) | (shift(rounded, -1) == rounded))
            if (!any(need_prec, na.rm=TRUE)) {
                break
            }
            digits <- digits + ifelse(is.na(need_prec), 0, 1)
        }
        return(paste0(rounded, units, suffix))
    }
}


lseq <- function(from=1, to=100000, length.out=6) {
  # logarithmic spaced sequence
  # blatantly stolen from library("emdbook"), because need only this
  exp(seq(log(from), log(to), length.out = length.out))
}




speedup <- function(df, baseline_filter, column, by, drop.baseline = FALSE, ...) {
  baseline_filter <- enquo(baseline_filter)
  column          <- enquo(column)
  by              <- as.character(by)

  # Ensure dplyr semantics regardless of input class
  df <- dplyr::as_tibble(df)

  baseline_df <- df %>%
    dplyr::filter(!!baseline_filter) %>%
    dplyr::select(dplyr::all_of(by), baseline_value = (!!column))

  # Unique baseline per 'by' combination (no data.table [i] involved)
  n_groups <- baseline_df %>%
    dplyr::distinct(!!!rlang::syms(by)) %>%
    nrow()

  if (nrow(baseline_df) != n_groups) {
    stop("Baseline selection does not yield a unique match for each 'by' combination.")
  }

  joined_df <- df %>%
    dplyr::left_join(baseline_df, by = by) %>%
    dplyr::mutate(speedup = baseline_value / (!!column)) %>%
    dplyr::select(-baseline_value)

  if (isTRUE(drop.baseline)) {
    joined_df <- joined_df %>% dplyr::filter(!(!!baseline_filter))
  }

  joined_df
}

assign_labels <- function(data, column, new_column, dict) {
  column <- enquo(column)
  new_column <- quo_name(enquo(new_column))
  case_expr <- purrr::map2(names(dict), dict, ~ expr(!!column == !!.x ~ !!.y))
  
  data <- data %>%
    mutate(!!new_column := case_when(!!!case_expr, TRUE ~ '?')) %>%
    mutate(!!new_column := factor(!!sym(new_column), levels = unique(dict)))
  
  return(data)
}


format_size <- function(x_bytes, digits = 3, suffix = "/s") {
  stopifnot(is.numeric(x_bytes), is.numeric(digits), digits >= 1)
  units <- c("B", "KiB", "MiB", "GiB", "TiB", "PiB")
  step  <- 1024

  out <- character(length(x_bytes))
  bad <- !is.finite(x_bytes)
  out[bad] <- NA_character_
  if (all(bad)) return(out)

  # choose unit index (0=B, 1=KiB, ...)
  i   <- pmin(floor(log(pmax(x_bytes[!bad], 1), base = step)), length(units) - 1L)
  val <- x_bytes[!bad] / (step ^ i)

  # --- bump rule generalized for configurable digits ---
  # If value would have >= digits+1 integer digits (e.g., 1000 for digits=3), go up one unit.
  bump_threshold <- 10^digits
  bump <- (val >= bump_threshold) & (i < length(units) - 1L)
  i[bump]   <- i[bump] + 1L
  val[bump] <- val[bump] / step

  # decimals to print so total significant digits = `digits`
  int_digits <- pmax(1L, floor(log10(pmax(val, 1))) + 1L)
  dec <- pmax(0L, digits - int_digits)

  # format with fixed decimals, keep trailing zeros
  txt <- vapply(seq_along(val), function(k) {
    formatC(val[k], format = "f", digits = dec[k], drop0trailing = FALSE)
  }, character(1))

  out[!bad] <- paste0(txt, units[i + 1L], suffix)
  out
}





add_bar_arrow <- function(
  p, data, x, y,
  from, to, label,
  colour     = "black",
  from_pad   = 0,
  to_pad     = 0,
  curvature  = 0,
  arrow_len  = grid::unit(4, "pt"),
  linewidth  = 0.5,
  text_angle = 0,
  text_vjust = 0.5,
  text_hjust = 0.5,
  size       = 3,
  curve_angle = 90,
  fontface   = "plain",
  curve_params = list(),   # extra args for geom_curve (e.g., linetype, alpha)
  text_params  = list()    # extra args for geom_text  (e.g., hjust, family)
) {
  # ---- checks
  stopifnot(length(from) == 1L, length(to) == 1L, length(label) == 1L)
  if (!nrow(data)) stop("`data` is empty after your filtering.")

  # infer facet columns = everything except x & y
  df <- as.data.frame(data)               # <- neutralize data.table semantics
  facet_cols <- setdiff(names(df), c(x, y))

  # ensure `data` represents only one facet combo (so arrow is unambiguous)
  #if (length(facet_cols)) {
  #    print(facet_cols)
  #  keys <- unique(data[facet_cols])
  #  if (nrow(keys) != 1L) {
  #    stop("`data` spans multiple facet combinations. ",
  #         "Filter to a single facet before calling `add_bar_arrow()`.")
  #  }
  #}

  # validate x names within this facet
  cats <- unique(df[[x]])
  bad  <- setdiff(c(from, to), cats)
  if (length(bad))
    stop("`from`/`to` not present in the filtered data for `", x, "`: ",
         paste(bad, collapse = ", "))

  # bar tops in this facet
  tops <- aggregate(df[[y]], list(cat = df[[x]]), max, na.rm = TRUE)
  names(tops)[2] <- "y_top"
  y_of <- function(val) tops$y_top[match(val, tops$cat)]

  # map discrete x to integer positions (use global levels from filtered data)
  x_levels <- levels(factor(df[[x]]))
  x0i <- match(from, x_levels)
  x1i <- match(to,   x_levels)

  # coords
  x0 <- x0i + sign(x1i - x0i) * from_pad
  x1 <- x1i - sign(x1i - x0i) * to_pad
  y0 <- y_of(from)
  y1 <- y_of(to)
  xm <- (x0 + x1) / 2
  ym <- (y0 + y1) / 2

  # build one-row df that carries facet columns (so it only draws in those panels)
  ann_df <- if (length(facet_cols)) unique(df[, facet_cols])[1, , drop = FALSE] else data.frame()
  ann_df$x <- x0; ann_df$y <- y0; ann_df$xend <- x1; ann_df$yend <- y1
  ann_df$xm <- xm; ann_df$ym <- ym; ann_df$label <- label

  # prepare extra params
  curve_args <- c(
    list(
      data = ann_df,
      mapping = aes(x = x, y = y, xend = xend, yend = yend),
      inherit.aes = FALSE,
      curvature = curvature,
      arrow = grid::arrow(type = "closed", length = arrow_len),
      angle=curve_angle,
      linewidth = linewidth,
      colour = colour
    ),
    curve_params
  )
  text_args <- c(
    list(
      data = ann_df,
      mapping = aes(x = xm, y = ym, label = label),
      inherit.aes = FALSE,
      angle = text_angle,
      hjust = text_hjust,
      vjust = text_vjust,
      size = size,
      fontface = fontface,
      colour = colour
    ),
    text_params
  )

  p + do.call(geom_curve, curve_args) + do.call(geom_text, text_args)
}



add_vert_arrow <- function(
  p, data, x, y,
  from, to, label,
  x_value      = NULL,
  colour       = "black",
  arrow_len    = grid::unit(4, "pt"),
  linewidth    = 0.5,
  text_angle   = 0,
  text_vjust   = 0.5,
  text_hjust   = 0.5,
  size         = 3,
  fontface     = "plain",
  extrapolate  = FALSE,
  pad_top      = 0,   # proportion of the top half trimmed (0..1)
  pad_bottom   = 0,   # proportion of the bottom half trimmed (0..1)
  lineheight = 1.2,
  segment_params = list(),
  text_params    = list()
) {
  stopifnot(length(from) == 1L, length(to) == 1L, length(label) == 1L)
  if (!nrow(data)) stop("`data` is empty after filtering.")
  if (!all(c(x, y) %in% names(data))) stop("`x` and `y` must be in `data`.")
  if (pad_top  < 0 || pad_bottom < 0 || pad_top >= 1 || pad_bottom >= 1)
    stop("`pad_top` and `pad_bottom` must be in [0, 1).")

  other_cols <- setdiff(names(data), c(x, y))

  # find grouping column that contains BOTH from & to
  gcol <- NULL
  for (col in other_cols) {
    vals <- unique(as.character(data[[col]]))
    if (from %in% vals && to %in% vals) { gcol <- col; break }
  }
  if (is.null(gcol)) {
    stop("Could not find a column containing both `from` (", from, ") and `to` (", to, "). ",
         "Checked: ", paste(other_cols, collapse = ", "))
  }

  # carry only facet columns that are constant across provided data
  cand_facets   <- setdiff(other_cols, gcol)
  const_facets  <- cand_facets[sapply(cand_facets, function(cl) length(unique(data[[cl]])) == 1)]

  as_num <- function(v) if (inherits(v, c("Date","POSIXct","POSIXlt"))) as.numeric(v) else as.numeric(v)

  y_at <- function(df_group, xout) {
    xs <- as_num(df_group[[x]])
    ys <- df_group[[y]]
    ord <- order(xs)
    xs <- xs[ord]; ys <- ys[ord]
    keep <- !duplicated(xs, fromLast = TRUE)
    xs <- xs[keep]; ys <- ys[keep]
    rule <- if (extrapolate) 2 else 1
    approx(xs, ys, xout = as_num(xout), rule = rule, ties = "ordered")$y
  }

  df_from <- data[data[[gcol]] == from, , drop = FALSE]
  df_to   <- data[data[[gcol]] == to,   , drop = FALSE]
  if (!nrow(df_from) || !nrow(df_to)) {
    vals <- unique(as.character(data[[gcol]]))
    stop("`from`/`to` not present after filtering. Available in ", gcol, ": ",
         paste(vals, collapse = ", "))
  }

  xv <- x_value
  if (is.null(xv)) {
    common_x <- intersect(df_from[[x]], df_to[[x]])
    if (!length(common_x))
      stop("No shared x-values between `from` and `to`. Provide `x_value` or use `extrapolate = TRUE`.")
    xv <- if (inherits(common_x, c("Date","POSIXct","POSIXlt"))) {
      common_x[which.max(as_num(common_x))]
    } else max(common_x, na.rm = TRUE)
  }

  y0 <- y_at(df_from, xv)
  y1 <- y_at(df_to,   xv)
  if (is.na(y0) || is.na(y1)) {
    if (!extrapolate)
      stop("`x_value` is outside range for one of the groups. Set `extrapolate = TRUE` or pick a different `x_value`.")
  }

  # base endpoints + padded reduction while keeping midpoint fixed
  y_low  <- min(y0, y1)
  y_high <- max(y0, y1)
  ym     <- (y0 + y1) / 2
  half_h <- (y_high - y_low) / 2
  # trim fractions from each half; midpoint stays ym
  top_half    <- half_h * (1 - pad_top)
  bottom_half <- half_h * (1 - pad_bottom)
  if ((top_half + bottom_half) <= 0)
    stop("Padding removes the entire arrow. Decrease `pad_top`/`pad_bottom`.")

  y_new_low  <- ym - bottom_half
  y_new_high <- ym + top_half

  # annotation df (carry constant facets so it draws in correct panel)
  ann_df <- if (length(const_facets)) unique(data[const_facets])[1, , drop = FALSE] else data.frame()
  ann_df$x     <- xv
  ann_df$y     <- y_new_low
  ann_df$xend  <- xv
  ann_df$yend  <- y_new_high
  ann_df$xm    <- xv
  ann_df$ym    <- ym
  ann_df$label <- label

  seg_args <- c(
    list(
      data = ann_df,
      mapping = ggplot2::aes(x = x, y = y, xend = xend, yend = yend),
      inherit.aes = FALSE,
      linewidth = linewidth,
      colour = colour,
      arrow = grid::arrow(type = "closed", ends = "both", length = arrow_len)
    ),
    segment_params
  )

  text_args <- c(
    list(
      data = ann_df,
      mapping = ggplot2::aes(x = xm, y = ym, label = label),
      inherit.aes = FALSE,
      angle = text_angle,
      vjust = text_vjust,
      hjust = text_hjust,
      size = size,
      fontface = fontface,
      lineheight=lineheight,
      colour = colour
    ),
    text_params
  )

  p + do.call(ggplot2::geom_segment, seg_args) + do.call(ggplot2::geom_text, text_args)
}


annotate_points <- function(plot, data, condition, label,
                            point.padding=0.5,
                            ...) {
    filtered_data <- data %>% filter(!!enquo(condition))

    if(nrow(filtered_data) == 0) {
        stop("filtered_data is empty")
    }
  
    plot +
        geom_text_repel(
            data = filtered_data,
            label=label,
            segment.color = "black",
            segment.size = 0.3,
            arrow = arrow(length = unit(0.05, "npc"), type = "closed"),
            min.segment.length = 0,
            box.padding = 0.4,
            point.padding = point.padding,
            show.legend=F,
            ...
        )
}




