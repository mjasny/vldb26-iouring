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
  pad_h        = 0,
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
  if (!is.numeric(pad_h) || length(pad_h) != 1L || !is.finite(pad_h))
    stop("`pad_h` must be one finite numeric value.")

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
    keep <- !is.na(xs) & !is.na(ys)
    xs <- xs[keep]; ys <- ys[keep]
    if (!length(xs)) {
      return(NA_real_)
    }
    if (length(xs) == 1) {
      return(ys[[1]])
    }
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
  ym_text    <- (y_new_low + y_new_high) / 2

  draw_x <- shift_x_value(p, xv, pad_h)

  # annotation df (carry constant facets so it draws in correct panel)
  ann_df <- if (length(const_facets)) unique(data[const_facets])[1, , drop = FALSE] else data.frame()
  ann_df$x     <- draw_x
  ann_df$y     <- y_new_low
  ann_df$xend  <- draw_x
  ann_df$yend  <- y_new_high
  ann_df$xm    <- draw_x
  ann_df$ym    <- ym_text
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


shift_x_value <- function(plot, x_value, pad_h = 0) {
  if (pad_h == 0) {
    return(x_value)
  }

  x_scale <- plot$scales$get_scales("x")
  trans <- if (!is.null(x_scale)) x_scale$trans else NULL

  if (inherits(x_value, "Date")) {
    x_num <- as.numeric(x_value)
    shifted_num <- if (!is.null(trans)) {
      trans$inverse(trans$transform(x_num) + pad_h)
    } else {
      x_num + pad_h
    }
    return(as.Date(shifted_num, origin = "1970-01-01"))
  }

  if (inherits(x_value, c("POSIXct", "POSIXlt"))) {
    x_num <- as.numeric(x_value)
    shifted_num <- if (!is.null(trans)) {
      trans$inverse(trans$transform(x_num) + pad_h)
    } else {
      x_num + pad_h
    }
    return(as.POSIXct(shifted_num, origin = "1970-01-01", tz = attr(x_value, "tzone")))
  }

  if (!is.numeric(x_value)) {
    stop("`pad_h` currently only supports numeric/date x values.")
  }

  if (!is.null(trans)) {
    return(trans$inverse(trans$transform(x_value) + pad_h))
  }

  x_value + pad_h
}


infer_plot_aes_name <- function(plot, aes_name) {
  mapping_expr <- plot$mapping[[aes_name]]

  if (is.null(mapping_expr)) {
    for (layer in plot$layers) {
      mapping_expr <- layer$mapping[[aes_name]]
      if (!is.null(mapping_expr)) {
        break
      }
    }
  }

  if (is.null(mapping_expr)) {
    return(NULL)
  }

  rlang::as_label(mapping_expr)
}


infer_facet_columns <- function(plot) {
  extract_vars <- function(x) {
    if (is.null(x)) {
      return(character())
    }
    if (rlang::is_quosure(x)) {
      return(all.vars(rlang::get_expr(x)))
    }
    if (is.list(x)) {
      return(unique(unlist(lapply(x, extract_vars), use.names = FALSE)))
    }
    character()
  }

  unique(c(
    extract_vars(plot$facet$params$rows),
    extract_vars(plot$facet$params$cols),
    extract_vars(plot$facet$params$facets)
  ))
}


relative_panel_position <- function(plot, data, axis, column, pos_frac, override_min = NULL, override_max = NULL) {
  if (!is.numeric(pos_frac) || length(pos_frac) != 1L || !is.finite(pos_frac) || pos_frac < 0 || pos_frac > 1) {
    stop("`pos_x`/`pos_y` must be one numeric value in [0, 1].")
  }
  if (!is.null(override_min) && (!is.numeric(override_min) || length(override_min) != 1L || !is.finite(override_min))) {
    stop("`override_min` must be one finite numeric value.")
  }
  if (!is.null(override_max) && (!is.numeric(override_max) || length(override_max) != 1L || !is.finite(override_max))) {
    stop("`override_max` must be one finite numeric value.")
  }

  scale_obj <- plot$scales$get_scales(axis)
  trans <- if (!is.null(scale_obj)) scale_obj$trans else NULL

  vec <- data[[column]]
  keep <- !is.na(vec)
  vec <- vec[keep]

  if (!length(vec)) {
    return(NA_real_)
  }

  if (inherits(vec, "Date")) {
    vec_num <- as.numeric(vec)
    if (!is.null(override_min)) vec_num <- c(vec_num, override_min)
    if (!is.null(override_max)) vec_num <- c(vec_num, override_max)
    if (!is.null(trans)) {
      vec_t <- trans$transform(vec_num)
      value_num <- trans$inverse(min(vec_t) + pos_frac * diff(range(vec_t)))
    } else {
      value_num <- min(vec_num) + pos_frac * diff(range(vec_num))
    }
    return(as.Date(value_num, origin = "1970-01-01"))
  }

  if (inherits(vec, c("POSIXct", "POSIXlt"))) {
    vec_num <- as.numeric(vec)
    if (!is.null(override_min)) vec_num <- c(vec_num, override_min)
    if (!is.null(override_max)) vec_num <- c(vec_num, override_max)
    if (!is.null(trans)) {
      vec_t <- trans$transform(vec_num)
      value_num <- trans$inverse(min(vec_t) + pos_frac * diff(range(vec_t)))
    } else {
      value_num <- min(vec_num) + pos_frac * diff(range(vec_num))
    }
    return(as.POSIXct(value_num, origin = "1970-01-01", tz = attr(vec, "tzone")))
  }

  if (!is.numeric(vec)) {
    stop("`annotate_point()` only supports numeric/date x and y aesthetics.")
  }

  vec_num <- as.numeric(vec)
  if (!is.null(override_min)) vec_num <- c(vec_num, override_min)
  if (!is.null(override_max)) vec_num <- c(vec_num, override_max)
  if (!is.null(trans)) {
    vec_t <- trans$transform(vec_num)
    return(trans$inverse(min(vec_t) + pos_frac * diff(range(vec_t))))
  }

  min(vec_num) + pos_frac * diff(range(vec_num))
}


transform_axis_values <- function(plot, data, axis, column, values) {
  scale_obj <- plot$scales$get_scales(axis)
  trans <- if (!is.null(scale_obj)) scale_obj$trans else NULL

  vec <- data[[column]]
  vec <- vec[!is.na(vec)]
  if (!length(vec)) {
    stop("Cannot transform values without panel data.")
  }

  if (inherits(vec, "Date")) {
    range_num <- as.numeric(vec)
    value_num <- as.numeric(values)
    if (!is.null(trans)) {
      return(list(
        values = trans$transform(value_num),
        range = range(trans$transform(range_num))
      ))
    }
    return(list(values = value_num, range = range(range_num)))
  }

  if (inherits(vec, c("POSIXct", "POSIXlt"))) {
    range_num <- as.numeric(vec)
    value_num <- as.numeric(values)
    if (!is.null(trans)) {
      return(list(
        values = trans$transform(value_num),
        range = range(trans$transform(range_num))
      ))
    }
    return(list(values = value_num, range = range(range_num)))
  }

  if (!is.numeric(vec)) {
    stop("Only numeric/date axes are supported.")
  }

  range_num <- as.numeric(vec)
  value_num <- as.numeric(values)
  if (!is.null(trans)) {
    return(list(
      values = trans$transform(value_num),
      range = range(trans$transform(range_num))
    ))
  }

  list(values = value_num, range = range(range_num))
}


inverse_axis_values <- function(plot, axis, template, values_t) {
  scale_obj <- plot$scales$get_scales(axis)
  trans <- if (!is.null(scale_obj)) scale_obj$trans else NULL

  values_num <- if (!is.null(trans)) trans$inverse(values_t) else values_t

  if (inherits(template, "Date")) {
    return(as.Date(values_num, origin = "1970-01-01"))
  }
  if (inherits(template, c("POSIXct", "POSIXlt"))) {
    return(as.POSIXct(values_num, origin = "1970-01-01", tz = attr(template, "tzone")))
  }

  values_num
}


trim_segment_tip <- function(plot, data, x_col, y_col, x_text, y_text, x_point, y_point, point_padding) {
  if (is.null(point_padding) || point_padding <= 0) {
    return(list(x = x_point, y = y_point))
  }

  x_info <- transform_axis_values(plot, data, "x", x_col, c(x_text, x_point))
  y_info <- transform_axis_values(plot, data, "y", y_col, c(y_text, y_point))

  x_span <- diff(x_info$range)
  y_span <- diff(y_info$range)
  if (!is.finite(x_span) || x_span == 0) x_span <- 1
  if (!is.finite(y_span) || y_span == 0) y_span <- 1

  x_text_n <- (x_info$values[1] - x_info$range[1]) / x_span
  x_point_n <- (x_info$values[2] - x_info$range[1]) / x_span
  y_text_n <- (y_info$values[1] - y_info$range[1]) / y_span
  y_point_n <- (y_info$values[2] - y_info$range[1]) / y_span

  dx_n <- x_point_n - x_text_n
  dy_n <- y_point_n - y_text_n
  seg_len_n <- sqrt(dx_n^2 + dy_n^2)
  if (!is.finite(seg_len_n) || seg_len_n == 0) {
    return(list(x = x_point, y = y_point))
  }

  pad_n <- min(point_padding * 0.02, seg_len_n * 0.9)
  x_end_n <- x_point_n - pad_n * dx_n / seg_len_n
  y_end_n <- y_point_n - pad_n * dy_n / seg_len_n

  x_end_t <- x_info$range[1] + x_end_n * x_span
  y_end_t <- y_info$range[1] + y_end_n * y_span

  list(
    x = inverse_axis_values(plot, "x", x_point, x_end_t),
    y = inverse_axis_values(plot, "y", y_point, y_end_t)
  )
}


is_degenerate_segment <- function(plot, data, x_col, y_col, x0, y0, x1, y1, tol = 1e-9) {
  x_info <- transform_axis_values(plot, data, "x", x_col, c(x0, x1))
  y_info <- transform_axis_values(plot, data, "y", y_col, c(y0, y1))

  dx <- x_info$values[2] - x_info$values[1]
  dy <- y_info$values[2] - y_info$values[1]

  isTRUE(is.finite(dx)) && isTRUE(is.finite(dy)) && abs(dx) < tol && abs(dy) < tol
}


add_gap_arrow <- function(
  plot, data, condition,
  x = NULL, y = NULL,
  from, to, label,
  ...
) {
  extra_args <- list(...)
  filtered_data <- data %>% filter(!!enquo(condition))

  if (!nrow(filtered_data)) {
    stop("filtered_data is empty")
  }

  if (is.null(x)) {
    x <- infer_plot_aes_name(plot, "x")
  }
  if (is.null(y)) {
    y <- infer_plot_aes_name(plot, "y")
  }
  if (is.null(x) || is.null(y)) {
    stop("Could not infer `x`/`y` from plot mapping. Pass them explicitly.")
  }

  if (is.null(extra_args$x_value)) {
    x_values <- unique(filtered_data[[x]])
    x_values <- x_values[!is.na(x_values)]
    if (length(x_values) == 1) {
      extra_args$x_value <- x_values[[1]]
    }
  }

  do.call(
    add_vert_arrow,
    c(
      list(
        p = plot,
        data = filtered_data,
        x = x,
        y = y,
        from = from,
        to = to,
        label = label
      ),
      extra_args
    )
  )
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


## Like annotate_points(), but uses a single fixed text label for one or more
## matching points and places that text by panel-relative coordinates in [0, 1].
annotate_point <- function(plot, data, condition, label,
                           x = NULL, y = NULL,
                           pos_x = 0.5,
                           pos_y = 0.5,
                           pos_from = c("facet", "condition"),
                           override_y_max = NULL,
                           curvature = 0,
                           point.padding = 0.5,
                           ...) {
    filtered_data <- data %>% filter(!!enquo(condition))

    if (nrow(filtered_data) == 0) {
        stop("filtered_data is empty")
    }

    if (is.null(x)) {
        x <- infer_plot_aes_name(plot, "x")
    }
    if (is.null(y)) {
        y <- infer_plot_aes_name(plot, "y")
    }
    if (is.null(x) || is.null(y)) {
        stop("Could not infer `x`/`y` from plot mapping. Pass them explicitly.")
    }

    pos_from <- match.arg(pos_from)
    other_cols <- setdiff(names(filtered_data), c(x, y))
    const_facets <- other_cols[sapply(other_cols, function(cl) length(unique(filtered_data[[cl]])) == 1)]
    facet_cols <- intersect(infer_facet_columns(plot), names(filtered_data))
    draw_facets <- if (length(facet_cols)) facet_cols else const_facets

    panel_data <- if (pos_from == "condition") filtered_data else data
    if (pos_from == "facet" && length(draw_facets)) {
        for (cl in draw_facets) {
            panel_data <- panel_data %>% filter(.data[[cl]] == filtered_data[[cl]][[1]])
        }
    }

    text_x <- relative_panel_position(plot, panel_data, "x", x, pos_x)
    text_y <- relative_panel_position(plot, panel_data, "y", y, pos_y, override_max = override_y_max)

    text_data <- if (length(draw_facets)) unique(filtered_data[draw_facets])[1, , drop = FALSE] else data.frame()
    text_data$x <- text_x
    text_data$y <- text_y
    text_data$label <- label

    segment_data <- filtered_data
    segment_data$x_text <- text_x
    segment_data$y_text <- text_y
    segment_data$x_point <- filtered_data[[x]]
    segment_data$y_point <- filtered_data[[y]]
    trimmed_tips <- purrr::pmap(
        list(segment_data$x_text, segment_data$y_text, segment_data$x_point, segment_data$y_point),
        function(x_text_i, y_text_i, x_point_i, y_point_i) {
            trim_segment_tip(
                plot = plot,
                data = panel_data,
                x_col = x,
                y_col = y,
                x_text = x_text_i,
                y_text = y_text_i,
                x_point = x_point_i,
                y_point = y_point_i,
                point_padding = point.padding
            )
        }
    )
    segment_data$x_end <- do.call(c, lapply(trimmed_tips, `[[`, "x"))
    segment_data$y_end <- do.call(c, lapply(trimmed_tips, `[[`, "y"))
    keep_segments <- !vapply(
        seq_len(nrow(segment_data)),
        function(i) {
            is_degenerate_segment(
                plot = plot,
                data = panel_data,
                x_col = x,
                y_col = y,
                x0 = segment_data$x_text[[i]],
                y0 = segment_data$y_text[[i]],
                x1 = segment_data$x_end[[i]],
                y1 = segment_data$y_end[[i]]
            )
        },
        logical(1)
    )
    segment_data <- segment_data[keep_segments, , drop = FALSE]

    plot +
        geom_curve(
            data = segment_data,
            mapping = aes(x = x_text, y = y_text, xend = x_end, yend = y_end),
            inherit.aes = FALSE,
            curvature = curvature,
            colour = "black",
            linewidth = 0.3,
            arrow = arrow(length = unit(0.05, "npc"), type = "closed"),
            arrow.fill = "white"
        ) +
        geom_text(
            data = text_data,
            mapping = aes(x = x, y = y, label = label),
            inherit.aes = FALSE,
            show.legend = FALSE,
            ...
        )
}
