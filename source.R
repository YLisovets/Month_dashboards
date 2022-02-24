library(DBI)
library(tidyverse)
library(lubridate)
library(dbplyr)
library(RMySQL)

con <- dbConnect(MySQL(),
                 host = "192.168.6.209",
                 dbname = "ukmserver",
                 user = "read_only",
                 password = "Rjhdtn20"
)

dbListTables(con)
dbListFields(con, "trm_out_receipt_header")
dbListFields(con, "trm_out_receipt_subtotal")
dbListFields(con, "trm_out_receipt_discounts")
dbListFields(con, "trm_in_pos")
dbListFields(con, "trm_in_store")
df <- dbReadTable(con, "trm_out_receipt_discounts") %>% 
    head()
dbDataType(con, "trm_out_receipt_discounts")

query <- str_c('select `t1`.*, `t2`.*, `t1`.`id` as "id_1", `t2`.`id` as "id_2",
      SUM(`t3`.`amount`) as "amount_2",
      SUM(`t3`.`account_amount`) as "account_amount_2",
      CONCAT_WS(".", `t1`.`pos`, `t1`.`shift_open`,
                `t1`.`local_number`) as "receipt_number",
      `t4`.`name` as "store",
      `t4`.`code_subdivision` as "store_1c"
      from `trm_out_receipt_header` as `t1`
      left join `trm_out_receipt_subtotal` as `t2` on
          `t1`.`id` = `t2`.`id` and `t1`.`cash_id` = `t2`.`cash_id`
      left join `trm_out_receipt_discounts` as `t3` on
          `t1`.`id` = `t3`.`receipt_header` and
          `t1`.`cash_id` = `t3`.`cash_id`
      left join `trm_in_pos` as `t5` on
          `t1`.`cash_id` = `t5`.`cash_id`
      left join `trm_in_store` as `t4` on
          `t5`.`store_id` = `t4`.`store_id`
      where `t1`.`client` is not null and
          `t3`.`discount_type` in(23, 22) and
          `t3`.`deleted` = 0 and
          `t2`.`deleted` = 0 and
          (cast(`t1`.`date` as date) BETWEEN 
                cast("',
      as.character(date_start),
      '" as date) and
                cast("',
      as.character(date_finish- days(1)),
      '" as date)) and
          `t1`.`deleted` = 0 and
          `t1`.`type` not in(5)
      group by `t3`.`cash_id`, `t3`.`receipt_header`
      order by `t1`.`date` desc')
a <- dbGetQuery(con, query)

a <- dbGetQuery(con,'
      select `t1`.*, `t2`.*, `t1`.`id` as "id_1", `t2`.`id` as "id_2",
      SUM(`t3`.`amount`) as "amount_2",
      SUM(`t3`.`account_amount`) as "account_amount_2",
      CONCAT_WS(".", `t1`.`pos`, `t1`.`shift_open`,
                `t1`.`local_number`) as "receipt_number",
      `t4`.`name` as "store",
      `t4`.`code_subdivision` as "store_1c"
      from `trm_out_receipt_header` as `t1`
      left join `trm_out_receipt_subtotal` as `t2` on
          `t1`.`id` = `t2`.`id` and `t1`.`cash_id` = `t2`.`cash_id`
      left join `trm_out_receipt_discounts` as `t3` on
          `t1`.`id` = `t3`.`receipt_header` and
          `t1`.`cash_id` = `t3`.`cash_id`
      left join `trm_in_pos` as `t5` on
          `t1`.`cash_id` = `t5`.`cash_id`
      left join `trm_in_store` as `t4` on
          `t5`.`store_id` = `t4`.`store_id`
      where `t1`.`client` is not null and
          `t3`.`discount_type` in(23, 22) and
          `t3`.`deleted` = 0 and
          `t2`.`deleted` = 0 and
          (cast(`t1`.`date` as date) BETWEEN 
                cast("2022-01-01" as date) and
                cast("2022-01-31" as date)) and
          `t1`.`deleted` = 0 and
          `t1`.`type` not in(5)
      group by `t3`.`cash_id`, `t3`.`receipt_header`
      order by `t1`.`date` desc
      ')
a <- dbSendQuery(con, 'set character set "UTF-8"')
x <- a$store[1:10]
x <- a$pos_name[1:10]
Encoding(a$store) <- "UTF-8"
Encoding(a$pos_name) <- "UTF-8"

names <- names(a)
new_names <- make.unique(names, sep="_")
names(a) <- new_names
tbl_receipt <- tbl(con, "trm_out_receipt_header") %>% 
    select(cash_id, date, id, pos, shift_open,
           local_number, deleted, type, client) %>% 
    filter(deleted == 0,
           !is.na(client),
           type != 5,
           date >= date_start,
           date <  date_finish) %>% 
    select(-deleted)

tbl_subtotal <- tbl(con, "trm_out_receipt_subtotal") %>% 
    select(id, cash_id, deleted,
           amount, clear_amount, items_count, discounts_amount,
           discounts_account_amount) %>% 
    filter(deleted == 0) %>% 
    select(-deleted)

tbl_discount <- tbl(con, "trm_out_receipt_discounts") %>% 
    select(receipt_header, cash_id, deleted, amount,
           discount_type) %>% 
    filter(deleted == 0,
           discount_type %in% c(23, 22)) %>%
    group_by(receipt_header, cash_id) %>% 
    summarise(bonus_reduced = sum(amount))

tbl_pos <- tbl(con, "trm_in_pos") %>% 
    select(cash_id, store_id) %>% 
    collect()

tbl_store <- tbl(con, "trm_in_store") %>% 
    select(store_id, name) %>% 
    collect()

df_checks_discount <- tbl_receipt %>% 
    left_join(tbl_subtotal) %>% 
    # left_join(tbl_discount, by = c(id = receipt_header,
    #                                cash_id)) %>% 
    left_join(tbl_pos) %>% 
    left_join(tbl_store) %>% 
    select(date, check_nmbr = local_number, cash_id,
           store_name = name, client, amount, clear_amount,
           items_count, discounts_amount, discounts_account_amount,
           #bonus_reduced, 
           type) %>% 
    collect()


tbl_receipt <- tbl(con, "trm_out_receipt_header") %>% 
    select(cash_id, date, id, pos, shift_open,
           local_number, deleted, type, client) %>% 
    filter(deleted == 0,
           !is.na(client),
           type != 5,
           date >= date_start,
           date <  date_finish
           ) %>% 
    collect()
str(tbl_receipt)
dbDisconnect(con)


source("global.R", local = FALSE)
rm(list = ls(pattern = "tbl", all.names = TRUE))

date_start <- floor_date(today(), unit = "month") - months(1)
date_finish <- date_start + months(1)

regular_cust <- regular_cust_data(date_start,
                                  date_finish) %>% 
    group_by(store_1c) %>% 
    summarise(total_regular_checks = sum(items_count > 0),
              total_reg_cust = n_distinct(client),
              reg_cust_reduce_sum =  - sum(discounts_amount),
              reg_cust_bonus_acrrued = sum(discounts_account_amount),
              reg_cust_bonus_reduced = - sum(amount_2)) %>% 
    mutate(reg_cust_discounts = reg_cust_reduce_sum - reg_cust_bonus_reduced) %>% 
    left_join(select(ref_store, store_id, subdiv_name),
              by = c("store_1c" = "store_id")) %>% 
    relocate(subdiv_name, .after = store_1c) %>% 
    select(-store_1c)

area_data <- get_shop_area() %>% 
    select(-date) %>% 
    pivot_wider(names_from = type_area,
                values_from = area_value)

sale_check_data <- checks_data(date_start + years(2000),
                               date_finish + years(2000))

sale_check_df <- sale_check_data %>% 
    group_by(date, subdiv_name, cashbox, check_nmbr) %>% 
    summarise(check_sum = sum(sum),
              check_discount = round(sum(price * qty) - check_sum, 2),
              sum_moneybox = sum(sum[item_id == "000532240"]), # в Скарбничку
              check_items = n(),
              check_items_clr = check_items - (sum_moneybox > 0),
              check_qty = sum(qty),
              check_time = first(check_time)
    ) %>% 
    ungroup() %>% 
    mutate(date = as.Date(date) - years(2000))

empl_work_time_data <- work_time_data(date_start + years(2000),
                                      date_finish + years(2000))

work_duration <- sale_check_df %>% 
    filter(subdiv_name != "Копіцентр ХМ СМ") %>% 
    group_by(subdiv_name, date) %>% 
    summarise(checks = n(),
              first_check = min(check_time[check_time != ""]),
              last_check = max(check_time)) %>% 
    ungroup() %>% 
    mutate(weekday = wday(date, label = TRUE)) %>% 
    left_join(select(ref_shops, subdiv_name, weekday, start_work, finish_work)) %>% 
    mutate(diff_first = as.integer(make_difftime(
                      hms::as_hms(first_check) - start_work, units = "hour")),
           diff_last = as.integer(make_difftime(
                   hms::as_hms(last_check) - finish_work, units = "hour")),
           work_duration = as.integer(finish_work - start_work) / 3600 -
               diff_first + diff_last) %>% 
    group_by(subdiv_name) %>% 
    summarise(period_work_time = sum(work_duration))

empl_work_time_retail <- empl_work_time_data %>%
    filter(!position_id %in% c("000000049", "000000036", "000000200",
                               "000000041")) %>% 
    left_join(unique(select(ref_shops, subdiv_name, parent_shop))) %>% 
    filter(!is.na(parent_shop)) %>% 
    mutate(working_hours = ifelse(position_id == "000000031" &
                                      (parent_shop %in% c("Группа А",
                                                          "Группа Б",
                                                          "Группа С",
                                                          "Группа Д") &
                                      !subdiv_name %in% c("Володимир Волинський",
                                                          "Луцьк Ковельськ",
                                                          "Чернівці")),
                                  working_hours * 0.2,
                                  working_hours)) %>% 
    group_by(subdiv_name) %>% 
    summarise(subdiv_empl_work_time = sum(working_hours))


cost_raw_data <- cost_data(date_start + years(2000),
                     date_finish + years(2000)) %>% 
    filter(!is.na(nmbr_check_doc)) %>%
    mutate(subdiv_name = ifelse(subdiv_name == "Копіцентр ХМ СМ",
                                "Хмельницький см роздріб",
                                subdiv_name))
cost_df <- cost_raw_data %>%
    group_by(subdiv_name) %>% 
    summarise(total_cost = sum(item_sum) + sum(item_vat))

sale_plan_df <- sale_plan_data(date_start + years(2000),
                          date_finish + years(2000)) %>% 
    filter(project == "Розница") %>% 
    mutate(subdiv_name = ifelse(subdiv_name == "Копіцентр ХМ СМ",
                                "Хмельницький см роздріб",
                                subdiv_name)) %>% 
    group_by(subdiv_name) %>% 
    summarise(plan_sum = sum(saleplan_doc_sum))

plan_indicators <- plan_data(date_start + years(2000),
                             date_finish + years(2000)) %>% 
    filter(indicator_name %in% c("КількістьЧеківПлан",
                                 "План продаж,шт В2С")) %>% 
    select(-plan_period) %>% 
    pivot_wider(names_from = indicator_name,
                values_from = value)

sale_history <- checks_data_global(date_start + years(2000) - months(35),
                                   date_finish + years(2000)) %>% 
    mutate(date = as.Date(date) - years(2000))

sale_prev_year <- sale_history %>% 
    filter(date >= date_start - years(1),
           date < date_finish - years(1)) %>% 
    mutate(subdiv_name = ifelse(subdiv_name %in% c("Копіцентр ХМ СМ",
                                                   "Подарунки ХМ СМ"),
                                "Хмельницький см роздріб",
                                subdiv_name)) %>% 
    group_by(subdiv_name) %>% 
    summarise(revenue_prev_year = sum(checks_sum),
              checks_prev_year = sum(checks_qty))

retail_df <- sale_check_df %>% 
    mutate(subdiv_name = ifelse(subdiv_name == "Копіцентр ХМ СМ",
                                "Хмельницький см роздріб",
                                subdiv_name)) %>% 
    group_by(subdiv_name) %>% 
    summarise(revenue = sum(check_sum),
              discounts = sum(check_discount),
              total_checks = sum(check_sum > 0),
              one_item_checks_qty = sum(check_items_clr == 1),
              total_items = sum(check_items_clr),
              total_units = sum(check_qty),
              avr_unit = round(revenue / total_units, 1)
    ) %>% 
    ungroup() %>% 
    left_join(distinct(select(ref_shops, subdiv_name, parent_shop),
                       subdiv_name, .keep_all = TRUE)) %>% 
    left_join(select(area_data, subdiv_name:trade_area)) %>% 
    left_join(cost_df) %>% 
    left_join(work_duration) %>%
    left_join(empl_work_time_retail) %>%
    left_join(sale_plan_df) %>% 
    left_join(plan_indicators) %>% 
    left_join(sale_prev_year) %>% 
    left_join(regular_cust) %>% 
    janitor::adorn_totals(where = "row", name = "ВСЕГО") %>%
    mutate(plan_implement = round(revenue / plan_sum, 3),
           revenue_change = round(revenue / revenue_prev_year, 3),
           checks_nmbr_impl = round(total_checks / `КількістьЧеківПлан`, 3),
           total_checks_change = round(total_checks / checks_prev_year, 3),
           discount_share = round(discounts / revenue, 3),
           one_item_checks_share = round(one_item_checks_qty/total_checks, 3),
           avr_check_sum = round(revenue / total_checks, 1),
           avr_check_impl = round(avr_check_sum /
                                      (plan_sum / `КількістьЧеківПлан`), 3),
           avr_check_change = round(avr_check_sum / (revenue_prev_year /
                                                         checks_prev_year),
                                    3),
           avr_check_sku = round(total_items / total_checks, 1),
           avr_check_sku_impl = round(avr_check_sku /
                                          (`План продаж,шт В2С`), 3),
           share_regular_cust = round(total_regular_checks / total_checks, 3),
           rev_per_sq = round(revenue / trade_area / 1000, 2),
           gross_profit = revenue - total_cost,
           profitability = round(gross_profit / revenue, 3),
           gmros = round(gross_profit / trade_area),
           gmrol = round(gross_profit / subdiv_empl_work_time),
           row_n = row_number(),
           staff_factor = subdiv_empl_work_time / period_work_time,
           
           staff_factor = ifelse(row_n == max(row_n),
                                 sum(staff_factor) - staff_factor,
                                 staff_factor),
           service_factor = round(total_checks / pmax(staff_factor, 1)),
           area_served = round(trade_area / staff_factor))


total_shops <- nrow(retail_df) - 1
plan_inplemention <- retail_df$plan_implement[total_shops + 1] * 100
revenue_per_area <- retail_df$rev_per_sq[total_shops + 1]

checks_nmbr_inplemention <- retail_df$checks_nmbr_impl[total_shops + 1] * 100
avr_check_plan <- round(retail_df$plan_sum[total_shops + 1] /
                            retail_df$`КількістьЧеківПлан`[total_shops + 1], 1)
avr_check_inplemention <- retail_df$avr_check_impl[total_shops + 1] * 100
avr_check_sku_plan <- round(weighted.mean(
                                retail_df$`План продаж,шт В2С`[1:total_shops],
                                retail_df$plan_sum[1:total_shops]),
                            1)
avr_check_sku_inplemention <- round(retail_df$avr_check_sku[total_shops + 1] /
                                        avr_check_sku_plan * 100, 1)

retail_df %>% 
    select(subdiv_name, rev_per_sq) %>% 
    filter(subdiv_name != "ВСЕГО") %>% 
    arrange(rev_per_sq) %>% 
    filter(between(row_number(), 1, 3) | between(row_number(),total_shops-2,
                                                 total_shops)) %>%
    mutate(type = ifelse(row_number() <= 3,
                         "bottom",
                         "top")) %>% 
    ggplot(aes(x = reorder(subdiv_name, rev_per_sq), y = rev_per_sq, fill = type)) +
    geom_col() +
    geom_text(aes(label = rev_per_sq), hjust=0.5, size = 5) +
    labs(x = "", y = "Выручка на 1м2, тыс.грн") +
    coord_flip() +
    theme_minimal() +
    theme(legend.position="none",
          axis.text.y = element_text(size = 12))

last_4week_data <- sale_check_df %>% 
    filter(date >= date_finish - days(28)) %>% 
    mutate(week_last_day = case_when(
                       date >= date_finish - days(7)  ~ date_finish - days(1),
                       date >= date_finish - days(14) ~ date_finish - days(8),
                       date >= date_finish - days(21) ~ date_finish - days(15),
                       date >= date_finish - days(28) ~ date_finish - days(22))
           ) %>% 
    group_by(week_last_day) %>% 
    summarise(revenue = sum(check_sum),
              total_checks = sum(check_sum > 0),
              total_items = sum(check_items_clr)) %>% 
    mutate(avr_check = round(revenue / total_checks, 1),
           avr_check_sku = round(total_items / total_checks, 1))

last_4week_data %>% 
    ggplot(aes(x = week_last_day, y = avr_check)) +
    geom_point(aes(colour = "steelblue")) +
    geom_line(aes(colour = "steelblue"),
                  size = 1) +
    geom_hline(aes(yintercept = avr_check_plan,
                   colour = "darkred"),
               linetype = "dashed",
               size = 1) +
    ggplot2::annotate("text", 
                      last_4week_data$week_last_day[2], 
                      avr_check_plan + 1, 
                      label = "План",
                      color = "darkred",
                      size = 5) +
    labs(x = "", y = "") +
    theme_minimal() +
    theme(legend.position="none",
          axis.text.x = element_text(size = 12),
          axis.text.y = element_text(size = 12))


last_4week_data %>% 
    ggplot(aes(x = week_last_day, y = avr_check_sku)) +
    geom_point(aes(colour = "steelblue")) +
    geom_line(aes(colour = "steelblue"),
              size = 1) +
    geom_hline(aes(yintercept = avr_check_sku_plan,
                   colour = "darkred"),
               linetype = "dashed",
               size = 1) +
    ggplot2::annotate("text", 
                      last_4week_data$week_last_day[1], 
                      avr_check_sku_plan + .1, 
                      label = "План",
                      color = "darkred") +
    labs(x = "", y = "") +
    theme_minimal() +
    theme(legend.position="none")

RdYlGr <- function(x) rgb(colorRamp(c("red", "#ffffbf", "green"))(x), maxColorValue = 255)

library(reactable)
retail_df %>% 
    select(subdiv_name, parent_shop, revenue, revenue_change, discount_share,
           trade_area, rev_per_sq, gross_profit, profitability, gmros) %>% 
    filter(subdiv_name != "ВСЕГО") %>%
    replace_na(list(revenue_change = 0)) %>% 
    reactable(striped = TRUE,
              groupBy = c("parent_shop"),
              columns = list(
                  parent_shop = colDef(name = "Группа"),
                  subdiv_name = colDef(name = "Магазин"),
                  trade_area = colDef(name = "Торг.пл-дь",
                                      aggregate = "sum",
                                      minWidth = 50,
                                      format = colFormat(separators = TRUE,
                                                         digits = 0)),
                  revenue = colDef(aggregate = "sum",
                                   name = "Выручка",
                                   minWidth = 60,
                                   format = colFormat(separators = TRUE,
                                                      digits = 0)),
                  revenue_change = colDef(name = "Изм(год)",
                                          minWidth = 60, 
                                          style = function(value) {
                                              normalized <- (value - min(
                                                  retail_df$revenue_change[1:total_shops],
                                                  na.rm = TRUE)) /
                                                  (max(retail_df$revenue_change[1:total_shops],
                                                       na.rm = TRUE)-
                                                       min(retail_df$revenue_change[1:total_shops],
                                                           na.rm = TRUE))
                                              normalized <- ifelse(normalized < 0,
                                                                   0.5,
                                                                   normalized)
                                              color <- RdYlGr(normalized)
                                              list(background = color)
                                          },
                                          format = colFormat(percent = TRUE)),
                  discount_share = colDef(name = "Скидки",
                                          minWidth = 50,
                                          format = colFormat(percent = TRUE)),
                  rev_per_sq = colDef(name = "Выр.1м2",
                                      minWidth = 50,
                                      # aggregate = JS("function(values, rows) {
                                      #             let totalRevenue = 0
                                      #             let totalArea = 0
                                      #             rows.forEach(function(row) {
                                      #             totalRevenue += row['revenue']
                                      #             totalArea += row['trade_area']
                                      #             })
                                      #             return totalRevenue / totalArea
                                      #             }"),
                                      style = function(value) {
                                          normalized <- (value - min(
                                              retail_df$rev_per_sq[1:total_shops])) /
                                              (max(retail_df$rev_per_sq[1:total_shops]) -
                                                   min(retail_df$rev_per_sq[1:total_shops]))
                                          color <- RdYlGr(normalized)
                                          list(background = color)
                                      }),
                  gross_profit = colDef(aggregate = "sum",
                                        name = "Вал.приб.",
                                        minWidth = 60,
                                        format = colFormat(separators = TRUE,
                                                           digits = 0)),
                  profitability = colDef(name = "Рентаб.",
                                         minWidth = 50,
                                         format = colFormat(percent = TRUE)),
                  gmros = colDef(name = "GMROS",
                                 minWidth = 50,
                                 aggregate = JS("function(values, rows) {
                                     let totalProfit = 0
                                     let totalArea = 0
                                     rows.forEach(function(row) {
                                         totalProfit += row['gross_profit']
                                         totalArea += row['trade_area']
                                     })
                                     return totalProfit / totalArea
                                 }")
                                 )
              ),
              bordered = TRUE
    )

cost_category <- cost_raw_data %>% 
    left_join(select(ref_items, item_id, group_id)) %>% 
    left_join(select(ref_item_group, group_id, category_name)) %>% 
    group_by(subdiv_name, category_name) %>% 
    summarise(category_cost_sum = sum(item_sum) + sum(item_vat)) %>% 
    filter(!is.na(category_name))

sale_category_raw <- sale_check_data %>% 
    mutate(subdiv_name = ifelse(subdiv_name == "Копіцентр ХМ СМ",
                                "Хмельницький см роздріб",
                                subdiv_name)) %>%
    left_join(select(ref_items, item_id, group_id)) %>% 
    left_join(select(ref_item_group, group_id, category_name)) %>% 
    group_by(subdiv_name, date, cashbox, check_nmbr, category_name) %>% 
    summarise(category_check_sale = sum(sum)) %>% 
    filter(!is.na(category_name))

sale_category <- sale_category_raw %>% 
    group_by(subdiv_name, category_name) %>% 
    summarise(category_sale_sum = sum(category_check_sale)) %>% 
    left_join(cost_category)

total_category <- sale_category %>% 
    group_by(category_name) %>% 
    summarise(total_sale = sum(category_sale_sum),
              total_cost = sum(category_cost_sum)) %>% 
    mutate(cat_profit = total_sale - total_cost,
           cat_profitability = round(cat_profit / total_sale, 3))

total_category %>% 
    ggplot(aes(x = category_name, label = scales::percent(cat_profitability,
                                                          accuracy = 0.1))) +
    geom_col(aes(y = total_sale, fill = "Продажи")) +
    geom_col(aes(y = cat_profit, fill = "Вал.прибыль")) +
    geom_text(aes(y=cat_profit), size = 4) +
    coord_flip() +
    scale_y_continuous(label = scales::comma) +
    labs(x = "", y = "", fill = "") +
    scale_fill_manual(values = c("Вал.прибыль" = "darkgrey",
                                 "Продажи" = "cornflowerblue")) +
    guides(fill = guide_legend(#title.position = "top",
                               direction = "vertical")) +
    theme_minimal() +
    theme(legend.position = "top",
          axis.text.x = element_text(size = 12),
          axis.text.y = element_text(size = 12),
          legend.text = element_text(size = 12)
          )


checks_time <- sale_check_data %>% 
    filter(qty > 0) %>% 
    mutate(subdiv_name = ifelse(subdiv_name == "Копіцентр ХМ СМ",
                                "Хмельницький см роздріб",
                                subdiv_name)) %>%
    distinct(date, subdiv_name, cashbox, check_nmbr, check_time) %>% 
    mutate(check_time = as.character(substr(check_time, 1, 2)),
           week_day = wday(as.Date(date) - years(2000),
                           label = TRUE,
                           week_start = getOption("lubridate.week.start", 1))) %>%
    filter(check_time != "") %>% 
    group_by(week_day, check_time) %>%
    summarise(trans_nbr = n())

checks_time %>%
    #filter(!time %in% c("00", "07", "21", "23")) %>% 
    ggplot(aes(week_day, check_time, fill = trans_nbr)) +
    geom_tile() +
    labs(x="", y="", title = "", fill = "Кол-во") +
    scale_fill_distiller(palette = "Spectral") +
    theme_minimal() +
    theme(axis.text.x = element_text(size = 12),
          axis.text.y = element_text(size = 12))
        

items_data <- sale_check_data %>% 
    filter(qty > 0) %>% 
    mutate(subdiv_name = ifelse(subdiv_name == "Копіцентр ХМ СМ",
                                "Хмельницький см роздріб",
                                subdiv_name)) %>% 
    group_by(subdiv_name, item_id) %>% 
    summarise(item_checks_nmbr = n(),
              item_sale_sum = sum(sum)) %>% 
    ungroup() %>% 
    left_join(select(ref_items, item_id, item_name, group_id)) %>% 
    filter(!group_id %in% c("000001912", # Бонус (в Скарбничку)
                            "000000504", # Пакети
                            "000003220", # Пакети фасув
                            "000003368", # Друк Ч/Б
                            "000001899"  # Друк кольоp
    ),
    !is.na(group_id)             # Дисконтная карта
    )

items_df <- items_data %>% 
    group_by(item_id) %>% 
    summarise(item_checks = sum(item_checks_nmbr),
              item_total_sale = sum(item_sale_sum),
              subdiv_nmbr = n())
    

items_df %>% 
    slice_max(item_checks, n = 15) %>% 
    ggplot(aes(x = item_name, y = item_checks, label = subdiv_nmbr)) +
    geom_col(fill = "cornflowerblue") +
    geom_text(aes(y = item_checks)) +
    coord_flip() +
    scale_y_continuous(label = scales::comma) +
    labs(x = "", y = "Кол-во чеков") +
    theme_minimal() +
    theme(axis.text.x = element_text(size = 10),
          axis.text.y = element_text(size = 10))

items_qty <- nrow(items_df)
g <- items_df %>% 
    select(item_name, item_total_sale) %>% 
    arrange(desc(item_total_sale)) %>%
    mutate(
        pct = item_total_sale / sum(item_total_sale),
        cumulative_pct = cumsum(pct),
        importance_product = ifelse(cumulative_pct <= 0.8, "Да", "Нет")
    ) %>%
    rowid_to_column(var = "rank") %>% 
    mutate(label_text = str_glue(
        "Ранг: {rank}
         Товар: {item_name}
         Сумма: {item_total_sale}
         Доля: {scales::percent(pct, accuracy = 0.01)}
         Кумул.доля: {scales::percent(cumulative_pct, accuracy = 0.01)}")) %>% 
    ggplot(aes(rank, cumulative_pct)) +
    geom_point(aes(color = importance_product, text = label_text), alpha = 0.2) +
    scale_y_continuous(label = scales::percent) +
    theme_minimal() +
    theme(legend.direction = "vertical", 
          legend.position  = "right") +
    labs(title = paste0("Общее кол-во проданных товаров - ", items_qty), 
         x = "Ранг",
         y = "Доля в продажах",
         color = "ТОП-80%")

plotly::ggplotly(g, tooltip = "text")


p <- items_data %>%
    group_by(subdiv_name) %>% 
    summarise(items_nmbr = n()) %>% 
    left_join(distinct(select(ref_shops, subdiv_name, parent_shop),
                       subdiv_name, .keep_all = TRUE)) %>% 
    ggplot(aes(x = reorder(subdiv_name, items_nmbr),
               y = items_nmbr, fill = parent_shop)) +
    geom_bar(stat="identity") +
    scale_fill_viridis_d() +
    labs(x = "", y = "Кол-во проданных SKU", fill = "Категория") +
    coord_flip()+
    theme_minimal()
plotly::ggplotly(p)


library(factoextra)
category_checks <- sale_category_raw %>% 
    group_by(subdiv_name, category_name) %>% 
    summarise(category_checks = n()) %>% 
    ungroup() %>% 
    filter(!category_name %in% c("15. Копіцентр",
                                "08. Стільці та крісла",
                                "09. Меблі в офіс")) %>% 
    pivot_wider(names_from  = category_name,
                values_from = category_checks,
                values_fill = 0) %>% 
    column_to_rownames(var = "subdiv_name")

res.agnes <- cluster::agnes(x = category_checks,
                            stand = TRUE,
                            metric = "euclidean",
                            method = "ward",
                            keep.data = TRUE)

factoextra::fviz_dend(res.agnes, cex = .6, h = 3.5, horiz = TRUE,
                      k_colors = "jco", rect = TRUE, rect_border = "jco",
                      rect_fill = TRUE,
                      main = "")

store_address <- ref_delivery_addresses %>% 
    filter(!is.na(store_name),
           is_main)

population <- readxl::read_xlsx("data/Геоданные_население.xlsx") %>% 
    filter(!is.na(longtitude)) %>% 
    mutate(latitude = as.numeric(latitude),
           longtitude = as.numeric(longtitude),
           town_name = str_extract(UKR_NAME,
                                   "(?<=м\\.\\s?|смт\\s?)\\S[^,]+")) %>% 
    select(-c(EN_NAME, UKR_NAME))

map_df <- retail_df %>% 
    select(subdiv_name, revenue) %>%
    filter(subdiv_name != "ВСЕГО") %>% 
    left_join(distinct(select(ref_store, subdiv_name, store_location),
              subdiv_name, .keep_all =TRUE)) %>% 
    mutate(subdiv_town = str_extract(store_location,
                                     "(?<=м\\.\\s?|смт\\.\\s?)\\S[^,]+(?=,)")) %>% 
    group_by(subdiv_town) %>% 
    summarise(town_revenue = sum(revenue)) %>%
    ungroup() %>% 
    left_join(population,
              by = c("subdiv_town" = "town_name")) %>% 
    mutate(revenue_per_popul = round(town_revenue / population, 1))

leaflet(map_df) %>% 
    addProviderTiles("OpenStreetMap.Mapnik") %>% 
    setView(26.41525, 49.84463, zoom = 7.5) %>%
    addCircleMarkers(lat = ~latitude, lng = ~longtitude,
                     radius = ~sqrt(revenue_per_popul)*2,
                     color = "#8b0000",
                     popup = ~paste0(
                         subdiv_town, '<br>',
                         '<b>Население</b>: ', population, '<br>',
                         '<b>Продажи на человека</b>: ', revenue_per_popul, '<br>'
                     ))
