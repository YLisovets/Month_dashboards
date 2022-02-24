## Данные по рабочему времени работников
work_time_data <- function(date_start, date_finish) {
    
    library(DBI)
    library(odbc)
    library(tidyverse)
    library(lubridate)
    library(dbplyr)
    
    con <- dbConnect(odbc(),
                     Driver = "SQL Server",
                     Server = "SERVER2",
                     Database = "work",
                     UID = "sa",
                     PWD = "server232", # rstudioapi::askForPassword("Database password")
    )
    
    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_work_time_rg <- tbl(con, "_AccumRgTN17516") %>% 
        select(date = "_Period",
               link_employee = "_Fld17505RRef",
               link_organization = "_Fld17507RRef", link_type = "_Fld17508RRef",
               working_days = "_Fld17511", working_hours = "_Fld17512") %>% 
        filter(date >= date_start,
               date < date_finish)
    
    tbl_organization <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organization_id = "_Code")
    tbl_employees <- tbl(con, "_Reference159") %>% 
        select("_IDRRef", employee_name = "_Description", employee_id = "_Code",
               link_individuals = "_Fld2322RRef")
    tbl_position <- tbl(con, "_Reference81") %>% 
        select("_IDRRef", position_name = "_Description", position_id = "_Code")
    tbl_type_work_time <- tbl(con, "_Reference94") %>% 
        select("_IDRRef", type_id = "_Fld1685")
    tbl_individuals <- tbl(con,"_Reference191") %>% 
        select("_IDRRef", individ_id ="_Code", individ_name = "_Description")
    tbl_workers <- tbl(con,"_InfoRg15330") %>%
        select(date_rec = "_Period", link_individ = "_Fld15331RRef",
               link_subdiv = "_Fld15332RRef", link_position = "_Fld15333RRef") %>% 
        filter(date_rec < date_finish)
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")
  
    
    data_workers <- tbl_workers %>% 
        left_join(tbl_individuals, by = c(link_individ = "_IDRRef")) %>% 
        left_join(tbl_position, by = c(link_position = "_IDRRef")) %>% 
        left_join(tbl_subdiv, by = c(link_subdiv = "_IDRRef")) %>% 
        select(date_rec, individ_id, position_id, subdiv_name) %>% 
        #collect() %>% 
        filter(!is.na(position_id)) %>% 
        group_by(individ_id) %>%
        mutate(last_date = max(date_rec)) %>% 
        ungroup() %>% 
        filter(date_rec == last_date) %>% 
        select(individ_id, position_id, subdiv_name)
    
    df_work_time <- tbl_work_time_rg %>%
        left_join(tbl_organization,  by = c(link_organization = "_IDRRef")) %>%
        left_join(tbl_type_work_time,  by = c(link_type = "_IDRRef")) %>%
        left_join(tbl_employees,  by = c(link_employee = "_IDRRef")) %>%
        left_join(tbl_individuals,  by = c(link_individuals = "_IDRRef")) %>%
        #left_join(data_workers, by = (individ_id)) %>% 
        select(organization_id, individ_id,
               type_id, working_days, working_hours) %>%
        filter(
               organization_id == "000000034",
               type_id == "01",
                working_days > 0
               ) %>%
        left_join(data_workers) %>%
        select(subdiv_name, individ_id,  position_id,
               working_days, working_hours) %>% 
        collect()
 
    return(df_work_time)

     dbDisconnect(con)    
}
