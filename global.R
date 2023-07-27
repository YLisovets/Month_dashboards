connect_to_db <- function() {
    
    con_db <- dbConnect(odbc(),
                        Driver = "SQL Server",
                        Server = Sys.getenv("SERVER_NAME"),
                        Database = Sys.getenv("DATABASE_NAME"),
                        UID = Sys.getenv("UID_DATABASE"),
                        PWD = Sys.getenv("PWD_DATABASE")
    )
    
}


get_references <- function() {
    
    tbl_item <- tbl(con, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code", item_name = "_Description",
               pack_qty = "_Fld20858","_Fld2048RRef","_Fld2045RRef","_ParentIDRRef",
               "_Fld2060RRef", is_tva = "_Fld20858", item_marked = "_Marked",
               link_unit_size = "_Fld2039RRef") %>% 
        mutate(is_tva = as.logical(is_tva),
               item_marked = as.logical(item_marked))
    
    tbl_group <- tbl(con, "_Reference122") %>% 
        select("_IDRRef", group_id = "_Code", group_name = "_Description",
               "_Fld19599RRef", "_Fld18403RRef", "_ParentIDRRef",
               group_marked = "_Marked") %>% 
        mutate(group_marked = as.logical(group_marked))
    
    tbl_category <- tbl(con, "_Reference19598") %>% 
        select("_IDRRef", category_id = "_Code", category_name = "_Description")
    
    tbl_price_group <- tbl(con, "_Reference235") %>% 
        select("_IDRRef", price_group = "_Description")
    
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", marked = "_Marked", is_folder = "_Folder",
               partner_id = "_Code", partner_name = "_Description",
               "_ParentIDRRef", is_customer = "_Fld1741", is_supplier = "_Fld1742",
               edrp_code = "_Fld1754", link_main_cust = "_Fld21315RRef",
               link_main_manager = "_Fld1747RRef", registr_date = "_Fld1762",
               link_type_relationship = "_Fld1763RRef",
               link_main_act_form = "_Fld1753RRef",
               link_segment_form = "_Fld22000RRef"
        ) %>% 
        mutate(is_customer = as.logical(is_customer),
               is_supplier = as.logical(is_supplier),
               marked = as.logical(marked),
               is_folder = as.logical(is_folder))
    
    tbl_partner_tbl <- tbl(con, "_Reference102_VT1772") %>%
        select("_Reference102_IDRRef", link_subdiv = "_Fld1775RRef",
               link_manager = "_Fld1774RRef", date_entry = "_Fld1776")
    
    ref_partner_folders <<- tbl_partner %>% 
        filter(is_folder == "FALSE") %>%
        left_join(select(tbl_partner, "_IDRRef", parent_id = partner_id),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>%
        select(partner_id, parent_id) %>% 
        collect()
    
    tbl_type_relationship <- tbl(con, "_Reference232") %>% 
        select("_IDRRef", type_relationship = "_Description")
    
    tbl_segment_form <- tbl(con, "_Reference21996") %>% 
        select("_IDRRef", segment_form = "_Description")
    
    tbl_main_act_form <- tbl(con, "_Reference51") %>% 
        select("_IDRRef", main_act_form = "_Description")
    
    tbl_character_types <- tbl(con, "_Chrc806") %>% 
        select("_IDRRef", code = "_Code")
    
    tbl_char_types_users_set <- tbl(con, "_Chrc804") %>% 
        select("_IDRRef", code = "_Code")
    
    tbl_object_property_value_ref <- tbl(con, "_Reference85") %>% 
        select("_IDRRef", name = "_Description")
    
    tbl_user_set_rg <- tbl(con, "_InfoRg14842") %>% 
        select("_Fld14843RRef", link_setting = "_Fld14844RRef",
               link_user_subdiv = "_Fld14845_RRRef")
    
    tbl_object_property_value_rg <- tbl(con, "_InfoRg14478") %>% 
        select("_Fld14479_RRRef", property = "_Fld14480RRef",
               value = "_Fld14481_RRRef")
    
    tbl_users <- tbl(con, "_Reference138") %>% 
        select("_IDRRef", marked = "_Marked", user_id = "_Code",
               user_name = "_Description") %>% 
        mutate(marked = as.logical(marked))
    
    tbl_store <- tbl(con, "_Reference156") %>%
        select("_IDRRef", store_id = "_Code", store_name = "_Description",
               "_Fld2296RRef", "_ParentIDRRef", store_location = "_Fld2312",
               link_distribution_store = "_Fld19506RRef")
    
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", "_ParentIDRRef", subdiv_id = "_Code",
               subdiv_name = "_Description",
               subdiv_marked = "_Marked", link_store = "_Fld18502RRef") %>% 
        mutate(subdiv_marked = as.logical(subdiv_marked))
    
    tbl_individuals <- tbl(con,"_Reference191") %>% 
        select("_IDRRef", user_id = "_Code", user_name = "_Description")
    
    tbl_projects <- tbl(con, "_Reference143") %>% 
        select("_IDRRef", project_name = "_Description", project_id = "_Code",
               "_ParentIDRRef")
    
    tbl_unit_size <- tbl(con, "_Reference84") %>% 
        select("_IDRRef", width = "_Fld18500", height = "_Fld18501", 
               depth = "_Fld18499", weight = "_Fld1642", volume = "_Fld1643")
    
    
    ref_individuals <<- tbl_individuals %>% 
        select(user_id, user_name) %>% 
        collect()
    
    ref_items <<- tbl_item %>% 
        left_join(select(tbl_item, "_IDRRef", parent_itemId = item_id),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>%
        left_join(tbl_price_group, by = c("_Fld2060RRef" = "_IDRRef")) %>%
        left_join(tbl_group, by = c("_Fld2045RRef" = "_IDRRef")) %>%
        left_join(tbl_partner, by = c("_Fld2048RRef" = "_IDRRef")) %>%
        left_join(tbl_unit_size, by = c(link_unit_size = "_IDRRef")) %>%
        select(item_id, item_name, price_group, group_id, partner_id, pack_qty,
               is_tva, item_marked, parent_itemId, width, height, depth, weight,
               volume) %>% 
        collect()
    
    ref_item_group <<- tbl_group %>%
        left_join(select(tbl_group, "_IDRRef", parent_group = group_id),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>%
        left_join(tbl_category, by = c("_Fld19599RRef" = "_IDRRef")) %>% 
        left_join(tbl_users, by = c("_Fld18403RRef" = "_IDRRef")) %>% 
        select(group_marked, group_id, group_name, parent_group, category_name,
               category_manager = user_name) %>% 
        collect()
    
    ref_subdiv_category <<- tbl_subdiv %>% 
        left_join(tbl_object_property_value_rg,
                  by = c("_IDRRef" = "_Fld14479_RRRef")) %>%
        inner_join(tbl_character_types, by = c(property = "_IDRRef")) %>%
        filter(code == "000000191") %>%
        left_join(tbl_object_property_value_ref, by = c(value = "_IDRRef")) %>% 
        select(subdiv_name, subdiv_category = name) %>% 
        collect()
    
    ref_store <<- tbl_store %>% 
        left_join(select(tbl_store, "_IDRRef", parent_store = store_name),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>% 
        left_join(tbl_subdiv, 
                  by = c("_Fld2296RRef" = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", distribution_store = store_id),
                  by = c(link_distribution_store = "_IDRRef")) %>%
        select(store_id, subdiv_name, subdiv_marked, store_name, parent_store,
               distribution_store, store_location) %>% 
        collect() %>% 
        filter(!is.na(subdiv_name))
    
    ref_users <<- tbl_users %>%
        left_join(tbl_user_set_rg, by = c("_IDRRef" = "_Fld14843RRef")) %>% 
        inner_join(tbl_char_types_users_set, by = c(link_setting = "_IDRRef")) %>% 
        filter(code == "00005") %>%
        left_join(select(tbl_subdiv, "_IDRRef", subdiv_name),
                  by = c(link_user_subdiv = "_IDRRef")) %>% 
        select(user_name, user_subdiv = subdiv_name, marked) %>% 
        collect()
    
    tbl_type_customer <- select(tbl_partner, "_IDRRef", partner_id) %>% 
        left_join(tbl_object_property_value_rg, by = c("_IDRRef" = "_Fld14479_RRRef")) %>%
        inner_join(tbl_character_types, by = c(property = "_IDRRef")) %>%
        filter(code == "000000002") %>%
        left_join(tbl_object_property_value_ref, by = c(value = "_IDRRef")) %>% 
        select(partner_id, name) 
    
    
    
    ref_customers <<- tbl_partner %>% 
        filter(is_customer == "TRUE") %>%
        left_join(select(tbl_partner, "_IDRRef", main_cust_id = partner_id),
                  by = c(link_main_cust = "_IDRRef")) %>%
        left_join(select(tbl_users, "_IDRRef", main_manager = user_id),
                  by = c(link_main_manager = "_IDRRef")) %>% 
        left_join(tbl_type_relationship, by = c(link_type_relationship = "_IDRRef")) %>%
        left_join(tbl_segment_form, by = c(link_segment_form = "_IDRRef")) %>% 
        left_join(tbl_type_customer) %>%
        left_join(tbl_main_act_form, by = c(link_main_act_form = "_IDRRef")) %>% 
        left_join(select(tbl_partner, "_IDRRef", customer_folder = partner_name,
                         customer_folder_id = partner_id),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>%
        select(customer_id = partner_id, customer_name = partner_name,
               edrp_code, main_cust_id, main_manager, type_relationship,
               segment_form, customer_type = name, main_act_form,
               customer_folder, customer_folder_id, registr_date, marked
        ) %>%
        collect() 
    
    
    ref_suppliers <<- tbl_partner %>%
        filter(is_supplier == "TRUE") %>% 
        left_join(select(tbl_partner, "_IDRRef", main_cust_id = partner_id),
                  by = c(link_main_cust = "_IDRRef")) %>%
        left_join(select(tbl_partner, "_IDRRef", supplier_parent_id = partner_id),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>% 
        left_join(tbl_type_relationship, by = c(link_type_relationship = "_IDRRef")) %>%
        select(supplier_id = partner_id, supplier_name = partner_name, main_cust_id,
               supplier_parent_id, type_relationship, edrp_code, marked) %>% 
        collect()
    
    tbl_contract <- tbl(con, "_Reference78") %>% 
        select("_IDRRef", contract_id = "_Code", link_partner = "_OwnerIDRRef",
               contract_name = "_Description", link_contract_type = "_Fld1600RRef",
               link_project = "_Fld1614_RRRef",
               contract_arrears_day = "_Fld1589", contract_arrears_sum = "_Fld1588")
    tbl_type_contract <- tbl(con, "_Enum484") %>% 
        select("_IDRRef", type_order = "_EnumOrder")
    
    ref_contract <<- tbl_contract %>% 
        left_join(select(tbl_partner, "_IDRRef", partner_id, partner_name),
                  by = c(link_partner = "_IDRRef")) %>% 
        left_join(tbl_type_contract, by = c(link_contract_type = "_IDRRef")) %>% 
        left_join(tbl_projects,      by = c(link_project = "_IDRRef")) %>%
        select(contract_id, contract_name, owner_partner = partner_id, partner_name,
               type_order, project_id, contract_arrears_day,
               contract_arrears_sum) %>%
        filter(type_order %in% c(0,1)) %>% 
        collect()
    
    ref_partner_manager <<- tbl_partner_tbl %>% 
        left_join(select(tbl_partner, "_IDRRef", partner_id, partner_name),
                  by = c("_Reference102_IDRRef" = "_IDRRef")) %>% 
        left_join(select(tbl_subdiv, "_IDRRef", subdiv_name),
                  by = c(link_subdiv = "_IDRRef")) %>% 
        left_join(select(tbl_users, "_IDRRef", user_name),
                  by = c(link_manager = "_IDRRef")) %>%
        select(partner_id, partner_name, subdiv_name, user_name, date_entry) %>% 
        collect()
    
    
    tbl_delivery_addresses <- tbl(con, "_Reference194") %>% 
        select("_IDRRef", link_owner = "_OwnerID_RRRef", marked = "_Marked",
               address_name = "_Description", link_locality = "_Fld18128RRef",
               link_route = "_Fld19663RRef", link_region = "_Fld2738RRef",
               is_main = "_Fld2740", lat = "_Fld24538", long = "_Fld24539") %>% 
        mutate(marked = as.logical(marked),
               is_main = as.logical(is_main))
    tbl_route <- tbl(con, "_Reference19642") %>% 
        select("_IDRRef", route = "_Description")
    tbl_region <- tbl(con, "_Reference149") %>% 
        select("_IDRRef", region_name = "_Description", "_ParentIDRRef")
    tbl_locality <- tbl(con, "_Reference199") %>% 
        select("_IDRRef", locality_name = "_Description")
    
    ref_delivery_addresses <<- tbl_delivery_addresses %>% 
        left_join(tbl_route,    by = c(link_route = "_IDRRef")) %>% 
        left_join(tbl_region,   by = c(link_region = "_IDRRef")) %>%
        left_join(select(tbl_region, "_IDRRef", parent_region = region_name),  
                  by = c("_ParentIDRRef" = "_IDRRef")) %>%
        left_join(tbl_locality, by = c(link_locality = "_IDRRef")) %>%
        left_join(select(tbl_partner, "_IDRRef", partner_id, partner_name),
                  by = c(link_owner = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", store_id, store_name),
                  by = c(link_owner = "_IDRRef")) %>% 
        select(partner_id, partner_name, store_id, store_name, address_name, locality_name,
               region_name, parent_region, route, marked, is_main, lat, long) %>% 
        collect()
    
    
    ref_projects <<- tbl_projects %>% 
        left_join(select(tbl_projects, "_IDRRef", parent_project = project_name,
                         parent_project_id = project_id),  
                  by = c("_ParentIDRRef" = "_IDRRef")) %>%
        select(project_id, project_name, parent_project, parent_project_id) %>% 
        collect()
    
    tbl_shops <- tbl(con, "_Reference24140") %>% 
        select("_IDRRef", marked = "_Marked", "_ParentIDRRef",
               folder = "_Folder", shop_name = "_Description", 
               link_subdiv = "_Fld24141RRef") %>%
        mutate(folder = as.logical(folder),
               marked = as.logical(marked)) %>% 
        filter(marked == "FALSE")
    tbl_shops_tbl <- tbl(con, "_Reference24140_VT24143") %>% 
        select("_Reference24140_IDRRef", link_weekday = "_Fld24145RRef",
               start_work = "_Fld24146", finish_work = "_Fld24147")
    tbl_weekday <- tbl(con, "_Enum609") %>% 
        select("_IDRRef", weekday = "_EnumOrder")
    
    ref_shops <<- tbl_shops %>% 
        left_join(select(tbl_shops, "_IDRRef", parent_shop = shop_name),  
                  by = c("_ParentIDRRef" = "_IDRRef")) %>%
        left_join(select(tbl_subdiv, "_IDRRef", subdiv_name),  
                  by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_shops_tbl, by = c("_IDRRef" = "_Reference24140_IDRRef")) %>%
        left_join(tbl_weekday,   by = c(link_weekday = "_IDRRef")) %>%
        filter(folder == "TRUE") %>%
        select(shop_name, parent_shop, subdiv_name,
               weekday, start_work, finish_work) %>% 
        collect() %>% 
        mutate(start_work = hms::as_hms(start_work),
               finish_work = hms::as_hms(finish_work),
               weekday = case_when(
                   weekday == 0 ~ wday(2, label = TRUE),
                   weekday == 1 ~ wday(3, label = TRUE),
                   weekday == 2 ~ wday(4, label = TRUE),
                   weekday == 3 ~ wday(5, label = TRUE),
                   weekday == 4 ~ wday(6, label = TRUE),
                   weekday == 5 ~ wday(7, label = TRUE),
                   weekday == 6 ~ wday(1, label = TRUE)
               ))
    
    tbl_subdiv_accordance <- tbl(con, "_InfoRg15671") %>% 
        select(link_subdiv = "_Fld15672RRef", link_organization = "_Fld15673RRef", 
               link_subdiv_organaz = "_Fld15674RRef")
    tbl_organization <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organization_name = "_Description")
    tbl_subdiv_organization <- tbl(con, "_Reference136") %>% 
        select("_IDRRef", subdiv_organiz_name = "_Description")
    subdiv_accordance_tbl <<- tbl_subdiv_accordance %>% 
        left_join(select(tbl_subdiv, "_IDRRef", subdiv_name, subdiv_id),  
                  by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        left_join(tbl_subdiv_organization, by =c(link_subdiv_organaz="_IDRRef")) %>%
        select(subdiv_id, subdiv_name, subdiv_organiz_name, organization_name) %>% 
        collect() %>% 
        filter(organization_name != "КОРВЕТ")
    
    
    ref_subdiv <<- tbl_subdiv %>% 
        left_join(select(tbl_subdiv, "_IDRRef",
                         subdiv_parent = subdiv_name),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", store_id),
                  by = c(link_store = "_IDRRef")) %>%
        select(subdiv_id, subdiv_name, subdiv_parent, store_id) %>% 
        collect()
    
    tbl_expenses_items <- tbl(con, "_Reference169") %>% 
        select("_IDRRef", expItems_id = "_Code", expItems_name = "_Description",
               "_ParentIDRRef", expItems_marked = "_Marked") %>% 
        mutate(expItems_marked = as.logical(expItems_marked))
    
    ref_expenses <<- tbl_expenses_items %>% 
        left_join(select(tbl_expenses_items, "_IDRRef",
                         expItems_parent = expItems_name),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>% 
        select(expItems_id, expItems_name, expItems_parent, expItems_marked) %>% 
        distinct() %>% 
        collect()
    
    
    tbl_account <- tbl(con, "_Acc18") %>% 
        select("_IDRRef", acc_id = "_Code", acc_name = "_Description",
               "_ParentIDRRef", acc_marked = "_Marked") %>% 
        mutate(acc_marked = as.logical(acc_marked))
    
    ref_account <<- tbl_account %>% 
        left_join(select(tbl_account, "_IDRRef",
                         acc_parent = acc_id),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>% 
        select(acc_id, acc_name, acc_parent, acc_marked) %>% 
        collect()
    
    
    tbl_route <- tbl(con, "_Reference19642") %>% 
        select("_IDRRef", "_ParentIDRRef", route_id = "_Code",
               link_subdiv = "_Fld19647RRef", route_marked = "_Marked",
               route = "_Description") %>% 
        mutate(route_marked = as.logical(route_marked))
    ref_route <<- tbl_route %>% 
        left_join(select(tbl_route, "_IDRRef",
                         route_parent = route),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>% 
        left_join(select(tbl_subdiv, "_IDRRef", subdiv_id),
                  by = c(link_subdiv = "_IDRRef")) %>% 
        select(route_id, route, route_parent, subdiv_id, route_marked) %>% 
        collect()
    
    tbl_auto <- tbl(con, "_Reference195") %>% 
        select(auto_id = "_Code", auto_name = "_Description", is_cargo ="_Fld21584",
               marked = "_Marked", link_subdiv = "_Fld19665RRef",
               link_store = "_Fld19668RRef", tank ="_Fld21651",
               load_capacity = "_Fld19666", volume = "_Fld19667") %>% 
        mutate(marked = as.logical(marked),
               is_cargo = as.logical(is_cargo))
    ref_auto <<- tbl_auto %>% 
        left_join(select(tbl_subdiv, "_IDRRef", subdiv_id),
                  by = c(link_subdiv = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", store_id),
                  by = c(link_store = "_IDRRef")) %>% 
        select(auto_id, auto_name, marked, subdiv_id, store_id, is_cargo,
               load_capacity, volume, tank) %>% 
        collect()
    
    tbl_shelving <- tbl(con, "_Reference21028") %>% 
        select("_IDRRef", active = "_Fld24213", shelving_id = "_Code",
               shelving_name = "_Description", link_store = "_Fld21312RRef") %>% 
        mutate(active = as.logical(active))
    tbl_shelving_tbl <- tbl(con, "_Reference21028_VT21034") %>% 
        select("_Reference21028_IDRRef", link_shelf = "_Fld21036RRef")
    tbl_shelf <- tbl(con, "_Reference21029") %>% 
        select("_IDRRef", shelf_id = "_Code", shelf_name = "_Description",
               width = "_Fld21045", height = "_Fld21046", depth = "_Fld21044")
    
    ref_shelf <<- tbl_shelving %>%
        left_join(select(tbl_store, "_IDRRef", store_id),
                  by = c(link_store = "_IDRRef")) %>% 
        left_join(tbl_shelving_tbl,
                  by = c("_IDRRef" = "_Reference21028_IDRRef")) %>% 
        left_join(tbl_shelf,
                  by = c(link_shelf = "_IDRRef")) %>%
        select(store_id, shelving_id, shelving_name, active, shelf_id, shelf_name,
               width, height, depth) %>% 
        collect() %>% 
        filter(!is.na(shelf_id))
    
}


## Площади магазинов
get_shop_area <- function() {

    tbl_area <- tbl(con,"_InfoRg22018") %>% 
        select(date = "_Period", active = "_Active",
               link_subdiv = "_Fld22019RRef", link_type_area = "_Fld22020RRef",
               area_value = "_Fld22021") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(active == "TRUE")

    tbl_subdiv <- tbl(con,"_Reference135") %>%
        select("_IDRRef", subdiv_name = "_Description")
    
    tbl_type_area <- tbl(con, "_Enum22007") %>% 
        select("_IDRRef", type_area = "_EnumOrder")
    
    df_area <- tbl_area %>%
        left_join(tbl_subdiv,    by = c(link_subdiv = "_IDRRef")) %>% 
        left_join(tbl_type_area, by = c(link_type_area = "_IDRRef")) %>%
        select(date, subdiv_name, type_area, area_value) %>% 
        collect() %>% 
        mutate(type_area = case_when(
            type_area == 0 ~ "total_area",
            type_area == 1 ~ "trade_area",
            type_area == 2 ~ "storage",
            type_area == 3 ~ "b2b_area",
            type_area == 4 ~ "copycenter_area"
        ))
    
    return(df_area)
}


## Фейсы магазинов
get_face <- function() {

    tbl_face <- tbl(con,"_InfoRg16359") %>% 
        select(link_item = "_Fld16361RRef", link_store = "_Fld16360RRef",
               date = "_Period", face_qty = "_Fld16362", active = "_Active") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(active == "TRUE")
    
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    
    tbl_store <- tbl(con,"_Reference156") %>%
        select("_IDRRef", store_name = "_Description")

    df_face <- tbl_face %>%
        group_by(link_item, link_store) %>% 
        slice_max(date) %>%
        ungroup() %>% 
        left_join(tbl_item, by = c(link_item = "_IDRRef")) %>% 
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        select(date, item_id, store_name, face_qty) %>%
        filter(face_qty > 0) %>% 
        collect()
    
    return(df_face)
    
}


## Фейсы на дату
get_face <- function(date_finish) {

    date_finish <- as_datetime(date_finish)
    
    tbl_face <- tbl(con,"_InfoRg16359") %>% 
        select(link_item = "_Fld16361RRef", link_store = "_Fld16360RRef",
               date = "_Period", face_qty = "_Fld16362", active = "_Active") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(active == "TRUE",
               date < date_finish)
    
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    
    tbl_store <- tbl(con,"_Reference156") %>%
        select("_IDRRef", store_id = "_Code")
    
    df_face <- tbl_face %>%
        group_by(link_item, link_store) %>% 
        slice_max(date) %>%
        ungroup() %>% 
        left_join(tbl_item, by = c(link_item = "_IDRRef")) %>% 
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        select(date, item_id, store_id, face_qty) %>%
        filter(face_qty > 0) %>% 
        collect()
    
    return(df_face)
}


## Нормы магазинов
get_norm <- function(date_finish) {

    date_finish <- as_datetime(date_finish)
    
    tbl_norm <- tbl(con,"_InfoRg18379") %>% 
        select(link_item = "_Fld18381RRef", link_store = "_Fld18380RRef",
               date = "_Period", norm_qty = "_Fld18382", active = "_Active") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(active == "TRUE",
               date < date_finish)
    
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    
    tbl_store <- tbl(con,"_Reference156") %>%
        select("_IDRRef", store_id = "_Code")
    
    df_norm <- tbl_norm %>%
        group_by(link_item, link_store) %>% 
        slice_max(date) %>%
        ungroup() %>% 
        left_join(tbl_item, by = c(link_item = "_IDRRef")) %>% 
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        select(date, item_id, store_id, norm_qty) %>%
        filter(norm_qty > 0) %>% 
        collect()
    
    return(df_norm)
}


## Продажи по розничным чекам
checks_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_checks_tbl <- tbl(con, "_Document329_VT7992") %>% 
        select("_Fld8019", "_Fld8020", "_Fld7994RRef", "_Fld7998", "_Fld7999",
               "_Fld7997", "_Document329_IDRRef")
    tbl_checks <- tbl(con, "_Document329") %>% 
        select("_IDRRef", posted = "_Posted", date = "_Date_Time",
               nmbr_check_doc = "_Number", "_Fld7966RRef", "_Fld7960RRef") %>%
        mutate(posted = as.logical(posted)) %>%
        filter(date >= date_start,
               date < date_finish,
               posted == "TRUE")
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")
    tbl_cashbox <- tbl(con, "_Reference89") %>% 
        select("_IDRRef", cashbox = "_Description")

    df_checks <- left_join(tbl_checks, tbl_checks_tbl,
                           by = c("_IDRRef" = "_Document329_IDRRef")) %>%
        left_join(tbl_item, by = c("_Fld7994RRef" = "_IDRRef")) %>%
        left_join(tbl_subdiv, by = c("_Fld7966RRef" = "_IDRRef")) %>%
        left_join(tbl_cashbox, by = c("_Fld7960RRef" = "_IDRRef")) %>%
        select(date, subdiv_name, nmbr_check_doc, cashbox,
               check_nmbr = "_Fld8019", check_time = "_Fld8020", item_id,
               price = "_Fld7997", qty = "_Fld7998", sum = "_Fld7999") %>% 
        collect()
    
    return(df_checks)
}


## Продажи по розничным чекам
checks_data_global <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_checks <- tbl(con, "_Document329") %>% 
        select("_IDRRef", posted = "_Posted", date = "_Date_Time",
               "_Fld7966RRef", "_Fld7960RRef", "_Fld7967", "_Fld7990") %>%
        mutate(posted = as.logical(posted)) %>%
        filter(date >= date_start,
               date < date_finish,
               posted == "TRUE")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")
    tbl_cashbox <- tbl(con, "_Reference89") %>% 
        select("_IDRRef", cashbox = "_Description")
    
    df_checks <- tbl_checks %>%
        left_join(tbl_subdiv, by = c("_Fld7966RRef" = "_IDRRef")) %>%
        select(date, subdiv_name,
               checks_qty = "_Fld7990", checks_sum = "_Fld7967") %>% 
        collect()
    
    return(df_checks)
}


## Поступления товаров
receipt_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_receipt_tbl <- tbl(con,"_Document360_VT9812") %>% 
        select("_Document360_IDRRef", link_item = "_Fld9814RRef",
               item_qty = "_Fld9819", item_sum = "_Fld9821" ,
               item_vat = "_Fld9823")
    tbl_receipt <- tbl(con,"_Document360") %>% 
        select("_IDRRef", posted_receipt = "_Posted", nmbr_receipt = "_Number",
               date_receipt = "_Date_Time", is_managerial = "_Fld9766",
               doc_sum_currency = "_Fld9772", doc_rate = "_Fld9760",
               link_supplier = "_Fld9758RRef", link_suppl_contr = "_Fld9755RRef",
               link_subdiv = "_Fld9767RRef", link_store = "_Fld9770_RRRef",
               link_organization = "_Fld9762RRef",
               date_ready_for_receipt = "_Fld18354",
               date_processed = "_Fld18386",
               vat_marker = "_Fld9771") %>%
         mutate(doc_sum_currency = as.numeric(doc_sum_currency),
                doc_rate = as.numeric(doc_rate),
                doc_sum = doc_sum_currency * doc_rate,
                posted_receipt = as.logical(posted_receipt),
                is_managerial = as.logical(is_managerial),
                vat_marker = as.logical(vat_marker)
               ) %>% 
        filter(date_receipt >= date_start,
               date_receipt < date_finish,
               posted_receipt == "TRUE",
               is_managerial == "TRUE"
               )
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description")
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", supplier_name = "_Description")
    tbl_contract <- tbl(con, "_Reference78") %>% 
        select("_IDRRef", supplier_contract = "_Description",
               link_payment_type = "_Fld1585RRef")
    tbl_organization <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organiz_name = "_Description")
    tbl_payment_type <- tbl(con, "_Reference49") %>% 
        select("_IDRRef", payment_type = "_Code", is_f2 = "_Fld18856") %>% 
        mutate(is_f2 = as.logical(is_f2))
    
    df_receipt <- tbl_receipt %>%
        left_join(tbl_receipt_tbl, by = c("_IDRRef" = "_Document360_IDRRef")) %>%
        left_join(tbl_item   ,     by = c(link_item = "_IDRRef")) %>%
        left_join(tbl_partner,     by = c(link_supplier = "_IDRRef")) %>% 
        left_join(tbl_contract,    by = c(link_suppl_contr = "_IDRRef")) %>% 
        left_join(tbl_subdiv,      by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_store,       by = c(link_store = "_IDRRef")) %>%
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        left_join(tbl_payment_type, by = c(link_payment_type = "_IDRRef")) %>%
        select(nmbr_receipt, date_receipt, doc_sum, vat_marker, doc_rate, 
               subdiv_name, store_name,
               date_ready_for_receipt, date_processed, organiz_name,
               supplier_name, supplier_contract, payment_type, is_f2, 
               item_id, item_qty, item_sum, item_vat
               ) %>%
        collect() %>% 
        mutate(vat_marker = ifelse(vat_marker == FALSE,
                                   1,
                                   0),
               item_sum = item_sum * doc_rate + vat_marker * item_vat)

    return(df_receipt)
}


## Заказ поставщику по дате документа
supplier_order_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_supplier_order_tbl <- tbl(con,"_Document263_VT4682") %>% 
        select("_Document263_IDRRef", link_item = "_Fld4690RRef",
               item_qty = "_Fld4687", item_price = "_Fld4696",
               item_sum = "_Fld4693",
               item_pack_qty = "_Fld4698", link_unit = "_Fld4684RRef",
               link_order = "_Fld4686_RRRef") 
    tbl_supplier_order <- tbl(con,"_Document263") %>% 
        select("_IDRRef", posted = "_Posted", nmbr_supplier_order = "_Number",
               date_supplier_order = "_Date_Time",
               doc_sum_currency = "_Fld4672", doc_rate = "_Fld4665",
               link_supplier = "_Fld4663RRef",
               link_suppl_contr = "_Fld4658RRef",
               link_subdiv = "_Fld4669RRef", link_store = "_Fld4670RRef",
               link_organization = "_Fld4667RRef",
               date_receipt = "_Fld4657",         # дата поступления
               date_creation = "_Fld4679"         # дата создания)
               ) %>%  
        mutate(doc_sum_currency = as.numeric(doc_sum_currency),
               doc_rate = as.numeric(doc_rate),
               supplier_order_doc_sum = doc_sum_currency * doc_rate,
               posted = as.logical(posted)
        ) %>% 
        filter(date_supplier_order >= date_start,
               date_supplier_order < date_finish,
               posted == "TRUE"
        )
    
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description")
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", supplier_id = "_Code", supplier_name = "_Description")
    tbl_contract <- tbl(con, "_Reference78") %>% 
        select("_IDRRef", supplier_contract = "_Description")
    tbl_organization <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organiz_name = "_Description")
    tbl_unit <- tbl(con, "_Reference84") %>% 
        select("_IDRRef", item_unit = "_Description")
    tbl_customer_order <- tbl(con,"_Document262") %>% 
        select("_IDRRef", nmbr_cust_order_doc = "_Number",
               date_cust_order_doc = "_Date_Time")
    tbl_internal_order <- tbl(con,"_Document249") %>% 
        select("_IDRRef", nmbr_internal_order = "_Number",
               date_internal_order = "_Date_Time")
    
    df_supplier_order <- tbl_supplier_order %>%
        left_join(tbl_partner,        by = c(link_supplier = "_IDRRef")) %>% 
        left_join(tbl_contract,       by = c(link_suppl_contr = "_IDRRef")) %>% 
        left_join(tbl_subdiv,         by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_store,          by = c(link_store = "_IDRRef")) %>%
        left_join(tbl_organization,   by = c(link_organization = "_IDRRef")) %>%
        left_join(tbl_supplier_order_tbl,
                                  by = c("_IDRRef" = "_Document263_IDRRef")) %>%
        left_join(tbl_item,           by = c(link_item = "_IDRRef")) %>%
        left_join(tbl_unit,           by = c(link_unit = "_IDRRef")) %>%
        left_join(tbl_customer_order, by = c(link_order = "_IDRRef")) %>%
        left_join(tbl_internal_order, by = c(link_order = "_IDRRef")) %>%
        select(nmbr_supplier_order, date_supplier_order, supplier_order_doc_sum,
               subdiv_name, store_name,
               supplier_id, supplier_name, supplier_contract,
               organiz_name, date_creation, date_receipt,
               item_id, item_unit, item_qty, item_price, item_sum,item_pack_qty,
               nmbr_cust_order_doc, date_cust_order_doc,
               nmbr_internal_order, date_internal_order
        ) %>% 
        collect()
    
    return(df_supplier_order)
}


## Заказ поставщику по дате прихoда
supplier_order_by_receipt_date <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_supplier_order_tbl <- tbl(con,"_Document263_VT4682") %>% 
        select("_Document263_IDRRef", link_item = "_Fld4690RRef",
               item_qty = "_Fld4687") 
    tbl_supplier_order <- tbl(con,"_Document263") %>% 
        select("_IDRRef", posted = "_Posted", nmbr_supplier_order = "_Number",
               date_supplier_order = "_Date_Time",
               link_supplier = "_Fld4663RRef",
               link_suppl_contr = "_Fld4658RRef",
               date_receipt = "_Fld4657"        # дата поступления
        ) %>%  
        mutate(posted = as.logical(posted)
        ) %>% 
        filter(date_receipt  >= date_start,
               date_receipt  < date_finish,
               posted == "TRUE"
        )
    
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", supplier_name = "_Description")
    tbl_contract <- tbl(con, "_Reference78") %>% 
        select("_IDRRef", supplier_contract = "_Description")

    df_supplier_order <- tbl_supplier_order %>%
        left_join(tbl_partner,        by = c(link_supplier = "_IDRRef")) %>%
        left_join(tbl_contract, by = c(link_suppl_contr = "_IDRRef")) %>%
        left_join(tbl_supplier_order_tbl,
                  by = c("_IDRRef" = "_Document263_IDRRef")) %>%
        left_join(tbl_item,     by = c(link_item = "_IDRRef")) %>%
        select(nmbr_supplier_order, date_supplier_order, date_receipt,
               supplier_name, supplier_contract, item_id, item_qty
        ) %>% 
        collect() %>% 
        filter(str_detect(supplier_contract, "^685", negate = TRUE))
    
    return(df_supplier_order)
}


## Возврат товаров поставщикам
return_supplier_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_return_supplier <- tbl(con,"_Document252") %>% 
        select("_IDRRef", posted_return = "_Posted", nmbr_return = "_Number",
               date_return = "_Date_Time", is_managerial = "_Fld4048",
               link_supplier = "_Fld4056RRef", link_suppl_contr = "_Fld4055RRef",
               link_subdiv = "_Fld4051RRef", link_store = "_Fld4054RRef",
               link_organization = "_Fld4045RRef",
               doc_sum_currency = "_Fld4066", doc_rate = "_Fld4061") %>%
        mutate(doc_sum_currency = as.numeric(doc_sum_currency),
               doc_rate = as.numeric(doc_rate),
               doc_sum = doc_sum_currency * doc_rate,
               posted_return = as.logical(posted_return),
               is_managerial = as.logical(is_managerial)) %>% 
        filter(date_return >= date_start,
               date_return < date_finish,
               posted_return == "TRUE",
               is_managerial == "TRUE")
        
    tbl_return_supplier_tbl <- tbl(con,"_Document252_VT4098") %>% 
        select("_Document252_IDRRef", link_item = "_Fld4100RRef",
               item_qty = "_Fld4101", item_sum = "_Fld4106",
               link_receipt = "_Fld4111_RRRef")
    tbl_receipt <- tbl(con,"_Document360") %>% 
        select("_IDRRef", nmbr_receipt = "_Number", date_receipt = "_Date_Time")
        
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description")
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", supplier_name = "_Description")
    tbl_contract <- tbl(con, "_Reference78") %>% 
        select("_IDRRef", supplier_contract = "_Description")
    tbl_organization <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organiz_name = "_Description")
    
    df_return <- tbl_return_supplier %>%
        left_join(tbl_return_supplier_tbl,
                                    by = c("_IDRRef" = "_Document252_IDRRef")) %>%
        left_join(tbl_receipt,      by = c(link_receipt = "_IDRRef")) %>% 
        left_join(tbl_item,         by = c(link_item = "_IDRRef")) %>%
        left_join(tbl_partner,      by = c(link_supplier = "_IDRRef")) %>% 
        left_join(tbl_contract,     by = c(link_suppl_contr = "_IDRRef")) %>% 
        left_join(tbl_subdiv,       by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_store,        by = c(link_store = "_IDRRef")) %>%
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        select(nmbr_return, date_return, doc_sum,
               subdiv_name, store_name,
               supplier_name, supplier_contract, organiz_name,
               item_id, item_qty, item_sum, nmbr_receipt, date_receipt
        ) %>% 
        collect()
    
    return(df_return)
}


## Поступления товаров (приемка)
inspection_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    date_start_2 <- date_start - days(45)

    tbl_inspection_tbl <- tbl(con,"_Document21634_VT21637") %>% 
        select("_Document21634_IDRRef", link_item = "_Fld21639RRef",
               item_qty = "_Fld21640", deviation_qty = "_Fld21642") 
    tbl_inspection <- tbl(con,"_Document21634") %>% 
        select("_IDRRef", posted_inspection = "_Posted", nmbr_inspection = "_Number",
               date_inspection = "_Date_Time", link_picker = "_Fld21648RRef",
               time_start = "_Fld21649", time_finish = "_Fld21650",
               link_document_base = "_Fld21635_RRRef") %>%
        mutate(posted_inspection = as.logical(posted_inspection)) %>% 
        filter(date_inspection >= date_start,
               date_inspection < date_finish,
               posted_inspection == "TRUE")
    tbl_individuals <- tbl(con,"_Reference191") %>% 
        select("_IDRRef", user_id = "_Code")
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description")
    
    tbl_receipt <- tbl(con,"_Document360") %>% 
        select("_IDRRef", nmbr_receipt = "_Number",  date_receipt = "_Date_Time",
               link_store = "_Fld9770_RRRef", link_supplier = "_Fld9758RRef") %>% 
        filter(date_receipt >= date_start_2,
               date_receipt < date_finish)
    tbl_moving_out <- tbl(con,"_Document411") %>% 
        select("_IDRRef", nmbr_moving_out = "_Number",  date_moving_out = "_Date_Time",
               link_store_sender = "_Fld12656RRef",
               link_store_recipient = "_Fld12657RRef") %>% 
        filter(date_moving_out >= date_start_2,
               date_moving_out < date_finish)
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", supplier_id = "_Code")

    
    df_inspection <- tbl_inspection %>%
        left_join(tbl_individuals,    by = c(link_picker = "_IDRRef")) %>%
        left_join(tbl_inspection_tbl, by = c("_IDRRef" = "_Document21634_IDRRef")) %>%
        left_join(tbl_item,           by = c(link_item = "_IDRRef")) %>%
        left_join(tbl_receipt,        by = c(link_document_base = "_IDRRef")) %>%
        left_join(tbl_partner,        by = c(link_supplier = "_IDRRef")) %>% 
        left_join(tbl_store,          by = c(link_store = "_IDRRef")) %>%
        mutate(store_receipt = store_name) %>% 
        select(-store_name) %>% 
        left_join(tbl_moving_out,     by = c(link_document_base = "_IDRRef")) %>%
        left_join(tbl_store,          by = c(link_store_sender = "_IDRRef")) %>%
        mutate(store_sender = store_name) %>% 
        select(-store_name) %>%
        left_join(tbl_store,          by = c(link_store_recipient = "_IDRRef")) %>%
        mutate(store_recipient = store_name) %>%
        select(nmbr_inspection, date_inspection, user_id, time_start, time_finish, 
               item_id, item_qty, deviation_qty, 
               nmbr_receipt, date_receipt, store_receipt, supplier_id,
               nmbr_moving_out, date_moving_out, store_sender, store_recipient
        ) %>% 
        collect()
    
    return(df_inspection)
}


## Сборка товаров
assembly_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    date_start_2 <- date_start - days(30)
    date_finish_2 <- date_finish + days(3)
    
    tbl_assembly_tbl_1 <- tbl(con,"_Document21718_VT21727") %>% 
        select("_Document21718_IDRRef", link_item = "_Fld21729RRef",
               item_qty = "_Fld21730")
    tbl_assembly_tbl_2 <- tbl(con,"_Document21718_VT21734") %>% 
        select("_Document21718_IDRRef", link_box = "_Fld21736RRef",
               box_qty = "_Fld21737")
    tbl_assembly <- tbl(con,"_Document21718") %>% 
        select("_IDRRef", posted_assembly = "_Posted", nmbr_assembly = "_Number",
               date_assembly = "_Date_Time",
               time_start = "_Fld21725", time_finish = "_Fld21726",
               lieb_qty = "_Fld21755",
               link_document_base = "_Fld21719RRef") %>%
        mutate(posted_assembly = as.logical(posted_assembly)) %>% 
        filter(date_assembly >= date_start,
               date_assembly < date_finish,
               posted_assembly == "TRUE")
    tbl_individuals <- tbl(con,"_Reference191") %>% 
        select("_IDRRef", user_id = "_Code")
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_box <- tbl(con,"_Reference192") %>% 
        select("_IDRRef", box_id = "_Code")
    tbl_deliv_order <- tbl(con,"_Document409") %>% 
        select("_IDRRef", nmbr_deliv_order = "_Number",
               date_deliv_order = "_Date_Time", link_picker = "_Fld12599RRef",
               link_store = "_Fld12588_RRRef") %>% 
        filter(date_deliv_order >= date_start_2,
               date_deliv_order < date_finish_2)
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description")

    df_assembly <- tbl_assembly %>%
        left_join(tbl_assembly_tbl_2, by = c("_IDRRef" = "_Document21718_IDRRef")) %>%
        left_join(tbl_box,            by = c(link_box = "_IDRRef")) %>%
        left_join(tbl_assembly_tbl_1, by = c("_IDRRef" = "_Document21718_IDRRef")) %>%
        left_join(tbl_item,           by = c(link_item = "_IDRRef")) %>%
        left_join(tbl_deliv_order,    by = c(link_document_base = "_IDRRef")) %>%
        left_join(tbl_individuals,    by = c(link_picker = "_IDRRef")) %>%
        left_join(tbl_store,          by = c(link_store = "_IDRRef")) %>%
        select(nmbr_assembly, date_assembly, user_id,
               time_start, time_finish, lieb_qty, box_id, box_qty,
               item_id, item_qty,
               nmbr_deliv_order, date_deliv_order, store_name
        ) %>% 
        collect()
    
    return(df_assembly)
}


## Рекламации
reclamation_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)

    tbl_reclamation <- tbl(con,"_Document429") %>% 
        select("_IDRRef", posted = "_Posted", date_reclamation = "_Date_Time",
               link_subdiv = "_Fld13246RRef", link_base_doc = "_Fld13249_RRRef",
               link_culprit = "_Fld13247_RRRef",
               link_culprit_subdiv = "_Fld13248RRef") %>% 
        mutate(posted = as.logical(posted)) %>% 
        filter(date_reclamation >= date_start,
               date_reclamation < date_finish,
               posted == "TRUE")

    tbl_individuals <- tbl(con,"_Reference191") %>% 
        select("_IDRRef", culprit_user_id = "_Code")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_id = "_Code")
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", supplier_id = "_Code")
    tbl_receipt_doc <- tbl(con,"_Document360") %>% 
        select("_IDRRef", nmbr_receipt = "_Number", date_receipt = "_Date_Time")
    tbl_moving_out <- tbl(con,"_Document411") %>% 
        select("_IDRRef", nmbr_moving_out = "_Number",
               date_moving_out = "_Date_Time")

    
    df_reclamation <- tbl_reclamation %>%
        left_join(tbl_subdiv,      by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_moving_out,  by = c(link_base_doc = "_IDRRef")) %>%
        left_join(select(tbl_subdiv, "_IDRRef", culprit_subdiv = subdiv_id),
                                   by = c(link_culprit_subdiv = "_IDRRef")) %>%
        left_join(tbl_individuals, by = c(link_culprit = "_IDRRef")) %>%
        left_join(tbl_receipt_doc, by = c(link_base_doc = "_IDRRef")) %>%
        select(date_reclamation, subdiv_id,
               nmbr_moving_out, date_moving_out, culprit_subdiv, culprit_user_id,
               nmbr_receipt, date_receipt
        ) %>% 
        collect()
    
    return(df_reclamation)
}


## Заказ на доставку
delivery_order_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    date_start_2 <- date_start - days(30)
    
    tbl_deliv_order <- tbl(con,"_Document409") %>% 
        select("_IDRRef", posted_deliv_order = "_Posted", nmbr_deliv_order = "_Number",
               date_deliv_order = "_Date_Time", link_picker = "_Fld12599RRef",
               link_store = "_Fld12588_RRRef",
               time_start = "_Fld12618", time_finish = "_Fld12619",
               pack_duration = "_Fld12613", box_qty = "_Fld12609",
               order_weight = "_Fld12605", date_delivery = "_Fld12603",
               link_direction = "_Fld12592RRef", link_region = "_Fld12591RRef",
               link_route = "_Fld19669RRef", link_delivery_type = "_Fld19733RRef",
               link_address = "_Fld12593RRef", link_recipient = "_Fld12590_RRRef",
               link_intermediate = "_Fld12589RRef", nmbr_declar = "_Fld21329",
               link_document_base = "_Fld12587_RRRef") %>%
        mutate(posted_deliv_order = as.logical(posted_deliv_order)) %>% 
        filter(date_deliv_order >= date_start,
               date_deliv_order < date_finish,
               posted_deliv_order == "TRUE")
    tbl_individuals <- tbl(con,"_Reference191") %>% 
        select("_IDRRef", user_id = "_Code")
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description", store_id = "_Code")
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", customer_id = "_Code")
    tbl_direction <- tbl(con, "_Reference193") %>% 
        select("_IDRRef", direction = "_Description")
    tbl_region <- tbl(con, "_Reference149") %>% 
        select("_IDRRef", region = "_Description")
    tbl_route <- tbl(con, "_Reference19642") %>% 
        select("_IDRRef", route = "_Description")
    tbl_deliv_type <- tbl(con, "_Reference19643") %>% 
        select("_IDRRef", deliv_type = "_Description")
    tbl_address <- tbl(con, "_Reference194") %>% 
        select("_IDRRef", deliv_address = "_Description")
    tbl_sale_doc <- tbl(con,"_Document375") %>% 
        select("_IDRRef", nmbr_sale_doc = "_Number", date_sale_doc = "_Date_Time") %>% 
        filter(date_sale_doc >= date_start_2,
               date_sale_doc < date_finish)
    tbl_moving_out <- tbl(con,"_Document411") %>% 
        select("_IDRRef", nmbr_moving_out = "_Number",
               date_moving_out = "_Date_Time") %>% 
        filter(date_moving_out >= date_start_2,
               date_moving_out < date_finish)
    tbl_event <- tbl(con,"_Document382") %>% 
        select("_IDRRef", nmbr_event = "_Number",
               date_event = "_Date_Time") %>% 
        filter(date_event >= date_start_2,
               date_event < date_finish)
    
    df_deliv_order <- tbl_deliv_order %>%
        left_join(tbl_individuals, by = c(link_picker = "_IDRRef")) %>%
        left_join(tbl_store,       by = c(link_store = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", recipient_store = store_id),
                                   by = c(link_recipient = "_IDRRef")) %>%
        left_join(tbl_partner,     by = c(link_recipient = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", intermediate_store = store_name),
                                   by = c(link_intermediate = "_IDRRef")) %>%
        left_join(tbl_direction,   by = c(link_direction = "_IDRRef")) %>%
        left_join(tbl_region,      by = c(link_region = "_IDRRef")) %>%
        left_join(tbl_route,       by = c(link_route = "_IDRRef")) %>%
        left_join(tbl_deliv_type,  by = c(link_delivery_type = "_IDRRef")) %>%
        left_join(tbl_address,     by = c(link_address = "_IDRRef")) %>%
        left_join(tbl_sale_doc,    by = c(link_document_base = "_IDRRef")) %>%
        left_join(tbl_moving_out,  by = c(link_document_base = "_IDRRef")) %>%
        left_join(tbl_event     ,  by = c(link_document_base = "_IDRRef")) %>%
        select(nmbr_deliv_order, date_deliv_order,
               direction, region, route, deliv_type, deliv_address, store_id,
               store_name, intermediate_store, user_id, time_start, time_finish,
               pack_duration, box_qty, order_weight,
               nmbr_sale_doc, date_sale_doc, date_delivery, customer_id,
               nmbr_moving_out, date_moving_out, recipient_store,
               nmbr_event, date_event, nmbr_declar
        ) %>% 
        collect()
    
    return(df_deliv_order)
}


## Заказы покупателей
customer_order_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_order_doc <- tbl(con,"_Document262") %>% 
        select("_IDRRef", posted_doc="_Posted", nmbr_cust_order_doc = "_Number",
               date_cust_order_doc = "_Date_Time", link_organiz ="_Fld4552RRef",
               link_subdiv = "_Fld4554RRef", link_customer = "_Fld4548RRef",
               link_cust_contract = "_Fld4543RRef",
               link_responsible = "_Fld4553RRef",
               cust_order_doc_sum = "_Fld4558") %>% 
        mutate(posted_doc = as.logical(posted_doc)) %>% 
        filter(date_cust_order_doc >= date_start,
               date_cust_order_doc < date_finish,
               posted_doc == "TRUE")
    tbl_order_doc_tbl <- tbl(con, "_Document262_VT4577") %>% 
        select(link_item = "_Fld4584RRef", item_qty = "_Fld4581",
               item_sum = "_Fld4589", item_auto_discount = "_Fld4593",
               "_Document262_IDRRef")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")    
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", customer_id = "_Code")
    tbl_contract <- tbl(con, "_Reference78") %>% 
        select("_IDRRef", cust_contract_id = "_Code")
    tbl_organization <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organization = "_Description")
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_users <- tbl(con, "_Reference138") %>% 
        select("_IDRRef", responsible_name = "_Description")
    
    df_order<- tbl_order_doc %>%
        left_join(tbl_organization, by = c(link_organiz = "_IDRRef")) %>% 
        left_join(tbl_subdiv,       by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_partner,      by = c(link_customer = "_IDRRef")) %>% 
        left_join(tbl_contract,     by = c(link_cust_contract = "_IDRRef")) %>% 
        left_join(tbl_users,        by = c(link_responsible = "_IDRRef")) %>% 
        left_join(tbl_order_doc_tbl,by = c("_IDRRef"="_Document262_IDRRef")) %>%
        left_join(tbl_item,         by = c(link_item = "_IDRRef")) %>%
        select(nmbr_cust_order_doc, date_cust_order_doc, cust_order_doc_sum,
               organization, subdiv_name, customer_id, cust_contract_id,
               responsible_name,
               item_id, item_qty, item_sum, item_auto_discount
        ) %>% 
        collect()
    
    return(df_order)
}


## Перемещения-расход по выбранному складу
moving_out_data <- function(store, date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_moving_out <- tbl(con,"_Document411") %>% 
        select("_IDRRef", nmbr_moving_out = "_Number",
               date_moving_out = "_Date_Time", posted = "_Posted",
               link_sender = "_Fld12656RRef", link_receiver ="_Fld12657RRef")%>%
        mutate(posted = as.logical(posted)) %>% 
        filter(date_moving_out >= date_start,
               date_moving_out < date_finish)

    tbl_moving_out_tbl <- tbl(con,"_Document411_VT12661") %>% 
        select(link_item = "_Fld12663RRef", item_qty = "_Fld12668",
               "_Document411_IDRRef")
    
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_id = "_Code")
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    
    df_moving_out <- tbl_moving_out %>% 
        left_join(select(tbl_store, "_IDRRef", sender_store = store_id),
                  by = c(link_sender = "_IDRRef")) %>%
        filter(sender_store == store) %>% 
        left_join(select(tbl_store, "_IDRRef", receiver_store = store_id),
                  by = c(link_receiver = "_IDRRef")) %>%
        left_join(tbl_moving_out_tbl,
                  by = c("_IDRRef" = "_Document411_IDRRef")) %>%
        left_join(tbl_item, by = c(link_item = "_IDRRef")) %>%
        select(nmbr_moving_out, date_moving_out, sender_store, receiver_store,
               item_id, item_qty) %>% 
        collect()

    return(df_moving_out)
}


## Реализация товаров и услуг детальная
sale_data_full <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)

    tbl_sale_doc <- tbl(con,"_Document375") %>% 
        select("_IDRRef", posted_sale_doc = "_Posted", nmbr_sale_doc = "_Number",
               is_managerial = "_Fld10901", date_sale_doc = "_Date_Time",
               link_subdiv = "_Fld10904RRef", link_store = "_Fld10907RRef", 
               link_customer = "_Fld10909RRef", link_project = "_Fld10927_RRRef") %>% 
        mutate(posted_sale_doc = as.logical(posted_sale_doc),
               is_managerial = as.logical(is_managerial)) %>% 
        filter(date_sale_doc >= date_start,
               date_sale_doc < date_finish,
               posted_sale_doc == "TRUE",
               is_managerial == "TRUE")
    tbl_sale_doc_tbl <- tbl(con, "_Document375_VT10956") %>% 
        select(link_item = "_Fld10964RRef", item_qty = "_Fld10961",
               item_sum = "_Fld10969", item_auto_discount = "_Fld10974",
               "_Document375_IDRRef")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")    
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description")
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", customer_id = "_Code")
    tbl_project <- tbl(con, "_Reference143") %>% 
        select("_IDRRef", project = "_Description")
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")

    df_sale <- tbl_sale_doc %>%
        left_join(tbl_sale_doc_tbl, by = c("_IDRRef" = "_Document375_IDRRef")) %>%
        left_join(tbl_item,         by = c(link_item = "_IDRRef")) %>%
        left_join(tbl_partner,      by = c(link_customer = "_IDRRef")) %>% 
        left_join(tbl_subdiv,       by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_store,        by = c(link_store = "_IDRRef")) %>%
        left_join(tbl_project,      by = c(link_project = "_IDRRef")) %>%
        select(nmbr_sale_doc, date_sale_doc,
               subdiv_name, store_name, customer_id, project,
               item_id, item_qty, item_sum, item_auto_discount
        ) %>% 
        collect()
    
    return(df_sale)
}


## Реализация товаров и услуг общая
sale_data_global <- function(date_start, date_finish) {
  
    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)

    tbl_sale_doc <- tbl(con,"_Document375") %>% 
        select("_IDRRef", posted_sale_doc = "_Posted", nmbr_sale_doc = "_Number",
               is_managerial = "_Fld10901", date_sale_doc = "_Date_Time", 
               link_subdiv = "_Fld10904RRef", link_store = "_Fld10907RRef", 
               link_customer = "_Fld10909RRef", link_project = "_Fld10927_RRRef",
               link_order = "_Fld10906_RRRef", doc_sum = "_Fld10920") %>% 
        mutate(posted_sale_doc = as.logical(posted_sale_doc),
               is_managerial = as.logical(is_managerial)) %>% 
        filter(date_sale_doc >= date_start,
               date_sale_doc < date_finish,
               posted_sale_doc == "TRUE",
               is_managerial == "TRUE")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")    
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description")
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", customer_id = "_Code", customer_name = "_Description")
    tbl_project <- tbl(con, "_Reference143") %>% 
        select("_IDRRef", project = "_Description")
    tbl_cust_order <- tbl(con, "_Document262") %>% 
        select("_IDRRef", nmbr_cust_order_doc = "_Number",
               date_cust_order_doc = "_Date_Time", cust_order_sum = "_Fld4558",
               cust_order_date_delivery = "_Fld4541")

    df_sale <- tbl_sale_doc %>%
        left_join(tbl_partner,    by = c(link_customer = "_IDRRef")) %>% 
        left_join(tbl_subdiv,     by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_store,      by = c(link_store = "_IDRRef")) %>%
        left_join(tbl_project,    by = c(link_project = "_IDRRef")) %>%
        left_join(tbl_cust_order, by = c(link_order = "_IDRRef")) %>%
        select(nmbr_sale_doc, date_sale_doc, doc_sum,
               nmbr_cust_order_doc, date_cust_order_doc, cust_order_sum,
               cust_order_date_delivery, subdiv_name, store_name, project,
               customer_id, customer_name
        ) %>% 
        collect()
    
    return(df_sale)
}


## Возврат товаров от покупателей
refund_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)

    tbl_refund_doc <- tbl(con,"_Document251") %>% 
        select("_IDRRef", posted_refund_doc = "_Posted", nmbr_refund_doc = "_Number",
               is_managerial = "_Fld3903", date_refund_doc = "_Date_Time",
               link_subdiv = "_Fld3917RRef", link_store = "_Fld3907_RRRef", 
               link_customer = "_Fld3913RRef", link_project = "_Fld3922_RRRef") %>% 
        mutate(posted_refund_doc = as.logical(posted_refund_doc),
               is_managerial = as.logical(is_managerial)) %>% 
        filter(date_refund_doc >= date_start,
               date_refund_doc < date_finish,
               posted_refund_doc == "TRUE",
               is_managerial == "TRUE")
    tbl_refund_doc_tbl <- tbl(con, "_Document251_VT3939") %>% 
        select(link_item = "_Fld3941RRef", item_qty = "_Fld3942",
               item_sum = "_Fld3948", link_sale_doc = "_Fld3953_RRRef",
               "_Document251_IDRRef")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")    
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description")
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", customer_id = "_Code")
    tbl_project <- tbl(con, "_Reference143") %>% 
        select("_IDRRef", project = "_Description")
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_sale_doc <- tbl(con,"_Document375") %>% 
        select("_IDRRef", nmbr_sale_doc = "_Number", date_sale_doc = "_Date_Time")
    
    df_refund <- tbl_refund_doc %>%
        left_join(tbl_refund_doc_tbl, by = c("_IDRRef" = "_Document251_IDRRef")) %>%
        left_join(tbl_item,           by = c(link_item = "_IDRRef")) %>%
        left_join(tbl_partner,        by = c(link_customer = "_IDRRef")) %>% 
        left_join(tbl_subdiv,         by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_store,          by = c(link_store = "_IDRRef")) %>%
        left_join(tbl_project,        by = c(link_project = "_IDRRef")) %>%
        left_join(tbl_sale_doc,       by = c(link_sale_doc = "_IDRRef")) %>% 
        select(nmbr_refund_doc, date_refund_doc,
               subdiv_name, store_name, customer_id, project,
               item_id, item_qty, item_sum,
               nmbr_sale_doc, date_sale_doc
        ) %>% 
        collect()
    
    return(df_refund)
}


## План продаж
sale_plan_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_saleplan_doc <- tbl(con,"_Document350") %>% 
        select("_IDRRef", posted_saleplan_doc = "_Posted",
               date_saleplan_doc = "_Fld8946",
               link_subdiv = "_Fld8951RRef", link_project = "_Fld8952RRef", 
               link_script = "_Fld8953RRef", saleplan_doc_sum = "_Fld8954") %>% 
        mutate(posted_saleplan_doc = as.logical(posted_saleplan_doc)) %>% 
        filter(date_saleplan_doc >= date_start,
               date_saleplan_doc < date_finish,
               posted_saleplan_doc == "TRUE")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")    
    tbl_project <- tbl(con, "_Reference143") %>% 
        select("_IDRRef", project = "_Description")
    tbl_script <- tbl(con,"_Reference178") %>% 
        select("_IDRRef", script_name = "_Description")

    df_saleplan <- tbl_saleplan_doc %>%
        left_join(tbl_subdiv,         by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_project,        by = c(link_project = "_IDRRef")) %>%
        left_join(tbl_script,         by = c(link_script = "_IDRRef")) %>%
        select(date_saleplan_doc,  subdiv_name, project, script_name,
               saleplan_doc_sum

        ) %>% 
        collect()
    
    return(df_saleplan)
}


## План продаж детальный
sale_plan_data_full <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_saleplan_doc <- tbl(con,"_Document350") %>% 
        select("_IDRRef", posted_saleplan_doc = "_Posted",
               date_saleplan_doc = "_Fld8946",
               link_subdiv = "_Fld8951RRef", link_project = "_Fld8952RRef", 
               link_script = "_Fld8953RRef", saleplan_doc_sum = "_Fld8954") %>% 
        mutate(posted_saleplan_doc = as.logical(posted_saleplan_doc)) %>% 
        filter(date_saleplan_doc >= date_start,
               date_saleplan_doc < date_finish,
               posted_saleplan_doc == "TRUE")
    tbl_saleplan_doc_tbl <- tbl(con, "_Document350_VT8960") %>% 
        select(link_partner = "_Fld8973RRef", link_item = "_Fld8962_RRRef",
               sum = "_Fld8968", "_Document350_IDRRef")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")    
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", customer_name = "_Description")
    tbl_project <- tbl(con, "_Reference143") %>% 
        select("_IDRRef", project = "_Description")
    tbl_script <- tbl(con,"_Reference178") %>% 
        select("_IDRRef", script_name = "_Description")
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    
    df_saleplan <- tbl_saleplan_doc %>%
        left_join(tbl_subdiv,         by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_project,        by = c(link_project = "_IDRRef")) %>%
        left_join(tbl_script,         by = c(link_script = "_IDRRef")) %>%
        left_join(tbl_saleplan_doc_tbl, by = c("_IDRRef" = "_Document350_IDRRef")) %>%
        left_join(tbl_partner,        by = c(link_partner = "_IDRRef")) %>%
        left_join(tbl_item,           by = c(link_item = "_IDRRef")) %>%
        select(date_saleplan_doc,  subdiv_name, project, script_name,
               saleplan_doc_sum, customer_name, item_id, sum
        ) %>% 
        collect()
    
    return(df_saleplan)
}


## План продаж розничный с вал.приб.
sale_plan_retail <- function(date_start, date_finish) {
 
    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_saleplan_doc <- tbl(con,"_Document350") %>% 
        select("_IDRRef", posted_saleplan_doc = "_Posted",
               date_saleplan_doc = "_Fld8946",
               link_subdiv = "_Fld8951RRef", link_script = "_Fld8953RRef") %>% 
        mutate(posted_saleplan_doc = as.logical(posted_saleplan_doc)) %>% 
        filter(date_saleplan_doc >= date_start,
               date_saleplan_doc < date_finish,
               posted_saleplan_doc == "TRUE")
    tbl_saleplan_doc_tbl <- tbl(con, "_Document350_VT22292") %>% 
        select(link_category = "_Fld22294RRef", link_manager = "_Fld22295RRef",
               plan_sale = "_Fld22299", plan_profit = "_Fld22828",
               "_Document350_IDRRef")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")    
    tbl_script <- tbl(con,"_Reference178") %>% 
        select("_IDRRef", script_code = "_Code")
    tbl_category <- tbl(con,"_Reference19598") %>% 
        select("_IDRRef", category = "_Description")
    tbl_manager <- tbl(con,"_Reference138") %>% 
        select("_IDRRef", manager = "_Description")
    
    df_saleplan <- tbl_saleplan_doc_tbl %>%
        left_join(tbl_category,     by = c(link_category = "_IDRRef")) %>%
        left_join(tbl_manager,      by = c(link_manager = "_IDRRef")) %>%
        left_join(tbl_saleplan_doc, by = c("_Document350_IDRRef"="_IDRRef")) %>%
        left_join(tbl_subdiv,       by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_script,       by = c(link_script = "_IDRRef")) %>%
        filter(script_code == "000000075") %>%
        select(date_saleplan_doc,  subdiv_name, category, manager,
               plan_sale, plan_profit
        ) %>% 
        collect()
    
    return(df_saleplan)
}


## План продаж по КН с вал.приб.
sale_plan_category <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_saleplan_doc <- tbl(con,"_Document350") %>% 
        select("_IDRRef", posted_saleplan_doc = "_Posted",
               date_saleplan_doc = "_Fld8946", link_script = "_Fld8953RRef") %>% 
        mutate(posted_saleplan_doc = as.logical(posted_saleplan_doc)) %>% 
        filter(date_saleplan_doc >= date_start,
               date_saleplan_doc < date_finish,
               posted_saleplan_doc == "TRUE")
    tbl_saleplan_doc_tbl <- tbl(con, "_Document350_VT22302") %>% 
        select(link_category = "_Fld22305RRef", link_manager = "_Fld22306RRef",
               plan_sale = "_Fld22308", plan_profit = "_Fld22833",
               "_Document350_IDRRef")
   
    tbl_script <- tbl(con,"_Reference178") %>% 
        select("_IDRRef", script_code = "_Code")
    tbl_category <- tbl(con,"_Reference19598") %>% 
        select("_IDRRef", category = "_Description")
    tbl_manager <- tbl(con,"_Reference138") %>% 
        select("_IDRRef", manager = "_Description")
    
    df_saleplan <- tbl_saleplan_doc_tbl %>%
        left_join(tbl_category,     by = c(link_category = "_IDRRef")) %>%
        left_join(tbl_manager,      by = c(link_manager = "_IDRRef")) %>%
        left_join(tbl_saleplan_doc, by = c("_Document350_IDRRef"="_IDRRef")) %>%
        left_join(tbl_script,       by = c(link_script = "_IDRRef")) %>%
        filter(script_code == "000000076") %>%
        select(date_saleplan_doc, category, manager,
               plan_sale, plan_profit
        ) %>% 
        collect()
    
    return(df_saleplan)
}


## Себестоимость проданых товаров
cost_data <- function(date_start, date_finish) {
 
    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_cost <- tbl(con,"_AccumRg17487") %>% 
        select(date = "_Period", link_doc = "_RecorderRRef",
               link_item = "_Fld17488RRef",
               link_subdiv = "_Fld17492RRef", link_project = "_Fld17493RRef",
               item_qty = "_Fld17494", item_sum = "_Fld17495",
               item_vat = "_Fld17496") %>% 
        filter(date >= date_start,
               date < date_finish)
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description", subdiv_id = "_Code")    
    tbl_project <- tbl(con, "_Reference143") %>% 
        select("_IDRRef", project_id = "_Code", project = "_Description")
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description", store_id = "_Code")
    tbl_sale_doc <- tbl(con,"_Document375") %>% 
        select("_IDRRef", nmbr_sale_doc = "_Number",
               link_store = "_Fld10907RRef")
    tbl_refund_doc <- tbl(con,"_Document251") %>% 
        select("_IDRRef", nmbr_refund_doc = "_Number",
               link_store_return = "_Fld3907_RRRef")
    tbl_check_doc <- tbl(con,"_Document329") %>% 
        select("_IDRRef", nmbr_check_doc = "_Number",
               link_check_store = "_Fld7972RRef")

    df_cost <- tbl_cost %>%
        left_join(tbl_item,        by = c(link_item = "_IDRRef")) %>%
        left_join(tbl_subdiv,      by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_project,     by = c(link_project = "_IDRRef")) %>%
        left_join(tbl_sale_doc,    by = c(link_doc = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", sale_doc_store = store_id),
                  by = c(link_store = "_IDRRef")) %>%
        left_join(tbl_refund_doc,  by = c(link_doc = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", return_store = store_id),
                  by = c(link_store_return = "_IDRRef")) %>%
        left_join(tbl_check_doc,   by = c(link_doc = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", check_store = store_id),
                  by = c(link_check_store = "_IDRRef")) %>%
        mutate(store_id = case_when(
            !is.na(check_store)    ~ check_store,
            !is.na(sale_doc_store) ~ sale_doc_store,
            !is.na(return_store)   ~ return_store
        )) %>%
        select(date, nmbr_sale_doc, nmbr_refund_doc, nmbr_check_doc,
               nmbr_refund_doc,
               subdiv_id, subdiv_name, project_id, project, store_id,
               item_id, item_qty, item_sum, item_vat
        ) %>% 
        collect()
    
    return(df_cost)
}


## Багажка
laggage_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)

    tbl_laggage <- tbl(con,"_Document417") %>% 
        select("_IDRRef", posted = "_Posted", laggage_weight = "_Fld12883",
               nmbr_laggage = "_Number", date_laggage = "_Date_Time",
               link_carrier = "_Fld12899RRef", declar_nmbr = "_Fld12895",
               link_sender = "_Fld12898_RRRef", link_receiver="_Fld12900_RRRef",
               link_intermediate = "_Fld18338RRef") %>% 
        mutate(posted = as.logical(posted)) %>% 
        filter(date_laggage >= date_start,
               date_laggage <  date_finish,
               posted == "TRUE")

    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_id = "_Code")
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", partner_id = "_Code")

    df_laggage <- tbl_laggage %>%
        left_join(select(tbl_partner, "_IDRRef", carrier_id = partner_id),
                  by = c(link_carrier = "_IDRRef")) %>%
        left_join(select(tbl_partner, "_IDRRef", sender_partner_id = partner_id),
                  by = c(link_sender = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", sender_store_id = store_id),
                  by = c(link_sender = "_IDRRef")) %>%
        left_join(select(tbl_partner, "_IDRRef", receiver_partner_id = partner_id),
                  by = c(link_receiver = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", receiver_store_id = store_id),
                  by = c(link_receiver = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", intermediate_id = store_id),
                  by = c(link_intermediate = "_IDRRef")) %>%
        select(nmbr_laggage, date_laggage, laggage_weight,
               carrier_id, declar_nmbr,
               sender_partner_id, sender_store_id, 
               receiver_partner_id, receiver_store_id,
               intermediate_id
        ) %>% 
        collect()
    
    return(df_laggage)
}


## Развозка
delivery_data <- function(date_start, date_finish) {
 
    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    date_start_2 <- date_start - weeks(2)

    tbl_delivery <- tbl(con,"_Document416") %>% 
        select("_IDRRef", posted_delivery = "_Posted", nmbr_delivery = "_Number",
               date_delivery = "_Date_Time", 
               link_store = "_Fld12841RRef", 
               time_depart = "_Fld12837", time_return = "_Fld12838",
               link_direction = "_Fld12831RRef", link_route = "_Fld19689RRef",
               link_vehicle = "_Fld12834RRef", delivery_weight = "_Fld12830",
               speedo_start = "_Fld12835", speedo_finish = "_Fld12836") %>%
        mutate(posted_delivery = as.logical(posted_delivery)) %>% 
        filter(time_depart >= date_start,
               time_depart < date_finish,
               posted_delivery == "TRUE")
    tbl_delivery_tbl_forwarder <- tbl(con, "_Document416_VT12850") %>% 
        select(link_forwarder = "_Fld12852RRef", "_Document416_IDRRef")
    tbl_delivery_tbl_driver <- tbl(con, "_Document416_VT12853") %>% 
        select(link_driver = "_Fld12855RRef", "_Document416_IDRRef")
    tbl_delivery_tbl_fuel <- tbl(con, "_Document416_VT12867") %>% 
        select(link_fuel = "_Fld12869RRef", fuel_consumption = "_Fld21170",
               "_Document416_IDRRef")
    tbl_delivery_tbl_order_deliv <- tbl(con, "_Document416_VT12842") %>% 
        select(link_order_deliv = "_Fld12844_RRRef", "_Document416_IDRRef",
               to_intermediate = "_Fld12845", is_not_delivered = "_Fld12846") %>% 
        mutate(to_intermediate = as.logical(to_intermediate),
               is_not_delivered = as.logical(is_not_delivered))
    tbl_individuals <- tbl(con,"_Reference191") %>% 
        select("_IDRRef", user_id = "_Code")
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description", store_id = "_Code")
    tbl_direction <- tbl(con, "_Reference193") %>% 
        select("_IDRRef", direction = "_Description")
    tbl_route <- tbl(con, "_Reference19642") %>% 
        select("_IDRRef", route_id = "_Code", route = "_Description")
    tbl_vehicle <- tbl(con, "_Reference195") %>% 
        select("_IDRRef", vehicle_id = "_Code", vehicle_name = "_Description")
    # tbl_fuel <- tbl(con, "_Reference197") %>% 
    #     select("_IDRRef", fuel_item_link = "_Fld2760RRef")
    # tbl_item <- tbl(con,"_Reference120") %>% 
    #     select("_IDRRef", item_id = "_Code")
    tbl_deliv_order <- tbl(con,"_Document409") %>% 
        select("_IDRRef", posted_deliv_order = "_Posted", nmbr_deliv_order = "_Number",
               date_deliv_order = "_Date_Time") %>% 
        mutate(posted_deliv_order = as.logical(posted_deliv_order)) %>% 
        filter(date_deliv_order >= date_start_2,
               date_deliv_order < date_finish,
               posted_deliv_order == "TRUE")
    tbl_laggage <- tbl(con,"_Document417") %>% 
        select("_IDRRef", nmbr_laggage = "_Number", date_laggage = "_Date_Time")

    df_delivery <- tbl_delivery %>%
        # left_join(tbl_delivery_tbl_fuel, 
        #           by = c("_IDRRef" = "_Document416_IDRRef")) %>%
        # left_join(tbl_fuel,        by = c(link_fuel = "_IDRRef")) %>%
        # left_join(tbl_item,        by = c(fuel_item_link = "_IDRRef")) %>%
        left_join(tbl_delivery_tbl_forwarder, 
                                   by = c("_IDRRef" = "_Document416_IDRRef")) %>% 
        left_join(select(tbl_individuals, "_IDRRef", forwarder = user_id),
                                   by = c(link_forwarder = "_IDRRef")) %>%
        left_join(tbl_delivery_tbl_driver, 
                                   by = c("_IDRRef" = "_Document416_IDRRef")) %>% 
        left_join(select(tbl_individuals, "_IDRRef", driver = user_id),
                                   by = c(link_driver = "_IDRRef")) %>%
        left_join(tbl_store,       by = c(link_store = "_IDRRef")) %>%
        left_join(tbl_direction,   by = c(link_direction = "_IDRRef")) %>%
        left_join(tbl_route,       by = c(link_route = "_IDRRef")) %>%
        left_join(tbl_vehicle,     by = c(link_vehicle = "_IDRRef")) %>%
        left_join(tbl_delivery_tbl_order_deliv, 
                                   by = c("_IDRRef" = "_Document416_IDRRef")) %>%
        left_join(tbl_deliv_order, by = c(link_order_deliv = "_IDRRef")) %>%
        left_join(tbl_laggage,     by = c(link_order_deliv = "_IDRRef")) %>%
        select(nmbr_delivery, date_delivery,
               forwarder, driver, vehicle_id, vehicle_name,
               direction, route_id, route, store_id,store_name, delivery_weight,
               time_depart, time_return, speedo_start, speedo_finish,
               # item_id, fuel_consumption,
               to_intermediate, is_not_delivered,
               nmbr_deliv_order, date_deliv_order,
               nmbr_laggage, date_laggage
        ) %>% 
        collect()
    
    return(df_delivery)
}


## Маршрутный лист
route_sheet_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)

    tbl_route_sheet <- tbl(con,"_Document23048") %>% 
        select("_IDRRef", posted_route_sheet = "_Posted", 
               nmbr_route_sheet = "_Number", date_route_sheet = "_Date_Time",
               link_delivery = "_Fld23049RRef", link_individual="_Fld23051RRef",
               link_data_terminal = "_Fld23050RRef",
               crew_hours = "_Fld23112", engine_running_time = "_Fld23115",
               avr_speed = "_Fld23113", max_speed = "_Fld23114") %>% 
        mutate(posted_route_sheet = as.logical(posted_route_sheet)) %>% 
        filter(date_route_sheet >= date_start,
               date_route_sheet < date_finish,
               posted_route_sheet == "TRUE")
    tbl_route_sheet_tbl_deliv_order <- tbl(con, "_Document23048_VT23087") %>% 
        select(link_doc = "_Fld23089_RRRef",
               is_scaned = "_Fld23091", scan_time = "_Fld23158",
               reject_reason = "_Fld23092", "_Document23048_IDRRef",
               latitude = "_Fld24340", longitude = "_Fld24341") %>% 
        mutate(is_scaned = as.logical(is_scaned))
    
    tbl_delivery <- tbl(con,"_Document416") %>% 
        select("_IDRRef", nmbr_delivery = "_Number", date_delivery="_Date_Time")
        

    tbl_individuals <- tbl(con,"_Reference191") %>% 
        select("_IDRRef", terminal_user = "_Code")
    tbl_data_terminal <- tbl(con,"_Reference18908") %>% 
        select("_IDRRef", terminal_id = "_Code")

    tbl_deliv_order <- tbl(con,"_Document409") %>% 
        select("_IDRRef", nmbr_deliv_order = "_Number",
               date_deliv_order = "_Date_Time")
    tbl_laggage <- tbl(con,"_Document417") %>% 
        select("_IDRRef", nmbr_laggage = "_Number", date_laggage = "_Date_Time")
    
    df_route_sheet <- tbl_route_sheet %>%
        left_join(tbl_route_sheet_tbl_deliv_order,
                  by = c("_IDRRef" = "_Document23048_IDRRef")) %>%
        left_join(tbl_delivery,      by = c(link_delivery = "_IDRRef")) %>%
        left_join(tbl_data_terminal, by = c(link_data_terminal = "_IDRRef")) %>%
        left_join(tbl_individuals,   by = c(link_individual = "_IDRRef")) %>%
        left_join(tbl_deliv_order,   by = c(link_doc = "_IDRRef")) %>%
        left_join(tbl_laggage,       by = c(link_doc = "_IDRRef")) %>%
        select(nmbr_route_sheet, date_route_sheet, nmbr_delivery, date_delivery,
               terminal_id, terminal_user, crew_hours, engine_running_time,
               avr_speed, max_speed,
               nmbr_deliv_order, date_deliv_order, nmbr_laggage, date_laggage,
               is_scaned, scan_time, reject_reason, latitude, longitude
        ) %>% 
        collect()
    
    return(df_route_sheet)
}


## Текущая задолженность контрагентов
partner_current_debt_data <- function() {

    date_start <- as_datetime(today() + years(2000))

    
    tbl_partner_settlements<- tbl(con,"_AccumRgT16915") %>% 
        select(date_settlements = "_Period", link_organization = "_Fld16910RRef",
               link_partner = "_Fld16911RRef", link_contract = "_Fld16906RRef",
               debt_sum = "_Fld16913") %>% 
        filter(date_settlements > date_start)


    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", partner_id = "_Code")
    tbl_contract <- tbl(con, "_Reference78") %>% 
        select("_IDRRef", contract_id = "_Code",
               link_contract_type = "_Fld1600RRef")
    tbl_type_contract <- tbl(con, "_Enum484") %>% 
        select("_IDRRef", type_contract = "_EnumOrder")
    tbl_organization <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organiz_name = "_Description")
    
    df_partner_debt <- tbl_partner_settlements  %>%
        left_join(tbl_partner,       by = c(link_partner = "_IDRRef")) %>% 
        left_join(tbl_contract,      by = c(link_contract = "_IDRRef")) %>% 
        left_join(tbl_type_contract, by = c(link_contract_type = "_IDRRef")) %>% 
        left_join(tbl_organization,  by = c(link_organization = "_IDRRef")) %>%
        # select(organiz_name, partner_id, type_contract, contract_id, date_settlements,
        #        debt_sum) %>% 
        # collect()
        group_by(organiz_name, partner_id, contract_id, type_contract) %>% 
        summarise(debt_sum = sum(debt_sum)) %>% 
        ungroup() %>% 
        filter(type_contract %in% c(0,1)) %>% 
        collect() %>% 
        mutate(type_contract = ifelse(type_contract == 0,
                                     "supplier",
                                     "customer"))
    
    return(df_partner_debt)
}


## Данные по взаиморасчетам с покупателями по документам расчета
customer_debt_moving_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_partner_debt_movement <- tbl(con,"_AccumRg16905") %>% 
        select(date_movement = "_Period", link_organization = "_Fld16910RRef",
               link_partner = "_Fld16911RRef", link_contract = "_Fld16906RRef",
               link_sale_doc = "_Fld16908_RRRef",
               type_moving = "_RecordKind", moving_sum = "_Fld16913") %>% 
        filter(date_movement >= date_start,
               date_movement < date_finish)
    
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", partner_id = "_Code")
    tbl_contract <- tbl(con, "_Reference78") %>% 
        select("_IDRRef", contract_id = "_Code",
               link_contract_type = "_Fld1600RRef")
    tbl_organization <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organization_name = "_Description")
    tbl_type_contract <- tbl(con, "_Enum484") %>% 
        select("_IDRRef", type_contract = "_EnumOrder")
    tbl_sale_doc <- tbl(con,"_Document375") %>% 
        select("_IDRRef", nmbr_sale_doc = "_Number",
               date_sale_doc = "_Date_Time", link_subdiv = "_Fld10904RRef",
               link_responsible = "_Fld10922RRef")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")
    tbl_users <- tbl(con, "_Reference138") %>% 
        select("_IDRRef", responsible_name = "_Description")

    df_customer_motion <- tbl_partner_debt_movement %>%
        left_join(tbl_partner,       by = c(link_partner = "_IDRRef")) %>% 
        left_join(tbl_contract,      by = c(link_contract = "_IDRRef")) %>% 
        left_join(tbl_organization,  by = c(link_organization = "_IDRRef")) %>%
        left_join(tbl_type_contract, by = c(link_contract_type = "_IDRRef")) %>%
        left_join(tbl_sale_doc,      by = c(link_sale_doc = "_IDRRef")) %>%
        left_join(tbl_subdiv,        by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_users,         by = c(link_responsible = "_IDRRef")) %>%
        select(date_movement, organization_name, partner_id, contract_id,
               type_contract, nmbr_sale_doc, date_sale_doc, subdiv_name,
               responsible_name, type_moving, moving_sum) %>% 
        filter(type_contract == 1) %>% 
        collect()

    return(df_customer_motion)
}


## Неоплаченные документы по договорам поупателей на дату
customer_debt_docs <- function(date_finish) {

    date_finish <- as_datetime(date_finish)
    
    tbl_partner_debt_movement <- tbl(con,"_AccumRg16905") %>% 
        select(date_movement = "_Period",
               link_partner = "_Fld16911RRef", link_contract = "_Fld16906RRef",
               link_sale_doc = "_Fld16908_RRRef",
               type_moving = "_RecordKind", moving_sum = "_Fld16913") %>% 
        filter(date_movement < date_finish)
    
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", partner_id = "_Code")
    tbl_contract <- tbl(con, "_Reference78") %>% 
        select("_IDRRef", contract_id = "_Code",
               link_contract_type = "_Fld1600RRef")
    tbl_type_contract <- tbl(con, "_Enum484") %>% 
        select("_IDRRef", type_contract = "_EnumOrder")
    tbl_sale_doc <- tbl(con,"_Document375") %>% 
        select("_IDRRef", nmbr_sale_doc = "_Number",
               date_sale_doc = "_Date_Time", link_subdiv = "_Fld10904RRef")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")
    
    
    customer_motion_tbl <- tbl_partner_debt_movement %>%
        left_join(tbl_partner,       by = c(link_partner = "_IDRRef")) %>% 
        left_join(tbl_contract,      by = c(link_contract = "_IDRRef")) %>% 
        left_join(tbl_type_contract, by = c(link_contract_type = "_IDRRef")) %>%
        left_join(tbl_sale_doc,      by = c(link_sale_doc = "_IDRRef")) %>%
        left_join(tbl_subdiv,        by = c(link_subdiv = "_IDRRef")) %>%
        filter(type_contract == 1) %>% 
        mutate(moving_sum = ifelse(type_moving == 1,
                                   moving_sum * (-1),
                                   moving_sum)) %>%
        group_by(partner_id, contract_id, subdiv_name, 
                 nmbr_sale_doc, date_sale_doc) %>% 
        summarise(doc_moving_sum = sum(moving_sum)) %>% 
        filter(doc_moving_sum > 0.01,
               !is.na(nmbr_sale_doc),
               !is.na(subdiv_name)) %>% 
        collect()
    
    return(customer_motion_tbl)
}

## Данные по взаиморасчетам с поставщиками по документам расчета
supplier_debt_moving_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_partner_debt_movement <- tbl(con,"_AccumRg16905") %>% 
        select(date_movement = "_Period", link_organization = "_Fld16910RRef",
               link_partner = "_Fld16911RRef", link_contract = "_Fld16906RRef",
               link_receipt_doc = "_Fld16908_RRRef",
               type_moving = "_RecordKind", moving_sum = "_Fld16913") %>% 
        filter(date_movement >= date_start,
               date_movement < date_finish)
    
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", partner_id = "_Code")
    tbl_contract <- tbl(con, "_Reference78") %>% 
        select("_IDRRef", contract_id = "_Code",
               link_contract_type = "_Fld1600RRef")
    tbl_organization <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organization_name = "_Description")
    tbl_type_contract <- tbl(con, "_Enum484") %>% 
        select("_IDRRef", type_contract = "_EnumOrder")
    tbl_receipt_doc <- tbl(con,"_Document360") %>% 
        select("_IDRRef", nmbr_receipt_doc = "_Number",
               date_receipt_doc = "_Date_Time")
    
    df_supplier_motion <- tbl_partner_debt_movement %>%
        left_join(tbl_partner,       by = c(link_partner = "_IDRRef")) %>% 
        left_join(tbl_contract,      by = c(link_contract = "_IDRRef")) %>% 
        left_join(tbl_organization,  by = c(link_organization = "_IDRRef")) %>%
        left_join(tbl_type_contract, by = c(link_contract_type = "_IDRRef")) %>%
        left_join(tbl_receipt_doc,      by = c(link_receipt_doc = "_IDRRef")) %>%
        select(date_movement, organization_name, partner_id, contract_id,
               type_contract, nmbr_receipt_doc, date_receipt_doc,
               type_moving, moving_sum) %>% 
        filter(type_contract == 1) %>% 
        collect()
    
    return(df_supplier_motion)
}


## Документ телемаркетинг
telemarketing_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_telemarketing_doc <- tbl(con,"_Document437") %>% 
        select("_IDRRef", posted_doc = "_Posted", nmbr_telemark_doc = "_Number",
               date_telemark_doc = "_Date_Time", link_subdiv = "_Fld13498RRef",
               link_project = "_Fld13499RRef", link_status = "_Fld13500RRef",
               link_cause = "_Fld21890RRef") %>% 
        mutate(posted_doc = as.logical(posted_doc)) %>% 
        filter(date_telemark_doc >= date_start,
               date_telemark_doc < date_finish,
               posted_doc == "TRUE")
    tbl_telemarketing_tbl <- tbl(con, "_Document437_VT13505") %>% 
        select(link_customer = "_Fld13508RRef", link_event = "_Fld13512RRef",
               "_Document437_IDRRef")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")    
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", customer_id = "_Code")
    tbl_project <- tbl(con,"_Reference143") %>% 
        select("_IDRRef", project = "_Description")
    tbl_cause <- tbl(con,"_Reference21889") %>% 
        select("_IDRRef", cause = "_Description")
    tbl_status <-tbl(con, "_Enum677") %>% 
        select("_IDRRef", type_status = "_EnumOrder")
    tbl_event <- tbl(con,"_Document382") %>% 
        select("_IDRRef", nmbr_event_doc = "_Number",
               date_event_doc = "_Date_Time")
    
    df_telemarketing <- tbl_telemarketing_doc %>%
        left_join(tbl_subdiv,         by = c(link_subdiv  = "_IDRRef")) %>%
        left_join(tbl_project,        by = c(link_project = "_IDRRef")) %>%
        #filter(project == "Сервис") %>% 
        left_join(tbl_status,         by = c(link_status = "_IDRRef")) %>%
        filter(type_status == 1) %>% 
        left_join(tbl_cause,          by = c(link_cause = "_IDRRef")) %>% 
        left_join(tbl_telemarketing_tbl, by = c("_IDRRef" = "_Document437_IDRRef")) %>%
        left_join(tbl_partner,        by = c(link_customer = "_IDRRef")) %>% 
        left_join(tbl_event,          by = c(link_event = "_IDRRef")) %>% 
        select(nmbr_telemark_doc, date_telemark_doc, subdiv_name, project,
               cause, type_status, customer_id, nmbr_event_doc, date_event_doc
        ) %>% 
        collect()
    
    return(df_telemarketing)
}


## Документ событие с конечным покупателем
event_final_buyer_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", customer_id = "_Code", parent = "_ParentIDRRef")
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_event <- tbl(con,"_Document382") %>% 
        select("_IDRRef", nmbr_event_doc = "_Number",
               date_event_doc = "_Date_Time", link_customer = "_Fld11368_RRRef") %>% 
        filter(date_event_doc >= date_start,
               date_event_doc <  date_finish)
    tbl_event_tbl <- tbl(con, "_Document382_VT11418") %>% 
        select(link_item = "_Fld11424RRef", item_qty = "_Fld11422",
               item_sum = "_Fld11428", "_Document382_IDRRef")
    
    df_event <- tbl_event %>%
        left_join(tbl_partner,   by = c(link_customer = "_IDRRef")) %>% 
        left_join(select(tbl_partner, "_IDRRef", folder = customer_id ),
                                 by = c(parent = "_IDRRef")) %>%
        filter(folder == "001004385") %>% 
        left_join(tbl_event_tbl, by = c("_IDRRef" = "_Document382_IDRRef")) %>%
        left_join(tbl_item,   by = c(link_customer = "_IDRRef")) %>% 
        select(nmbr_event_doc, date_event_doc, customer_id, item_id, item_qty,
               item_sum
        ) %>% 
        group_by(nmbr_event_doc, date_event_doc, customer_id) %>% 
        summarise(customer_sum = sum(item_sum, na.rm = TRUE)) %>% 
        ungroup() %>% 
        collect()
    
    return(df_event)
}


## Текущие остатки на складах (кол-во)
current_stock_balance <- function() {

    date_start <- as_datetime(today() + years(2000))
    
    tbl_stock_balance <- tbl(con,"_AccumRgT17710") %>% 
        select(date_balance = "_Period", link_store = "_Fld17703RRef",
               link_item = "_Fld17704RRef", item_store_qty ="_Fld17708") %>% 
        filter(date_balance > date_start)

    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", "_ParentIDRRef", item_id = "_Code")
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description")
    
    df_stock_balance <- tbl_stock_balance  %>%
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        left_join(tbl_item,  by = c(link_item = "_IDRRef")) %>%
        left_join(select(tbl_items, "_IDRRef", parent_item_id = item_id),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>%
        filter(!parent_item_id %in% c("000500558",       # Бухгалтерские
                                      "000300000",        # Торгів.обладнання
                                      "000400010")) %>%   # Рекламні материали
        select(store_name, item_id, item_store_qty) %>% 
        collect()

    return(df_stock_balance)
}


## Данные для расчета остатков на складах на определенную дату
stock_motion_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_stock_movement <- tbl(con,"_AccumRg17702") %>% 
        select(date_movement = "_Period", active = "_Active",
               link_store = "_Fld17703RRef", link_item = "_Fld17704RRef",
               type_motion = "_RecordKind", item_motion_qty = "_Fld17708") %>%
        mutate(active = as.logical(active)) %>%
        filter(active == "TRUE",
               date_movement >= date_start)
    
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", "_ParentIDRRef", item_id = "_Code")
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description", store_id = "_Code")
    
    df_stock_motion <- tbl_stock_movement %>%
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        left_join(tbl_item,  by = c(link_item = "_IDRRef")) %>% 
        left_join(select(tbl_item, "_IDRRef", parent_item_id = item_id),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>%
        filter(!parent_item_id %in% c("000500558",       # Бухгалтерские
                                      "000300000",        # Торгів.обладнання
                                      "000400010")) %>%   # Рекламні материали
        select(date_movement, store_name, store_id, type_motion,
               item_id, item_motion_qty) %>% 
        collect()
    
    return(df_stock_motion)
}


## Данные по рабочему времени работников
work_time_data <- function(date_start, date_finish) {
  
    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_work_time_rg <- tbl(con, "_AccumRgTN17516") %>% 
        select(date = "_Period",
               link_employee = "_Fld17505RRef",
               link_organization = "_Fld17507RRef", link_type = "_Fld17508RRef",
               working_days = "_Fld17511", working_hours = "_Fld17512") %>% 
        filter(date >= date_start,
               date < date_finish)
    
    # tbl_work_time_rg2 <- tbl(con, "_AccumRg17504") %>% 
    #     select(date = "_Period",
    #            link_employee = "_Fld17505RRef", link_table_doc ="_RecorderRRef",
    #            link_organization = "_Fld17507RRef", link_type = "_Fld17508RRef",
    #            working_days = "_Fld17511", working_hours = "_Fld17512") %>% 
    #     filter(date >= date_start,
    #            date <= date_finish)
    # 
    # tbl_work_time_table <- tbl(con, "_Document392") %>% 
    #     select("_IDRRef", posted_doc = "_Posted", doc_date = "_Date_Time",
    #            period = "_Fld11915", link_organization = "_Fld11914RRef", 
    #            link_subdiv_organiz = "_Fld11916RRef", doc_nmbr = "_Number") %>% 
    # mutate(posted_doc = as.logical(posted_doc)) %>% 
    # filter(doc_date >= date_start,
    #        doc_date < date_finish,
    #        posted_doc == "TRUE")
    
    tbl_organization <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organization_id = "_Code")
    # tbl_subdiv_organization <- tbl(con, "_Reference136") %>% 
    #     select("_IDRRef", subdiv_organiz_name = "_Description")
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
        mutate(last_date = max(date_rec, na.rm = TRUE)) %>% 
        ungroup() %>% 
        filter(date_rec == last_date) %>% 
        select(individ_id, position_id, subdiv_name)
    
    # df_work_time2 <- tbl_work_time_rg2 %>%
    #     left_join(tbl_organization,  by = c(link_organization = "_IDRRef")) %>%
    #     left_join(tbl_type_work_time,  by = c(link_type = "_IDRRef")) %>%
    #     left_join(tbl_employees,  by = c(link_employee = "_IDRRef")) %>%
    #     left_join(tbl_individuals,  by = c(link_individuals = "_IDRRef")) %>%
    #     left_join(tbl_work_time_table, by = c(link_table_doc = "_IDRRef")) %>% 
    #     left_join(tbl_subdiv_organization, by =c(link_subdiv_organiz = "_IDRRef")) %>% 
    #     select(date, organization_name, subdiv_organiz_name, employee_name,
    #            individ_name, individ_id, type_name, working_days, working_hours) %>% 
    #     group_by(organization_name, subdiv_organiz_name, individ_id, type_name) %>%
    #     summarise(total_working_days = sum(working_days),
    #               total_working_hours = sum(working_hours)) %>%
    #     collect() %>% 
    #     filter(organization_name == "Офис Центр",
    #            type_name == "р (01)")
    
    df_work_time <- tbl_work_time_rg %>%
        left_join(tbl_organization,  by = c(link_organization = "_IDRRef")) %>%
        left_join(tbl_type_work_time,  by = c(link_type = "_IDRRef")) %>%
        left_join(tbl_employees,  by = c(link_employee = "_IDRRef")) %>%
        left_join(tbl_individuals,  by = c(link_individuals = "_IDRRef")) %>%
        select(date, organization_id, individ_id,
               type_id, working_days, working_hours) %>%
        filter(
            organization_id == "000000034",
            type_id == "01",
            working_days > 0
        ) %>%
        left_join(data_workers) %>%
        select(date, subdiv_name, individ_id,  position_id,
               working_days, working_hours) %>% 
        collect()
    
    
    return(df_work_time)
}


## План продаж
plan_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_plan <- tbl(con,"_InfoRg14468") %>% 
        select(plan_period = "_Fld14469",
               link_subdiv = "_Fld14472_RRRef",
               link_indicator = "_Fld14470RRef", 
               value = "_Fld14474") %>%
        filter(plan_period >= date_start,
               plan_period < date_finish)
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")    
    tbl_indicator <- tbl(con, "_Reference137") %>% 
        select("_IDRRef", indicator_name = "_Description")

    
    df_plan <- tbl_plan %>%
        left_join(tbl_subdiv,    by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_indicator, by = c(link_indicator = "_IDRRef")) %>%
        select(plan_period,  subdiv_name, indicator_name, value
        ) %>% 
        collect()
    
    return(df_plan)
}


## Данные о розничных продажах постоянным клиентам (владельцы карт)
regular_cust_data <- function(date_start, date_finish) {

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
      as.character(date_finish - days(1)),
      '" as date)) and
          `t1`.`deleted` = 0 and
          `t1`.`type` not in(5)
      group by `t3`.`cash_id`, `t3`.`receipt_header`
      order by `t1`.`date` desc')
    
    tbl_raw <- dbGetQuery(con, query)
    
    dbDisconnect(con)
    
    names <- names(tbl_raw)
    new_names <- make.unique(names, sep="_")
    names(tbl_raw) <- new_names
    
    df <- tbl_raw %>% 
        select(date, store, store_1c, pos_name, local_number, client, card,
               type, amount, clear_amount, items_count, discounts_amount,
               discounts_account_amount, amount_2) %>% 
        mutate(amount = ifelse(type == 4,
                               amount * (-1),
                               amount),
               clear_amount = ifelse(type == 4,
                               clear_amount * (-1),
                               clear_amount),
               items_count = ifelse(type == 4,
                                    items_count * (-1),
                                    items_count),
               discounts_account_amount = ifelse(type == 4,
                                                 discounts_account_amount * (-1),
                                                 discounts_account_amount),
               amount_2 = ifelse(type == 4,
                                 amount_2 * (-1),
                                 amount_2))
    
    Encoding(df$store) <- "UTF-8"
    Encoding(df$pos_name) <- "UTF-8"
    
    return(df)
}


## Продажи по выбранной номенклатурной группе за период
sale_group_data <- function(date_start, date_finish, group_query) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_sale_doc <- tbl(con,"_Document375") %>% 
        select("_IDRRef", posted_sale_doc = "_Posted", nmbr_sale_doc = "_Number",
               is_managerial = "_Fld10901", date_sale_doc = "_Date_Time",
               link_project = "_Fld10927_RRRef") %>% 
        mutate(posted_sale_doc = as.logical(posted_sale_doc),
               is_managerial = as.logical(is_managerial)) %>% 
        filter(date_sale_doc >= date_start,
               date_sale_doc < date_finish,
               posted_sale_doc == "TRUE",
               is_managerial == "TRUE")
    tbl_sale_doc_tbl <- tbl(con, "_Document375_VT10956") %>% 
        select(link_item = "_Fld10964RRef", item_qty = "_Fld10961",
               item_sum = "_Fld10969",
               "_Document375_IDRRef")
    tbl_project <- tbl(con, "_Reference143") %>% 
        select("_IDRRef", project = "_Description")
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code", "_ParentIDRRef")

    
    df_sale <- tbl_sale_doc %>%
        left_join(tbl_sale_doc_tbl, by = c("_IDRRef" = "_Document375_IDRRef")) %>%
        left_join(tbl_item,         by = c(link_item = "_IDRRef")) %>%
        left_join(select(tbl_item, item_group = item_id, "_IDRRef"),
                                    by = c("_ParentIDRRef" = "_IDRRef")) %>%
        filter(item_group == group_query) %>% 
        left_join(tbl_project,      by = c(link_project = "_IDRRef")) %>%
        select(nmbr_sale_doc, date_sale_doc,
               project,
               item_id, item_qty, item_sum, item_group
        ) %>% 
        collect()
    
    return(df_sale)
}   


## Цены номенклатуры (У_Розн_1)
price_items_data <- function(date_start, price_type) {

    date_start <- as_datetime(date_start)
    
    tbl_price_setting <- tbl(con,"_InfoRg16225") %>% 
        select(date_setting = "_Period",
               link_priceType = "_Fld16226RRef", link_item = "_Fld16227RRef",
               active = "_Active", item_price = "_Fld16230") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(active == "TRUE")
    
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_priceType <- tbl(con, "_Reference183") %>% 
        select("_IDRRef", priceType_id = "_Code")
    
    df_price_item <- tbl_price_setting %>%
        left_join(tbl_priceType, by = c(link_priceType = "_IDRRef")) %>% 
        filter(priceType_id == price_type) %>% 
        left_join(tbl_item,  by = c(link_item = "_IDRRef")) %>% 
        select(date_setting, item_id, item_price, priceType_id) %>%
        filter(date_setting <= date_start) %>% 
        group_by(item_id) %>%
        filter(date_setting == max(date_setting)) %>%
        ungroup() %>%
        collect()
    
    return(df_price_item)
}


## Журнал Цен номенклатуры за период (У_Розн_1)
price_items_log <- function(date_start, date_finish, price_type) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_price_setting <- tbl(con,"_InfoRg16225") %>% 
        select(date_setting = "_Period",
               link_priceType = "_Fld16226RRef", link_item = "_Fld16227RRef",
               active = "_Active", item_price = "_Fld16230") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(active == "TRUE")
    
    tbl_item <- tbl(con,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_priceType <- tbl(con, "_Reference183") %>% 
        select("_IDRRef", priceType_id = "_Code")
    
    df_price_item <- tbl_price_setting %>%
        left_join(tbl_priceType, by = c(link_priceType = "_IDRRef")) %>% 
        filter(priceType_id == price_type) %>% 
        left_join(tbl_item,  by = c(link_item = "_IDRRef")) %>% 
        select(date_setting, item_id, item_price, priceType_id) %>%
        #filter(date_setting <= date_start) %>% 
        group_by(item_id) %>%
        mutate(date_start_price = max(date_setting[date_setting <= date_start])) %>% 
        filter((date_start_price <= date_start & date_setting >= date_start_price) |
                   is.na(date_start_price),
               date_setting <  date_finish) %>%
        ungroup() %>%
        collect()
    
    return(df_price_item)
}


## Затраты
expenses_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_expenses <- tbl(con,"_AccumRg17131") %>% 
        select(date = "_Period", active = "_Active", type_exp = "_RecordKind",
               link_subdiv = "_Fld17132RRef", link_expItem = "_Fld17133RRef",
               link_project = "_Fld21330RRef",
               exp_sum = "_Fld17136", exp_vat = "_Fld17137") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(active == "TRUE",
               date >= date_start,
               date < date_finish)
    
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description", subdiv_id = "_Code")
    tbl_project <- tbl(con,"_Reference143") %>% 
        select("_IDRRef", project = "_Description")
    tbl_expItem <- tbl(con,"_Reference169") %>% 
        select("_IDRRef", expItems_id = "_Code", expItems_name = "_Description")
    
    df_expenses <- tbl_expenses %>%
        left_join(tbl_subdiv,  by = c(link_subdiv = "_IDRRef")) %>% 
        left_join(tbl_project, by = c(link_project = "_IDRRef")) %>% 
        left_join(tbl_expItem, by = c(link_expItem = "_IDRRef")) %>%
        filter(expItems_id != "100000421") %>% 
        select(date, expItems_id, expItems_name, subdiv_id, subdiv_name, project,
               type_exp, exp_sum, exp_vat) %>%
        collect()
    
    return(df_expenses)
}


## Реализация товаров и услуг полная
sale_data_total <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_sale_reg <- tbl(con,"_AccumRg17435") %>% 
        select(date = "_Period", active = "_Active",
               link_organiz = "_Fld17443RRef", link_subdiv = "_Fld17441RRef",
               link_sale_doc = "_Fld17440_RRRef",
               link_item = "_Fld17436RRef", link_project = "_Fld17442RRef",
               item_qty = "_Fld17445", item_sum = "_Fld17446",
               item_sum_without_disc = "_Fld17446",item_vat = "_Fld17448") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date >= date_start,
               date < date_finish,
               active == "TRUE")
    tbl_checks <- tbl(con, "_Document329") %>% 
        select("_IDRRef", check_nmbr = "_Number",
               link_check_store = "_Fld7972RRef")
    tbl_sale_doc <- tbl(con,"_Document375") %>% 
        select("_IDRRef", sale_doc_nmbr = "_Number",
               link_store = "_Fld10907RRef",
               link_contract = "_Fld10908RRef")
    tbl_returns <- tbl(con, "_Document251") %>% 
        select("_IDRRef", returns_doc_nmbr = "_Number",
               link_store_return = "_Fld3907_RRRef")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description", subdiv_id = "_Code")    
    tbl_item <- tbl(con, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_project <- tbl(con, "_Reference143") %>% 
        select("_IDRRef", project_id = "_Code", project = "_Description")
    tbl_organiz <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organiz_id = "_Code")
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description", store_id = "_Code")
    tbl_contract <- tbl(con, "_Reference78") %>%
        select("_IDRRef", link_type = "_Fld1585RRef")
    tbl_type_settlement <- tbl(con, "_Reference49") %>%
        select("_IDRRef", is_cash = "_Fld18856") %>% 
        mutate(is_cash = as.logical(is_cash))
    
    df_sale <- tbl_sale_reg %>%
        left_join(tbl_checks,   by = c(link_sale_doc = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", check_store = store_id),
                  by = c(link_check_store = "_IDRRef")) %>%
        
        left_join(tbl_sale_doc, by = c(link_sale_doc = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", sale_doc_store = store_id),
                  by = c(link_store = "_IDRRef")) %>%
        left_join(tbl_contract,    by = c(link_contract = "_IDRRef")) %>%
        left_join(tbl_type_settlement, by = c(link_type = "_IDRRef")) %>%
        
        left_join(tbl_returns , by = c(link_sale_doc = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", return_store = store_id),
                  by = c(link_store_return = "_IDRRef")) %>%
        left_join(tbl_item,     by = c(link_item = "_IDRRef")) %>% 
        left_join(tbl_organiz,  by = c(link_organiz = "_IDRRef")) %>% 
        left_join(tbl_subdiv,   by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_project,  by = c(link_project = "_IDRRef")) %>%
        mutate(store_id = case_when(
            !is.na(check_store)    ~ check_store,
            !is.na(sale_doc_store) ~ sale_doc_store,
            !is.na(return_store)   ~ return_store
        )) %>% 
        select(date, organiz_id, subdiv_id, subdiv_name, project_id, project,
               store_id,
               check_nmbr, sale_doc_nmbr, is_cash, returns_doc_nmbr,
               item_id, item_qty, item_sum, item_sum_without_disc, item_vat) %>% 
        collect()
    
    return(df_sale)
}


## Сальдо на дату по заданному счету Корвет
account_balance <- function(date_finish, account) {

    date_finish <- as_datetime(date_finish)

    tbl_acc_credit <- tbl(con, "_AccRg855") %>% 
        select("_AccountCtRRef", date = "_Period", active = "_Active",
               link_organization = "_Fld856RRef", moving_sum = "_Fld859") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date < date_finish,
               active == "TRUE")
    tbl_acc_debet <- tbl(con, "_AccRg855") %>% 
        select("_AccountDtRRef", date = "_Period", active = "_Active",
               link_organization = "_Fld856RRef", moving_sum = "_Fld859") %>%
        mutate(active = as.logical(active)) %>%
        filter(date < date_finish,
               active == "TRUE")
    tbl_account <- tbl(con, "_Acc18") %>% 
        select("_IDRRef", acc_id = "_Code")
    tbl_organization <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organization_id = "_Code")
    
    acc_ct <- tbl_acc_credit %>% 
        left_join(tbl_account,      by = c("_AccountCtRRef" = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        filter(organization_id == "000000001",
               acc_id == account) %>% 
        summarise(ct_sum = sum(moving_sum)) %>% 
        collect() %>% 
        pull
    acc_dt <- tbl_acc_debet %>% 
        left_join(tbl_account,      by = c("_AccountDtRRef" = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        filter(organization_id == "000000001",
               acc_id == account) %>% 
        summarise(ct_sum = sum(moving_sum)) %>% 
        collect() %>% 
        pull()
    
    balance = acc_dt - acc_ct
    
    return(balance)
}


## Зарплата по работникам за период
salary_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_payroll <- tbl(con,"_Document312") %>% 
        select(date = "_Date_Time", posted = "_Posted", period_reg = "_Fld6706",
               # link_subdiv_org = "_Fld6708RRef",
               is_managerial = "_Fld6712", "_IDRRef") %>% 
        mutate(posted = as.logical(posted),
               is_managerial = as.logical(is_managerial)) %>% 
        filter(date >= date_start,
               date < date_finish,
               posted == "TRUE",
               is_managerial == "TRUE")
    tbl_payroll_tbl_1 <- tbl(con,"_Document312_VT6743") %>% 
        select("_Document312_IDRRef", link_employee_org = "_Fld6745RRef",
               link_position = "_Fld18155RRef", link_subdiv = "_Fld6763RRef",
               salary = "_Fld6753")
    tbl_payroll_tbl_2 <- tbl(con,"_Document312_VT7084") %>% 
        select("_Document312_IDRRef", link_employee_org = "_Fld7086RRef",
               sum = "_Fld7096")
    tbl_payroll_tbl_3 <- tbl(con,"_Document312_VT7114") %>% 
        select("_Document312_IDRRef", link_employee_org = "_Fld7116RRef",
               sum = "_Fld7119")
    # tbl_subdiv_org <- tbl(con, "_Reference136") %>% 
    #     select("_IDRRef", subdiv_organiz_id = "_Code")    
    tbl_employee_org <- tbl(con, "_Reference159") %>% 
        select("_IDRRef", link_individ = "_Fld2322RRef")
    tbl_individuals <- tbl(con,"_Reference191") %>% 
        select("_IDRRef", individ_id = "_Code")
    tbl_position <- tbl(con,"_Reference81") %>% 
        select("_IDRRef", position_id = "_Code")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description") 
    tbl_workers <- tbl(con,"_InfoRg15330") %>%
        select(date_rec = "_Period", link_individ = "_Fld15331RRef",
               link_subdiv = "_Fld15332RRef", link_position = "_Fld15333RRef") %>% 
        filter(date_rec < date_finish)
    
    data_workers <- tbl_workers %>% 
        left_join(tbl_individuals, by = c(link_individ = "_IDRRef")) %>% 
        left_join(tbl_position, by = c(link_position = "_IDRRef")) %>% 
        select(date_rec, individ_id, position_id) %>% 
        filter(!is.na(position_id)) %>% 
        group_by(individ_id) %>%
        filter(date_rec == max(date_rec, na.rm = TRUE)) %>% 
        ungroup() %>%
        select(individ_id, position_id)
    
    df_salary <- tbl_payroll %>%
        # left_join(tbl_subdiv_org,    by = c(link_subdiv_org = "_IDRRef")) %>%
        # filter(subdiv_organiz_id == subdiv_org) %>%
        left_join(tbl_payroll_tbl_1, by = c("_IDRRef"="_Document312_IDRRef")) %>% 
        left_join(tbl_subdiv,        by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_employee_org,  by = c(link_employee_org = "_IDRRef")) %>%
        left_join(tbl_individuals,   by = c(link_individ = "_IDRRef")) %>%
        left_join(data_workers,      by = "individ_id") %>%
        select(individ_id, subdiv_name, position_id, salary) %>% 
        group_by(individ_id, subdiv_name, position_id) %>%
        summarise(salary_sum = sum(salary, na.rm = TRUE)) %>%
        ungroup() %>%
        collect() %>% 
        group_by(individ_id) %>% 
        mutate(subdiv_name = subdiv_name[!is.na(subdiv_name)][1L]) %>% 
        mutate(position_id = position_id[!is.na(position_id)][1L]) %>% 
        ungroup() %>% 
        filter(salary_sum != 0) %>% 
        group_by(individ_id, subdiv_name, position_id) %>% 
        summarise(salary_sum = sum(salary_sum)) %>% 
        ungroup()
    
    df_contribution <- tbl_payroll %>%
        # left_join(tbl_subdiv_org,    by = c(link_subdiv_org = "_IDRRef")) %>%
        # filter(subdiv_organiz_id == subdiv_org) %>%
        left_join(tbl_payroll_tbl_2, by = c("_IDRRef"="_Document312_IDRRef")) %>% 
        left_join(tbl_employee_org,  by = c(link_employee_org = "_IDRRef")) %>%
        left_join(tbl_individuals,   by = c(link_individ = "_IDRRef")) %>%
        select(individ_id, sum) %>% 
        group_by(individ_id) %>% 
        summarise(contr_sum = sum(sum, na.rm = TRUE)) %>% 
        ungroup() %>% 
        collect()
    
    df_taxes <- tbl_payroll %>%
        left_join(tbl_payroll_tbl_3, by = c("_IDRRef"="_Document312_IDRRef")) %>% 
        # left_join(tbl_subdiv_org,    by = c(link_subdiv_org = "_IDRRef")) %>%
        # filter(subdiv_organiz_id == subdiv_org) %>% 
        left_join(tbl_employee_org,  by = c(link_employee_org = "_IDRRef")) %>%
        left_join(tbl_individuals,   by = c(link_individ = "_IDRRef")) %>%
        select(individ_id, sum) %>% 
        group_by(individ_id) %>% 
        summarise(taxes_sum = sum(sum, na.rm = TRUE)) %>% 
        ungroup() %>% 
        collect()
    
    df_data <- df_salary %>% 
        left_join(df_contribution) %>% 
        left_join(df_taxes) %>% 
        mutate(across(where(is.numeric), ~ifelse(is.na(.), 0, .)),
               total_salary = salary_sum + contr_sum + taxes_sum) %>%
        select(individ_id, subdiv_name, position_id, total_salary)

    return(df_data)
}


## Прочие доходы (регистр накопления "Доходы")
other_income_data <- function(date_start, date_finish) {
   
    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_other_income <- tbl(con,"_AccumRg17913") %>% 
        select(date = "_Period", active = "_Active",
               link_debt_adjust = "_RecorderRRef",
               link_subdiv = "_Fld17914RRef", link_income_item = "_Fld17915RRef",
               income_item_sum = "_Fld17916", income_item_vat = "_Fld17917") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date >= date_start,
               date < date_finish,
               active == "TRUE")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")    
    tbl_income_item <- tbl(con, "_Reference168") %>% 
        select("_IDRRef", income_item_id = "_Code",
               income_item_name = "_Description")
    tbl_debt_agjust <- tbl(con, "_Document300") %>% 
        select("_IDRRef", doc_agjust_nmbr = "_Number",
               link_supplier = "_Fld5879RRef")
    tbl_supplier <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", supplier_id = "_Code")

    
    df_other_income <- tbl_other_income %>%
        left_join(tbl_debt_agjust, by = c(link_debt_adjust = "_IDRRef")) %>%
        left_join(tbl_supplier,    by = c(link_supplier = "_IDRRef")) %>%
        left_join(tbl_subdiv,      by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_income_item, by = c(link_income_item = "_IDRRef")) %>%
        select(date, subdiv_name, doc_agjust_nmbr, supplier_id,income_item_id,
               income_item_name, income_item_sum, income_item_vat) %>% 
        collect()
    
    return(df_other_income)
}


## Сотрудники на начало месяца
workers_data <- function(date_start) {

    date_start <- as_datetime(date_start)
    
    tbl_individuals <- tbl(con,"_Reference191") %>% 
        select("_IDRRef", individ_id ="_Code", individ_name = "_Description")
    tbl_workers <- tbl(con,"_InfoRg15330") %>%
        select(date_rec = "_Period", link_individ = "_Fld15331RRef",
               link_subdiv = "_Fld15332RRef", link_position = "_Fld15333RRef") %>% 
        filter(date_rec <= date_start)
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")
    tbl_position <- tbl(con, "_Reference81") %>% 
        select("_IDRRef", position_name = "_Description", position_id = "_Code")
    data_workers <- tbl_workers %>% 
        left_join(tbl_individuals, by = c(link_individ = "_IDRRef")) %>% 
        left_join(tbl_position, by = c(link_position = "_IDRRef")) %>% 
        left_join(tbl_subdiv, by = c(link_subdiv = "_IDRRef")) %>% 
        select(date_rec, individ_id, position_id, subdiv_name) %>% 
        filter(!is.na(position_id)) %>% 
        group_by(individ_id) %>%
        mutate(last_date = max(date_rec)) %>% 
        ungroup() %>% 
        filter(date_rec == last_date) %>% 
        select(individ_id, position_id_start = position_id, subdiv_name) %>% 
        collect()
    
    return(data_workers)
}


## Движение денежных средств за период
cash_flow_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_cash_flow <- tbl(con,"_AccumRg17010") %>% 
        select(date ="_Period", active ="_Active",link_registr ="_RecorderRRef",
               link_organiz = "_Fld17022RRef", link_cf_item = "_Fld17014RRef",
               link_type_money = "_Fld17012RRef",link_type_cf = "_Fld17013RRef",
               link_cash = "_Fld17011_RRRef", link_partner = "_Fld17016_RRRef",
               cf_sum = "_Fld17024") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date >= date_start,
               date < date_finish,
               active == "TRUE")
    tbl_cash_inner_move <- tbl(con, "_Document248") %>% 
        select("_IDRRef", doc_move_nmbr = "_Number")
    tbl_income_cash_order <- tbl(con, "_Document366") %>% 
        select("_IDRRef", doc_ico_nmbr = "_Number",
               link_ico_responsible = "_Fld10493RRef")
    tbl_outcome_cash_order <- tbl(con, "_Document370") %>% 
        select("_IDRRef", doc_oco_nmbr = "_Number",
               link_oco_responsible = "_Fld10707RRef")
    tbl_organization <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organiz_id = "_Code")    
    tbl_cf_item <- tbl(con, "_Reference167") %>% 
        select("_IDRRef", cf_item_id = "_Code",
               cf_item_name = "_Description")
    tbl_type_money <- tbl(con, "_Enum480") %>% 
        select("_IDRRef", type_money_order = "_EnumOrder")
    tbl_type_cf <- tbl(con, "_Enum474") %>% 
        select("_IDRRef", type_cf_order = "_EnumOrder")
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", partner_id = "_Code",
               partner_parent_link = "_ParentIDRRef")
    tbl_cash <- tbl(con, "_Reference88") %>% 
        select("_IDRRef", cash_id = "_Code")
    tbl_users <- tbl(con, "_Reference138") %>% 
        select("_IDRRef", "_Code")
    
    df_cash_flow <- tbl_cash_flow %>%
        left_join(tbl_cash_inner_move,    by = c(link_registr = "_IDRRef")) %>%
        left_join(tbl_income_cash_order,  by = c(link_registr = "_IDRRef")) %>%
        left_join(select(tbl_users, "_IDRRef", ico_responsible = "_Code"),       
                  by = c(link_ico_responsible = "_IDRRef")) %>%
        left_join(tbl_outcome_cash_order, by = c(link_registr = "_IDRRef")) %>%
        left_join(select(tbl_users, "_IDRRef", oco_responsible = "_Code"), 
                  by = c(link_oco_responsible = "_IDRRef")) %>%
        left_join(tbl_cash,         by = c(link_cash = "_IDRRef")) %>%
        left_join(tbl_organization, by = c(link_organiz = "_IDRRef")) %>%
        left_join(tbl_cf_item,      by = c(link_cf_item = "_IDRRef")) %>%
        left_join(tbl_partner,      by = c(link_partner = "_IDRRef")) %>%
        left_join(select(tbl_partner, "_IDRRef", partner_parent = partner_id),
                  by = c(partner_parent_link = "_IDRRef")) %>%
        left_join(tbl_type_money,   by = c(link_type_money = "_IDRRef")) %>%
        left_join(tbl_type_cf,      by = c(link_type_cf = "_IDRRef")) %>%
        select(date, doc_move_nmbr, doc_ico_nmbr, ico_responsible,
               doc_oco_nmbr, oco_responsible,
               organiz_id, cf_item_id, cf_item_name,
               cash_id, partner_id, partner_parent,
               type_money_order, type_cf_order, cf_sum) %>% 
        collect()
    
    return(df_cash_flow)
}


## Остатки на определенную дату  по складу (кол-во)
inventory_store_data <- function(store, date_balance) {
   
    date_balance <- as_datetime(date_balance)

    tbl_store <- tbl(con, "_Reference156") %>%
        select("_IDRRef", link_parent_store = "_ParentIDRRef",
               store_id = "_Code")
    tbl_items <- tbl(con, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code", "_ParentIDRRef")


    tbl_stock <- tbl(con,"_AccumRg17702") %>% 
        select(date_movement = "_Period", active = "_Active",
               link_store = "_Fld17703RRef", link_item = "_Fld17704RRef",
               type_motion = "_RecordKind", item_motion_qty = "_Fld17708") %>%
        mutate(active = as.logical(active)) %>%
        filter(date_movement < date_balance,
               active == "TRUE")
    
    result_df <- tbl_stock %>% 
        left_join(tbl_items, by = c(link_item = "_IDRRef")) %>%
        left_join(select(tbl_items, "_IDRRef", parent_1_id = item_id,
                         link_parent_2 = "_ParentIDRRef"),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>%
        left_join(select(tbl_items, "_IDRRef", parent_2_id = item_id,
                         link_parent_3 = "_ParentIDRRef"),
                  by = c(link_parent_2 = "_IDRRef")) %>%
        left_join(select(tbl_items, "_IDRRef", parent_3_id = item_id),
                  by = c(link_parent_3 = "_IDRRef")) %>%
        filter(!parent_1_id %in% c("000500558",           # Бухгалтерские
                                   "000300000"),       # Торгів.обладнання

               is.na(parent_2_id) | !parent_2_id %in% c("000500558",
                                                        "000300000"), 
               
               is.na(parent_3_id) | !parent_3_id %in% c("000500558",
                                                        "000300000")
        ) %>%
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>%
        filter(store_id == store) %>%
        mutate(item_motion_qty = ifelse(type_motion == 0,
                                        item_motion_qty,
                                        -item_motion_qty)) %>% 
        
        group_by(store_id, item_id) %>% 
        summarise(item_qty = sum(item_motion_qty, na.rm = TRUE)) %>% 
        ungroup() %>% 
        filter(item_qty != 0) %>% 
        collect()
    
    
    return(result_df)
}


## Остатки на определенную дату (сумма)
inventory_data <- function(date_balance) {
 
    date_balance <- as_datetime(date_balance)
    date_today <- as_datetime(today() + years(2000))

    tbl_store <- tbl(con, "_Reference156") %>%
        select("_IDRRef", link_parent_store = "_ParentIDRRef",
               store_id = "_Code")
    tbl_items <- tbl(con, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code", "_ParentIDRRef")
    tbl_receipt <- tbl(con, "_Document360") %>% 
        select("_IDRRef", date_receipt = "_Date_Time")
    
    tbl_stock_balance <- tbl(con,"_AccumRgT17710") %>% 
        select(date_balance = "_Period", link_store = "_Fld17703RRef",
               link_item = "_Fld17704RRef", item_store_qty ="_Fld17708") %>% 
        filter(date_balance > date_today) %>% 
        left_join(tbl_items, by = c(link_item = "_IDRRef")) %>% 
        left_join(select(tbl_items, "_IDRRef", parent_1_id = item_id,
                         link_parent_2 = "_ParentIDRRef"),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>%
        left_join(select(tbl_items, "_IDRRef", parent_2_id = item_id,
                         link_parent_3 = "_ParentIDRRef"),
                  by = c(link_parent_2 = "_IDRRef")) %>%
        left_join(select(tbl_items, "_IDRRef", parent_3_id = item_id),
                  by = c(link_parent_3 = "_IDRRef")) %>%
        filter(!parent_1_id %in% c("000500558",           # Бухгалтерские
                                   "000300000",           # Торгів.обладнання
                                   "000400010"),          # Рекламні материали

               is.na(parent_2_id) | !parent_2_id %in% c("000500558",
                                                        "000300000",
                                                        "000400010"),

               is.na(parent_3_id) | !parent_3_id %in% c("000500558",
                                                        "000300000",
                                                        "000400010")
               ) %>%
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", parent_store = store_id),
                  by = c(link_parent_store = "_IDRRef")) %>% 
        filter(is.na(parent_store) | !parent_store %in% "000001096") %>% 
        select(store_id, item_id, item_store_qty)

   
    tbl_stock_movement <- tbl(con,"_AccumRg17702") %>% 
        select(date_movement = "_Period", active = "_Active",
               link_store = "_Fld17703RRef", link_item = "_Fld17704RRef",
               type_motion = "_RecordKind", item_motion_qty = "_Fld17708") %>%
        mutate(active = as.logical(active)) %>%
        filter(date_movement >= date_balance,
               active == "TRUE") %>% 
        left_join(tbl_items, by = c(link_item = "_IDRRef")) %>%
        left_join(select(tbl_items, "_IDRRef", parent_1_id = item_id,
                         link_parent_2 = "_ParentIDRRef"),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>%
        left_join(select(tbl_items, "_IDRRef", parent_2_id = item_id,
                         link_parent_3 = "_ParentIDRRef"),
                  by = c(link_parent_2 = "_IDRRef")) %>%
        left_join(select(tbl_items, "_IDRRef", parent_3_id = item_id),
                  by = c(link_parent_3 = "_IDRRef")) %>%
        filter(!parent_1_id %in% c("000500558",           # Бухгалтерские
                                   "000300000",           # Торгів.обладнання
                                   "000400010"),          # Рекламні материали

                is.na(parent_2_id) | !parent_2_id %in% c("000500558",
                                                         "000300000",
                                                         "000400010"), 

                is.na(parent_3_id) | !parent_3_id %in% c("000500558",
                                                         "000300000",
                                                         "000400010")
        ) %>%
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", parent_store = store_id),
                  by = c(link_parent_store = "_IDRRef")) %>% 
        filter(is.na(parent_store) | !parent_store %in% "000001096") %>%
        mutate(item_motion_qty = ifelse(type_motion == 0,
                                        item_motion_qty,
                                        -item_motion_qty)) %>% 

        group_by(store_id, item_id) %>% 
        summarise(store_item_moving = sum(item_motion_qty, na.rm = TRUE)) %>% 
        ungroup() 

    
    tbl_inventory <- tbl_stock_balance %>% 
        full_join(tbl_stock_movement) %>% 
        # collect() %>% 
        replace_na(list(item_store_qty = 0, store_item_moving = 0)) %>% 
        mutate(balance = item_store_qty - store_item_moving) %>% 
        filter(balance != 0) %>% 
        select(store_id, item_id, balance) %>% 
        collect()
    
    tbl_goods_batches_remainder <- tbl(con,"_AccumRgT17307") %>% 
        select(date_remainder = "_Period",
               link_items = "_Fld17287RRef", link_store = "_Fld17288RRef",
               link_receipt = "_Fld17291_RRRef",
               remainder_qty = "_Fld17295", remainder_value = "_Fld17296",
               remainder_vat = "_Fld17297") %>% 
        filter(date_remainder > date_today,
               remainder_qty != 0)
    
    tbl_goods_batches <- tbl(con,"_AccumRg17286") %>% 
        select(date_moving = "_Period", active = "_Active",
               type_moving = "_RecordKind", 
               link_items = "_Fld17287RRef", link_store = "_Fld17288RRef",
               link_receipt = "_Fld17291_RRRef",
               moving_qty = "_Fld17295", moving_value = "_Fld17296",
               moving_vat = "_Fld17297") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date_moving >= date_balance,
               active == "TRUE"
        ) %>% 
        left_join(tbl_items, by = c(link_items = "_IDRRef")) %>% 
        left_join(select(tbl_items, "_IDRRef", parent_1_id = item_id,
                         link_parent_2 = "_ParentIDRRef"),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>%
        left_join(select(tbl_items, "_IDRRef", parent_2_id = item_id,
                         link_parent_3 = "_ParentIDRRef"),
                  by = c(link_parent_2 = "_IDRRef")) %>%
        left_join(select(tbl_items, "_IDRRef", parent_3_id = item_id),
                  by = c(link_parent_3 = "_IDRRef")) %>%
        filter(!parent_1_id %in% c("000500558",           # Бухгалтерские
                                   "000300000",           # Торгів.обладнання
                                   "000400010"),          # Рекламні материали
               
               is.na(parent_2_id) | !parent_2_id %in% c("000500558",
                                                        "000300000",
                                                        "000400010"), 
               
               is.na(parent_3_id) | !parent_3_id %in% c("000500558",
                                                        "000300000",
                                                        "000400010")
        ) %>%
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", parent_store = store_id),
                  by = c(link_parent_store = "_IDRRef")) %>% 
        filter(is.na(parent_store) | !parent_store %in% "000001096") %>%
        left_join(tbl_receipt, by = c(link_receipt = "_IDRRef")) %>% 
        # select(date_moving, type_moving, store_id, item_id, date_receipt, moving_qty,
        #        moving_value, moving_vat) %>% 
        filter(moving_qty != 0,
               is.na(date_receipt) | date_receipt < date_balance,
               type_moving == 1) %>% 
        # mutate(moving_qty = ifelse(type_moving == 0,
        #                            moving_qty,
        #                            -moving_qty),
        #        moving_sum = ifelse(type_moving == 0,
        #                            moving_value + moving_vat,
        #                            -(moving_value + moving_vat))) %>% 
        group_by(store_id, item_id, date_receipt) %>%
        summarise(total_item_qty = sum(moving_qty),
                  total_item_sum = sum(moving_value) + sum(moving_vat)) %>%
        filter(total_item_qty != 0) %>% 
        collect()

    df_inventory <- tbl_goods_batches_remainder  %>%
        left_join(tbl_items, by = c(link_items = "_IDRRef")) %>% 
        left_join(select(tbl_items, "_IDRRef", parent_1_id = item_id,
                         link_parent_2 = "_ParentIDRRef"),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>%
        left_join(select(tbl_items, "_IDRRef", parent_2_id = item_id,
                         link_parent_3 = "_ParentIDRRef"),
                  by = c(link_parent_2 = "_IDRRef")) %>%
        left_join(select(tbl_items, "_IDRRef", parent_3_id = item_id),
                  by = c(link_parent_3 = "_IDRRef")) %>%
        filter(!parent_1_id %in% c("000500558",           # Бухгалтерские
                                   "000300000",           # Торгів.обладнання
                                   "000400010"),          # Рекламні материали
               
               is.na(parent_2_id) | !parent_2_id %in% c("000500558",
                                                        "000300000",
                                                        "000400010"), 
               
               is.na(parent_3_id) | !parent_3_id %in% c("000500558",
                                                        "000300000",
                                                        "000400010")
        ) %>%
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", parent_store = store_id),
                  by = c(link_parent_store = "_IDRRef")) %>% 
        filter(is.na(parent_store) | !parent_store %in% "000001096") %>%
        left_join(tbl_receipt, by = c(link_receipt = "_IDRRef")) %>% 
        # group_by(store_id, item_id, date_receipt) %>%
        # summarise(total_item_qty = sum(remainder_qty),
        #           total_item_sum = sum(remainder_value) + sum(remainder_vat)) %>%
        # ungroup() %>%
        filter(remainder_qty > 0,
               is.na(date_receipt) | date_receipt < date_balance) %>%
        mutate(total_item_sum = remainder_value + remainder_vat) %>% 
        select(store_id, item_id, date_receipt, total_item_qty = remainder_qty,
               total_item_sum) %>% 
        collect() %>% 
        bind_rows(tbl_goods_batches) %>% 
        group_by(store_id, item_id) %>% 
        mutate(total_receipt = sum(total_item_qty)) %>% 
        full_join(tbl_inventory) %>% 
        filter(!is.na(balance))
        
    correct_amount <- df_inventory %>%
        filter(total_receipt == balance)
    
    wrong_amount_more <- df_inventory %>% filter(total_receipt > balance)%>% 
        arrange(store_id, item_id, desc(date_receipt)) %>% 
        group_by(store_id, item_id) %>% 
        mutate(party_inc_qty = cumsum(total_item_qty),
               diff = balance - party_inc_qty,
               first_negative = party_inc_qty[which(diff <= 0)][1]) %>%
        ungroup() %>% 
        filter(party_inc_qty <= first_negative) %>% 
        mutate(new_sum = ifelse(diff >= 0,
                                total_item_sum,
                                total_item_sum + diff * (total_item_sum / total_item_qty)),
               new_qty = ifelse(diff >= 0,
                                total_item_qty,
                                total_item_qty + diff))
    
    wrong_amount_less <- df_inventory %>% filter(total_receipt < balance) %>% 
        arrange(store_id, item_id, date_receipt) %>% 
        group_by(store_id, item_id) %>% 
        group_modify(~ add_row(.x)) %>% 
        mutate(total_item_qty = ifelse(is.na(balance),
                                       first(balance) - first(total_receipt),
                                       total_item_qty),
               total_item_sum = ifelse(is.na(balance),
                                       sum(total_item_sum, na.rm = TRUE) / 
                                           sum(total_item_qty[!is.na(balance)], na.rm = TRUE) *
                                           total_item_qty,
                                       total_item_sum))
    
    prices_df <- price_items_data(date_start + years(2000),
                                  "000000005")

    without_amount <- df_inventory %>% 
        filter(is.na(total_receipt)) %>% 
        left_join(distinct(select(prices_df, item_id, item_price))) %>% 
        filter(!is.na(item_price)) %>% 
        mutate(total_item_sum = balance * item_price,
               total_item_qty = balance)    

    result_df <- bind_rows(select(correct_amount, store_id, item_id, total_item_qty,
                                  total_item_sum),
                           select(wrong_amount_more, store_id, item_id,
                                  total_item_qty = new_qty,
                                  total_item_sum = new_sum),
                           select(wrong_amount_less, store_id, item_id,
                                  total_item_qty, total_item_sum),
                           select(without_amount, store_id, item_id,
                                  total_item_qty, total_item_sum))
    
    return(result_df)
}


main_parent <- function(id) {
    current_id <- id
    current_rec <- left_join(enframe(current_id, name = "row", value = "item_id"),
                             select(ref_items, item_id, parent_id = parent_itemId)) 
    while(!is.na(current_rec$parent_id[nrow(current_rec)])) {
        prev_parent <- current_rec$parent_id[nrow(current_rec)]
        next_parent <- left_join(current_rec,  select(ref_items, item_id, next_parent_id = parent_itemId),
                                 by = c("parent_id" = "item_id")) %>% 
            pull(next_parent_id)
        current_rec <- bind_rows(current_rec, tibble(row = nrow(current_rec) + 1,
                                                     item_id = prev_parent,
                                                     parent_id = next_parent))
    }
    
    return(current_rec$parent_id[nrow(current_rec) - 1])
}


## Закупки за период
purchases_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_purchases <- tbl(con,"_AccumRg17111") %>% 
        select(date ="_Period", active ="_Active",
               link_order = "_Fld17115_RRRef", link_purch = "_Fld17117_RRRef",
               link_organiz = "_Fld17120RRef", link_item = "_Fld17112RRef",
               link_subdiv = "_Fld17119RRef",
               link_contr = "_Fld17116RRef", link_supplier = "_Fld17121RRef",
               item_qty = "_Fld17122", item_sum = "_Fld17123",
               item_vat = "_Fld17124") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date >= date_start,
               date < date_finish,
               active == "TRUE")

    tbl_doc_purch <- tbl(con, "_Document360") %>%
        select("_IDRRef", doc_purch_nmbr = "_Number")
    tbl_suppl_order <- tbl(con, "_Document263") %>%
        select("_IDRRef", order_nmbr = "_Number", order_date = "_Date_Time")
    tbl_organization <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organiz_id = "_Code")    
    tbl_item <- tbl(con, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", supplier_id = "_Code")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_id = "_Code")
    tbl_contract <- tbl(con, "_Reference78") %>% 
        select("_IDRRef", link_type_contr = "_Fld1600RRef",
               contract_name = "_Description")
    tbl_type_contract <- tbl(con, "_Enum484") %>% 
        select("_IDRRef", type_contr = "_EnumOrder")
    
    df_purchases <- tbl_purchases %>%
        left_join(tbl_organization,  by = c(link_organiz = "_IDRRef")) %>%
        left_join(tbl_partner,       by = c(link_supplier = "_IDRRef")) %>%
        left_join(tbl_subdiv,        by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_contract,      by = c(link_contr = "_IDRRef")) %>%
        #left_join(tbl_type_contract, by = c(link_type_contr = "_IDRRef")) %>%
        # filter(type_contr == 0) %>% 
        left_join(tbl_item,          by = c(link_item = "_IDRRef")) %>%
        left_join(tbl_suppl_order,   by = c(link_order = "_IDRRef")) %>%
        left_join(tbl_doc_purch,     by = c(link_purch = "_IDRRef")) %>%
        select(date, doc_purch_nmbr,
               organiz_id, subdiv_id, supplier_id, contract_name,
               order_nmbr, order_date,
               item_id, item_qty, item_sum, item_vat) %>% 
        collect()
    
    return(df_purchases)
}


## Новые SKU за период
purchases_new_sku_fn <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_purchases <- tbl(con,"_AccumRg17111") %>% 
        select(date ="_Period", active ="_Active",
               link_item = "_Fld17112RRef", link_contr = "_Fld17116RRef",
               item_qty = "_Fld17122") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date < date_finish,
               active == "TRUE")
    
  
    tbl_item <- tbl(con, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_contract <- tbl(con, "_Reference78") %>% 
        select("_IDRRef", link_type_contr = "_Fld1600RRef",
               contract_name = "_Description")
    tbl_type_contract <- tbl(con, "_Enum484") %>% 
        select("_IDRRef", type_contr = "_EnumOrder")
    
    df_purchases <- tbl_purchases %>%
        left_join(tbl_contract,      by = c(link_contr = "_IDRRef")) %>%
        left_join(tbl_type_contract, by = c(link_type_contr = "_IDRRef")) %>%
        filter(type_contr == 0) %>% 
        left_join(tbl_item,          by = c(link_item = "_IDRRef")) %>%
        group_by(item_id) %>% 
        mutate(first_purch = min(date)) %>% 
        ungroup() %>% 
        filter(date == first_purch,
               date >= date_start) %>% 
        select(date, contract_name, item_id, item_qty) %>% 
        collect() %>% 
        filter(str_detect(contract_name, "^685", negate = TRUE))
    
    return(df_purchases)
}


## Данные по задолженности контрагентов на дату
partner_debt_data <- function(date_balance) {
 
    date_balance <- as_datetime(date_balance)
    
    tbl_partner_debt_movement <- tbl(con,"_AccumRg16896") %>% 
        select(date_movement = "_Period", active = "_Active",
               link_organization = "_Fld16899RRef",
               link_partner = "_Fld16900RRef", link_contract = "_Fld16897RRef",
               type_moving = "_RecordKind", moving_sum = "_Fld16901") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date_movement < date_balance,
               active == "TRUE")
    
    tbl_partner <- tbl(con, "_Reference102") %>% 
        select("_IDRRef", partner_id = "_Code")
    tbl_contract <- tbl(con, "_Reference78") %>% 
        select("_IDRRef", contract_id = "_Code",
               link_contract_type = "_Fld1600RRef",
               link_currency = "_Fld1583RRef")
    tbl_type_contract <- tbl(con, "_Enum484") %>% 
        select("_IDRRef", type_contract = "_EnumOrder")
    tbl_organization <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organization_name = "_Description")
    tbl_currency <- tbl(con, "_Reference47") %>% 
        select("_IDRRef", currency_id = "_Code")
    tbl_currency_rate <- tbl(con, "_InfoRg14698") %>% 
        select(date = "_Period", link_currency = "_Fld14699RRef",
               currency_rate = "_Fld14700")
    
    currency_rate_data <- tbl_currency_rate %>% 
        filter(date == max(date[date < date_balance])) %>% 
        left_join(tbl_currency, by = c(link_currency = "_IDRRef")) %>%
        select(currency_id, currency_rate)
    
    df_partner_motion <- tbl_partner_debt_movement %>%
        left_join(tbl_partner,       by = c(link_partner = "_IDRRef")) %>% 
        left_join(tbl_contract,      by = c(link_contract = "_IDRRef")) %>% 
        left_join(tbl_type_contract, by = c(link_contract_type = "_IDRRef")) %>%
        left_join(tbl_organization,  by = c(link_organization = "_IDRRef")) %>%
        left_join(tbl_currency, by = c(link_currency = "_IDRRef")) %>%
        mutate(moving_sum = ifelse(type_moving == 0,
                                   moving_sum,
                                   -moving_sum)) %>% 
        group_by(organization_name, partner_id, contract_id, type_contract,
                 currency_id) %>% 
        summarise(balance = sum(moving_sum)) %>% 
        ungroup() %>% 
        filter(balance != 0,
               type_contract %in% c(0,1)) %>% 
        left_join(currency_rate_data, by = "currency_id") %>% 
        replace_na(list(currency_rate = 1)) %>% 
        mutate(balance = balance * currency_rate) %>% 
        collect() %>% 
        mutate(type_contract = ifelse(type_contract == 0,
                                      "supplier",
                                      "customer"))
    
    return(df_partner_motion)
}


## Денежные средства на дату
cash_balance_data <- function(date_finish) {
  
    date_finish <- as_datetime(date_finish)

    tbl_cash <- tbl(con,"_AccumRg17031") %>% 
        select(date ="_Period", active ="_Active", type_moving ="_RecordKind",
               link_organiz = "_Fld17034RRef", link_account = "_Fld17033_RRRef",
               link_type_money = "_Fld17032RRef", moving_sum = "_Fld17035") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date < date_finish,
               active == "TRUE")

    tbl_organization <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organiz_id = "_Code")    
    tbl_type_money <- tbl(con, "_Enum480") %>% 
        select("_IDRRef", type_money_order = "_EnumOrder")
    tbl_bank_account <- tbl(con, "_Reference45") %>% 
        select("_IDRRef", account_id = "_Code", link_bank = "_Fld1385RRef",
               link_currency = "_Fld1388RRef")
    tbl_bank <- tbl(con, "_Reference44") %>% 
        select("_IDRRef", bank_name = "_Description")
    tbl_cashbox <- tbl(con, "_Reference88") %>% 
        select("_IDRRef", account_id = "_Code", link_currency = "_Fld1667RRef")
    tbl_currency <- tbl(con, "_Reference47") %>% 
        select("_IDRRef", currency_id = "_Code")
    tbl_exchange <- tbl(con, "_InfoRg14698") %>% 
        select(date_exchange ="_Period", link_currency = "_Fld14699RRef",
               currency_rate = "_Fld14700") %>% 
        filter(date_exchange == max(date_exchange[date_exchange < date_finish])) %>%
        left_join(tbl_currency,     by = c(link_currency = "_IDRRef")) %>%
        select(currency_id, currency_rate) %>% 
        collect()
 
    
    df_bank_account <- tbl_cash %>%
        left_join(tbl_organization, by = c(link_organiz = "_IDRRef")) %>%
        left_join(tbl_bank_account, by = c(link_account = "_IDRRef")) %>%
        filter(!is.na(account_id)) %>% 
        left_join(tbl_currency,     by = c(link_currency = "_IDRRef")) %>%
        mutate(moving_sum = ifelse(type_moving == 0,
                                   moving_sum,
                                   -moving_sum)) %>%
        group_by(organiz_id, account_id, currency_id) %>%
        summarise(acc_balance = sum(moving_sum)) %>%
        ungroup() %>% 
        select(organiz_id, account_id, acc_balance, currency_id) %>% 
        collect() %>% 
        filter(!near(acc_balance, 0)) %>% 
        mutate(type_acc = "безготівковий")
    
    df_cash_account <- tbl_cash %>%
        left_join(tbl_organization, by = c(link_organiz = "_IDRRef")) %>%
        left_join(tbl_cashbox,      by = c(link_account = "_IDRRef")) %>%
        filter(!is.na(account_id)) %>% 
        left_join(tbl_currency,     by = c(link_currency = "_IDRRef")) %>%
        mutate(moving_sum = ifelse(type_moving == 0,
                                   moving_sum,
                                   -moving_sum)) %>%
        group_by(account_id, currency_id) %>%
        summarise(acc_balance = sum(moving_sum)) %>%
        ungroup() %>% 
        select(#date,
               account_id, acc_balance, currency_id) %>% 
        collect() %>% 
        filter(!near(acc_balance, 0)) %>% 
        mutate(type_acc = "готівковий",
               organiz_id = "невідомий") %>% 
        relocate(organiz_id, .before = account_id)
    
    
    df_total <- df_bank_account %>% 
        bind_rows(df_cash_account) %>% 
        left_join(tbl_exchange) %>% 
        replace_na(list(currency_rate = 1)) %>% 
        mutate(acc_bal_grn = ifelse(currency_id == "980",
                                    acc_balance,
                                    acc_balance * currency_rate))
        
    return(df_total)
}


# Гроші в дорозі
get_money_on_way <- function(date_balance) {
    
    account = "333"
    
    date_balance <- as_datetime(date_balance + years(2000))
    
    tbl_acc_credit <- tbl(con, "_AccRg855") %>% 
        select("_AccountCtRRef", date = "_Period", active = "_Active",
               link_organization = "_Fld856RRef", moving_sum = "_Fld859") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(active == "TRUE",
               date < date_balance
        )
    
    tbl_acc_debet <- tbl(con, "_AccRg855") %>% 
        select("_AccountDtRRef", date = "_Period", active = "_Active",
               link_organization = "_Fld856RRef", moving_sum = "_Fld859",
               link_doc = "_RecorderRRef") %>%
        mutate(active = as.logical(active)) %>%
        filter(date < date_balance,
               active == "TRUE")
    
    tbl_account <- tbl(con, "_Acc18") %>% 
        select("_IDRRef", acc_id = "_Code")
    tbl_organization <- tbl(con, "_Reference128") %>% 
        select("_IDRRef", organization_id = "_Code")
    # tbl_operation_doc <- tbl(con, "_Document319") %>%
    #     select("_IDRRef", date_operation_doc = "_Date_Time")
    
    acc_ct <- tbl_acc_credit %>% 
        left_join(tbl_account,      by = c("_AccountCtRRef" = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        filter(acc_id == account) %>%
        group_by(organization_id) %>% 
        summarise(ct_sum = sum(moving_sum)) %>% 
        collect()
    
    acc_dt <- tbl_acc_debet %>% 
        left_join(tbl_account,       by = c("_AccountDtRRef" = "_IDRRef")) %>% 
        left_join(tbl_organization,  by = c(link_organization = "_IDRRef")) %>%
        # left_join(tbl_operation_doc, by = c(link_doc = "_IDRRef")) %>%
        filter(acc_id == account) %>% 
        group_by(organization_id) %>%
        summarise(dt_sum = sum(moving_sum)) %>% 
        collect()
    
    balance <- inner_join(acc_dt, acc_ct, by = "organization_id") %>% 
        mutate(balance_sum = dt_sum - ct_sum) %>% 
        select(organization_id, balance_sum)
    
    return(balance)
    
}


## Реализация товаров и услуг со складом
sale_data_total_store <- function(date_start, date_finish) {
  
    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_sale_doc <- tbl(con,"_AccumRg17435") %>% 
        select(date = "_Period", active = "_Active",
               link_subdiv = "_Fld17441RRef",
               link_document = "_RecorderRRef",
               link_item = "_Fld17436RRef", item_qty = "_Fld17445",
               item_sum = "_Fld17446", item_vat = "_Fld17448") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date >= date_start,
               date < date_finish,
               active == "TRUE")
    tbl_subdiv <- tbl(con, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description")    
    tbl_item <- tbl(con, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_retail_sales <- tbl(con, "_Document329") %>% 
        select("_IDRRef", retail_doc_nmbr = "_Number", link_store_retail = "_Fld7972RRef")
    tbl_goods_sale <- tbl(con, "_Document375") %>% 
        select("_IDRRef", sale_doc_nmbr = "_Number", link_store_sale = "_Fld10907RRef")
    tbl_returns <- tbl(con, "_Document251") %>% 
        select("_IDRRef", returns_doc_nmbr = "_Number", link_store_return = "_Fld3907_RRRef")
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_id = "_Code")
    
    df_sale <- tbl_sale_doc %>%
        left_join(tbl_item,         by = c(link_item = "_IDRRef")) %>% 
        left_join(tbl_subdiv,       by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_retail_sales, by = c(link_document = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", retail_store = store_id),
                  by = c(link_store_retail = "_IDRRef")) %>%
        left_join(tbl_goods_sale,   by = c(link_document = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", sale_store = store_id),
                                    by = c(link_store_sale = "_IDRRef")) %>%
        left_join(tbl_returns,      by = c(link_document = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", return_store = store_id),
                  by = c(link_store_return = "_IDRRef")) %>%
        select(date, subdiv_name, retail_doc_nmbr, retail_store,
               sale_doc_nmbr, sale_store, returns_doc_nmbr, return_store,
               item_id, item_qty, item_sum, item_vat) %>% 
        collect()
    
    return(df_sale)
}


## Фейс и норма по планограммам
planogram_data <- function(date_finish) {

    date_finish <- as_datetime(date_finish)
    
    tbl_planogram <- tbl(con,"_Document21048") %>% 
        select("_IDRRef", date = "_Date_Time", posted = "_Posted",
               link_store = "_Fld21132RRef", link_shelving = "_Fld21133RRef") %>% 
        mutate(posted = as.logical(posted)) %>%
        filter(date   <= date_finish,
               posted == "TRUE")
    tbl_planogram_tbl <- tbl(con,"_Document21048_VT21138") %>% 
        select("_Document21048_IDRRef", link_shelf = "_Fld21145RRef",
               link_item = "_Fld21140RRef", facing = "_Fld21172",
               norm = "_Fld21173")
    tbl_store <- tbl(con, "_Reference156") %>% 
        select("_IDRRef", store_id = "_Code")
    tbl_shelving <- tbl(con, "_Reference21028") %>% 
        select("_IDRRef", shelving_id = "_Code")
    tbl_shelf <- tbl(con, "_Reference21029") %>% 
        select("_IDRRef", shelf_id = "_Code")
    tbl_item <- tbl(con, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    
    planogram_df <- tbl_planogram %>% 
        left_join(tbl_planogram_tbl,
                                by = c("_IDRRef" = "_Document21048_IDRRef")) %>%
        left_join(tbl_store,    by = c(link_store = "_IDRRef")) %>% 
        left_join(tbl_shelving, by = c(link_shelving = "_IDRRef")) %>%
        left_join(tbl_shelf,    by = c(link_shelf = "_IDRRef")) %>%
        left_join(tbl_item,     by = c(link_item = "_IDRRef")) %>% 
        group_by(store_id, item_id) %>% 
        mutate(date_max = max(date, na.rm = TRUE)) %>% 
        ungroup %>% 
        filter(date == date_max) %>% 
        select(store_id, shelving_id, shelf_id, item_id, facing, norm) %>% 
        collect() %>% 
        filter(!is.na(shelf_id))
    
    
    return(planogram_df)
}


## ТВА на дату
tva_data <- function(date_finish) {
 
    date_finish <- as_datetime(date_finish)
    
    tbl_tva <- tbl(con,"_InfoRg21832") %>% 
        select(date = "_Period", active = "_Active",
               link_item = "_Fld21833RRef", is_tva = "_Fld21834") %>% 
        mutate(active = as.logical(active),
               is_tva = as.logical(is_tva)) %>%
        filter(date    < date_finish,
               active == "TRUE")
    
    tbl_item <- tbl(con, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    
    tva_vec <- tbl_tva %>% 
        left_join(tbl_item,  by = c(link_item = "_IDRRef")) %>% 
        select(date, item_id, is_tva) %>% 
        group_by(item_id) %>%
        mutate(max_data = max(date)) %>%
        ungroup() %>% 
        filter(date == max_data,
               is_tva == "TRUE") %>%
        select(item_id) %>% 
        collect() %>% 
        pull()
    
    
    return(tva_vec)
}


## Закрытие заказов поставщикам
closing_supplier_order <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_closing_supplier_order_tbl <- tbl(con,"_Document266_VT4772") %>% 
        select("_Document266_IDRRef", link_order = "_Fld4774RRef",
               link_reason = "_Fld4775RRef") 
    tbl_closing_supplier_order <- tbl(con,"_Document266") %>% 
        select("_IDRRef", posted ="_Posted", date_closing_doc ="_Date_Time") %>%  
        mutate(posted = as.logical(posted)
        ) %>% 
        filter(date_closing_doc  >= date_start,
               date_closing_doc  < date_finish,
               posted == "TRUE"
        )
    
    tbl_supplier_order<- tbl(con,"_Document263") %>% 
        select("_IDRRef", nmbr_supplier_order = "_Number",
               date_supplier_order = "_Date_Time")
    tbl_closing_reason <- tbl(con,"_Reference140") %>% 
        select("_IDRRef", reason_id = "_Code", reason_name = "_Description")

    df_closing_supplier_order <- tbl_closing_supplier_order %>%
        left_join(tbl_closing_supplier_order_tbl,
                  by = c("_IDRRef" = "_Document266_IDRRef")) %>%
        left_join(tbl_closing_reason, by = c(link_reason = "_IDRRef")) %>%
        left_join(tbl_supplier_order, by = c(link_order = "_IDRRef")) %>%
        select(date_closing_doc, nmbr_supplier_order, date_supplier_order,
               reason_id, reason_name
        ) %>% 
        collect()
    
    return(df_closing_supplier_order)
}


## Закрытие заказов покупателей
closing_customer_order <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_closing_customer_order_tbl <- tbl(con,"_Document265_VT4762") %>% 
        select("_Document265_IDRRef", link_order = "_Fld4764_RRRef",
               link_reason = "_Fld4765RRef") 
    tbl_closing_customer_order <- tbl(con,"_Document265") %>% 
        select("_IDRRef", posted ="_Posted", date_closing_doc ="_Date_Time") %>%  
        mutate(posted = as.logical(posted)
        ) %>% 
        filter(date_closing_doc  >= date_start,
               date_closing_doc  < date_finish,
               posted == "TRUE"
        )
    
    tbl_customer_order<- tbl(con,"_Document262") %>% 
        select("_IDRRef", nmbr_customer_order = "_Number",
               date_customer_order = "_Date_Time")
    tbl_closing_reason <- tbl(con,"_Reference140") %>% 
        select("_IDRRef", reason_id = "_Code", reason_name = "_Description")

    df_closing_customer_order <- tbl_closing_customer_order %>%
        left_join(tbl_closing_customer_order_tbl,
                  by = c("_IDRRef" = "_Document265_IDRRef")) %>%
        left_join(tbl_closing_reason, by = c(link_reason = "_IDRRef")) %>%
        left_join(tbl_customer_order, by = c(link_order = "_IDRRef")) %>%
        select(date_closing_doc, nmbr_customer_order, date_customer_order,
               reason_id, reason_name
        ) %>% 
        collect()
    
    return(df_closing_customer_order)
}


## Остатки на определенную дату (кол-во) по выбранным складу и товарам
inventory_items_store_data <- function(date_start, store, items_vec) {
  
    date_start <- as_datetime(date_start)
    date_finish <- date_start + months(1)

    tbl_store <- tbl(con, "_Reference156") %>%
        select("_IDRRef", link_parent_store = "_ParentIDRRef",
               store_id = "_Code")
    tbl_items <- tbl(con, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code", "_ParentIDRRef")
    
    tbl_stock_movement <- tbl(con,"_AccumRg17702") %>% 
        select(date_movement = "_Period", active = "_Active",
               link_store = "_Fld17703RRef", link_item = "_Fld17704RRef",
               type_motion = "_RecordKind", item_motion_qty = "_Fld17708") %>%
        mutate(active = as.logical(active)) %>%
        filter(date_movement <  date_finish,
               active == "TRUE") %>% 
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>%
        left_join(tbl_items, by = c(link_item = "_IDRRef")) %>%
        filter(store_id == store,
               item_id %in% items_vec) %>% 
        mutate(item_motion_qty = ifelse(type_motion == 1,
                                        -item_motion_qty,
                                        item_motion_qty))

    df_stock_start <- tbl_stock_movement %>% 
        filter(date_movement < date_start) %>% 
        group_by(item_id) %>%
        summarise(item_balance_start = sum(item_motion_qty)) %>%
        select(item_id, item_balance_start) %>% 
        collect() %>% 
        arrange(item_id)
    
    df_movement_data <- tbl_stock_movement %>% 
        filter(date_movement >= date_start,
               date_movement <  date_finish) %>%
        mutate(date_movement = as.Date(date_movement)) %>% 
        group_by(item_id, date_movement) %>% 
        summarise(day_moving_qty = sum(item_motion_qty)) %>% 
        select(item_id, date_movement, day_moving_qty) %>% 
        ungroup() %>% 
        collect() %>% 
        mutate(date_movement = as.Date(date_movement))
    
    date_vec <- seq(from = date_start, to = date_finish - days(1), by = "days")
    
    df_items_days_balance <- tibble(item_id = rep(items_vec, each =
                                                      length(date_vec)),
                          date = as.Date(rep(date_vec, length(items_vec)))) %>%
        left_join(df_movement_data, by = c("item_id", 
                                           "date" = "date_movement")) %>% 
        replace_na(list(day_moving_qty = 0)) %>%
        rowwise() %>% 
        mutate(start_balance = ifelse(
            is.null(df_stock_start$item_balance_start[df_stock_start$item_id == item_id]),
            0,
            df_stock_start$item_balance_start[df_stock_start$item_id == item_id]),
            day_moving_qty = ifelse(date == as.Date(date_start),
                                    day_moving_qty + start_balance,
                                    day_moving_qty)) %>% 
        ungroup() %>% 
        arrange(item_id, date) %>% 
        group_by(item_id) %>% 
        mutate(item_day_balance = cumsum(day_moving_qty)) %>% 
        ungroup() %>% 
        select(-day_moving_qty, start_balance)
    
  
    return(df_items_days_balance)
}
        