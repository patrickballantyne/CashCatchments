library(parallel)
library(hereR)
library(sf)
library(tidyverse)
library(vroom)
library(tmap)

## HERE API params
set_key("5zgYrNtYojJ0DPRRAnufXo_dLijIAav_a6-3j-bg768")


# 1. Data Cleaning --------------------------------------------------------

## Data - ATMs (LINK)
atm <- vroom("CashCatchments/ATM_Jun22.csv")
atm <- atm %>%
  select(Street.Address, Postcode, long, lat) %>%
  distinct() %>%
  setNames(c("Street", "Postcode", "LON", "LAT")) %>%
  rownames_to_column(var = "rowID") %>%
  mutate(atmID = paste0("atm", rowID)) %>%
  select(atmID, Street, Postcode, LON, LAT)
atm_sf <- atm %>%
  st_as_sf(coords = c("LON", "LAT"), crs = 4326) %>%
  st_transform(27700)

## Create Lookup Table for Jennie
write.csv(atm, "CashCatchments/atm-lookup.csv")

## Data - Post Offices
po <- vroom("CashCatchments/PO_2022_05_27.csv")
po <- po  %>%
  select(LOCATION_OU_CODE, LOCATION_POSTCODE,
         BRANCH_LONGITUDE, BRANCH_LATITUDE) %>%
  distinct() %>%
  setNames(c("LocationCode", "Postcode",
           "LON", "LAT")) %>%
  rownames_to_column(var = "rowID") %>%
  mutate(poID = paste0("po", rowID)) %>%
  select(poID, LocationCode, Postcode, LON, LAT)
po_sf <- po %>%
  st_as_sf(coords = c("LON", "LAT"), crs = 4326) 

## Create Lookup Table for Jennie
write.csv(po, "CashCatchments/po-lookup.csv")


# 2. Cash Catchments ------------------------------------------------------

## Function for building catchments for Post Offices - 10 minutes walking distance for each pedestrian
buildPOCatchment <- function(x) {
  iso <- hereR::isoline(x, range = (10 * 60), 
                        transport_mode = "pedestrian", range_type = "time" )
  iso <- iso %>%
    select(-c(id, rank, departure, arrival, range))
  iso$poID <- x$poID
  iso$duration = "10 mins"
  return(iso)
}

## Loop through and parallelise catchment extraction for post offices
po_ls <- split(po_sf, seq(nrow(po_sf)))
out <- do.call(rbind, lapply(po_ls, buildPOCatchment))
st_write(out, "CashCatchments/po-catchments.gpkg", append = FALSE)
st_write(po_sf, "CashCatchments/po.gpkg")

## Function for building catchments for ATMs - 10 minutes walking distance for each pedestrian
buildATMCatchment <- function(x) {
  iso <- hereR::isoline(x, range = (10 * 60), 
                        transport_mode = "pedestrian", range_type = "time" )
  iso <- iso %>%
    select(-c(id, rank, departure, arrival, range))
  iso$atmID <- x$atmID
  iso$duration = "10 mins"
  return(iso)
}

## Loop through and parallelise catchment extraction for atms
atm_ls <- split(atm_sf, seq(nrow(atm_sf)))
out2 <- do.call(rbind, lapply(atm_ls, buildATMCatchment))
st_write(out2, "CashCatchments/atm-catchments.gpkg", append = FALSE)



