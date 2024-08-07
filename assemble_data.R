library(dplyr, warn.conflicts = FALSE)
library(fr)
library(readr)
library(addr)
library(purrr)
library(sf)
library(s2)
library(tidycensus)

d <-
  readr::read_csv(
    "~/Desktop/PhD/riseup_geomarker_pipeline/data/DR1767_r2.csv",
    na = c("NA", "-", "NULL", "null"),
    col_types = readr::cols_only(
      MRN = readr::col_character(),
      PAT_ENC_CSN_ID = readr::col_character(),
      ADMIT_DATE_TIME = col_datetime(format = "%m/%d/%Y %I:%M:%S %p"),
      ADDRESS = readr::col_character(),
      CITY = readr::col_character(),
      STATE = readr::col_character(),
      ZIP = readr::col_character()
    )
  ) |>
  # hamilton county zip codes
  filter(ZIP %in% cincy::zcta_tigris_2020$zcta_2020) |>
  tidyr::unite("raw_address", c(ADDRESS, CITY, STATE, ZIP), sep = " ", na.rm = TRUE) |>
  mutate(clean_address = clean_address_text(raw_address)) |>
  filter(!clean_address == "") |>
  mutate(ADMIT_DATE = as.Date(ADMIT_DATE_TIME)) |>
  filter(!duplicated(PAT_ENC_CSN_ID)) |>
  # study period
  filter(ADMIT_DATE >= as.Date("2016-07-01") & ADMIT_DATE <= as.Date("2022-06-30")) |>
  select(-c(raw_address, ADMIT_DATE, ADMIT_DATE_TIME))

d$addr <- as_addr(d$clean_address)
d$cagis_addr_matches <- addr_match(d$addr, cagis_addr$cagis_addr)

saveRDS(d, "~/Desktop/PhD/RISEUP_parcels/data/visits_addr_match.rds")

d <- readRDS("~/Desktop/PhD/RISEUP_parcels/data/visits_addr_match.rds")

#### look at these - 0 or more than 1 matches
d_unmatched0 <- d |>
  filter(purrr::map_dbl(cagis_addr_matches, vctrs::vec_size) == 0)
# 33311 patients, 72894 visits, 32076 addresses
d_unmatched2<- d |>
  filter(purrr::map_dbl(cagis_addr_matches, vctrs::vec_size) > 1)
# 15 patients, 49 visits, 14 addresses

d <- d |>
  # filter to addresses with one match and join to cagis data
  filter(purrr::map_dbl(cagis_addr_matches, vctrs::vec_size) == 1) |>
  dplyr::mutate(cagis_addr = purrr::list_c(cagis_addr_matches)) |>
  dplyr::left_join(cagis_addr, by = "cagis_addr") |>
  select(-c(addr, cagis_addr_matches, clean_address))
# dim(d)
# [1] 383170      6

saveRDS(d, "~/Desktop/PhD/RISEUP_parcels/data/addr_matched_visits.rds")

##
d <- readRDS("~/Desktop/PhD/RISEUP_parcels/data/addr_matched_visits.rds")

# summarize visits by address
d <- d |>
  group_by(cagis_addr) |> 
  summarise(n_visit = n()) |>
  left_join(distinct(select(d, cagis_addr, cagis_addr_data))) |>
  ungroup()

# filter outliers 
d <- d |>
  mutate(address = as.character(cagis_addr)) |>
  filter(address != "222 E Central Parkway Cincinnati OH 45202") |>
  select(-c(address)) 

# all cagis data
all_cagis <- cagis_addr |>
  # addresses not in visits df
  filter(!cagis_addr %in% d$cagis_addr) |>
  mutate(n_visit = 0)

# join visits and all cagis addresses
d <- bind_rows(d, all_cagis)

# birth data
d_birth <- readRDS("~/Desktop/PhD/RISEUP_parcels/data/cagis_addr_births.rds")
# dim(d_birth)
# [1] 307206      3

# add births
d <- d |>
  left_join(d_birth, by = c("cagis_addr", "cagis_addr_data"))

# unnest cagis addr data tibbles and format parcel ids
d <- d |>
  # remove duplicate parcel ids within each address
  mutate(cagis_addr_data_filtered = map(cagis_addr_data, 
                                        ~ .x %>% distinct(cagis_parcel_id, .keep_all = TRUE))) |>
  select(-cagis_addr_data) |>
  tidyr::unnest(cagis_addr_data_filtered) |>
  # edit parcel ids so they match violations, etc.
  mutate(cagis_parcel_id = paste0(sub("^0", "", cagis_parcel_id), "00")) |>
  filter(!cagis_parcel_id == "<New parcel>00")

d2 <- d

## violation data
d_violation <-
  read_csv("https://raw.githubusercontent.com/geomarker-io/curated_violations/main/curated_violations/curated_violations.csv") |>
  distinct() |> 
  filter(date >= as.Date("2016-07-01") & date <= as.Date("2022-06-30")) |>
  # 6595 parcels
  mutate(paint_violation = stringr::str_detect(violation_type, "PAINT")) |>
  group_by(parcel_number) |>
  summarize(n_violation = n(), n_paint_violation = sum(paint_violation))

## county online data (year built and mtv)
d_online <- read_csv(paste0(fs::path_package("parcel", "hamilton_online_parcels"), "/hamilton_online_parcels.csv")) |>
  select(c(parcel_id, year_built, online_market_total_value))

## CAGIS parcel data from parcel package
cagis_parcels <- read_csv(paste0(fs::path_package("parcel", "cagis_parcels"), "/cagis_parcels.csv")) |>
  select(c(parcel_id, centroid_lat, centroid_lon, land_use, market_total_value, acreage)) 

# add violations and online data
d <- d |>
  left_join(d_violation, by = c("cagis_parcel_id" = "parcel_number")) |>
  left_join(d_online, by = c("cagis_parcel_id" = "parcel_id")) |>
  left_join(cagis_parcels, by = c("cagis_parcel_id" = "parcel_id")) 

# filter out parcels without land use type
d <- d |>
  filter(!is.na(land_use))

# filter out parcels not within city of cincinnati
nh <- st_transform(cincy::neigh_sna, "WGS84")
nh_union <- st_union(nh)

d <- st_as_sf(d, coords = c("centroid_lon", "centroid_lat"), crs = "WGS84")
#d <- st_transform(d, 4326)
d <- d[apply(st_within(d, nh_union, sparse = FALSE), 1, any), ]

# add parcel buffered crime
d_crime <- st_read("~/Desktop/PhD/crime_incidents-main/data/n_crimes_by_street_range_2024_07_30.gpkg")
d_crime <- st_transform(d_crime, crs = 32616)

d <- st_transform(d, crs = 32616)

# buffer parcels with 200 meter radius
parcels_buffer <- st_buffer(d, dist = 200)

# crime within each buffer
crimes_within_buffer <- st_join(d_crime, parcels_buffer, join = st_intersects)

# summarize crime for each parcel
crime_sums <- crimes_within_buffer %>%
  st_drop_geometry() %>% 
  group_by(cagis_parcel_id, cagis_addr) %>% 
  summarize(nv_crime_parcel = sum(property, na.rm = TRUE) + sum(other, na.rm = TRUE),
            violent_crime_parcel = sum(violent, na.rm = TRUE),
            .groups = 'drop')

# merge crime counts back to parcels
d <- d |>
  left_join(crime_sums, by = c("cagis_addr", "cagis_parcel_id"))

# transform back to original coordinates, remove geometry
d <- st_transform(d, "WGS84")
d <- as.data.frame(st_drop_geometry(d)) |>
  mutate(centroid_lon = sf::st_coordinates(d)[,1], 
         centroid_lat = sf::st_coordinates(d)[,2])
# dim(d)
# [1] 81466    20

# summary stats for addresses matched to multiple parcels
# 127 unique addresses are matched to multiple parcels
# get rid of some columns: online mtv, cagis address place/type
d <- d |>
  group_by(cagis_address) |>
  summarise(
    market_total_value = median(market_total_value),
    online_market_total_value = median(online_market_total_value),
    year_built = median(year_built),
    acreage = median(acreage),
    violent_crime_parcel = median(violent_crime_parcel),
    nv_crime_parcel = median(nv_crime_parcel),
    n_violation = mean(n_violation),
    n_paint_violation = mean(n_paint_violation),
    n_visit = first(n_visit),
    n_births = first(n_births),
    cagis_addr = first(cagis_addr),
    cagis_s2 = first(cagis_s2),
    cagis_parcel_id = first(cagis_parcel_id),
    land_use = first(land_use), 
    centroid_lat = mean(centroid_lat),
    centroid_lon = mean(centroid_lon)) |>
  ungroup()
# dim(d)
# [1] 77077    17

# sapply(d, function(x) sum(is.na(x)))

### add census tract data
# 55 do not have s2
# d <- d |>
#   filter(!is.na(cagis_s2))
# d$census_tract_id <- tiger_block_groups(s2::as_s2_cell(d$cagis_s2), year = "2023")

hc_tracts <- 
  tigris::tracts(state = 'oh', county = 'Hamilton', year = 2010) |>
  mutate(s2_geography = as_s2_geography(geometry)) |>
  tibble::as_tibble() |>
  select(-geometry)

d_cp <- d |>
  select(cagis_parcel_id, centroid_lon, centroid_lat, cagis_address) |>
  mutate(s2_geography = s2_geog_point(centroid_lon, centroid_lat)) 
d_cp <- d_cp |> 
  mutate(census_tract_id_2010 = hc_tracts[s2_closest_feature(d_cp$s2_geography, 
                                                             hc_tracts$s2_geography), "GEOID10", drop = TRUE]) |>
  select(-centroid_lon, -centroid_lat, -s2_geography) 

## join census ids back to main df
d <- d |> 
  left_join(d_cp, by = "cagis_address")

hh_acs_2019 <-
  # https://github.com/geomarker-io/hh_acs_measures
  read_fr_tdr("https://github.com/geomarker-io/hh_acs_measures/releases/download/v1.1.0") |>
  fr_filter(year == 2019) |>
  fr_select(-year, -census_tract_vintage) |>
  as_tibble() |> 
  # subset
  mutate(fraction_uninsured = 1 - fraction_insured,
         fraction_housing_owners = 1 - fraction_housing_renters,
         n_children_lt18_per_hh = n_children_lt18 / n_household,
         fraction_household_lt18 = n_household_lt18 / n_household) |> 
  select(census_tract_id_2010 = census_tract_id,
         fraction_vacant_housing = fraction_vacant,
         fraction_assisted_income = fraction_snap,
         fraction_high_school_edu = fraction_hs,
         fraction_no_health_ins = fraction_uninsured,
         fraction_owner_occupied = fraction_housing_owners,
         fraction_houses_built_before_1970 = fraction_builtbf1970,
         fraction_poverty,  
         median_income,
         median_rent_to_income_percentage,
         n_population = n_pop,
         fraction_fam_nospouse,
         fraction_lesh,
         fraction_employment,
         fraction_conditions,
         n_children_lt18_per_hh,
         fraction_household_lt18,
         median_home_value) 

# calculate crowding
acs_crowding <- tidycensus::get_acs(geography = 'tract',
                                    variables = c('B25014_005', 'B25014_006', 'B25014_007', 'B25014_011', 'B25014_012', 'B25014_013'),
                                    summary_var = 'B25014_001',
                                    year = 2019,
                                    state = 'ohio') |>
  group_by(GEOID) |>
  summarize(n_crowded = sum(estimate),
            total = unique(summary_est)) |>
  mutate(fraction_crowding = n_crowded / total) |>
  select(GEOID, fraction_crowding)

acs_tract_data <- hh_acs_2019 |> 
  left_join(acs_crowding, by = c("census_tract_id_2010" = "GEOID")) 

# get ALAND (area of land in sq m) to calculate population density
oh_tracts <- tigris::tracts(state = 'ohio', year = 2019)
oh_GEOID <- oh_tracts |> pull(GEOID)

# calculate population density
acs_tract_data <- hh_acs_2019 |> 
  left_join(acs_crowding, by = c("census_tract_id_2010" = "GEOID")) |> 
  left_join(oh_tracts, by = c("census_tract_id_2010" = "GEOID")) |> 
  mutate(population_density = n_population / ALAND / 0.000001) |> 
  sf::st_drop_geometry() |> 
  select(-(STATEFP:geometry)) |> 
  filter(census_tract_id_2010 %in% oh_GEOID)

# crime data for census tracts
# 2016-07-01 through 2022-06-30
ct_crime <- as.data.frame(sf::st_read("~/Desktop/PhD/crime_incidents-main/data/n_crimes_by_tract_2024_07_30.gpkg")) |>
  select(-c(geom, property, other))
colnames(ct_crime) <- c("census_tract_id_2010", "violent_crime_ct", "total_crime_ct")
ct_crime$census_tract_id_2010 <- as.character(ct_crime$census_tract_id_2010)

# join acs and crime data to main df
d <- d |>
  left_join(acs_tract_data, by = "census_tract_id_2010") |>
  left_join(ct_crime, by = "census_tract_id_2010") |>
  rename(cagis_parcel_id = cagis_parcel_id.x) |>
  select(-cagis_parcel_id.y) 

saveRDS(d, "~/Desktop/PhD/RISEUP_parcels/data/data_for_analysis_20240806.rds")

##########

# visualizing crime
hc_tracts <- 
  tigris::tracts(state = 'oh', county = 'Hamilton', year = 2010) |>
  mutate(s2_geography = as_s2_geography(geometry))
d <- st_as_sf(d, coords = c("centroid_lon", "centroid_lat"), crs = "WGS84")
dd <- ct_crime |>
  left_join(hc_tracts, join_by("census_tract_id_2010" == "GEOID10")) |>
  st_as_sf()
mapview::mapview(select(dd, c(geometry, total_crime_ct))) +
  mapview::mapview(select(d, c(geometry, total_crime_parcel))) +
  mapview::mapview(select(d_crime, c(geom, total)), zcol = "total", at = seq(0, 700, length.out = 200))



# cagis addresses matched to multiple parcels
# d_multiple_parcels <- d |>
#   filter(map_lgl(cagis_addr_data, ~ n_distinct(.x$cagis_parcel_id) > 1))


# count_unique_parcel_ids <- function(tibble_df) {
#   num_unique_parcel_ids <- tibble_df %>%
#       distinct(cagis_parcel_id) %>%
#       nrow()
#   return(num_unique_parcel_ids)
# }
# 
# # Apply the function to each row of the data frame
# d_multiple_parcels <- d_multiple_parcels |>
#   mutate(num_parcel_ids = map_int(cagis_addr_data, ~ count_unique_parcel_ids(.x))) |> 
#   mutate(difference = num_rows - num_parcel_ids)
# d2 <- filter(d_multiple_parcels, difference > 0)
# 
# filter_unique_parcel_ids <- function(tibble_df) {
#   tibble_df %>%
#     distinct(cagis_parcel_id, .keep_all = TRUE)  # Keep only the first occurrence of each unique cagis_parcel_id
# }
# 
# d_multiple_parcels <- d_multiple_parcels |>
#   mutate(tibbles_column_filtered = map(cagis_addr_data, filter_unique_parcel_ids)) |>
#   mutate(num_rows = map_int(tibbles_column_filtered, nrow))|>
#   mutate(num_parcel_ids = map_int(tibbles_column_filtered, ~ count_unique_parcel_ids(.x))) |> 
#   mutate(difference = num_rows - num_parcel_ids)
# 
# d_multiple_parcels <- d_multiple_parcels |>
#   tidyr::unnest(tibbles_column_filtered)
# length(unique(d_multiple_parcels$cagis_parcel_id))
# [1] 6071
# there are parcels matched to multiple addresses

# d3 <- d |>
#   filter(map_lgl(cagis_addr_data, ~ n_distinct(.x$cagis_parcel_id) > 1)) |>
#   mutate(cagis_addr_data_filtered = map(cagis_addr_data, 
#                               ~ .x %>% distinct(cagis_parcel_id, .keep_all = TRUE)))
# d3 <- d3 |>
#   select(-cagis_addr_data) |>
#   tidyr::unnest(cagis_addr_data_filtered)
# 
# repeated_parcels <- d3 |>
#   group_by(cagis_parcel_id) |>
#   filter(n() > 1) |>
#   ungroup()
# 1163 parcel ids are matched to more than one address
# 74 are associated with addresses that were matched to multiple parcels





# # trying to get census geography
# cagis <- cagis |>
#   filter(!is.na(cagis_s2)) |>
#   mutate(census_block_group = tiger_block_groups(s2::as_s2_cell(cagis_s2), year = "2023")) |>
#   mutate(census_tract_2010 = substr(census_block_group, 1, nchar(census_block_group) - 1))
# 
# missing_s2 <- cagis %>% filter(is.na(cagis_s2))
# # none of the visit addresses are missing s2


