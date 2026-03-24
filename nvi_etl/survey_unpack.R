library(haven)
library(dplyr)
library(purrr)
library(tidyr)
library(labelled)
library(survey)

frame <- read_sav("Q:\\3_Projects\\NVI\\2025\\archive\\spss.sav")
design <- svydesign( ids = ~1, data = frame )

# This will show readable names for all variables
sapply(frame, function(x) attr(x, "label"))

# Reduce race responses to a single column to match ACS categories for raking
# The number of people who report more than one are relatively small

race_cols = c(
    "var183O456", # Black
    "var183O461", # White
    "var183O460", # Hispanic
    "var183O457", # American Indian Alaskan Native
    "var183O458", # Arab American or Middle Eastern
    "var183O459", # Asian/Native Hawaiian
    "var183O462", # Some other race
    "var183O463"  # Prefer not to answer
)

race_codes = c(
    "Black",
    "White",
    "Hispanic",
    "American Indian Alaskan Native",
    "White", # "Arab American or Middle Eastern",
    "Asian/Native Hawaiian",
    "Some other race",
    "Prefer not to answer / Nothing selected"
)

counts <- rowSums(frame[, race_cols])

single_race <- apply(frame[, race_cols], 1, function(row) {
  idx <- which(row == 1)
  if (length(idx) == 1) race_codes[[idx]] else NA
})

frame$race_simplified <- case_when(
  counts == 0 ~ race_codes[[8]],
  counts > 1  ~ "Two or more races",
  TRUE        ~ single_race
)

# Example: show value counts and proportions
table(as_factor(frame$race_simplified))
prop.table(table(as_factor(frame$race_simplified)))

# ACS to collect
# Gender: var135
# Race: race_reduced
# Education: var35
# Age: var136
# Income: var184

prop.table(
  table(
    as_factor(frame$var35),
    as_factor(frame$var443O1124)
  ), 
  margin = 1
)

prop.table(table(as_factor(frame$var136)))

API_KEY = "f073dafdfcebff1b2157923d0ab50652a0c1c649"


library(tidycensus)

census_api_key(API_KEY)

vars <- load_variables(2024, "acs5", cache = TRUE)
b01001 <- vars[startsWith(vars$name, "B01001"), ]
write.csv(b01001, "b01001.csv")

table(frame$Vdatesub)