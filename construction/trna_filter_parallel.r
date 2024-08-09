#!/opt/local/bin/Rscript
library(odbc)
library(tidyverse)
library(stringr)
library(data.table)


arg <- base::commandArgs(trailingOnly = TRUE)
dest <- arg[1]
YEAR <- arg[2]

print(dest)
print(YEAR)

# database connection -----------------------------------------------------
# connect to the database
conn <- dbConnect()
dbGetQuery(conn, 'use database name') #setting the uat db to query from

# filters!
usa_list <- c('Federal Reserve','the Fed', 'fed funds rate','Federal Open Market Committee', ' fomc',  # Taken from Husted, Rogers, Sun (2020)
              'Greenspan', 'Bernanke', 'Yellen', 'fed chairman', ' fed chair', # Taken from policyuncertainty.com website https://www.policyuncertainty.com/categorical_terms.html (Monetary policy terms, US specific) 
              'Gramm-Rudman', 'debt ceiling', # Taken from policyuncertainty.com (fiscal policy and government spending)
              #    'Medicaid', 'Medicare', 'food and drug administration', 'fda', 'prescription drug act',' part d ', 'affordable care act', 'Obamacare', # Taken from policyuncertainty.com (health care)
              #    '9/11', # Taken from policyuncertainty.com (National security)
              'Aid to Families with Dependent Children', 'Temporary Assistance for Needy Families', 'Old-Age, Survivors, and Disability Insurance', ' afdc', ' tanf', ' oasdi',
              ' wic program', 'Supplemental Nutrition Assistance Program', 'Earned Income Tax Credit', ' eitc', 'head start program', # Taken from policyuncertainty.com (entitlement programs)'
              'glass-steagall','troubled asset relief program', ' tarp', 'dodd-frank','commodity futures trading commission', ' cftc', 'house financial services committee', 'Volcker rule',
              'securities and exchange commission', ' sec ', ' sec.', 'Federal Deposit Insurance Corporation', ' fdic', 'Office of the Comptroller of the Currency', ' occ ', 
              'Office of Thrift Supervision', ' ots ', 'Financial Institutions Reform', ' firrea ', ' fslic', ' ots', ' firrea', 
              'truth in lending','national labor relations board', ' nlrb ', ' nlrb', 'davis-bacon', 'equal employment opportunity', ' eeo ', 'Occupational Safety and Health Administration', ' osha ', ' eeo', ' osha',
              'federal trade commission', ' ftc ', ' ftc', 'clean air act', 'clean water act', 'environmental protection agency', ' epa ', ' epa', # Taken from policyuncertainty.com (regulation)
              'White House', # U.S. specific words from Baker, Bloom, Davis (2016)
              'Wall Street', 'Chicago Board Options Exchange', ' CBOE',  'consumer financial protection bureau', ' cfpb')

usa_str <- "("
for (usa_word in usa_list) {
  usa_str <- paste0(usa_str, "body ILIKE '%", usa_word, "%' OR ")
}
# drop the last OR
usa_str <- substr(usa_str, 1, nchar(usa_str) - 3)
usa_str <- paste0(usa_str, ")")


asset_list <- c('Treasury', 'Treasuries', 'T-bill', 'risk-free', 'risk free', 'corporate bond', 'corporate spread', 'corporate earning', 'junk bond', 'junk-bond', 'investment-grade', 
                'investment grade', 'high-yield', 'high yield', 'fixed-income', 'fixed income', 'leveraged loan', 'secondary market', 'default rate', 'stock price', 'options price', 
                'options market', 'Dow Jones', 'DJIA', 'equity price', 'equity premium', 'equity premia', 'equity market', 'S&P 500', 'NASDAQ', 'VIX', 'real estate', ' CRE ', 'vacancy rate', 'house price', 
                'mortgage', 'borrowing cost', 'LIBOR', 'underwriting', 'capitalization rate', 'capitalization spread', 'market liquidity', 'bid-ask', 'risk appetite') # Asset valuations in FSR and derivatives?
asset_str <- "("
for (asset_word in asset_list) {
  asset_str <- paste0(asset_str, "body ILIKE '%", asset_word, "%' OR ")
}
# drop the last OR
asset_str <- substr(asset_str, 1, nchar(asset_str) - 3)
asset_str <- paste0(asset_str, ")")


headline_exclude_list <- c('DIARY', 'BUZZ', 'Morning Call','factors to watch','Research roundup', 'Research round-up', 'take a look', 'fimmda', 'IFR Markets ForexWatch', 'Top News','BRIEF', 'NSEI Block Deal', "TABLE", "PRESS DIGEST",
                           'REPORT CODE DIRECTORY', 'SUBJECT CODE DIRECTORY', 'Fitch', 'GLANCE')
headline_str <- "("
for (headline_word in headline_exclude_list) {
  headline_str <- paste0(headline_str, "headline NOT ILIKE '%", headline_word, "%' AND ")
}
# drop the last OR
headline_str <- substr(headline_str, 1, nchar(headline_str) - 4)
headline_str <- paste0(headline_str, ")")


  
text <- dbGetQuery(conn, paste0("SELECT DISTINCT SUBSTRING(start, 1, 10) as newdate,
                                        headline,
                                        CASE WHEN LENGTH(body) = 0 THEN headline
                                             ELSE body END as body
                                 FROM database_name
                                 WHERE pubstatus = 'stat:usable' and LENGTH(headline) > 0 AND language ILIKE 'en' AND last_article = 1 AND SUBSTRING(start,1,4) = '",
                                 YEAR,
                                 "' AND ",
                                 usa_str, 
                                 " AND ",
                                 asset_str,
                                 " AND ",
                                 headline_str)) 

clean_data <- setDT(text) %>%
  filter(str_detect(substr(body, 1, 40), "TOP STORIES", negate = T), #filter out any top stories
         str_detect(body, "^Click the following link to watch video", negate = T), #maybe could filter out description idk
         str_detect(headline, fixed("U.S./Canada daily earnings hits & misses", ignore_case = T), negate = T) #annoying tables
  ) %>%
  mutate(date = as.Date(newdate, format = "%Y-%m-%d")) %>%
  mutate(body = str_squish(body), #get rid of weird white spaces
         firstP = str_extract(substr(body, 1, 100), "^\\s*\\([^\\)]+\\)"), #extract things from parenths
         
         
         #Remove the authors
         bodyCl= ifelse(str_detect(substr(body, 1, 75), "By "),
                        str_replace(body, paste("By Publishing Editors","By.{5,75}(Reuters\\)|Reuters Breakingviews\\)|-\\s|\\*)", sep = "|"), ' '),
                        body),
         
         bodyCl = ifelse(str_detect(headline, "DIARY"),
                         str_replace(body, regex("^.{10,500}Indicates new events",ignore_case = T), ' '),
                         bodyCl),
         
         
         #Find instances of 5+
         bodyCl = str_replace_all(bodyCl, " \\.{7,}.*", " "), #everything after a series of periods.
         
         
         #Get rid of parents and such
         bodyCl = str_replace_all(bodyCl, "\\s*\\(\\([^\\)\\)]+\\)\\)", " "), #things inside (( ))
         bodyCl = str_replace_all(bodyCl, "\\s*\\([^\\)]+\\)", " "), #things inside ( )
         bodyCl = str_replace_all(bodyCl, "\\s*\\[[^\\]]+\\]", " "), #things inside []
         bodyCl = str_replace_all(bodyCl, "\\s*\\<[^\\>]+\\>", " "), #things inside []
         
         # get rid of sequences of periods or dashes
         bodyCl = str_replace_all(bodyCl, "-{2,}", " "),
         bodyCl = str_replace_all(bodyCl, "\\.{2,}", " "),
         
         # Remove all instances of * 
         bodyCl = str_replace_all(bodyCl, "\\*+", " "),
         
         #Get rid of links
         bodyCl = str_replace_all(bodyCl, "(http|www)[[:alnum:][:punct:]]*[[:graph:]]+", " "), #removes links
         bodyCl = str_replace_all(bodyCl, "\\s*\\S\\.(com|net|gov)", " "),
         
         #Get rid of useless phrases
         bodyCl = str_replace_all(bodyCl, regex(paste("Listen to the podcast",
                                                      "Reuters customers can click on",
                                                      "For previous columns by the author",
                                                      "sign up for breakingviews email alerts",
                                                      "Contact page editor",
                                                      "Click the following link to watch video",
                                                      "video transcript: verified transcript not available",
                                                      "Further company coverage",
                                                      "Source text for Eikon",
                                                      "Inclusion of diary items does not necessarily mean that Reuters will file a story based on the event",
                                                      "NOTE: The inclusion of items in this diary does not necessarily mean that Reuters will file a story based on the event",
                                                      "For technical issues, please contact Thomson Reuters Customer Support",
                                                      "Reuter Terminal users, double-click on number to read story",
                                                      "To access stories and prices on the Reuter terminal, use the mouseto click on the codes in brackets",
                                                      "To access stories and prices on the Reuter terminal, use the mouse to click on the codes in brackets",
                                                      "TO ACCESS ANY STORIES AND PRICES ON THE REUTER TERMINAL CLICK ON CODES IN BRACKETS",
                                                      sep = "|"), ignore_case = T), " "),
         
         
         #Replace US notation with United States so it doesn't get filtered out
         #bodyCl = str_replace_all(bodyCl, fixed("U.S.", ignore_case = T), " United States "),
         #bodyCl = str_replace_all(bodyCl, regex("US | US", ignore_case = F), " United States "),
         #bodyCl = str_replace_all(bodyCl, fixed("US Bank", ignore_case = T), " United States Bank "),
         
         #Get rid anything that isn't numbers
         #bodyCl = str_replace_all(bodyCl, "[^a-zA-Z ]", " "), #get rid of all numbers and such
         
         #Get rid of words that are less than 2 characters
         #bodyCl = str_replace_all(bodyCl, " *\\b[[:alpha:]]{1,2}\\b *", " "), #words less than 1-2 characters
         
         #Clean up
         bodyCl = str_squish(bodyCl), #gets rid of unnecessary spacing
         #bodyCl = tolower(bodyCl) #puts everything in lowercase
  ) %>%
  collect()

clean_data <- clean_data[, list(newdate, headline, body, bodyCl)]

names(clean_data) <- c("newdate", "headline", "body_raw", "body")


fwrite(clean_data, paste0(dest, "filtered_", YEAR, ".csv"))
print(paste0("saved ", YEAR, " file"))



#full <- dbGetQuery(conn, paste0("SELECT *
#                                 FROM if_trna.news_story
#                                 WHERE language = 'en' and headline ILIKE '%NSEI Block Deal%'
#                                 LIMIT 10")) 
