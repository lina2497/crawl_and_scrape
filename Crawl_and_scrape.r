#Packages
library(curl)
library(xml2)
library(purrr)
library(dplyr)
library(readr)
library(rvest)

stopifnot(packageVersion('xml2') >= 1.0)
stopifnot(packageVersion('curl') >= 2.0)
##--------------------------------------------------------------------------------------------
#Functions
# Extracts hyperlinks from HTML page
get_links <- function(html, url){
  tryCatch({
    doc <- xml2::read_html(html)
    nodes <- xml2::xml_find_all(doc, "//a[@href]")
    links <- xml2::xml_attr(nodes, "href")
    links <- xml2:::url_absolute(links, url)
    links <- grep("^https?://", links, value = TRUE)
    links <- sub("#.*", "", links)
    links <- sub("/index.html$", "/", links)
    unique(sub("/$", "", links))
  }, error = function(e){character()})
}



crawl <- function(root, timeout = 300){
  total_visited = 0
  pages <- new.env()
  pool <- curl::new_pool(total_con = 50, host_con = 6, multiplex = TRUE)
  
  crawl_page <- function(url){
    pages[[url]] <- NA
    h <- curl::new_handle(failonerror = TRUE, nobody = TRUE)
    curl_fetch_multi(url, handle = h, pool = pool, done = function(res){
      total_visited <<- total_visited + 1
      if(!identical(url, res$url)){
        pages[[url]] = res$url
        url = res$url
      }
      headers <- curl::parse_headers(res$headers)
      ctype <- headers[grepl("^content-type", headers, ignore.case = TRUE)]
      cat(sprintf("[%d] Found '%s' @ %s\n", total_visited, ctype, url))
      if(isTRUE(grepl("text/html", ctype))){
        handle_setopt(h, nobody = FALSE, maxfilesize = 1e6)
        curl::curl_fetch_multi(url, handle = h, pool = pool, done = function(res){
          total_visited <<- total_visited + 1
          links <- get_links(res$content, res$url)
          cat(sprintf("[%d] Extracted %d hyperlinks from %s\n", total_visited, length(links), url))
          followlinks <- grep(paste0("^", root), links, value = TRUE)
          pages[[url]] <- followlinks
          lapply(followlinks, function(href){
            if(is.null(pages[[href]]))
              crawl_page(href)
          })
        }, fail = function(errmsg){
          cat(sprintf("Fail: %s (%s)\n", url, errmsg))
        })
      }
    }, fail = function(errmsg){
      cat(sprintf("Fail: %s (%s)\n", url, errmsg))
    })
  }
  crawl_page(root)
  curl::multi_run(pool = pool, timeout = timeout)
  as.list(pages)
}

error_proof <-
  function(X) {
    tryCatch(
      X,
      error = function(e) {
        cat("ERROR :", conditionMessage(e), "\n")
      }
    )
  }


parse_loc8nearme<-function(page){
  
  
  out<-data.frame(name=html_nodes(page,css = "h1")%>%
                    html_text(trim = T),
                  
                  address=html_node(page,css = ".bread_info")%>%
                    html_children()%>%
                    .[2]%>%
                    html_text(trim = T),
                  
                  phone_number=html_node(page,css = ".bread_info")%>%
                    html_children()%>%
                    .[4]%>%
                    html_children()%>%
                    html_children()%>%
                    html_text(trim = T)%>%
                    .[1],
                  url=url,
                  date=Sys.Date())
  return(out)
  
}
##--------------------------------------------------------------------------------------------
#Workflow

# Create a sitemap

####
####
####
#Important#
#The function "crawl" can take a long time, ~24-48 hours to fully map a site depending on the number of pages.




loc8nearme <- crawl(root = 'https://www.loc8nearme.co.uk/', timeout = Inf)

readr::write_rds(loc8nearme, paste0(Sys.Date(),"_loc8nearme.rds"))


#Process output
UK2<-purrr::map(loc8nearme,unlist)
UK3<-unlist(UK2)
UK4<-unique(UK3)
split<-strsplit(UK4,"/")
lengths<-map_int(split,length)
hist(lengths)


#Create a sitemap of useful URLS based on their structure e.g. number of slashes ==7
Seven<-do.call(rbind,split[lengths==7])%>%
  as_data_frame()%>%
  select(-c(1:3))%>%
  set_names("region","town","store","id")%>%
  mutate(id=as.numeric(id))%>%
  unique()%>%
  mutate(url=paste0("https://www.loc8nearme.co.uk/",region,"/",town,"/",store,"/",id))

write_rds(Seven,paste0(Sys.Date(),"_loca8nearme_branch_sitemap.rds"))




##Code for setting up scraping loop
test_urls<-Seven$url
pagelist<-list()
saves<-seq(from=10000,to =150000,by=10000)


##Read html
for (i in seq_along(Seven$url)) {
  print(i)
  X<-tryCatch(read_html(test_urls[i]),error=function(e){cat("ERROR :",conditionMessage(e), "\n")})

  pagelist[[i]]<-X
  
  if(i%in%saves){write_rds(pagelist,paste0(Sys.Date(),"_",length(pagelist),"_pagelist.rds"))}
  
  
}



write_rds(pagelist,paste0(Sys.Date(),"_",length(pagelist),"_pagelist.rds"))





out_list<-list()


## Parse html

for (i in seq_along(pagelist)) {
  print(i)
  
  name<-NA
  name<-error_proof(html_nodes(pagelist[[i]],css = "h1")%>%
                      html_text(trim = T))
  
  if(is.null(name)){name<-NA}
  address<-NA
  address<-error_proof(html_node(pagelist[[i]],css = ".bread_info")%>%
                         html_children()%>%
                         .[2]%>%
                         html_text(trim = T))
  
  if(is.null(address)){address<-NA}
  
  phone_number<-NA
  phone_number<-error_proof(html_node(pagelist[[i]],css = ".bread_info")%>%
                                           html_children()%>%
                                           .[4]%>%
                                           html_children()%>%
                                           html_children()%>%
                                           html_text(trim = T)%>%
                                           .[1])
  
  if(is.null(phone_number)){phone_number<-NA}
  
  
  
  out <- data.frame(
    name = name,
    address = address,
    phone_number = phone_number,
    url = Seven$url[i],
    date = Sys.Date()
  )
  
  out_list[[i]]<-out
}


out_df<-do.call(rbind,out_list)

out_looked_up<-left_join(out_df,Seven)

write_rds(out_looked_up,paste0(Sys.Date(),"_retailer_locations_from_loc8nearme.rds"))




###Speeding up the pipeline

urls <- readRDS("~/retailer_locations/loc8nearme/2019-04-19_loca8nearme_branch_sitemap.rds")


read_html_safely<-function(x){

error_proof <-
  function(X) {
    tryCatch(
      X,
      error = function(e) {
        cat("ERROR :", conditionMessage(e), "\n")
      }
    )
  }

out<-NA


out<-error_proof(read_html(x))

if(is.null(out)){out<-NA}

if(length(out)<2){out<-NA}



return(out)

}

pagelist<-list()
cl <- makeCluster(8)
registerDoParallel(cl)
pagelist<-foreach(i=1:nrow(urls),.packages = c("rvest", "doParallel")) %dopar% read_html_safely(urls$url[i])

