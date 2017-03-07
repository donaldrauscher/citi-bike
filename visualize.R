library(dplyr)
library(tidyr)
library(ggmap)

# get data
rebalances <- read.csv("rebalances.csv", header=TRUE)
stations <- read.csv("stations.csv", header=TRUE, stringsAsFactors=FALSE)

# compute net flows at station level
stations <- stations %>% mutate(net = inflow - outflow)

# compute net flows between stations
rebalances2 <- rebalances
rebalances2$flow <- apply(rebalances2[,c("from_station", "to_station")], 1, function(x) paste(sort(x), collapse="_"))

rebalances3 <- rebalances2 %>%
  group_by(flow) %>% summarise(n = sum(ifelse(from_station<to_station, n, -n))) %>%
  separate(flow, into=c("from_station", "to_station"), sep="_") %>% 
  mutate(from_station=as.numeric(from_station), to_station=as.numeric(to_station)) %>%
  filter(n!=0)

rebalances4 <- rbind(
    rebalances3 %>% filter(n>0),
    rebalances3 %>% filter(n<0) %>% rename(from_station=to_station, to_station=from_station) %>% mutate(n=-n)
  ) %>%
  inner_join(stations %>% select(id, lat, lon) %>% rename(from_lat=lat, from_lon=lon), by=c("from_station"="id")) %>%
  inner_join(stations %>% select(id, lat, lon) %>% rename(to_lat=lat, to_lon=lon), by=c("to_station"="id"))

# show top inflows / outflows
stations2 <- stations %>% filter(net>0)
stations3 <- stations %>% filter(net<0)
rebalances5 <- rebalances4 %>% 
  filter(from_station %in% stations2$id & to_station %in% stations3$id) %>%
  arrange(-n) %>% head(.,100)

stations4 <- stations %>% filter(id %in% unique(c(rebalances5$from_station, rebalances5$to_station)))

# plot
map1 <- get_googlemap(center=apply(stations4[,c("lon","lat")],2,median), zoom=12, color="bw", style="feature:poi|visibility:off")
ggmap(map1) + 
  geom_point(data=stations, aes(x=lon, y=lat, size=abs(net), colour=ifelse(net>0,"Net In","Net Out")), alpha=0.5) + 
  labs(colour="Net In/Out", size="# Bikes")

map2 <- get_googlemap(center=apply(stations4[,c("lon","lat")],2,median), zoom=13, color="bw", style="feature:poi|visibility:off")
ggmap(map2) + 
  geom_segment(data=rebalances5, aes(x=from_lon, y=from_lat, xend=to_lon, yend=to_lat), arrow=arrow(length = unit(0.2,"cm"))) +
  geom_point(data=stations4, aes(x=lon, y=lat, size=abs(net), colour=ifelse(net>0,"Net In","Net Out")), alpha=0.75) + 
  labs(colour="Net In/Out", size="# Bikes", alpha = "# Bikes")