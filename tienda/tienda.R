
library(sqldf)

ord <- read.csv('/home/kirito/liminaris/superintel/superstore_orders.csv',header = T,stringsAsFactors = F)
dev <- read.table('/home/kirito/liminaris/superintel/superstore_returns.csv',sep=',',header = T,stringsAsFactors = F)

head(ord)
head(dev)

ordenes <- merge(ord,dev,by='Order.ID',all.x=T)

dim(ord)
dim(ordenes)

head(ordenes)
names(ordenes) <- gsub('\\.','',names(ordenes))

sqldf('select ProductCategory,count(*) as n from ordenes group by ProductCategory')
sqldf('select ProductSubCategory,count(*) as n as ventas from ordenes group by ProductSubCategory')

sqldf('select ProductCategory,sum(sales)/1000 as ventas from ordenes group by ProductCategory')
sqldf('select ProductSubCategory,sum(sales)/1000 as ventas from ordenes group by ProductSubCategory')

vtascant1 <- sqldf('select Region,sum(sales) as Ventas,sum(OrderQuantity) as Cantidad from ordenes group by Region')
vtascant2 <- sqldf('select Region,count(distinct CustomerName) as numClients from ordenes group by Region')
vtascant <- merge(vtascant1,vtascant2,by='Region',all.x=T)
write.table(vtascant,'/home/kirito/learning/enron/correos_datos/enrontest/tienda/vtascant.csv',sep=',',row.names = F)


write.table(sqldf('select substr(ProductName,1,40),sum(sales) as Ventas from ordenes group by ProductName order by Ventas desc limit 20'),'/home/kirito/learning/enron/correos_datos/enrontest/tienda/top50.csv',sep=',',row.names = F)





library(streamgraph)

#sqldf('select substr(OrderDate,4,10) as ODate,OrderPriority as Prioridad,sum(Sales) as Ventas,sum(OrderQuantity) as Cantidad from ordenes group by OrderDate,OrderPriority')
#fechaprioridad1 <- sqldf('select "01/"||substr(OrderDate,4,10) as OrderDate,OrderPriority as Prioridad,Sales as Ventas,OrderQuantity as Cantidad from ordenes')
fechaprioridad1 <- sqldf('select "01/01/"||substr(OrderDate,7,10) as OrderDate,OrderPriority as Prioridad,Sales as Ventas,OrderQuantity as Cantidad from ordenes')
fechaprioridad2 <- sqldf('select OrderDate,Prioridad,sum(Ventas) as Ventas,sum(Cantidad) as Cantidad from fechaprioridad1 group by OrderDate,Prioridad')
fechaprioridad2
fechaprioridad2$Fecha <- as.Date(fechaprioridad2$OrderDate,'%d/%m/%Y')
fechaprioridad2 <- fechaprioridad2[with(fechaprioridad2,order(Prioridad,Fecha)),]
head(fechaprioridad2)
write.table(fechaprioridad2[,c(2,1,3)],'/home/kirito/learning/enron/correos_datos/enrontest/tienda/streamdata.csv',sep=',',row.names = F,quote = F,col.names = F)

streamgraph(fechaprioridad2,key=Prioridad,value=Ventas,date=Fecha)



#grafos
library(igraph)
head(ordenes)

ret1 <- sqldf('select CustomerName,substr(ProductName,1,25) as ProductName,ProductCategory,Sales,Status from ordenes where Sales > 10000 and Status="Returned"')
n1 <-  sqldf('select CustomerName as id,1 as grp,sum(Sales) as Sales from ret1 group by CustomerName 
      union select ProductName as id,2 as grp,sum(Sales) as Sales from ret1 group by ProductName 
      union select ProductCategory as id,3 as grp,sum(Sales) as Sales from ret1 group by ProductCategory')
mintam <- 50
maxtam <- 3
minsiz <- min(n1$Sales)
maxsiz <- max(n1$Sales)
n1$size <- round(((mintam-maxtam)*(n1$Sales-minsiz)/(maxsiz-minsiz))+maxtam)

a1 <- unique(ret1[,c(1,2)])
a2 <- unique(ret1[,c(2,3)])
names(a1) <- c('source','target')
names(a2) <- c('source','target')
a1$value <- 1
a2$value <- 1
a3 <- rbind(a1,a2)
g <- graph.data.frame(a3,directed = F)
g.lay <- layout.fruchterman.reingold(g)
plot(g,layout=g.lay)
nodos <- n1
aristas <- a3

library(rjson)
write(toJSON(unname(split(nodos,1:nrow(nodos)))),'/home/kirito/learning/enron/correos_datos/enrontest/tienda/nodos.json')
write(toJSON(unname(split(aristas,1:nrow(aristas)))),'/home/kirito/learning/enron/correos_datos/enrontest/tienda/aristas.json')


#



nodos <- n1[,c(1,2,4)]
names(nodos) <- c('id','group','size')




n1 <- sqldf('select distinct CustomerName as id,1 as grp from ret1 union select distinct ProductName as id,2 as grp from ret1 union select distinct ProductCategory as id,3 as grp from ret1 order by grp,id')
n2 <- sqldf('select ProductCategory as id,3 as grp,sum(Sales) as Sales from ret1 group by ProductCategory')
n2 <- merge(n2,sqldf('select ProductCategory as id,3 as grp,sum(Sales) as Sales from ret1 group by ProductCategory'),by=c('id','grp'),all.x=T)
n2 <- sqldf('select ProductCategory as id,3 as grp,sum(Sales) as Sales from ret1 group by ProductCategory')


ret1 <- ordenes[,c('CustomerName','ProductName','Sales','Status')]
ret2 <- ret1[which(ret1$Status=='Returned'),]
ret2$ProductName <- substr(ret2$ProductName,1,25)
hist(ret2$Sales)
ret3 <- ret2[which(ret2$Sales>10000),]
hist(ret3$Sales)
g <- graph.data.frame(ret3[,1:3],directed = F)
g.lay <- layout.kamada.kawai(g)
g.lay <- layout.fruchterman.reingold(g)
g.lay <- layout.circle(g)
plot(g,layout=g.lay)
nodos <- data.frame(id=V(g)$name)
nodos$group <- 1
nodos$group <- ifelse(nodos$id %in% ret3$ProductName,2,1)
mintam <- 3
maxtam <- 30
minsiz <- min(ret4$Sales)
maxsiz <- max(ret4$Sales)
#((b-a)*(v-vmin)/(vmax-vmin))+a
  
nodos$size <- merge(nodos,data.frame(id=ret4$ProductName,size=round(((mintam-maxtam)*(ret4$Sales-minsiz)/(maxsiz-minsiz))+maxtam)),by='id',all.x=T)$size
is.na(nodos$size) <- mintam
nodos

aristas <- ret3[,c(1,2,3)]
names(aristas) <- c('source','target','value')
aristas

library(rjson)
write(toJSON(unname(split(nodos,1:nrow(nodos)))),'/home/kirito/learning/enron/correos_datos/enrontest/tienda/nodos.json')
write(toJSON(unname(split(aristas,1:nrow(aristas)))),'/home/kirito/learning/enron/correos_datos/enrontest/tienda/aristas.json')




