
library(SparkR)
sc <- sparkR.init()

peopleRDD <- textFile(sc, paste(getwd(), "/people.txt", sep = ""))

lines <- flatMap(peopleRDD,
                 function(line) {
                   strsplit(line, ", ")
                 })

ageInt <- lapply(lines,
                 function(line) {
                   as.numeric(line[2])
                 })

avg <- reduce(ageInt, function(x,y) { x + y }) / count(peopleRDD)

############# With SparkSQL!! #####################

sqlCtx <- sparkRSQL.init(sc)
df <- jsonFile(sqlCtx, paste(getwd(), "/people.json", sep = ""))
avg <- select(df, avg(df$age))
