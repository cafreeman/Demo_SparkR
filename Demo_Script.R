library(SparkR)

sc <- sparkR.init("local[*]")
sqlCtx<- sparkRSQL.init(sc)

txnsRaw<- loadDF(sqlCtx, paste(getwd(), "/Customer_Transactions.parquet", sep = ""), "parquet")
demo <- withColumnRenamed(loadDF(sqlCtx, paste(getwd(), "/Customer_Demographics.parquet", sep = ""), "parquet"),
                          "cust_id", "ID")
sample <- loadDF(sqlCtx, paste(getwd(), "/DM_Sample.parquet", sep = ""), "parquet")

printSchema(txnsRaw)

# Aggregate Transaction Data ----
perCustomer <- agg(groupBy(txnsRaw,"cust_id"),
                   txnsRaw$cust_id,
                   txns = countDistinct(txnsRaw$day_num),
                   spend = sum(txnsRaw$extended_price))

head(perCustomer)

# Bring In Demographic Data ----
joinToDemo <- select(join(perCustomer, demo, perCustomer$cust_id == demo$ID),
                     demo$"*",
                     perCustomer$txns, 
                     perCustomer$spend)

explain(joinToDemo)

# Split Into Train/Test and convert to R data.frames ----
trainDF <- select(join(joinToDemo, sample, joinToDemo$ID == sample$cust_id),
                  joinToDemo$"*",
                  alias(cast(sample$respondYes, "double"), "respondYes"))

estDF <- select(filter(join(joinToDemo, sample, joinToDemo$ID == sample$cust_id, "left_outer"),
                       "cust_id IS NULL"),
                joinToDemo$"*")

printSchema(estDF)

persist(estDF, "MEMORY_ONLY")

train <- collect(trainDF) ; train$ID <- NULL

est <- collect(estDF)

class(est)

# Estimate logit model, create custom scoring function, score customers ----
theModel <- glm(respondYes ~ ., "binomial", train)

summary(theModel)

predictWithID <- function(modObj, data, idField) {
  scoringData <- data[, !names(data) %in% as.character(idField)]
  scores <- predict(modObj, scoringData, type = "response", se.fit = TRUE)
  idScores <- data.frame(ID = data[as.character(idField)], Score = scores$fit)
  idScores[order( -idScores$Score), ]
}

testScores <- predictWithID(theModel, est, "ID")

head(testScores, 25)
