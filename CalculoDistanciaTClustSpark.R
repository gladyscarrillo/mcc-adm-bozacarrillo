spark_path =  "/usr/local/src/spark-2.0.2-bin-hadoop2.7"

Sys.setenv('SPARKR_SUBMIT_ARGS'=' "sparkr-submit"')
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = spark_path)
}
library(SparkR, lib.loc = "/usr/local/src/spark-2.0.2-bin-hadoop2.7/R/lib/")

sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))




fileName = '/home/gcarrillo/Documentos/MCC/cursos/2016-II/ADM/proyecto/lista_archivos2.txt'
schema <- structType(structField("objeto", "string"))
                     
objetos <- loadDF(fileName, "csv",schema)
head(objetos)
#cargar los tiempos de cada objeto


testFileInterarrivals <- read.csv("/home/gcarrillo/Documentos/MCC/cursos/2016-II/ADM/proyecto/ClusterTimeSeries-1000/trace-Opens.txt-interarrivals-1000/it-10072@g.txt", header = FALSE, sep = " ")

schObjeto <- structType(structField("objeto", "string"), structField("serie", "string"))
schObjeto <- structType(structField("datos","string"))
ldf <- dapply(
  objetos,
  function(x) {
    
    tiempos = scan("/home/gcarrillo/Documentos/MCC/cursos/2016-II/ADM/proyecto/ClusterTimeSeries-1000/trace-Opens.txt-interarrivals-1000/it-10072@g.txt", character(), quote = "")
    striempos = paste(tiempos, collapse = ',')

    x <- cbind(x$objeto, striempos)
  },schObjeto)


head(ldf)
cartesianDF <- join(ldf, ldf)
names(cartesianDF) <- cbind('obj1','ser1','obj2','ser2')
head(cartesianDF)

library("TSclust")

calculaDistancia <- function(x) {
  serie1 <- data.frame(strsplit( x$ser1, ',' ))
  serie2 <- data.frame(strsplit( x$ser2, ',' ))

  colnames(serie1) <- "V1"
  colnames(serie2) <- "V1"
  serie1$V1 <- as.numeric(as.character(serie1$V1))
  serie2$V1 <- as.numeric(as.character(serie2$V1))
  distancia<-TSclust::diss.FRECHET(serie1,serie2)
  
  x <- cbind(x$obj1, x$obj2,distancia)
}


ldf2 <- dapplyCollect(cartesianDF,calculaDistancia)

ldf2 <- dapplyCollect(
        cartesianDF,
        function(x) {
          serie1 <- data.frame(strsplit( x$ser1, ',' ))
          serie2 <- data.frame(strsplit( x$ser2, ',' ))
         
          colnames(serie1) <- "V1"
          colnames(serie2) <- "V1"
          serie1$V1 <- as.numeric(as.character(serie1$V1))
          serie2$V1 <- as.numeric(as.character(serie2$V1))
          distancia<-diss.FRECHET(serie1,serie2)
          
          x <- cbind(x$obj1, x$obj2, distancia)
        })


head(ldf2)