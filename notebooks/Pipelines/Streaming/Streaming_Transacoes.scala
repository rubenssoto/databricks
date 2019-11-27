// Databricks notebook source
// DBTITLE 1,Configura Conexão com EventHubs
import org.apache.spark.eventhubs._
import org.apache.spark.sql.functions._

val connectionString = ConnectionStringBuilder()
  .setNamespaceName("eventhubsbigdata")
  .setEventHubName("transacoes")
  .setSasKeyName("RootManageSharedAccessKey")
  .setSasKey("KiqiKlQRZ178K7sW8qaG4SMXY8qW8ueSUXuzRTFh8aY=")
  .build


val eventHubsConf = EventHubsConf(connectionString)
    .setStartingPosition(EventPosition.fromStartOfStream)



val eventhubs = spark.readStream
   .format("eventhubs")
   .options(eventHubsConf.toMap)
   .load()
   .select(get_json_object(($"body").cast("string"), "$.Id_transacao").alias("Id_transacao"),
                        get_json_object(($"body").cast("string"), "$.DataTransacao").cast("timestamp").alias("DataTransacao"),
                        get_json_object(($"body").cast("string"), "$.NomeComprador").alias("NomeComprador"),
                        get_json_object(($"body").cast("string"), "$.Endereco").alias("Endereco"),
                        get_json_object(($"body").cast("string"), "$.ValorTotal").cast("decimal(18,2)").alias("ValorTotal"),
                        get_json_object(($"body").cast("string"), "$.Status").cast("integer").alias("Status"),
                        get_json_object(($"body").cast("string"), "$.Id_Loja").alias("Id_Loja"))

// COMMAND ----------

// DBTITLE 1,Limpa tabelas criadas anteriormente
dbutils.fs.rm("/mnt/sample/databricks_streaming", true)
dbutils.fs.rm("/mnt/sample/databricks_streaming_checkpoint", true)



// COMMAND ----------

// DBTITLE 1,Dropa tabela de Banco de Dados
// MAGIC %sql 
// MAGIC DROP TABLE IF EXISTS Vendas

// COMMAND ----------

// DBTITLE 1,Inicia Streaming Gravando os Dados em Formato Delta
import org.apache.spark.sql.functions._

val query =
  Streaming
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("path", "/mnt/sample/databricks_streaming")
    .option("checkpointLocation", "/mnt/sample/databricks_streaming_checkpoint")
    .start()

// COMMAND ----------

// DBTITLE 1,Conta a Quantidade de Linhas Gravadas
// MAGIC %python
// MAGIC 
// MAGIC df = spark.read.format('delta').load('/mnt/sample/databricks_streaming')
// MAGIC 
// MAGIC df.count()

// COMMAND ----------

// DBTITLE 1,Criada Tabela com Base nos Arquivos Criados
// MAGIC %sql
// MAGIC CREATE TABLE Vendas 
// MAGIC (Id_transacao STRING,
// MAGIC DataTransacao timestamp,
// MAGIC NomeComprador STRING,
// MAGIC Endereco STRING,
// MAGIC ValorTotal DECIMAL(18,2),
// MAGIC Status INT,
// MAGIC Id_Loja STRING)
// MAGIC USING DELTA LOCATION '/mnt/sample/databricks_streaming'

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(1) FROM Vendas

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT date_format(DataTransacao,'yyyy-MM-dd HH:mm:00') DataTransacao, COUNT(1)
// MAGIC FROM Vendas
// MAGIC GROUP BY date_format(DataTransacao,'yyyy-MM-dd HH:mm:00')
// MAGIC ORDER BY date_format(DataTransacao,'yyyy-MM-dd HH:mm:00')

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM Vendas
// MAGIC order by datatransacao desc
// MAGIC LIMIT 10