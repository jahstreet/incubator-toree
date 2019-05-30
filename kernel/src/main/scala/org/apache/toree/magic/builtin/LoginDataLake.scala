package org.apache.toree.magic.builtin

import java.io.PrintStream

import org.apache.toree.magic.LineMagic
import org.apache.toree.magic.dependencies.{IncludeKernel, IncludeOutputStream}
import org.apache.toree.plugins.annotations.Event

class LoginDataLake extends LineMagic
  with IncludeOutputStream with IncludeKernel {

  @Event(name = "logindatalake")
  override def execute(code: String): Unit = {
    val tenantId = System.getenv("TOREE_AZURE_DATALAKE_TENANT_ID")
    val appId = System.getenv("TOREE_AZURE_DATALAKE_APP_ID")
    loginToAzureDataLake(tenantId, appId)
  }

  private def loginToAzureDataLake(tenantId: String, appId: String): Unit = {
    import com.microsoft.azure.datalake.store.oauth2.AzureADClientAuthenticator

    import scala.collection.JavaConverters._
    val systemOut = System.out
    try {
      System.setOut(printStream)
      val tokenInfo = AzureADClientAuthenticator.authenticate(tenantId, appId)
      println("Token will expire on: " + tokenInfo.get("accessTokenExpiry"))
      val credentials = tokenInfo.asScala
      Map(
        "fs.adl.oauth2.access.token.provider.type" -> "Custom",
        "fs.adl.oauth2.access.token.provider" -> "com.epam.azure.datalake.store.oauth2.CustomRefreshTokenProvider",
        "fs.adl.oauth2.client.id" -> credentials("clientId").toString,
        "fs.adl.impl" -> "org.apache.hadoop.fs.adl.AdlFileSystem",
        "fs.AbstractFileSystem.adl.impl" -> "org.apache.hadoop.fs.adl.Adl",
        "fs.adl.oauth2.refresh.token" -> credentials("refreshToken").toString,
        "fs.adl.oauth2.tenant.id" -> tenantId
      ).foreach {
        case (k, v) => spark.sparkContext.hadoopConfiguration.set(k, v)
      }
    } finally {
      System.setOut(systemOut)
    }
  }

  private def printStream = new PrintStream(outputStream)

  private def spark = kernel.sparkSession

}
