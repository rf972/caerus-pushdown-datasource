// scalastyle:off
/*
 * Copyright 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// scalastyle:on
package com.github.datasource.s3

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.AmazonS3URI
import org.slf4j.LoggerFactory

/** Is a singleton object for the AmazonS3 object we create
 *  with the credentials and endpoint.
 *  Since these do not change, we are using a singleton to
 *  avoid the heavy performance cost we see as this is created.
 *
 *  @param options the set of options passed to the datasource
 */
class S3Client(options: java.util.Map[String, String]) {
  def staticCredentialsProvider(credentials: AWSCredentials): AWSCredentialsProvider = {
    new AWSCredentialsProvider {
      override def getCredentials: AWSCredentials = credentials
      override def refresh(): Unit = {}
    }
  }
  protected val s3Credential = new BasicAWSCredentials(options.get("accessKey"),
                                                       options.get("secretKey"))
  protected val s3Client: com.amazonaws.services.s3.AmazonS3 = AmazonS3ClientBuilder.standard()
    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                               options.get("endpoint"), Regions.US_EAST_1.name()))
    .withPathStyleAccessEnabled(true)
    .withCredentials(staticCredentialsProvider(s3Credential))
    .withClientConfiguration(new ClientConfiguration().withRequestTimeout(24 * 3600 * 1000)
                                                      .withSocketTimeout(24 * 3600* 1000)
                                                      .withTcpKeepAlive(true)
                                                      .withClientExecutionTimeout(24 * 3600 * 1000))
    .build()
  def getEndpoint(): String = options.get("endpoint")
  /** returns the current s3 singelton object
   *
   * @return AmazonS3 the s3 singleton object we saved.
   */
  def getClient(): com.amazonaws.services.s3.AmazonS3 = s3Client
}
/** Companion object for the S3Client class.
 *
 */
object S3Client {
  private val logger = LoggerFactory.getLogger(getClass)
  private var s3Client: Option[S3Client] = None

  /** Returns the single instance of the S3Client.
   *  If that object has not been created yet, one is created.
   *  @param options the current datasource options
   *  @return S3Client the single instance of the S3Client.
   *          We expect the user to simply call .getClient on this returned object.
   */
  def apply(options: java.util.Map[String, String]): S3Client = {
    // Create new client if there is none, or if the
    // endpoint differs from the old one.
    // Makes strong assumption that only one endpoint is used
    // at any one time, which is a limitation.
    if (s3Client == None || options.get("endpoint") != s3Client.get.getEndpoint) {
        logger.info(s"Created new S3 client " + options.get("endpoint"))
        s3Client = Some(new S3Client(options))
        s3Client.get
    } else {
      s3Client.get
    }
  }
}
