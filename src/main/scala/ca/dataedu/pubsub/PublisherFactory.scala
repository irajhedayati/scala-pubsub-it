package ca.dataedu.pubsub

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.TopicName
import io.grpc.ManagedChannelBuilder
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder

object PublisherFactory {

  def apply(projectId: String, topicName: String, localPubSubEndpoint: Option[String]): Publisher = {
    val baseBuilder = Publisher.newBuilder(TopicName.of(projectId, topicName))
    localPubSubEndpoint
      .map(h => handleLocalExecution(baseBuilder, h))
      .map(_.build())
      .getOrElse(baseBuilder.build())
  }

  private def handleLocalExecution(baseBuilder: Publisher.Builder, hostPort: String): Publisher.Builder = {
    val channel = ManagedChannelBuilder
      .forTarget(hostPort)
      .usePlaintext
      .asInstanceOf[ManagedChannelBuilder[NettyChannelBuilder]]
      .build
    baseBuilder
      .setChannelProvider(FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)))
      .setCredentialsProvider(NoCredentialsProvider.create)
  }

}
