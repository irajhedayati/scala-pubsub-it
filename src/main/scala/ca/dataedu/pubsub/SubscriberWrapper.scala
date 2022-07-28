package ca.dataedu.pubsub

import com.google.api.core.ApiService
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.{MessageReceiver, Subscriber}
import com.google.pubsub.v1.ProjectSubscriptionName
import io.grpc.ManagedChannelBuilder
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder

class SubscriberWrapper(subscriber: Subscriber) extends AutoCloseable {

  override def close(): Unit = subscriber.stopAsync()

  def startAsync: ApiService = subscriber.startAsync()

  def awaitTerminated(): Unit = subscriber.awaitTerminated()

}

object SubscriberWrapper {

  def apply(
      projectId: String,
      subscription: String,
      localPubSubEndpoint: Option[String],
      receiver: MessageReceiver
  ): SubscriberWrapper = {
    val baseBuilder = Subscriber.newBuilder(ProjectSubscriptionName.format(projectId, subscription), receiver)
    val subscriber =
      localPubSubEndpoint
        .map(h => handleLocalExecution(baseBuilder, h))
        .map(_.build())
        .getOrElse(baseBuilder.build())
    new SubscriberWrapper(subscriber)
  }

  private def handleLocalExecution(baseBuilder: Subscriber.Builder, localPubSubEndpoint: String): Subscriber.Builder = {
    val channel = ManagedChannelBuilder
      .forTarget(localPubSubEndpoint)
      .usePlaintext()
      .asInstanceOf[ManagedChannelBuilder[NettyChannelBuilder]]
      .build()
    baseBuilder
      .setChannelProvider(FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)))
      .setCredentialsProvider(NoCredentialsProvider.create)
  }

}
