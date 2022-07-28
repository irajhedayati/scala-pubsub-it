package ca.dataedu.pubsub

import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver}
import com.google.pubsub.v1.PubsubMessage
import com.google.cloud.pubsub.v1.Publisher

import scala.util.Using

object Main {

  def main(args: Array[String]): Unit = {
    Using.resource(getSubscriber(args)) { subscriber =>
      subscriber.startAsync.awaitRunning()
      subscriber.awaitTerminated()
    }
  }

  def getSubscriber(args: Array[String]): SubscriberWrapper = {
    val projectId = args(0)
    val subscription = args(1)
    val topic = args(2)
    val localPubSubEndpoint = if (args.length == 4) Option(args(3)) else None
    val publisher = PublisherFactory(projectId, topic, localPubSubEndpoint)
    val processor = new Processor(publisher)
    SubscriberWrapper(projectId, subscription, localPubSubEndpoint, processor)
  }

  class Processor(publisher: Publisher) extends MessageReceiver {
    override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
      println(message.getData.toStringUtf8)
      publisher.publish(message)
      consumer.ack()
    }
  }

}
