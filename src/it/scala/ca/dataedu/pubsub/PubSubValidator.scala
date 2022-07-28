package ca.dataedu.pubsub

import com.dimafeng.testcontainers.PubSubEmulatorContainer
import com.google.api.core.ApiService
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.pubsub.v1._
import org.scalatest.Assertions.fail
import org.scalatest.matchers.should.Matchers._

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

class PubSubValidator(projectId: String, topic: String, expectedRecords: List[String]) {

  private val topicName = TopicName.format(projectId, topic)
  private val subscriptionName = SubscriptionName.format(projectId, subscription)
  private lazy val subscription: String = s"$topic-sub"
  private val actualPulledMessages = new ConcurrentLinkedQueue[String]()
  private val numberOfRecordsToPull = new AtomicInteger(expectedRecords.size)
  private var subscriber: ApiService = _

  def start(container: PubSubEmulatorContainer): Unit = {
    container.subscriptionAdminClient.createSubscription(
      Subscription.newBuilder().setTopic(topicName).setName(subscriptionName).build()
    )
    subscriber = container
      .subscriber(
        ProjectSubscriptionName.of(projectId, subscription),
        (message: PubsubMessage, consumer: AckReplyConsumer) => {
          actualPulledMessages.add(message.getData.toStringUtf8)
          if (numberOfRecordsToPull.decrementAndGet() < 0)
            fail("The number of messages sent to Item Service is more than the number of messages expected")
          consumer.ack()
        }
      )
      .startAsync()
    subscriber.awaitRunning()
  }

  def assert(): Unit = {
    while (numberOfRecordsToPull.get() > 0) Thread.sleep(500)
    val actual = actualPulledMessages.asScala.toSeq
    subscriber.stopAsync()
    subscriber.awaitTerminated()
    actual.size shouldBe expectedRecords.size
    actual should contain theSameElementsAs expectedRecords
  }

}
