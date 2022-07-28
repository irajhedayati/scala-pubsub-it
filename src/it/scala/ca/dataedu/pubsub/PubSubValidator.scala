package ca.dataedu.pubsub

import com.dimafeng.testcontainers.PubSubEmulatorContainer
import com.google.api.core.ApiService
import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.pubsub.v1._
import org.scalatest.Assertions.fail
import org.scalatest.matchers.should.Matchers._

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._

class PubSubValidator(topicName: String, expectedRecords: List[String]) {

  private lazy val subscriptionName: String = s"$topicName-sub"
  private val actualPulledMessages = new ConcurrentLinkedQueue[String]()
  private val numberOfRecordsToPull = new AtomicInteger(expectedRecords.size)
  private var subscriber: ApiService = _

  def start(projectId: String, container: PubSubEmulatorContainer): Unit = {
    container.subscriptionAdminClient.createSubscription(
      Subscription
        .newBuilder()
        .setTopic(TopicName.format(projectId, topicName))
        .setName(SubscriptionName.format(projectId, subscriptionName))
        .build()
    )
    subscriber = container
      .subscriber(
        ProjectSubscriptionName.of(projectId, subscriptionName),
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
    while (numberOfRecordsToPull.get() > 0) {
      Thread.sleep(500)
    }
    val actual = actualPulledMessages.asScala.toSeq
    subscriber.stopAsync()
    subscriber.awaitTerminated()

    actual.size shouldBe expectedRecords.size
    actual should contain theSameElementsAs expectedRecords
  }

}
