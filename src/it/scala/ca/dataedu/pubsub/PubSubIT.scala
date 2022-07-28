package ca.dataedu.pubsub

import com.dimafeng.testcontainers.{ForAllTestContainer, PubSubEmulatorContainer}
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{PubsubMessage, Subscription, SubscriptionName, TopicName}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit

class PubSubIT extends AnyFunSuiteLike with Matchers with BeforeAndAfterAll with ForAllTestContainer {

  private val PubsubLocalProjectId = "local-project"
  private val InputTopicName = "input-topic"
  private val InputSubscriptionName = "input-topic-sub"
  private val OutputTopicName = "output-topic"

  override val container: PubSubEmulatorContainer = PubSubEmulatorContainer()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    container.start()
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  test("Test Pub/Sub application") {
    container.topicAdminClient.createTopic(TopicName.of(PubsubLocalProjectId, InputTopicName))
    container.topicAdminClient.createTopic(TopicName.of(PubsubLocalProjectId, OutputTopicName))
    container.subscriptionAdminClient.createSubscription(
      Subscription
        .newBuilder()
        .setTopic(TopicName.format(PubsubLocalProjectId, InputTopicName))
        .setName(SubscriptionName.format(PubsubLocalProjectId, InputSubscriptionName))
        .build()
    )

    val inputMessage =
      """{
        | "key1": "value1"
        |""".stripMargin
    val validator = new PubSubValidator(OutputTopicName, List(inputMessage))
    validator.start(PubsubLocalProjectId, container)

    val app = Main.getSubscriber(
      Array(PubsubLocalProjectId, InputSubscriptionName, OutputTopicName, container.emulatorEndpoint)
    )
    app.startAsync.awaitRunning()

    val publisher = container.publisher(TopicName.of(PubsubLocalProjectId, InputTopicName))
    val data = ByteString.copyFromUtf8(inputMessage)
    val pubsubMessage = PubsubMessage.newBuilder.setData(data).build
    val future = publisher.publish(pubsubMessage)
    future.get(1, TimeUnit.SECONDS)

    validator.assert()
    app.close()
    app.awaitTerminated()
  }

}
