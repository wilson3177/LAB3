package es.unizar.tmdad.lab3.flows;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.AggregatorSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.amqp.Amqp;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.support.Consumer;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.social.twitter.api.Tweet;

@Configuration
@Profile("fanout")
public class TwitterFlowTrends {

	@Autowired
	RabbitTemplate rabbitTemplate;
	
	@Autowired
	FanoutExchange exchange;
	
	final static String TWITTER_TRENDS_A_QUEUE_NAME = "twitter_trends_queue";

	@Bean
	public IntegrationFlow sendTrends() {
		return IntegrationFlows
				.from(requestChannelRabbitMQ())
				.filter("payload instanceof T(org.springframework.social.twitter.api.Tweet)")
				.aggregate(aggregationSpec(), null)
				.transform(getTopTenTrends())
				.handle("streamSendingService", "sendTrends").get();
	}
	
	private GenericTransformer<List<Tweet>, List<Map.Entry<String, Integer>>> getTopTenTrends() {
		return list -> {
			Map<String, Integer> map = new HashMap<String, Integer>();
			list.stream().forEach(tweet ->{
				tweet.getEntities().getHashTags().forEach( hashtag -> {
					Integer count = map.get(hashtag.getText());
					if(count == null){
						map.put(hashtag.getText(), 1);
					}
					else{
						map.put(hashtag.getText(), count + 1);
					}
				});
			});

			Comparator<Map.Entry<String, Integer>> comparator= (Map.Entry<String, Integer> e1,Map.Entry<String, Integer> e2) -> Integer
					.compare(e2.getValue(), e1.getValue());
			return map.entrySet().stream().sorted(comparator).limit(10).collect(Collectors.toList());
		};
	}

	@Bean
	Queue twitterTrendsQueue() {
		return new Queue(TWITTER_TRENDS_A_QUEUE_NAME, false);
	}

	@Bean
	Binding twitterTrendsBinding() {
		return BindingBuilder.bind(twitterTrendsQueue()).to(exchange);
	}
	
	@Bean
	public DirectChannel requestChannelRabbitMQ() {
		return MessageChannels.direct().get();
	}

	@Bean
	public AmqpInboundChannelAdapter amqpInbound() {
		SimpleMessageListenerContainer smlc = new SimpleMessageListenerContainer(
				rabbitTemplate.getConnectionFactory());
		smlc.setQueues(twitterTrendsQueue());
		return Amqp.inboundAdapter(smlc)
				.outputChannel(requestChannelRabbitMQ()).get();
	}

	private Consumer<AggregatorSpec> aggregationSpec(){
		return a -> a.correlationStrategy(m -> 1)
				.releaseStrategy(g -> g.size() == 1000)
				.expireGroupsUponCompletion(true);
	}

}
