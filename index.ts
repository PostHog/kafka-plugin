import { Plugin, PluginMeta, PluginEvent, RetryError } from '@posthog/plugin-scaffold'
import { Kafka, logLevel, Message, Producer, ProducerBatch, TopicMessages } from 'kafkajs'
import {ConnectionOptions} from 'tls'

type KafkaPlugin = Plugin<{
    global: {
        producer: Producer | null
    }
    config: {
        kafkaSslCert: string
        kafkaSslKey: string
        kafkaSslCa: string
        kafkaSslRejectUnauthorized: boolean
        clientId: string
        brokers: string
        topic: string
    },
}>

export const setupPlugin: KafkaPlugin['setupPlugin'] = async (meta) => {
    const { global, attachments, config } = meta
    const kafkaSsl : ConnectionOptions | undefined = {
            // cert: Buffer.from(serverConfig.KAFKA_CLIENT_CERT_B64, 'base64'),
            cert: config.kafkaSslCert,
            key: config.kafkaSslKey,
            ca: config.kafkaSslCa,
            rejectUnauthorized: config.kafkaSslRejectUnauthorized,
    }

    const kafka = new Kafka({
        clientId: config.clientId,
        brokers: config.brokers.split(','),
        logLevel: logLevel.DEBUG,
        ssl: kafkaSsl,
    })

    global.producer = kafka.producer()

    try {
        await global.producer.connect()
    } catch (error) {
        global.producer = null
        console.log('Error connecting the producer: ', error)
    }

    // TODO: topic existing vs not what happens what should happen?
}

export const teardownPlugin: KafkaPlugin['setupPlugin'] = async (meta) => {
    const { global } = meta
    if (global.producer) {
        await global.producer.disconnect()
    }
}

export async function exportEvents(events: PluginEvent[], { global, config }: PluginMeta<KafkaPlugin>) {
    if (!global.producer) {
        throw new Error('Kafka producer client not initialized properly!')
    }

    
  
    try {
        const start = Date.now()
        const messages = events.map((fullEvent) => {
            const { event, properties, $set, $set_once, distinct_id, team_id, site_url, now, sent_at, uuid, ...rest } =
                fullEvent
            const ip = properties?.['$ip'] || fullEvent.ip
            const timestamp = fullEvent.timestamp || properties?.timestamp || now || sent_at
            let ingestedProperties = properties
            let elements = []

            // only move prop to elements for the $autocapture action
            if (event === '$autocapture' && properties?.['$elements']) {
                const { $elements, ...props } = properties
                ingestedProperties = props
                elements = $elements
            }

            const message = {
                event,
                distinct_id,
                team_id,
                ip,
                site_url,
                timestamp,
                uuid: uuid!,
                properties: ingestedProperties || {},
                elements: elements || [],
                people_set: $set || {},
                people_set_once: $set_once || {},
            }
            return JSON.stringify(message)
        })

        const kafkaMessages: Array<Message> = messages.map((message) => {
            return {
              value: JSON.stringify(message)
            }
        })

        const topicMessages: TopicMessages = {
            topic: config.topic,
            messages: kafkaMessages
        }
    
        const batch: ProducerBatch = {
            topicMessages: [topicMessages]
        }
    
        await global.producer.sendBatch(batch)


        const end = Date.now() - start
        console.log(
            `Published ${events.length} ${events.length > 1 ? 'events' : 'event'} to ${config.topic}. Took ${
                end / 1000
            } seconds.`
        )
    } catch (error) {
        console.error(
            `Error publishing ${events.length} ${events.length > 1 ? 'events' : 'event'} to ${config.topic}: `,
            error
        )
        throw new RetryError(`Error publishing to Kafka! ${JSON.stringify(error.errors)}`)
    }
}