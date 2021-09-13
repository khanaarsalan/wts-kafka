const { Kafka } =  require('kafkajs')
const events = require('events');
const eventEmitter = new events.EventEmitter();

// default_consumer_group_id is used if no consumer_group_id is given for initializing a consumer
const DEFAULT_CONSUMER_GROUP_ID = 'test-group';

// default partition to use for your messages
const DEAULT_PARTITION = 'test'

class WTKAFKA{
    constructor({clientId, brokers, topic, partition, consumerGroupId, isFifo}) {
        this.kafka = new Kafka({clientId: clientId, brokers: brokers})
        this.topic = topic
        this.partition = partition
        this.consumerGroupId = consumerGroupId
        this.isFifo = isFifo
    }
    
    
    async enqueueOne(payload, messageKey = 'key', producerOptions = {}) {
        try {
            if (payload === undefined) throw new Error('payload is required')
            console.log(this.kafka)
            const producer = this.kafka.producer(producerOptions)
            const jsonPayload = JSON.stringify(payload) 
            const msgPartition = this.isFifo ? this.partition || DEAULT_PARTITION : undefined
            console.log(`connecting to the kafka broker`)
            await producer.connect()
            console.log(`producer is connected..`)
            await producer.send({
                topic: this.topic,
                messages: [{key: messageKey, value: jsonPayload, partition: msgPartition}],
            })
            console.log(`message send successfully.`)
            await producer.disconnect()
        }catch(err){
            console.log(err)
        }
    }

    async enqueueMany(payloads, messageKey = 'key', producerOptions = {}) {
        try{ 
            if (!(payloads instanceof Array)) throw new Error('payloads must be of type array')
            if (payloads.length === 0) return
            const producer = this.kafka.producer(producerOptions)
            const entries = payloads.map(payload => {
                const jsonPayload = JSON.stringify(payload)
                const msgPartition = this.isFifo ? this.defaultPartition || DEAULT_PARTITION : undefined

                return {
                    key: messageKey, 
                    value: jsonPayload, 
                    partition: msgPartition
                }

            })
            console.log(`connecting to the kafka broker with clientId ${this.clientId}`)
            await producer.connect()
            console.log(`producer is connected..`)
            await producer.send({
                topic: this.topic,
                messages: entries,
            })
            console.log(`message send successfully.`)
            await producer.disconnect()
        }catch(err){
            console.log(err)
        }

    }

    async popOne(consumerOptions = {}) {
       const result = await this.popMany(1, consumerOptions)
       return result
    }

    async popMany(noOfMessages, consumerOptions = {}) {
        if (noOfMessages === undefined) throw new Error('no of messages is required')
        const consumerGroupId = this.consumerGroupId || DEFAULT_CONSUMER_GROUP_ID
        const consumer = this.kafka.consumer({groupId: consumerGroupId, ...consumerOptions})
        console.log('connecting....')
        await consumer.connect()

        let consumedTopicPartitions = {}
        consumer.on(consumer.events.GROUP_JOIN, async ({payload}) => {
            const {memberAssignment} = payload
            consumedTopicPartitions = Object.entries(memberAssignment).reduce(
                (topics, [topic, partitions]) => {
                    for (const partition in partitions) {
                        topics[`${topic}-${partition}`] = false
                    }
                    console.log('topics', topics)
                    return topics
                },
                {}
            )
        })

        /*
        * This is extremely unergonomic, but if we are currently caught up to the head
        * of all topic-partitions, we won't actually get any batches, which means we'll
        * never find out that we are actually caught up. So as a workaround, what we can do
        * is to check in `FETCH_START` if we have previously made a fetch without
        * processing any batches in between. If so, it means that we received empty
        * fetch responses, meaning there was no more data to fetch.
        *
        * We need to initially set this to true, or we would immediately exit.
        */
        let processedBatch = true
        consumer.on(consumer.events.FETCH_START, async () => {
            if (processedBatch === false) {
                await consumer.disconnect()
            }

            processedBatch = false
        })

        /*
        * Now whenever we have finished processing a batch, we'll update `consumedTopicPartitions`
        * and exit if all topic-partitions have been consumed,
        */
        consumer.on(consumer.events.END_BATCH_PROCESS, async ({payload}) => {
            console.log('END_BATCH_PROCESS')
            const {topic, partition, offsetLag} = payload
            consumedTopicPartitions[`${topic}-${partition}`] = offsetLag === '0'

            if (Object.values(consumedTopicPartitions).every(consumed => Boolean(consumed))) {
                await consumer.disconnect()
            }

            processedBatch = true
        })

        consumer.subscribe({
            topic: this.topic || topic,
            fromBeginning: true
        })
        const messages = await consumeMessages(consumer, noOfMessages)
        console.log('messages', messages.length)
        return messages
    }

    async seekOne(consumerOptions = {}) {
        const results = await this.seekMany(1, consumerOptions)
        return results
    }

    async seekMany(messagesToRead, {topic, groupId} = {}, consumerOptions = {}){
        if (messagesToRead === undefined ) throw new Error('no of messages is required')
        const consumerGroupId = this.consumerGroupId || DEFAULT_CONSUMER_GROUP_ID
        const consumer = this.kafka.consumer({groupId: consumerGroupId, ...consumerOptions})
        console.log('connecting....')
        await consumer.connect()

        let consumedTopicPartitions = {}
        consumer.on(consumer.events.GROUP_JOIN, async ({payload}) => {
            const {memberAssignment} = payload
            consumedTopicPartitions = Object.entries(memberAssignment).reduce(
                (topics, [topic, partitions]) => {
                    for (const partition in partitions) {
                        topics[`${topic}-${partition}`] = false
                    }
                    console.log('topics', topics)
                    return topics
                },
                {}
            )
        })

        let processedBatch = true
        consumer.on(consumer.events.FETCH_START, async () => {
            if (processedBatch === false) {
                await consumer.disconnect()
            }

            processedBatch = false
        })
        /*
            * Now whenever we have finished processing a batch, we'll update `consumedTopicPartitions`
            * and exit if all topic-partitions have been consumed,
            */
        consumer.on(consumer.events.END_BATCH_PROCESS, async ({payload}) => {
            console.log('END_BATCH_PROCESS')
            const {topic, partition, offsetLag} = payload
            consumedTopicPartitions[`${topic}-${partition}`] = offsetLag === '0'

            if (Object.values(consumedTopicPartitions).every(consumed => Boolean(consumed))) {
                await consumer.disconnect()
            }

            processedBatch = true
        })
        await consumer.subscribe({topic: this.topic || topic, fromBeginning: true})
        const messages = await seekMessages(consumer, messagesToRead)
        return messages
    }

}



async function consumeMessages(consumer, messagesToRead) {
    let counter = 0
    const messages = []
    return new Promise((resolve, reject) => {
        consumer.run({
            autoCommitThreshold: 1,
            eachBatchAutoResolve: false,
            maxInFlightRequests: 1,
            eachBatch: async ({batch, resolveOffset, isRunning, isStale}) => {
                for (let message of batch.messages) {
                    if (!isRunning() || isStale()) break
                    try {
                        const prefix = `${message.offset} / ${message.timestamp}`
                        console.log(`- ${prefix} ${message.key}#${message.value}`)
                        messages.push(message)
                        await resolveOffset(message.offset)
                        if (++counter === messagesToRead) {
                            eventEmitter.emit(consumer.events.END_BATCH_PROCESS)
                            resolve(messages)
                            return null
                        }

                    } catch (err) {
                        reject(err)
                        return null
                    }
                }
                return null
            }
        })
    })  
}

async function seekMessages(consumer, messagesToRead) {
    let counter = 0
    const messages = []
    return new Promise((resolve, reject) => {
        consumer.run({
            autoCommit: false,
            eachBatch: async ({batch, isRunning, isStale}) => {
                for (let message of batch.messages) {
                    if (!isRunning() || isStale()) break
                    try {
                        const prefix = `${message.offset} / ${message.timestamp}`
                        console.log(`- ${prefix} ${message.key}#${message.value}`)
                        messages.push(message)
                        if (++counter === messagesToRead) {
                            eventEmitter.emit(consumer.events.END_BATCH_PROCESS)
                            resolve(messages)
                            return null
                        }

                    } catch (err) {
                        reject(err)
                        return null
                    }
                }
                return null
            }
        })
    })
}


function main() {
    const wtkafka = new WTKAFKA({
        clientId: "myapp",
        brokers: ["localhost:29092"],
        topic: "Users",
        messageKey: "key1",
        groupId: "test-group1"
    })

    

   // wtkafka.enqueueOne({name:"ahmad"}, {messageKey: 'key1', topic: 'ahmad'})
    //wtkafka.enqueueMultipleTopics(topicMessages)
    //wtkafka.enqueueMany(myMessages, {messageKey: "test1", topic: "Users"})
    //wtkafka.popMany(3, 'MyTopic', 'test-group1')
    //wtkafka.seekOne()
    //wtkafka.popMany(2)

    wtkafka.popOne().then(data => console.log(data))
    
}


main()