const { Kafka, KafkaJSGroupCoordinatorNotFound } =  require('kafkajs')

class WTKAFKA extends Kafka{
    constructor({clientId, brokers, topic, defaultMessageKey}) {
        super({clientId: clientId, brokers: brokers})
        this.topic = topic
        this.defaultGroupId = defaultMessageKey
    }
    
    
    async enqueueOne(payload, { messageKey, topic, partition } = {}, producerOptions = {}) {
        try {
            if (payload === undefined) throw new Error('payload is required')
            console.log(this.kafka)
            const producer = super.producer(producerOptions)
            const jsonPayload = JSON.stringify(payload) 
            const msgKey = messageKey || this.defaultGroupId
            const msgTopic = topic || this.topic
            const msgPartition = partition || undefined
            console.log(`connecting to the kafka broker with clientId ${this.clientId}`)
            await producer.connect()
            console.log(`producer is connected..`)
            await producer.send({
                topic: msgTopic,
                messages: [{key: msgKey, value: jsonPayload, partition: msgPartition}],
            })
            console.log(`message send successfully.`)
            await producer.disconnect()
        }catch(err){
            console.log(err)
        }
    }

    async enqueueMany(payloads, { messageKey, topic, partition } = {}, producerOptions = {}) {
        try{ 
            if (!(payloads instanceof Array)) throw new Error('payloads must be of type array')
            if (payloads.length === 0) return
            const producer = super.producer(producerOptions)
            const msgTopic = topic || this.topic
            const entries = payloads.map(payload => {
                const jsonPayload = JSON.stringify(payload)
                const msgKey = messageKey || this.defaultGroupId
                const msgPartition = partition || undefined

                return {
                    key: msgKey, 
                    value: jsonPayload, 
                    partition: msgPartition
                }

            })
            console.log(`connecting to the kafka broker with clientId ${this.clientId}`)
            await producer.connect()
            console.log(`producer is connected..`)
            await producer.send({
                topic: msgTopic,
                messages: entries,
            })
            console.log(`message send successfully.`)
            await producer.disconnect()
        }catch(err){
            console.log(err)
        }

    }
    
    async enqueueMultipleTopics(topicMessagess, producerOptions ={}) {
        try{
            if (!(topicMessagess instanceof Array)) throw new Error('payloads must be of type array')
            if (topicMessagess.length === 0) return
            const jsonPayload = JSON.stringify(topicMessagess)
            const producer = super.producer(producerOptions)
            console.log(`connecting to the kafka broker with clientId ${this.clientId}`)
            await producer.connect()
            console.log(`producer is connected..`)
            await producer.sendBatch(jsonPayload)
            console.log(`message send successfully.`)
            await producer.disconnect()


        }catch(err){
            console.log(err)
        }

    }
}


function main() {
    const wtkafka = new WTKAFKA({
        clientId: "myapp",
        brokers: ["localhost:29092"],
        topic: "Users",
        defaultMessageKey: "Key1",
    })

    const myMessages= [{
        name: "Zunnorain",
        age: 23,
        city: "Lahore",
        country: "Pakistan"
    }, {
        name: "arsalan",
        age: 24,
        city: "lahore",
        country: 'Pakistan'
    }]
    const topicMessages = [
        {
          topic: 'topic-a',
          messages: [{ key: 'key', value: 'hello topic-a' }],
        },
        {
          topic: 'topic-b',
          messages: [{ key: 'key', value: 'hello topic-b' }],
        },
        {
          topic: 'topic-c',
          messages: [
            {
              key: 'key',
              value: 'hello topic-c',
              headers: {
                'correlation-id': '2bfb68bb-893a-423b-a7fa-7b568cad5b67',
              },
            }
          ],
        }
      ]

   // wtkafka.enqueueOne({name:"ahmad"}, {messageKey: 'key1', topic: 'ahmad'})
    wtkafka.enqueueMultipleTopics(topicMessages)
}


main()