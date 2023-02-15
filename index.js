import * as MQTT from 'mqtt'
import { InfluxDB, Point } from '@influxdata/influxdb-client'
import { matches } from 'mqtt-pattern'
import * as dotenv from 'dotenv'
dotenv.config()

const influxApi = new InfluxDB({
    url: process.env.INFLUX_URL,
    token: process.env.INFLUX_TOKEN
})

const client = MQTT.connect({
    host: process.env.MQTT_HOST,
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    port: process.env.MQTT_PORT
})

const topicData = 'UBS/+/+/+/+'

client.on('connect', () => {
    console.log('========= Bridge Connected =========')
    client.subscribe(topicData)
})

client.on('message', async (topic, message) => {
    if (matches(topicData, topic)) {
        try {
            const gatewayId = topic.split('/')[1]
            const deviceId = topic.split('/')[2]
            const type = topic.split('/')[4]
            const measurement = topic.split('/')[3] + type
            
            const messageId = parseInt(message.toString().split(';')[0])
            const milisecond = parseInt(message.toString().split(';')[1]) * 1000 
            const value = parseFloat(message.toString().split(';')[2])

            const point = new Point(measurement)
                .floatField('value', value)
                .stringField('message_id', messageId)
                .tag('gateway_id', gatewayId)
                .tag('device_id', deviceId)
                .stringField(new Date(milisecond))

            const writeApi = influxApi.getWriteApi(process.env.INFLUX_ORG, process.env.INFLUX_BUCKET)
            writeApi.writePoint(point)
            await writeApi.close()
        } catch (error) {
            console.error(error)
        }
    }
})