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

const topicData = 'kbeacon/publish/+'

client.on('connect', () => {
    console.log('========= Bridge Connected =========')
    client.subscribe(topicData)
})

client.on('message', async (topic, message) => {
    if (matches(topicData, topic)) {
        try {
            const jsonMessage = JSON.parse(message.toString())
            const gatewayId = jsonMessage.gmac

            if (!jsonMessage.hasOwnProperty('obj')) return

            const points = jsonMessage.obj.map(element => {
                return new Point('beacon')
                    .intField('rssi', element.rssi)
                    .stringField('stored_time', new Date().toISOString())
                    .tag('gateway_id', gatewayId)
                    .tag('beacon_id', element.dmac)
                    .timestamp(new Date(element.time.replace(" ", "T").concat("Z")))
            })

            const writeApi = influxApi.getWriteApi(process.env.INFLUX_ORG, process.env.INFLUX_BUCKET)
            writeApi.writePoints(points)
            await writeApi.close()
        } catch (error) {
            console.error(new Date().toISOString(), error)
        }
    }
})