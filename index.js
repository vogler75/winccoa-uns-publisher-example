'use strict';

const { WinccoaManager } = require('winccoa-manager');
const winccoa = new WinccoaManager();

const mqtt = require('mqtt');

const brokerUrl = 'mqtt://test.mosquitto.org'; // Example broker, change to your broker URL

const client = mqtt.connect(brokerUrl);

client.on('connect', function() {
    console.log('Connected to MQTT broker');
    start();
});

function queryConnectCB(data, type, error, uns, convert) {
    if (error) {
        console.log("queryConnectCB: " + error);
    } else {
        try {
            for (let i = 1; i < data.length; i++) {
                const [dp, value, stime, status] = data[i];
                const tag = dp.replace(/^[^:]*:/, '').replace(/\.$/, '');
                if (tag.startsWith('_')) continue;
                const topic = uns+"/"+convert(tag.replace(/[\.]/g, '/'));
                const publish = {
                    "Value": value,
                    "Time": stime.toISOString(),
                    "Status": status.toString(2).padStart(16, '0')
                }
                client.publish(topic, JSON.stringify(publish));
            }
        } catch (error) {
            console.error('An error occurred:', error.message);
        }              
    }
}

function start() {
    {
        const uns = "ETM/Mattersburg/Vogler/Pumps"; // Change this to your UNS Base Line
        const query = `
        SELECT '_online.._value', '_original.._stime', '_original.._status'
        FROM '*.**' WHERE _DPT = "PUMP1"`; // Change this to your needs
        const cb = (data, type, error) => queryConnectCB(data, type, error, uns, (tag)=>tag)
        winccoa.dpQueryConnectSingle(cb, true, query);
    }
    {
        const uns = "ETM/Mattersburg/Vogler/Home"; // Change this to your UNS Base Line
        const query = `
        SELECT '_online.._value', '_original.._stime', '_original.._status'
        FROM '*.**' WHERE _DPT = "Home-Automation-Float"`; // Change this to your needs        
        const cb = (data, type, error) => queryConnectCB(
            data, type, error, 
            uns, (tag)=>tag.replace(/_/g, '/')); // In that case we additionally replace all "_" with "/"
        winccoa.dpQueryConnectSingle(cb, true, query);
    }    
}
