const { Kafka } = require('kafkajs');
const express = require('express');
const dotenv = require('dotenv');

dotenv.config();

const kafkaHost = process.env.KAFKA_HOST || 'localhost:9092';
const topic = process.env.KAFKA_TOPIC || 'my-topic';

const kafka = new Kafka({
    clientId: 'node-kafka-producer',
    brokers: [kafkaHost],
  });
  
const producer = kafka.producer();

async function sendMessage(message) {
    await producer.connect();
    await producer.send({
      topic: topic,
      messages: [{ value: JSON.stringify(message) }],
    });
  
    await producer.disconnect();
}

const app = express();
app.use(express.json());

app.post('/send', async (req, res) => {
    try {
      const message = req.body;
      await sendMessage(message);
      res.status(200).json({ status: 'success', message: 'Message sent successfully!' });
    } catch (error) {
      res.status(500).json({ status: 'error', message: error.message });
    }
});

const port = process.env.PORT || 3000;
  app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});