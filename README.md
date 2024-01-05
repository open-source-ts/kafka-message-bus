# Kafka Message Bus 🚌✨

[![Test Coveralls](https://github.com/tomer555/kafka-message-bus/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/tomer555/kafka-message-bus/actions/workflows/test.yml)
[![Coverage Status](https://coveralls.io/repos/github/open-source-ts/kafka-message-bus/badge.svg?branch=master)](https://coveralls.io/github/open-source-ts/kafka-message-bus?branch=master)

## Introduction

Kafka Message Bus is a wrapper for KafkaJS, enhancing its functionality with deadletter topic support and handlers. It efficiently manages failed message processing in Kafka topics by redirecting them to deadletter topics for resolution, improving reliability in Kafka-based systems.

## Getting Started

1. **Clone the repository**:
   ```
   git clone https://github.com/tomer555/kafka-message-bus.git
   ```
2. **Install dependencies**:
   ```
   npm install
   ```

## Running Tests 🧪

Execute tests using:

```
npm run test
```

## Kafka Provider Usage 📡

Configure, initialize, and use the Kafka provider in your application.

## In-Memory Provider Usage 🧠

Set up and use the In-Memory provider for testing or lightweight handling.

## Usage Examples

### Setting Up the Consumer

This example demonstrates how to define a consumer with deadletter handling using only the `kafka-message-bus` package. The consumer listens to a specified topic, processes each message, and handles errors by utilizing a deadletter queue.

```typescript
import { KafkaConsumerMessage } from "kafka-message-bus";

// Function to simulate processing of a generic event
async function processEvent(event: any) {
  // Replace with your actual event processing logic
  console.log(`Processing event: ${JSON.stringify(event)}`);
}

// Define the Kafka consumer
const exampleConsumer = {
  topic: "your-topic-name",
  deadletter: true,
  handler: async (kafkaMsg: KafkaConsumerMessage) => {
    console.log(
      `Received message in ${this.topic}: ${JSON.stringify(kafkaMsg)}`
    );
    try {
      // Assuming 'kafkaMsg.message' contains the event object
      await processEvent(kafkaMsg.message);
    } catch (error) {
      console.error(
        `Failed to process message in ${this.topic}: ${JSON.stringify(
          kafkaMsg
        )}`,
        error
      );
      throw error; // This will route the message to the deadletter topic
    }
  },
};

// Initialize and start the consumer (configure according to your Kafka setup)
// ...
```

## Star History

<a href="https://star-history.com/#tomer555/kafka-message-bus&Date">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=tomer555/kafka-message-bus&type=Date&theme=dark" />
    <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=tomer555/kafka-message-bus&type=Date" />
    <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=tomer555/kafka-message-bus&type=Date" />
  </picture>
</a>

## Contributing

We welcome contributions! Here's how you can contribute:

1. **Fork the Repository**: Create your own fork of the project.
2. **Create a Feature Branch**: Work on new features or bug fixes in your own branch.
3. **Commit Your Changes**: Make sure your changes are well-documented and tested.
4. **Submit a Pull Request**: Submit your changes for review.

For a detailed guide on contributing to projects on GitHub, please refer to the [GitHub contribution guide](https://docs.github.com/en/get-started/quickstart/contributing-to-projects).

## Support

<a href="https://www.buymeacoffee.com/app/tomer196112" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" style="height: 41px !important;width: 174px !important;" ></a>

## License

MIT License. See [LICENSE](LICENSE).

---

For more details and advanced usage, visit the [GitHub repository](https://github.com/tomer555/kafka-message-bus).
