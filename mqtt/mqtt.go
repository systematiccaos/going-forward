// This package simply wraps paho-mqtt and has some convenience functions, e.g. loading configs from env-variables.
package mqtt

import (
	MQTT "github.com/eclipse/paho.mqtt.golang"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// wraps the mqtt-connection
type Client struct {
	conn MQTT.Client
}

// has client and message to be able to push both to a go-channel
type MQTTSubscriptionMessage struct {
	Client  MQTT.Client
	Message MQTT.Message
}

// connects to the broker and returns errors if there are any
func (cl *Client) Connect(broker string, client_id string, user string, password string, set_clean_session bool, connection_lost_handler mqtt.ConnectionLostHandler) error {
	opts := MQTT.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(client_id)
	opts.SetUsername(user)
	opts.SetPassword(password)
	opts.SetCleanSession(set_clean_session)
	if connection_lost_handler == nil {
		connection_lost_handler = mqtt.DefaultConnectionLostHandler
	}
	opts.OnConnectionLost = connection_lost_handler
	cl.conn = MQTT.NewClient(opts)
	if tk := cl.conn.Connect(); tk.Wait() && tk.Error() != nil {
		return tk.Error()
	}
	return nil
}

// disconnects the broker
func (cl *Client) Disconnect() {
	cl.conn.Disconnect(250)
}

// publishes a message to the broker on the given topic
func (cl *Client) Publish(topic string, payload interface{}) MQTT.Token {
	tk := cl.conn.Publish(topic, byte(0), false, payload)
	return tk
}

// subscribes to a MQTT-topic and writes received messages to the given listench so it can be handled elsewhere
func (cl *Client) Subscribe(topic string, listench chan MQTTSubscriptionMessage) error {
	tk := cl.conn.Subscribe(topic, byte(0), func(c MQTT.Client, m MQTT.Message) {
		listench <- MQTTSubscriptionMessage{
			c,
			m,
		}
	})
	if tk.Wait() && tk.Error() != nil {
		return tk.Error()
	}
	return nil
}
