package mqtt

import (
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

type Client struct {
	conn MQTT.Client
}

type MQTTSubscriptionMessage struct {
	Client  MQTT.Client
	Message MQTT.Message
}

func (cl *Client) Connect(broker string, client_id string, user string, password string, set_clean_session bool) error {
	opts := MQTT.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(client_id)
	opts.SetUsername(user)
	opts.SetPassword(password)
	opts.SetCleanSession(set_clean_session)
	cl.conn = MQTT.NewClient(opts)
	if tk := cl.conn.Connect(); tk.Wait() && tk.Error() != nil {
		return tk.Error()
	}
	return nil
}

func (cl *Client) Disconnect() {
	cl.conn.Disconnect(250)
}

func (cl *Client) Publish(topic string, payload interface{}) MQTT.Token {
	tk := cl.conn.Publish(topic, byte(0), false, payload)
	return tk
}

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
