package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

type Comment struct {
	Text string `form:"text" json:"text"`
}

func main() {
	app := gin.Default()
	api := app.Group("/api/v1")
	api.POST("/comments",createComments)
	log.Fatal(app.Run(":3000"))
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl,config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
func PushCommentToQueue(topic string, message []byte) error {
	brokersUrl := []string{"localhost:29092"}
	producer, err := ConnectProducer(brokersUrl)
	if err !=nil {
		return err
	}
	defer producer.Close()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n",topic,partition,offset)
	return err
}

func createComments(c *gin.Context) {
	cmt := new(Comment)
	if err := c.BindJSON(cmt); err != nil {
		log.Println(err)
		c.JSON(http.StatusBadRequest,gin.H{"success": false, "message":err})
		return
	}
	cmtInBytes,_ := json.Marshal(cmt)
	if err := PushCommentToQueue("comments",cmtInBytes); err != nil {
		c.JSON(http.StatusInternalServerError,gin.H{
			"success": false, "message": "error creating product",
		})
	}

	c.JSON(http.StatusOK,gin.H{
		"success": true,
		"message":"Comment pushed successfully",
		"comment": cmt,
	})
}
